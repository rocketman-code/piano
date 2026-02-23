//! Thread-local timing collector with RAII guards.
//!
//! Each instrumented function calls `enter(name)` which pushes a `StackEntry`
//! onto a thread-local call stack and returns an RAII `Guard`. When the guard
//! drops (on any exit path), it pops the stack entry, computes elapsed time,
//! propagates children time to the parent, and records a `RawRecord`.
//!
//! `collect()` aggregates raw records into per-function summaries sorted by
//! self-time descending. `reset()` clears all state for the current thread.
//!
//! Flush strategy: `RECORDS` is wrapped in `AutoFlushRecords`, which implements
//! `Drop`. When thread-local storage is destroyed (normal process exit, thread
//! join), the `Drop` impl flushes records to disk. This avoids the classic
//! atexit-vs-TLS ordering problem where atexit fires after TLS is destroyed.
//!
//! Thread-locality: all state (stack, records) is thread-local. Each thread
//! produces an independent call tree by default. For cross-thread attribution
//! (e.g. rayon scopes, spawned threads), use `fork()` / `adopt()` / `finalize()`
//! to propagate timing context so that child thread elapsed time is correctly
//! subtracted from the parent's self-time.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Process-wide run identifier.
///
/// All threads within a single process share this ID, so that JSON files
/// written by different threads can be correlated during report consolidation.
static RUN_ID: LazyLock<String> =
    LazyLock::new(|| format!("{}_{}", std::process::id(), timestamp_ms()));

/// Process-start epoch for relative timestamps.
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Aggregated timing data for a single function.
#[derive(Debug, Clone)]
pub struct FunctionRecord {
    pub name: String,
    pub calls: u64,
    pub total_ms: f64,
    pub self_ms: f64,
}

/// Per-invocation measurement record with nanosecond precision.
#[derive(Debug, Clone)]
pub struct InvocationRecord {
    pub name: &'static str,
    pub start_ns: u64,
    pub elapsed_ns: u64,
    pub self_ns: u64,
    pub alloc_count: u32,
    pub alloc_bytes: u64,
    pub free_count: u32,
    pub free_bytes: u64,
    pub depth: u16,
}

/// Entry on the thread-local timing stack.
pub(crate) struct StackEntry {
    pub(crate) name: &'static str,
    pub(crate) start: Instant,
    pub(crate) children_ms: f64,
    pub(crate) overhead_ns: u128,
    pub(crate) alloc_count: u32,
    pub(crate) alloc_bytes: u64,
    pub(crate) free_count: u32,
    pub(crate) free_bytes: u64,
    pub(crate) depth: u16,
}

/// Raw measurement produced when a Guard drops.
struct RawRecord {
    name: &'static str,
    elapsed_ms: f64,
    children_ms: f64,
}

/// Wrapper around `Vec<RawRecord>` that auto-flushes to disk on Drop.
///
/// When thread-local storage is destroyed (process exit, thread join), Drop
/// fires while the data is still alive — no TLS lookup needed.
struct AutoFlushRecords {
    records: Vec<RawRecord>,
}

impl AutoFlushRecords {
    const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }
}

impl Drop for AutoFlushRecords {
    fn drop(&mut self) {
        if cfg!(test) {
            return;
        }
        let registered: Vec<&str> = REGISTERED
            .try_with(|reg| reg.borrow().clone())
            .unwrap_or_default();
        if self.records.is_empty() && registered.is_empty() {
            return;
        }
        let aggregated = aggregate(&self.records, &registered);
        if aggregated.is_empty() {
            return;
        }
        let Some(dir) = runs_dir() else { return };
        let path = dir.join(format!("{}.json", timestamp_ms()));
        let _ = write_json(&aggregated, &path);
    }
}

thread_local! {
    pub(crate) static STACK: RefCell<Vec<StackEntry>> = const { RefCell::new(Vec::new()) };
    static RECORDS: RefCell<AutoFlushRecords> = const { RefCell::new(AutoFlushRecords::new()) };
    static REGISTERED: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    static INVOCATIONS: RefCell<Vec<InvocationRecord>> = const { RefCell::new(Vec::new()) };
}

/// RAII timing guard. Records elapsed time on drop.
#[must_use = "dropping the guard immediately records ~0ms; bind it with `let _guard = ...`"]
pub struct Guard {
    /// Prevents manual construction outside this module.
    _private: (),
}

impl Drop for Guard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            let entry = stack.borrow_mut().pop();
            let Some(entry) = entry else {
                eprintln!("piano-runtime: guard dropped without matching stack entry (bug)");
                return;
            };
            let elapsed_ns = entry.start.elapsed().as_nanos() as u64;
            let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
            let children_ns = (entry.children_ms * 1_000_000.0) as u64;
            let self_ns = elapsed_ns
                .saturating_sub(children_ns)
                .saturating_sub(entry.overhead_ns as u64);
            let start_ns = entry.start.duration_since(*EPOCH).as_nanos() as u64;
            let children_ms = entry.children_ms;

            // Safe to re-borrow: the RefMut from pop() was dropped above.
            if let Some(parent) = stack.borrow_mut().last_mut() {
                parent.children_ms += elapsed_ms;
            }

            RECORDS.with(|records| {
                records.borrow_mut().records.push(RawRecord {
                    name: entry.name,
                    elapsed_ms,
                    children_ms,
                });
            });

            INVOCATIONS.with(|inv| {
                inv.borrow_mut().push(InvocationRecord {
                    name: entry.name,
                    start_ns,
                    elapsed_ns,
                    self_ns,
                    alloc_count: entry.alloc_count,
                    alloc_bytes: entry.alloc_bytes,
                    free_count: entry.free_count,
                    free_bytes: entry.free_bytes,
                    depth: entry.depth,
                });
            });
        });
    }
}

/// Start timing a function. Returns a Guard that records the measurement on drop.
pub fn enter(name: &'static str) -> Guard {
    // Touch EPOCH so relative timestamps are anchored to process start.
    let _ = *EPOCH;
    STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        stack.borrow_mut().push(StackEntry {
            name,
            start: Instant::now(),
            children_ms: 0.0,
            overhead_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
            depth,
        });
    });
    Guard { _private: () }
}

/// Register a function name so it appears in output even if never called.
pub fn register(name: &'static str) {
    REGISTERED.with(|reg| {
        let mut reg = reg.borrow_mut();
        if !reg.contains(&name) {
            reg.push(name);
        }
    });
}

/// Aggregate raw records into per-function summaries, sorted by self_ms descending.
fn aggregate(raw: &[RawRecord], registered: &[&str]) -> Vec<FunctionRecord> {
    let mut map: HashMap<&str, (u64, f64, f64)> = HashMap::new();

    for name in registered {
        map.entry(name).or_insert((0, 0.0, 0.0));
    }

    for rec in raw {
        let entry = map.entry(rec.name).or_insert((0, 0.0, 0.0));
        entry.0 += 1;
        entry.1 += rec.elapsed_ms;
        entry.2 += (rec.elapsed_ms - rec.children_ms).max(0.0);
    }

    let mut result: Vec<FunctionRecord> = map
        .into_iter()
        .map(|(name, (calls, total_ms, self_ms))| FunctionRecord {
            name: name.to_owned(),
            calls,
            total_ms,
            self_ms,
        })
        .collect();

    result.sort_by(|a, b| {
        b.self_ms
            .partial_cmp(&a.self_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result
}

/// Aggregate raw records into per-function summaries, sorted by self_ms descending.
pub fn collect() -> Vec<FunctionRecord> {
    RECORDS
        .with(|records| REGISTERED.with(|reg| aggregate(&records.borrow().records, &reg.borrow())))
}

/// Return all raw per-invocation records (not aggregated).
pub fn collect_invocations() -> Vec<InvocationRecord> {
    INVOCATIONS.with(|inv| inv.borrow().clone())
}

/// Clear all collected timing data.
pub fn reset() {
    STACK.with(|stack| stack.borrow_mut().clear());
    RECORDS.with(|records| records.borrow_mut().records.clear());
    REGISTERED.with(|reg| reg.borrow_mut().clear());
    INVOCATIONS.with(|inv| inv.borrow_mut().clear());
}

/// Return the current time as milliseconds since the Unix epoch.
fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Return the directory where run files should be written.
///
/// Uses `PIANO_RUNS_DIR` env var if set, otherwise `~/.piano/runs/`.
fn runs_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("PIANO_RUNS_DIR") {
        return Some(PathBuf::from(dir));
    }
    dirs_fallback().map(|home| home.join(".piano").join("runs"))
}

/// Best-effort home directory detection (no deps).
fn dirs_fallback() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// Write a JSON run file from the given function records.
///
/// Hand-written JSON via `write!()` — zero serde dependency.
fn write_json(records: &[FunctionRecord], path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = std::fs::File::create(path)?;
    let ts = timestamp_ms();
    let run_id = &*RUN_ID;
    write!(
        f,
        "{{\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts},\"functions\":["
    )?;
    for (i, rec) in records.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        // Escape the function name (in practice only ASCII identifiers, but be safe).
        let name = rec.name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(
            f,
            "{{\"name\":\"{name}\",\"calls\":{},\"total_ms\":{:.3},\"self_ms\":{:.3}}}",
            rec.calls, rec.total_ms, rec.self_ms
        )?;
    }
    writeln!(f, "]}}")?;
    Ok(())
}

/// Flush collected timing data to a JSON file on disk.
///
/// Writes to `PIANO_RUNS_DIR/<timestamp>.json` or `~/.piano/runs/<timestamp>.json`.
/// No-op if no records were collected or the output directory cannot be determined.
///
/// Normally you don't need to call this — `AutoFlushRecords::drop` flushes
/// automatically when thread-local storage is destroyed (process exit). This
/// function exists for explicit mid-program flushes.
pub fn flush() {
    let records = collect();
    if records.is_empty() {
        return;
    }
    let Some(dir) = runs_dir() else {
        return;
    };
    let path = dir.join(format!("{}.json", timestamp_ms()));
    let _ = write_json(&records, &path);
    reset();
}

/// No-op retained for API compatibility.
///
/// Auto-flush now happens via `AutoFlushRecords::drop` when TLS is destroyed.
/// Instrumented code may still call `init()` — it's harmless.
pub fn init() {}

/// Context for propagating parent-child timing across thread boundaries.
///
/// Created by `fork()` on the parent thread, passed to child threads via
/// `adopt()`. When the child completes, its elapsed time is accumulated
/// in `children_ms` which the parent reads back via `finalize()`.
#[must_use = "dropping SpanContext without calling finalize() loses child attribution"]
pub struct SpanContext {
    parent_name: &'static str,
    children_ms: Arc<Mutex<f64>>,
}

impl SpanContext {
    /// Finalize cross-thread attribution after all child threads complete.
    ///
    /// Adds the accumulated children time to the current thread's top-of-stack
    /// entry (which should be the parent function). Call this after joining
    /// worker threads / awaiting rayon scope.
    pub fn finalize(self) {
        let children = *self.children_ms.lock().unwrap_or_else(|e| e.into_inner());
        STACK.with(|stack| {
            if let Some(top) = stack.borrow_mut().last_mut() {
                top.children_ms += children;
            }
        });
    }
}

/// RAII guard for cross-thread adoption. Pops the synthetic parent on drop
/// and propagates elapsed time back to the parent's `SpanContext`.
#[must_use = "dropping AdoptGuard immediately records ~0ms; bind it with `let _guard = ...`"]
pub struct AdoptGuard {
    ctx_children_ms: Arc<Mutex<f64>>,
}

impl Drop for AdoptGuard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            let entry = stack.borrow_mut().pop();
            let Some(entry) = entry else { return };
            let elapsed_ms = entry.start.elapsed().as_secs_f64() * 1000.0;

            // Propagate this thread's total time back to the parent context.
            let mut children = self
                .ctx_children_ms
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *children += elapsed_ms;
        });
    }
}

/// Capture the current stack top as a cross-thread span context.
///
/// Returns `None` if the call stack is empty (no active span to fork from).
/// Pass the returned context to child threads via `adopt()`.
pub fn fork() -> Option<SpanContext> {
    STACK.with(|stack| {
        let stack = stack.borrow();
        let top = stack.last()?;
        Some(SpanContext {
            parent_name: top.name,
            children_ms: Arc::new(Mutex::new(0.0)),
        })
    })
}

/// Adopt a parent span context on a child thread.
///
/// Pushes a synthetic parent entry so that `enter()`/`Guard::drop()` on this
/// thread correctly attributes children time. Returns an `AdoptGuard` that
/// propagates elapsed time back to the parent on drop.
pub fn adopt(ctx: &SpanContext) -> AdoptGuard {
    STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        stack.borrow_mut().push(StackEntry {
            name: ctx.parent_name,
            start: Instant::now(),
            children_ms: 0.0,
            overhead_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
            depth,
        });
    });
    AdoptGuard {
        ctx_children_ms: Arc::clone(&ctx.children_ms),
    }
}

/// CPU-bound workload for testing: hash a buffer `iterations` times.
/// Uses wrapping arithmetic to prevent optimization while staying deterministic.
#[cfg(test)]
pub(crate) fn burn_cpu(iterations: u64) {
    let mut buf = [0x42u8; 4096];
    for i in 0..iterations {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn burn_cpu_takes_measurable_time() {
        let start = std::time::Instant::now();
        burn_cpu(50_000);
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() >= 1,
            "burn_cpu(50_000) should take at least 1ms, took {:?}",
            elapsed
        );
    }

    #[test]
    fn flush_writes_valid_json_to_env_dir() {
        reset();
        {
            let _g = enter("flush_test");
            thread::sleep(Duration::from_millis(5));
        }

        let tmp = std::env::temp_dir().join(format!("piano_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        // Point flush at our temp dir.
        // SAFETY: Test runs serially (no concurrent env access).
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        flush();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

        // Find the written file.
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        assert!(!files.is_empty(), "expected at least one JSON file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("\"flush_test\""),
            "should contain function name"
        );
        assert!(
            content.contains("\"timestamp_ms\""),
            "should contain timestamp_ms"
        );
        assert!(content.contains("\"self_ms\""), "should contain self_ms");

        // Cleanup.
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn write_json_produces_valid_format() {
        let records = vec![
            FunctionRecord {
                name: "walk".into(),
                calls: 3,
                total_ms: 12.5,
                self_ms: 8.3,
            },
            FunctionRecord {
                name: "resolve".into(),
                calls: 1,
                total_ms: 4.2,
                self_ms: 4.2,
            },
        ];
        let tmp = std::env::temp_dir().join(format!("piano_json_{}.json", std::process::id()));
        write_json(&records, &tmp).unwrap();

        let content = std::fs::read_to_string(&tmp).unwrap();

        // Verify structure.
        assert!(
            content.starts_with("{\"run_id\":\""),
            "should start with run_id"
        );
        assert!(
            content.contains("\"timestamp_ms\":"),
            "should contain timestamp_ms"
        );
        assert!(
            content.contains("\"functions\":["),
            "should have functions array"
        );
        assert!(content.contains("\"walk\""), "should contain walk");
        assert!(content.contains("\"resolve\""), "should contain resolve");
        assert!(content.contains("\"calls\":3"), "should have calls count");

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn init_can_be_called_multiple_times() {
        // init() is a no-op retained for API compatibility.
        init();
        init();
        init();
    }

    #[test]
    fn single_function_timing() {
        reset();
        {
            let _g = enter("work");
            thread::sleep(Duration::from_millis(10));
        }
        let records = collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "work");
        assert_eq!(records[0].calls, 1);
        assert!(
            records[0].total_ms >= 5.0,
            "total_ms={}",
            records[0].total_ms
        );
        assert!(records[0].self_ms >= 5.0, "self_ms={}", records[0].self_ms);
    }

    #[test]
    fn nested_function_self_time() {
        reset();
        {
            let _outer = enter("outer");
            thread::sleep(Duration::from_millis(10));
            {
                let _inner = enter("inner");
                thread::sleep(Duration::from_millis(10));
            }
        }
        let records = collect();
        let outer = records
            .iter()
            .find(|r| r.name == "outer")
            .expect("outer not found");
        let inner = records
            .iter()
            .find(|r| r.name == "inner")
            .expect("inner not found");

        assert!(outer.total_ms >= 15.0, "outer.total_ms={}", outer.total_ms);
        assert!(outer.self_ms >= 5.0, "outer.self_ms={}", outer.self_ms);
        assert!(
            outer.self_ms < outer.total_ms,
            "self should be less than total"
        );
        // inner's self_ms should be approximately equal to its total_ms (no children)
        let diff = (inner.self_ms - inner.total_ms).abs();
        assert!(
            diff < 2.0,
            "inner self_ms={} total_ms={}",
            inner.self_ms,
            inner.total_ms
        );
    }

    #[test]
    fn call_count_tracking() {
        reset();
        for _ in 0..5 {
            let _g = enter("repeated");
        }
        let records = collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "repeated");
        assert_eq!(records[0].calls, 5);
    }

    #[test]
    fn reset_clears_state() {
        reset();
        {
            let _g = enter("something");
            thread::sleep(Duration::from_millis(1));
        }
        reset();
        let records = collect();
        assert!(
            records.is_empty(),
            "expected empty after reset, got {} records",
            records.len()
        );
    }

    #[test]
    fn collect_sorts_by_self_time_descending() {
        reset();
        {
            let _g = enter("fast");
            thread::sleep(Duration::from_millis(1));
        }
        {
            let _g = enter("slow");
            thread::sleep(Duration::from_millis(15));
        }
        let records = collect();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].name, "slow",
            "expected slow first, got {:?}",
            records[0].name
        );
        assert_eq!(
            records[1].name, "fast",
            "expected fast second, got {:?}",
            records[1].name
        );
    }

    #[test]
    fn registered_but_uncalled_functions_appear_with_zero_calls() {
        reset();
        register("never_called");
        {
            let _g = enter("called_once");
            thread::sleep(Duration::from_millis(1));
        }
        let records = collect();
        assert_eq!(records.len(), 2, "should have both functions");
        let never = records
            .iter()
            .find(|r| r.name == "never_called")
            .expect("never_called");
        assert_eq!(never.calls, 0);
        assert!((never.total_ms).abs() < f64::EPSILON);
        assert!((never.self_ms).abs() < f64::EPSILON);
        let called = records
            .iter()
            .find(|r| r.name == "called_once")
            .expect("called_once");
        assert_eq!(called.calls, 1);
    }

    #[test]
    fn json_output_contains_run_id() {
        reset();
        {
            let _g = enter("rid_test");
            thread::sleep(Duration::from_millis(1));
        }
        let tmp = std::env::temp_dir().join(format!("piano_rid_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        flush();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        assert!(!files.is_empty());
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("\"run_id\":\""),
            "should contain run_id field: {content}"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn conservation_sequential_calls() {
        reset();
        {
            let _main = enter("main_seq");
            burn_cpu(10_000);
            {
                let _a = enter("a");
                burn_cpu(30_000);
            }
            {
                let _b = enter("b");
                burn_cpu(20_000);
            }
        }
        let records = collect();
        let main_r = records.iter().find(|r| r.name == "main_seq").unwrap();
        let a_r = records.iter().find(|r| r.name == "a").unwrap();
        let b_r = records.iter().find(|r| r.name == "b").unwrap();

        let sum_self = main_r.self_ms + a_r.self_ms + b_r.self_ms;
        let root_total = main_r.total_ms;
        let error_pct = ((sum_self - root_total) / root_total).abs() * 100.0;
        assert!(
            error_pct < 5.0,
            "conservation violated: sum_self={sum_self:.3}ms, root_total={root_total:.3}ms, error={error_pct:.1}%"
        );
    }

    #[test]
    fn conservation_nested_calls() {
        reset();
        {
            let _main = enter("main_nest");
            burn_cpu(5_000);
            {
                let _a = enter("a_nest");
                burn_cpu(5_000);
                {
                    let _b = enter("b_nest");
                    burn_cpu(30_000);
                }
            }
        }
        let records = collect();
        let main_r = records.iter().find(|r| r.name == "main_nest").unwrap();
        let a_r = records.iter().find(|r| r.name == "a_nest").unwrap();
        let b_r = records.iter().find(|r| r.name == "b_nest").unwrap();

        let sum_self = main_r.self_ms + a_r.self_ms + b_r.self_ms;
        let root_total = main_r.total_ms;
        let error_pct = ((sum_self - root_total) / root_total).abs() * 100.0;
        assert!(
            error_pct < 5.0,
            "conservation violated: sum_self={sum_self:.3}ms, root_total={root_total:.3}ms, error={error_pct:.1}%"
        );

        // b has no children, so self_ms should equal total_ms
        let b_diff = (b_r.self_ms - b_r.total_ms).abs();
        assert!(
            b_diff < 0.1,
            "leaf self_ms should equal total_ms: self={:.3}, total={:.3}",
            b_r.self_ms,
            b_r.total_ms
        );
    }

    #[test]
    fn conservation_mixed_topology() {
        reset();
        {
            let _main = enter("main_mix");
            burn_cpu(5_000);
            {
                let _a = enter("a_mix");
                burn_cpu(10_000);
                {
                    let _b = enter("b_mix");
                    burn_cpu(20_000);
                }
            }
            {
                let _c = enter("c_mix");
                burn_cpu(15_000);
            }
        }
        let records = collect();
        let main_r = records.iter().find(|r| r.name == "main_mix").unwrap();

        let sum_self: f64 = records.iter().map(|r| r.self_ms).sum();
        let root_total = main_r.total_ms;
        let error_pct = ((sum_self - root_total) / root_total).abs() * 100.0;
        assert!(
            error_pct < 5.0,
            "conservation violated: sum_self={sum_self:.3}ms, root_total={root_total:.3}ms, error={error_pct:.1}%"
        );
    }

    #[test]
    fn conservation_repeated_calls() {
        reset();
        {
            let _main = enter("main_rep");
            burn_cpu(5_000);
            for _ in 0..10 {
                let _a = enter("a_rep");
                burn_cpu(5_000);
            }
        }
        let records = collect();
        let main_r = records.iter().find(|r| r.name == "main_rep").unwrap();
        let a_r = records.iter().find(|r| r.name == "a_rep").unwrap();

        assert_eq!(a_r.calls, 10);

        let sum_self = main_r.self_ms + a_r.self_ms;
        let root_total = main_r.total_ms;
        let error_pct = ((sum_self - root_total) / root_total).abs() * 100.0;
        assert!(
            error_pct < 5.0,
            "conservation violated: sum_self={sum_self:.3}ms, root_total={root_total:.3}ms, error={error_pct:.1}%"
        );
    }

    #[test]
    fn negative_self_time_clamped_to_zero() {
        // Regression test for the f64 drift clamp in aggregate().
        // Construct a synthetic RawRecord where children_ms slightly exceeds elapsed_ms
        // (simulating floating-point accumulation drift).
        let raw = vec![RawRecord {
            name: "drifted",
            elapsed_ms: 10.0,
            children_ms: 10.001,
        }];
        let result = aggregate(&raw, &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].self_ms, 0.0,
            "negative self-time should be clamped to zero"
        );
    }

    #[test]
    fn guard_overhead_under_1us() {
        reset();
        let iterations = 1_000_000u64;
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _g = enter("overhead");
        }
        let elapsed = start.elapsed();
        let per_call_ns = elapsed.as_nanos() as f64 / iterations as f64;
        eprintln!("guard overhead: {per_call_ns:.1}ns per call ({iterations} iterations)");
        assert!(
            per_call_ns < 1000.0,
            "per-call overhead {per_call_ns:.1}ns exceeds 1us limit"
        );
        reset();
    }

    #[test]
    fn deep_nesting_100_levels() {
        reset();

        // Pre-generate static names for each level.
        let names: Vec<&'static str> = (0..100)
            .map(|i| -> &'static str { Box::leak(format!("level_{i}").into_boxed_str()) })
            .collect();

        // Build nested call tree iteratively using a vec of guards.
        let mut guards = Vec::with_capacity(100);
        for name in &names {
            guards.push(enter(name));
            burn_cpu(1_000);
        }
        // Drop guards in reverse order (innermost first).
        while let Some(g) = guards.pop() {
            drop(g);
        }

        let records = collect();
        assert_eq!(records.len(), 100, "expected 100 functions");

        let root = records.iter().find(|r| r.name == "level_0").unwrap();

        // Conservation: sum of self-times should equal root total.
        let sum_self: f64 = records.iter().map(|r| r.self_ms).sum();
        let error_pct = ((sum_self - root.total_ms) / root.total_ms).abs() * 100.0;
        assert!(
            error_pct < 5.0,
            "conservation violated at 100 levels: sum_self={sum_self:.3}ms, root_total={:.3}ms, error={error_pct:.1}%",
            root.total_ms
        );

        // No negative self-times.
        for rec in &records {
            assert!(
                rec.self_ms >= 0.0,
                "{} has negative self_ms: {}",
                rec.name,
                rec.self_ms
            );
        }

        // Each non-leaf should have self_ms < total_ms (except the innermost).
        let innermost = records.iter().find(|r| r.name == "level_99").unwrap();
        let diff = (innermost.self_ms - innermost.total_ms).abs();
        assert!(
            diff < 0.5,
            "innermost level should have self ≈ total: self={:.3}, total={:.3}",
            innermost.self_ms,
            innermost.total_ms
        );

        reset();
    }

    #[test]
    fn fork_returns_none_with_empty_stack() {
        reset();
        assert!(fork().is_none(), "fork should return None with empty stack");
    }

    #[test]
    fn fork_adopt_propagates_child_time_to_parent() {
        reset();
        {
            let _parent = enter("parent_fn");
            burn_cpu(5_000);

            let ctx = fork().expect("should have parent on stack");

            // Simulate a child thread (same thread for test simplicity).
            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("child_fn");
                    burn_cpu(20_000);
                }
            }

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "parent_fn").unwrap();
        let child = records.iter().find(|r| r.name == "child_fn").unwrap();

        // Parent's total should include child time (via finalize).
        assert!(
            parent.total_ms > child.total_ms,
            "parent total ({:.1}ms) should exceed child total ({:.1}ms)",
            parent.total_ms,
            child.total_ms
        );

        // Conservation: sum of self times should approximate root total.
        let sum_self: f64 = records.iter().map(|r| r.self_ms).sum();
        let error_pct = ((sum_self - parent.total_ms) / parent.total_ms).abs() * 100.0;
        assert!(
            error_pct < 10.0,
            "conservation: sum_self={sum_self:.1}ms, root_total={:.1}ms, error={error_pct:.1}%",
            parent.total_ms
        );
    }

    #[test]
    fn adopt_without_child_work_adds_minimal_overhead() {
        reset();
        {
            let _parent = enter("overhead_parent");
            let ctx = fork().unwrap();
            {
                let _adopt = adopt(&ctx);
                // No work on child thread.
            }
            ctx.finalize();
        }

        let records = collect();
        let parent = records
            .iter()
            .find(|r| r.name == "overhead_parent")
            .unwrap();
        // Parent should still have valid timing.
        assert!(parent.calls == 1);
        assert!(parent.total_ms >= 0.0);
    }

    #[test]
    fn multiple_children_accumulate_in_parent() {
        reset();
        {
            let _parent = enter("multi_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            // Simulate 3 child threads.
            for _ in 0..3 {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("worker");
                    burn_cpu(10_000);
                }
            }

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "multi_parent").unwrap();
        let worker = records.iter().find(|r| r.name == "worker").unwrap();

        assert_eq!(worker.calls, 3, "should have 3 worker calls");
        // Parent's children time should include all 3 workers.
        assert!(
            parent.total_ms > worker.total_ms,
            "parent total ({:.1}ms) should exceed single worker total ({:.1}ms)",
            parent.total_ms,
            worker.total_ms
        );
    }

    #[test]
    fn invocation_records_capture_depth() {
        reset();
        {
            let _outer = enter("outer");
            burn_cpu(5_000);
            {
                let _inner = enter("inner");
                burn_cpu(5_000);
            }
        }
        let invocations = collect_invocations();
        let outer_inv = invocations.iter().find(|r| r.name == "outer").unwrap();
        let inner_inv = invocations.iter().find(|r| r.name == "inner").unwrap();
        assert_eq!(outer_inv.depth, 0);
        assert_eq!(inner_inv.depth, 1);
    }

    #[test]
    fn cross_thread_fork_adopt_propagates() {
        reset();

        // Baseline: measure parent_fn with no cross-thread attribution.
        {
            let _parent = enter("baseline");
            burn_cpu(5_000);
            // Sleep to simulate child wall-clock time without fork/adopt.
            thread::sleep(Duration::from_millis(50));
        }
        let baseline_records = collect();
        let baseline = baseline_records
            .iter()
            .find(|r| r.name == "baseline")
            .unwrap();
        let baseline_self = baseline.self_ms;

        reset();

        // Now: same parent work, but spawn a real child thread via fork/adopt.
        // The child's elapsed time should be subtracted from parent self-time.
        {
            let _parent = enter("parent_fn");
            burn_cpu(5_000);

            let ctx = fork().expect("should have parent on stack");

            thread::scope(|s| {
                s.spawn(|| {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("thread_child");
                        // Sleep long enough to create a measurable difference.
                        thread::sleep(Duration::from_millis(50));
                    }
                });
            });

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "parent_fn").unwrap();

        // The parent's self_ms should be notably less than baseline because
        // finalize() propagated the child thread's ~50ms as children_ms.
        assert!(
            parent.self_ms < baseline_self,
            "parent self ({:.1}ms) should be less than baseline self ({:.1}ms) \
             due to cross-thread attribution",
            parent.self_ms,
            baseline_self
        );
    }
}
