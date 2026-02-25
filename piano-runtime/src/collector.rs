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
//! Flush strategy: each thread's records live in an `Arc<Mutex<Vec<RawRecord>>>`
//! registered in a global `THREAD_RECORDS` Vec. `shutdown()` (injected at the
//! end of main by the AST rewriter) iterates all Arcs to collect data from every
//! thread, including thread-pool workers whose TLS destructors may never fire.
//!
//! Thread-locality: stack and records are thread-local. Each thread produces an
//! independent call tree by default. For cross-thread attribution (e.g. rayon
//! scopes, spawned threads), use `fork()` / `adopt()` to propagate timing context
//! so that child thread elapsed time is correctly subtracted from the parent's
//! self-time. `SpanContext` auto-finalizes on Drop.

use std::cell::{RefCell, UnsafeCell};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Thread-safe, initialize-once cell using `Once` + `UnsafeCell`.
///
/// Equivalent to `OnceLock` (stabilized in 1.70) but works on Rust 1.56+.
/// The `Once` primitive guarantees single-writer semantics; after initialization
/// the value is read-only, so there are no data races.
struct SyncOnceCell<T> {
    once: Once,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: `Once` synchronizes initialization. After `call_once` completes,
// the inner value is only read, never mutated, so `Sync` is sound.
unsafe impl<T: Send + Sync> Sync for SyncOnceCell<T> {}

impl<T> SyncOnceCell<T> {
    const fn new() -> Self {
        Self {
            once: Once::new(),
            value: UnsafeCell::new(None),
        }
    }

    fn get_or_init(&self, f: impl FnOnce() -> T) -> &T {
        self.once.call_once(|| {
            // SAFETY: `call_once` guarantees this runs exactly once, with
            // all subsequent callers blocking until it completes.
            unsafe { *self.value.get() = Some(f()) };
        });
        // SAFETY: After `call_once` returns (on any thread), `value` is
        // `Some` and never mutated again.
        unsafe { (*self.value.get()).as_ref().unwrap() }
    }
}

/// Process-wide run identifier.
///
/// All threads within a single process share this ID, so that JSON files
/// written by different threads can be correlated during report consolidation.
static RUN_ID: SyncOnceCell<String> = SyncOnceCell::new();

fn run_id() -> &'static str {
    RUN_ID.get_or_init(|| format!("{}_{}", std::process::id(), timestamp_ms()))
}

/// Process-start epoch for relative timestamps.
static EPOCH: SyncOnceCell<Instant> = SyncOnceCell::new();

fn epoch() -> Instant {
    *EPOCH.get_or_init(Instant::now)
}

/// Aggregated timing data for a single function.
#[derive(Debug, Clone)]
pub struct FunctionRecord {
    pub name: String,
    pub calls: u64,
    pub total_ms: f64,
    pub self_ms: f64,
    #[cfg(feature = "cpu-time")]
    pub cpu_self_ms: f64,
}

/// Per-function summary within a single frame.
#[derive(Debug, Clone)]
pub struct FrameFnSummary {
    pub name: &'static str,
    pub calls: u32,
    pub self_ns: u64,
    #[cfg(feature = "cpu-time")]
    pub cpu_self_ns: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}

/// Per-invocation measurement record with nanosecond precision.
#[derive(Debug, Clone)]
pub struct InvocationRecord {
    pub name: &'static str,
    pub start_ns: u64,
    pub elapsed_ns: u64,
    pub self_ns: u64,
    #[cfg(feature = "cpu-time")]
    pub cpu_self_ns: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
    pub depth: u16,
}

/// Entry on the thread-local timing stack.
pub(crate) struct StackEntry {
    pub(crate) name: &'static str,
    pub(crate) start: Instant,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_start_ns: u64,
    pub(crate) children_ms: f64,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_children_ns: u64,
    /// Saved ALLOC_COUNTERS from before this scope, restored on Guard::drop.
    pub(crate) saved_alloc: crate::alloc::AllocSnapshot,
    pub(crate) depth: u16,
}

/// Raw measurement produced when a Guard drops.
#[derive(Clone)]
struct RawRecord {
    name: &'static str,
    elapsed_ms: f64,
    children_ms: f64,
    #[cfg(feature = "cpu-time")]
    cpu_self_ns: u64,
}

type ThreadRecordArc = Arc<Mutex<Vec<RawRecord>>>;

/// Global registry of per-thread record storage.
/// Each thread registers its Arc on first access. collect_all() iterates all Arcs.
static THREAD_RECORDS: SyncOnceCell<Mutex<Vec<ThreadRecordArc>>> = SyncOnceCell::new();

fn thread_records() -> &'static Mutex<Vec<ThreadRecordArc>> {
    THREAD_RECORDS.get_or_init(|| Mutex::new(Vec::new()))
}

thread_local! {
    pub(crate) static STACK: RefCell<Vec<StackEntry>> = RefCell::new(Vec::new());
    static RECORDS: Arc<Mutex<Vec<RawRecord>>> = {
        let arc = Arc::new(Mutex::new(Vec::new()));
        thread_records().lock().unwrap_or_else(|e| e.into_inner()).push(Arc::clone(&arc));
        arc
    };
    static REGISTERED: RefCell<Vec<&'static str>> = RefCell::new(Vec::new());
    #[cfg(test)]
    static INVOCATIONS: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    /// Invocations accumulated within the current frame (cleared on frame boundary).
    static FRAME_BUFFER: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    /// Completed per-frame summaries.
    static FRAMES: RefCell<Vec<Vec<FrameFnSummary>>> = RefCell::new(Vec::new());
}

/// RAII timing guard. Records elapsed time on drop.
///
/// Self-contained: carries its own timing state so that thread migration
/// (e.g. async runtimes moving futures between worker threads) produces
/// correct wall time instead of corrupting the enter thread's TLS stack.
#[must_use = "dropping the guard immediately records ~0ms; bind it with `let _guard = ...`"]
pub struct Guard {
    name: &'static str,
    start: Instant,
    enter_thread: std::thread::ThreadId,
    stack_depth: u16,
    /// Saved alloc counters from enter(). Retained for future use when
    /// migrated guards gain alloc attribution support.
    #[allow(dead_code)]
    saved_alloc: crate::alloc::AllocSnapshot,
}

// Guard must be Send so async runtimes can move futures containing guards
// across worker threads. All fields are Send (Instant, ThreadId, &'static str,
// u16, AllocSnapshot) so this is auto-derived, but we assert it at compile time
// to catch accidental regressions (e.g. adding a Cell or Rc field).
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<Guard>();
    }
};

impl Drop for Guard {
    fn drop(&mut self) {
        let drop_thread = std::thread::current().id();
        let migrated = drop_thread != self.enter_thread;

        if migrated {
            // Thread migration detected. Wall time is correct (Instant is
            // cross-thread) but we cannot access the enter thread's TLS stack,
            // alloc counters, or CPU clock. Record wall time only.
            let elapsed_ns = self.start.elapsed().as_nanos() as u64;
            let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
            let start_ns = self.start.duration_since(epoch()).as_nanos() as u64;

            RECORDS.with(|records| {
                records
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(RawRecord {
                        name: self.name,
                        elapsed_ms,
                        children_ms: 0.0,
                        #[cfg(feature = "cpu-time")]
                        cpu_self_ns: 0,
                    });
            });

            let invocation = InvocationRecord {
                name: self.name,
                start_ns,
                elapsed_ns,
                // No children info available on the drop thread, so self =
                // wall. May overcount when migrated children exist.
                self_ns: elapsed_ns,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
                depth: self.stack_depth,
            };

            #[cfg(test)]
            INVOCATIONS.with(|inv| {
                inv.borrow_mut().push(invocation.clone());
            });

            FRAME_BUFFER.with(|buf| {
                buf.borrow_mut().push(invocation);
            });
            // No frame boundary check: migrated guard's depth is from
            // the enter thread, not meaningful for this thread's frames.
            return;
        }

        // Same thread -- existing logic with orphan drain prefix.
        #[cfg(feature = "cpu-time")]
        let cpu_end_ns = crate::cpu_clock::cpu_now_ns();

        let scope_alloc = crate::alloc::ALLOC_COUNTERS
            .try_with(|cell| cell.get())
            .unwrap_or_default();

        STACK.with(|stack| {
            // Drain orphaned entries left by migrated child guards.
            {
                let mut s = stack.borrow_mut();
                while s.last().map_or(false, |e| e.depth > self.stack_depth) {
                    let orphan = s.pop().unwrap();
                    let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
                        cell.set(orphan.saved_alloc);
                    });
                }
            }

            let entry = match stack.borrow_mut().pop() {
                Some(e) => e,
                None => {
                    eprintln!("piano-runtime: guard dropped without matching stack entry (bug)");
                    return;
                }
            };

            // Restore parent's saved alloc counters.
            let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
                cell.set(entry.saved_alloc);
            });

            let elapsed_ns = entry.start.elapsed().as_nanos() as u64;
            let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
            let children_ns = (entry.children_ms * 1_000_000.0) as u64;
            let self_ns = elapsed_ns.saturating_sub(children_ns);
            let start_ns = entry.start.duration_since(epoch()).as_nanos() as u64;
            let children_ms = entry.children_ms;

            #[cfg(feature = "cpu-time")]
            let cpu_elapsed_ns = cpu_end_ns.saturating_sub(entry.cpu_start_ns);
            #[cfg(feature = "cpu-time")]
            let cpu_self_ns = cpu_elapsed_ns.saturating_sub(entry.cpu_children_ns);

            if let Some(parent) = stack.borrow_mut().last_mut() {
                parent.children_ms += elapsed_ms;
                #[cfg(feature = "cpu-time")]
                {
                    parent.cpu_children_ns += cpu_elapsed_ns;
                }
            }

            RECORDS.with(|records| {
                records
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(RawRecord {
                        name: entry.name,
                        elapsed_ms,
                        children_ms,
                        #[cfg(feature = "cpu-time")]
                        cpu_self_ns,
                    });
            });

            let invocation = InvocationRecord {
                name: entry.name,
                start_ns,
                elapsed_ns,
                self_ns,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns,
                alloc_count: scope_alloc.alloc_count,
                alloc_bytes: scope_alloc.alloc_bytes,
                free_count: scope_alloc.free_count,
                free_bytes: scope_alloc.free_bytes,
                depth: entry.depth,
            };

            #[cfg(test)]
            INVOCATIONS.with(|inv| {
                inv.borrow_mut().push(invocation.clone());
            });

            FRAME_BUFFER.with(|buf| {
                buf.borrow_mut().push(invocation);
            });
            if entry.depth == 0 {
                FRAME_BUFFER.with(|buf| {
                    let buffer = buf.borrow_mut().drain(..).collect::<Vec<_>>();
                    let summary = aggregate_frame(&buffer);
                    FRAMES.with(|frames| {
                        frames.borrow_mut().push(summary);
                    });
                });
            }
        });
    }
}

/// Start timing a function. Returns a Guard that records the measurement on drop.
pub fn enter(name: &'static str) -> Guard {
    // Touch EPOCH so relative timestamps are anchored to process start.
    let _ = epoch();
    let start = Instant::now();
    // Captured here in enter() but stored only in Guard, not StackEntry.
    // StackEntry lives in thread-local storage and is always accessed from its
    // own thread, so it never needs to know which thread created it. Guard, on
    // the other hand, can be moved across threads by async runtimes, so it
    // carries the originating thread ID to detect migration in Drop.
    let enter_thread = std::thread::current().id();

    // Save current alloc counters and zero them for this scope.
    let saved_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| {
            let snap = cell.get();
            cell.set(crate::alloc::AllocSnapshot::new());
            snap
        })
        .unwrap_or_default();

    let depth = STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        stack.borrow_mut().push(StackEntry {
            name,
            start,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns: crate::cpu_clock::cpu_now_ns(),
            children_ms: 0.0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            saved_alloc,
            depth,
        });
        depth
    });

    Guard {
        name,
        start,
        enter_thread,
        stack_depth: depth,
        saved_alloc,
    }
}

/// Register a function name so it appears in output even if never called.
///
/// Must be called from the same thread that will later call `collect_all()`
/// or `shutdown()`. In practice this means `main()` -- the AST rewriter
/// injects `register()` calls at the top of `main()` and `shutdown()` at
/// the end. Calling `register()` from worker threads will cause those
/// function names to be missing from aggregated output because `REGISTERED`
/// is thread-local.
pub fn register(name: &'static str) {
    REGISTERED.with(|reg| {
        let mut reg = reg.borrow_mut();
        if !reg.contains(&name) {
            reg.push(name);
        }
    });
}

/// Aggregate raw records into per-function summaries, sorted by self_ms descending.
struct AggEntry {
    calls: u64,
    total_ms: f64,
    self_ms: f64,
    #[cfg(feature = "cpu-time")]
    cpu_self_ns: u64,
}

impl AggEntry {
    fn new() -> Self {
        Self {
            calls: 0,
            total_ms: 0.0,
            self_ms: 0.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
        }
    }
}

fn aggregate(raw: &[RawRecord], registered: &[&str]) -> Vec<FunctionRecord> {
    let mut map: HashMap<&str, AggEntry> = HashMap::new();

    for name in registered {
        map.entry(name).or_insert_with(AggEntry::new);
    }

    for rec in raw {
        let entry = map.entry(rec.name).or_insert_with(AggEntry::new);
        entry.calls += 1;
        entry.total_ms += rec.elapsed_ms;
        entry.self_ms += (rec.elapsed_ms - rec.children_ms).max(0.0);
        #[cfg(feature = "cpu-time")]
        {
            entry.cpu_self_ns += rec.cpu_self_ns;
        }
    }

    let mut result: Vec<FunctionRecord> = map
        .into_iter()
        .map(|(name, e)| FunctionRecord {
            name: name.to_owned(),
            calls: e.calls,
            total_ms: e.total_ms,
            self_ms: e.self_ms,
            #[cfg(feature = "cpu-time")]
            cpu_self_ms: e.cpu_self_ns as f64 / 1_000_000.0,
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
/// Reads only from the current thread's record storage.
pub fn collect() -> Vec<FunctionRecord> {
    RECORDS.with(|records| {
        let recs = records.lock().unwrap_or_else(|e| e.into_inner());
        REGISTERED.with(|reg| aggregate(&recs, &reg.borrow()))
    })
}

/// Return all raw per-invocation records (not aggregated).
#[cfg(test)]
pub fn collect_invocations() -> Vec<InvocationRecord> {
    INVOCATIONS.with(|inv| inv.borrow().clone())
}

/// Return completed per-frame summaries.
pub fn collect_frames() -> Vec<Vec<FrameFnSummary>> {
    FRAMES.with(|frames| frames.borrow().clone())
}

/// Aggregate invocation records within a single frame into per-function summaries.
fn aggregate_frame(records: &[InvocationRecord]) -> Vec<FrameFnSummary> {
    let mut map: HashMap<&'static str, FrameFnSummary> = HashMap::new();
    for rec in records {
        let entry = map.entry(rec.name).or_insert(FrameFnSummary {
            name: rec.name,
            calls: 0,
            self_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        });
        entry.calls += 1;
        entry.self_ns += rec.self_ns;
        #[cfg(feature = "cpu-time")]
        {
            entry.cpu_self_ns += rec.cpu_self_ns;
        }
        entry.alloc_count += rec.alloc_count;
        entry.alloc_bytes += rec.alloc_bytes;
        entry.free_count += rec.free_count;
        entry.free_bytes += rec.free_bytes;
    }
    #[allow(clippy::iter_kv_map)]
    // into_values() requires Rust 1.54; we support 1.56 but keep the pattern uniform
    map.into_iter().map(|(_, v)| v).collect()
}

/// Collect records from ALL threads via the global registry.
/// This is the primary collection method for cross-thread profiling — it
/// captures data from thread-pool workers whose TLS destructors may never fire.
///
/// Clones the Arc handles under the global lock, then drops the lock before
/// iterating per-thread records. This avoids blocking new thread registrations
/// while aggregation is in progress.
///
/// Note: `REGISTERED` (the set of known function names) is read from the
/// calling thread's TLS only. Function names registered on other threads
/// will not appear in the output unless they were also recorded via `enter()`.
/// In the normal flow the AST rewriter injects all `register()` calls into
/// `main()`, so calling `collect_all()` from `main()` (via `shutdown()`)
/// sees every registered name.
pub fn collect_all() -> Vec<FunctionRecord> {
    let arcs: Vec<ThreadRecordArc> = {
        let registry = thread_records().lock().unwrap_or_else(|e| e.into_inner());
        registry.clone()
    };
    let mut all_raw: Vec<RawRecord> = Vec::new();
    for arc in &arcs {
        let records = arc.lock().unwrap_or_else(|e| e.into_inner());
        all_raw.extend(records.iter().cloned());
    }
    let registered: Vec<&str> = REGISTERED
        .try_with(|reg| reg.borrow().clone())
        .unwrap_or_default();
    aggregate(&all_raw, &registered)
}

/// Clear all collected timing data for the current thread.
pub fn reset() {
    STACK.with(|stack| stack.borrow_mut().clear());
    RECORDS.with(|records| {
        records.lock().unwrap_or_else(|e| e.into_inner()).clear();
    });
    REGISTERED.with(|reg| reg.borrow_mut().clear());
    #[cfg(test)]
    INVOCATIONS.with(|inv| inv.borrow_mut().clear());
    FRAME_BUFFER.with(|buf| buf.borrow_mut().clear());
    FRAMES.with(|frames| frames.borrow_mut().clear());
}

/// Clear collected timing data across ALL threads, plus the calling thread's
/// local state (stack, registrations, frame buffers).
///
/// Unlike `reset()` which only clears the calling thread's records,
/// `reset_all()` iterates every Arc in the global `THREAD_RECORDS` registry
/// so that a subsequent `collect_all()` sees no stale data from other threads.
#[cfg(test)]
pub fn reset_all() {
    // Clear every thread's record Arc.
    let arcs: Vec<ThreadRecordArc> = {
        let registry = thread_records().lock().unwrap_or_else(|e| e.into_inner());
        registry.clone()
    };
    for arc in &arcs {
        arc.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
    // Clear the calling thread's local state.
    reset();
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
    let run_id = run_id();
    write!(
        f,
        "{{\"run_id\":\"{}\",\"timestamp_ms\":{},\"functions\":[",
        run_id, ts
    )?;
    for (i, rec) in records.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        // Escape the function name (in practice only ASCII identifiers, but be safe).
        let name = rec.name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(
            f,
            "{{\"name\":\"{}\",\"calls\":{},\"total_ms\":{:.3},\"self_ms\":{:.3}",
            name, rec.calls, rec.total_ms, rec.self_ms
        )?;
        #[cfg(feature = "cpu-time")]
        write!(f, ",\"cpu_self_ms\":{:.3}", rec.cpu_self_ms)?;
        write!(f, "}}")?;
    }
    writeln!(f, "]}}")?;
    Ok(())
}

/// Write an NDJSON file with frame-level data.
///
/// Line 1: header with metadata and function name table.
/// Lines 2+: one line per frame with per-function summaries.
fn write_ndjson(
    frames: &[Vec<FrameFnSummary>],
    fn_names: &[&str],
    path: &std::path::Path,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = std::fs::File::create(path)?;
    let ts = timestamp_ms();
    let run_id = run_id();

    // Header line: metadata + function name table
    write!(
        f,
        "{{\"format_version\":3,\"run_id\":\"{}\",\"timestamp_ms\":{}",
        run_id, ts
    )?;
    #[cfg(feature = "cpu-time")]
    write!(f, ",\"has_cpu_time\":true")?;
    write!(f, ",\"functions\":[")?;
    for (i, name) in fn_names.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        let name = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(f, "\"{}\"", name)?;
    }
    writeln!(f, "]}}")?;

    // Build index for O(1) fn_id lookup
    let fn_id_map: HashMap<&str, usize> =
        fn_names.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // One line per frame
    for (frame_idx, frame) in frames.iter().enumerate() {
        write!(f, "{{\"frame\":{},\"fns\":[", frame_idx)?;
        for (i, s) in frame.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            let fn_id = fn_id_map.get(s.name).copied().unwrap_or(0);
            write!(
                f,
                "{{\"id\":{},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}",
                fn_id, s.calls, s.self_ns, s.alloc_count, s.alloc_bytes, s.free_count, s.free_bytes
            )?;
            #[cfg(feature = "cpu-time")]
            write!(f, ",\"csn\":{}", s.cpu_self_ns)?;
            write!(f, "}}")?;
        }
        writeln!(f, "]}}")?;
    }
    Ok(())
}

/// Flush collected timing data to disk.
///
/// If frame data is present, writes NDJSON format. Otherwise falls back
/// to JSON for non-frame workloads.
///
/// Normally you don't need to call this — `shutdown()` flushes all threads
/// at the end of main. This function exists for explicit mid-program flushes
/// of the current thread's data.
pub fn flush() {
    let dir = match runs_dir() {
        Some(d) => d,
        None => return,
    };

    let frames = collect_frames();
    if !frames.is_empty() {
        let mut seen = HashSet::new();
        let mut fn_names: Vec<&str> = Vec::new();
        for frame in &frames {
            for s in frame {
                if seen.insert(s.name) {
                    fn_names.push(s.name);
                }
            }
        }
        let path = dir.join(format!("{}.ndjson", timestamp_ms()));
        let _ = write_ndjson(&frames, &fn_names, &path);
    } else {
        let records = collect();
        if records.is_empty() {
            return;
        }
        let path = dir.join(format!("{}.json", timestamp_ms()));
        let _ = write_json(&records, &path);
    }
    reset();
}

/// No-op retained for API compatibility.
///
/// Flushing now happens via `shutdown()` at the end of main.
/// Instrumented code may still call `init()` — it's harmless.
pub fn init() {}

/// Flush all collected timing data from ALL threads and write to disk.
///
/// Collects from the global per-thread registry, so data from thread-pool
/// workers is included. Injected at the end of main() by the AST rewriter.
///
/// Writes NDJSON if frame data is present (from the calling thread), and
/// always writes JSON with cross-thread aggregation from all registered Arcs.
pub fn shutdown() {
    let dir = match runs_dir() {
        Some(d) => d,
        None => return,
    };
    shutdown_impl(&dir);
}

/// Like `shutdown`, but writes run files to the specified directory.
///
/// Used by the CLI to write to project-local `target/piano/runs/` instead
/// of the global `~/.piano/runs/`. `PIANO_RUNS_DIR` env var takes priority
/// if set (for testing and user overrides).
pub fn shutdown_to(dir: &str) {
    if let Ok(override_dir) = std::env::var("PIANO_RUNS_DIR") {
        shutdown_impl(std::path::Path::new(&override_dir));
    } else {
        shutdown_impl(std::path::Path::new(dir));
    }
}

fn shutdown_impl(dir: &std::path::Path) {
    let ts = timestamp_ms();

    // Write frame-level data if present (NDJSON format).
    let frames = collect_frames();
    if !frames.is_empty() {
        let mut seen = HashSet::new();
        let mut fn_names: Vec<&str> = Vec::new();
        for frame in &frames {
            for s in frame {
                if seen.insert(s.name) {
                    fn_names.push(s.name);
                }
            }
        }
        let path = dir.join(format!("{}.ndjson", ts));
        let _ = write_ndjson(&frames, &fn_names, &path);
    }

    // Always write aggregated cross-thread data (JSON format).
    let records = collect_all();
    if !records.is_empty() {
        let path = dir.join(format!("{}.json", ts));
        let _ = write_json(&records, &path);
    }
}

/// Context for propagating parent-child CPU timing across thread boundaries.
///
/// Created by `fork()` on the parent thread, passed to child threads via
/// `adopt()`. When the child completes, its CPU time is accumulated
/// in `children_cpu_ns` which the parent reads back via Drop (or explicit `finalize()`).
/// Wall time is NOT propagated cross-thread (it's not additive for parallel work).
pub struct SpanContext {
    parent_name: &'static str,
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc<Mutex<u64>>,
    finalized: bool,
}

impl SpanContext {
    /// Explicitly finalize cross-thread attribution.
    /// Equivalent to dropping the SpanContext, but makes intent clear.
    pub fn finalize(mut self) {
        self.apply_children();
        self.finalized = true;
    }

    fn apply_children(&self) {
        #[cfg(feature = "cpu-time")]
        {
            let children_cpu = *self
                .children_cpu_ns
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            STACK.with(|stack| {
                if let Some(top) = stack.borrow_mut().last_mut() {
                    top.cpu_children_ns += children_cpu;
                }
            });
        }
    }
}

impl Drop for SpanContext {
    fn drop(&mut self) {
        if !self.finalized {
            self.apply_children();
        }
    }
}

/// RAII guard for cross-thread adoption. Pops the synthetic parent on drop
/// and propagates CPU time back to the parent's `SpanContext`.
#[must_use = "dropping AdoptGuard immediately records ~0ms; bind it with `let _guard = ...`"]
pub struct AdoptGuard {
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc<Mutex<u64>>,
}

impl Drop for AdoptGuard {
    fn drop(&mut self) {
        // Restore the parent's saved alloc counters (same pattern as Guard::drop).
        // The adopted scope's alloc data isn't recorded into an InvocationRecord,
        // but the restore is necessary for correct nesting.
        STACK.with(|stack| {
            let entry = match stack.borrow_mut().pop() {
                Some(e) => e,
                None => return,
            };

            let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
                cell.set(entry.saved_alloc);
            });

            // Propagate this thread's CPU time back to the parent context.
            #[cfg(feature = "cpu-time")]
            {
                let cpu_elapsed_ns =
                    crate::cpu_clock::cpu_now_ns().saturating_sub(entry.cpu_start_ns);
                let mut cpu_children = self
                    .ctx_children_cpu_ns
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *cpu_children += cpu_elapsed_ns;
            }
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
            #[cfg(feature = "cpu-time")]
            children_cpu_ns: Arc::new(Mutex::new(0)),
            finalized: false,
        })
    })
}

/// Adopt a parent span context on a child thread.
///
/// Pushes a synthetic parent entry so that `enter()`/`Guard::drop()` on this
/// thread correctly attributes children time. Returns an `AdoptGuard` that
/// propagates CPU time back to the parent on drop.
pub fn adopt(ctx: &SpanContext) -> AdoptGuard {
    // Save current alloc counters and zero them, same as enter().
    let saved_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| {
            let snap = cell.get();
            cell.set(crate::alloc::AllocSnapshot::new());
            snap
        })
        .unwrap_or_default();

    STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        stack.borrow_mut().push(StackEntry {
            name: ctx.parent_name,
            start: Instant::now(),
            #[cfg(feature = "cpu-time")]
            cpu_start_ns: crate::cpu_clock::cpu_now_ns(),
            children_ms: 0.0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            saved_alloc,
            depth,
        });
    });
    AdoptGuard {
        #[cfg(feature = "cpu-time")]
        ctx_children_cpu_ns: Arc::clone(&ctx.children_cpu_ns),
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

    #[test]
    fn flush_writes_valid_output_to_env_dir() {
        reset();
        {
            let _g = enter("flush_test");
            burn_cpu(5_000);
        }

        let tmp = std::env::temp_dir().join(format!("piano_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        // Point flush at our temp dir.
        // SAFETY: Test runs serially (no concurrent env access).
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        flush();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

        // Find written file (NDJSON for frame workloads).
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let ext = e.path().extension().map(|e| e.to_owned());
                ext.as_deref() == Some(std::ffi::OsStr::new("ndjson"))
                    || ext.as_deref() == Some(std::ffi::OsStr::new("json"))
            })
            .collect();
        assert!(!files.is_empty(), "expected at least one output file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("flush_test"),
            "should contain function name"
        );
        assert!(
            content.contains("timestamp_ms"),
            "should contain timestamp_ms"
        );

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
                #[cfg(feature = "cpu-time")]
                cpu_self_ms: 7.0,
            },
            FunctionRecord {
                name: "resolve".into(),
                calls: 1,
                total_ms: 4.2,
                self_ms: 4.2,
                #[cfg(feature = "cpu-time")]
                cpu_self_ms: 4.1,
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

    #[cfg(feature = "cpu-time")]
    #[test]
    fn write_json_includes_cpu_self_ms() {
        let records = vec![FunctionRecord {
            name: "compute".into(),
            calls: 5,
            total_ms: 10.0,
            self_ms: 8.0,
            cpu_self_ms: 7.5,
        }];
        let tmp = std::env::temp_dir().join(format!("piano_cpu_json_{}.json", std::process::id()));
        write_json(&records, &tmp).unwrap();
        let content = std::fs::read_to_string(&tmp).unwrap();
        assert!(
            content.contains("\"cpu_self_ms\":7.500"),
            "JSON should contain cpu_self_ms, got: {content}"
        );
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
            burn_cpu(5_000);
        }
        let records = collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].name, "work");
        assert_eq!(records[0].calls, 1);
    }

    #[test]
    fn nested_function_self_time() {
        reset();
        {
            let _outer = enter("outer");
            burn_cpu(5_000);
            {
                let _inner = enter("inner");
                burn_cpu(10_000);
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

        // Structural: outer self < total because inner subtracts.
        assert!(
            outer.self_ms < outer.total_ms,
            "self ({:.3}) should be less than total ({:.3})",
            outer.self_ms,
            outer.total_ms
        );
        // Inner is a leaf -- self ~ total within 10%.
        let diff = (inner.self_ms - inner.total_ms).abs();
        assert!(
            diff < inner.total_ms * 0.1,
            "inner self_ms={:.3} total_ms={:.3}",
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
            burn_cpu(1_000);
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
            burn_cpu(1_000);
        }
        {
            let _g = enter("slow");
            burn_cpu(50_000);
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
            burn_cpu(1_000);
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
    fn output_contains_run_id() {
        reset();
        {
            let _g = enter("rid_test");
            burn_cpu(1_000);
        }
        let tmp = std::env::temp_dir().join(format!("piano_rid_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        flush();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let ext = e.path().extension().map(|e| e.to_owned());
                ext.as_deref() == Some(std::ffi::OsStr::new("ndjson"))
                    || ext.as_deref() == Some(std::ffi::OsStr::new("json"))
            })
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
    fn negative_self_time_clamped_to_zero() {
        // Regression test for the f64 drift clamp in aggregate().
        // Construct a synthetic RawRecord where children_ms slightly exceeds elapsed_ms
        // (simulating floating-point accumulation drift).
        let raw = vec![RawRecord {
            name: "drifted",
            elapsed_ms: 10.0,
            children_ms: 10.001,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
        }];
        let result = aggregate(&raw, &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].self_ms, 0.0,
            "negative self-time should be clamped to zero"
        );
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

        // No negative self-times.
        for rec in &records {
            assert!(
                rec.self_ms >= 0.0,
                "{} has negative self_ms: {}",
                rec.name,
                rec.self_ms
            );
        }

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

        // Both recorded with correct call counts.
        assert_eq!(parent.calls, 1);
        assert_eq!(child.calls, 1);
        // Parent total exceeds child total.
        assert!(
            parent.total_ms > child.total_ms,
            "parent total ({:.1}ms) should exceed child total ({:.1}ms)",
            parent.total_ms,
            child.total_ms
        );

        // Wall self no longer reduced by cross-thread children.
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
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

        assert_eq!(parent.calls, 1, "parent should have 1 call");
        assert_eq!(worker.calls, 3, "should have 3 worker calls");
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
        {
            let _parent = enter("parent_fn");
            burn_cpu(5_000);

            let ctx = fork().expect("should have parent on stack");

            thread::scope(|s| {
                s.spawn(|| {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("thread_child");
                        burn_cpu(10_000);
                    }
                });
            });

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "parent_fn").unwrap();

        // collect() is thread-local so we can only see the parent.
        // Wall self no longer reduced by cross-thread children.
        assert_eq!(parent.calls, 1);
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
            parent.total_ms
        );
    }

    #[test]
    fn write_ndjson_format() {
        reset();
        for _ in 0..2 {
            let _outer = enter("update");
            burn_cpu(5_000);
            {
                let _inner = enter("physics");
                burn_cpu(5_000);
            }
        }

        let tmp = std::env::temp_dir().join(format!("piano_ndjson_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        flush();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        assert!(!files.is_empty(), "should write .ndjson file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // First line is header
        assert!(lines[0].contains("\"format_version\":3"));
        assert!(lines[0].contains("\"functions\""));

        // Remaining lines are frames
        assert!(lines.len() >= 3, "header + 2 frames, got {}", lines.len());
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn frame_boundary_aggregation() {
        reset();
        // Simulate 3 frames: depth-0 function called 3 times
        for _frame in 0..3u32 {
            let _outer = enter("update");
            burn_cpu(5_000);
            {
                let _inner = enter("physics");
                burn_cpu(5_000);
            }
        }
        let frames = collect_frames();
        assert_eq!(frames.len(), 3, "should have 3 frames");
        for frame in &frames {
            let update = frame.iter().find(|s| s.name == "update").unwrap();
            assert_eq!(update.calls, 1);
            let physics = frame.iter().find(|s| s.name == "physics").unwrap();
            assert_eq!(physics.calls, 1);
        }
    }

    #[test]
    fn non_frame_workload_still_collects() {
        reset();
        // All calls at depth 0 but no "frame" structure
        {
            let _a = enter("parse");
            burn_cpu(5_000);
        }
        {
            let _b = enter("resolve");
            burn_cpu(5_000);
        }
        // Each depth-0 return is a frame boundary, so we get 2 single-function frames
        let frames = collect_frames();
        assert_eq!(frames.len(), 2, "each depth-0 return creates a frame");

        // Aggregate collect() should still work
        let records = collect();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn records_from_other_threads_are_captured_via_shutdown() {
        reset();
        // Spawn a thread that does work, then joins.
        // With TLS-only storage, the thread's records would be lost
        // if TLS destructors don't fire (as with rayon workers).
        // With per-thread Arc storage, collect_all() can collect them.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _g = enter("thread_work");
                burn_cpu(10_000);
            });
        });

        let records = collect_all();
        let thread_work = records.iter().find(|r| r.name == "thread_work");
        assert!(
            thread_work.is_some(),
            "thread_work should be captured via global registry. Got: {:?}",
            records.iter().map(|r| &r.name).collect::<Vec<_>>()
        );
        // Use >= instead of == because collect_all() reads all threads and
        // may include stale records from concurrent tests.
        assert!(thread_work.unwrap().calls >= 1);
    }

    #[test]
    fn span_context_auto_finalizes_on_drop() {
        reset();
        {
            let _parent = enter("auto_parent");
            burn_cpu(5_000);

            // fork + adopt, but do NOT call finalize() — rely on Drop.
            {
                let ctx = fork().expect("should have parent on stack");
                {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("auto_child");
                        burn_cpu(20_000);
                    }
                }
                // ctx drops here — should auto-finalize
            }
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "auto_parent").unwrap();

        // Wall self no longer reduced by cross-thread children.
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
            parent.total_ms
        );
    }

    #[test]
    fn shutdown_writes_json_with_all_thread_data() {
        reset();
        std::thread::scope(|s| {
            s.spawn(|| {
                let _g = enter("shutdown_thread_work");
                burn_cpu(10_000);
            });
        });
        {
            let _g = enter("shutdown_main_work");
            burn_cpu(5_000);
        }

        let tmp = std::env::temp_dir().join(format!("piano_shutdown_{}", timestamp_ms()));
        std::fs::create_dir_all(&tmp).unwrap();
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
        shutdown();
        unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        assert!(!files.is_empty(), "shutdown should write JSON");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("\"shutdown_thread_work\""),
            "should contain thread work: {content}"
        );
        assert!(
            content.contains("\"shutdown_main_work\""),
            "should contain main work: {content}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn fork_adopt_does_not_inflate_reported_times() {
        // Verify that fork/adopt overhead is NOT attributed to any function.
        // Only instrumented functions (via enter()) should appear in output.
        reset();
        {
            let _parent = enter("timed_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            // Simulate rayon: 4 children each doing work
            for _ in 0..4 {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("timed_child");
                    burn_cpu(10_000);
                }
            }
            // ctx auto-finalizes on drop
        }

        // No cross-thread spawning here, so thread-local collect() is sufficient
        // and avoids picking up stale records from other threads in parallel tests.
        let records = collect();

        // Only "timed_parent" and "timed_child" should appear. No adopt/fork entries.
        let names: Vec<&str> = records.iter().map(|r| r.name.as_str()).collect();
        assert!(
            !names
                .iter()
                .any(|n| n.contains("adopt") || n.contains("fork") || n.contains("piano")),
            "fork/adopt should not appear in output. Got: {names:?}",
        );

        let parent = records.iter().find(|r| r.name == "timed_parent").unwrap();
        let child = records.iter().find(|r| r.name == "timed_child").unwrap();

        // Parent should appear once, child 4 times.
        assert_eq!(parent.calls, 1);
        assert_eq!(child.calls, 4);
    }

    #[test]
    #[ignore] // reset_all() clears ALL threads' records; must run in isolation
    fn reset_all_clears_cross_thread_records() {
        reset();
        // Produce records on a spawned thread.
        std::thread::scope(|s| {
            s.spawn(|| {
                let _g = enter("reset_all_thread");
                burn_cpu(5_000);
            });
        });
        // Verify the spawned-thread record is visible via collect_all().
        let before = collect_all();
        assert!(
            before.iter().any(|r| r.name == "reset_all_thread"),
            "should see cross-thread record before reset_all"
        );

        // reset_all() should clear all threads' records.
        reset_all();

        let after = collect_all();
        assert!(
            !after.iter().any(|r| r.name == "reset_all_thread"),
            "reset_all should have cleared cross-thread records. Got: {:?}",
            after.iter().map(|r| &r.name).collect::<Vec<_>>()
        );
    }

    #[cfg(feature = "cpu-time")]
    #[test]
    fn cpu_time_propagated_across_threads_via_adopt() {
        reset();
        {
            let _parent = enter("cpu_parent");
            burn_cpu(5_000); // parent's own work

            let ctx = fork().expect("should have parent on stack");

            thread::scope(|s| {
                s.spawn(|| {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("cpu_child");
                        burn_cpu(50_000); // much more child CPU
                    }
                });
            });

            ctx.finalize();
        }

        let records = collect();
        let parent = records
            .iter()
            .find(|r| r.name == "cpu_parent")
            .expect("cpu_parent not found");

        // Key insight: after the wall-time fix, parent.self_ms is large because
        // wall time is NOT subtracted cross-thread. But parent.cpu_self_ms should
        // be small because CPU time IS propagated across thread boundaries via
        // fork/adopt, so the child's CPU time was subtracted from the parent's
        // CPU budget.
        eprintln!(
            "cpu_parent: self_ms={:.3}, cpu_self_ms={:.3}, total_ms={:.3}",
            parent.self_ms, parent.cpu_self_ms, parent.total_ms
        );
        assert!(
            parent.cpu_self_ms < parent.self_ms * 0.8,
            "cpu_self_ms ({:.3}) should be significantly less than self_ms ({:.3}) \
             because child CPU time is propagated cross-thread but wall time is not",
            parent.cpu_self_ms,
            parent.self_ms,
        );
    }

    #[test]
    fn fork_adopt_does_not_subtract_wall_time_from_parent() {
        // Wall time should NOT be subtracted cross-thread.
        // Parent wall self = elapsed - same-thread children only.
        reset();
        {
            let _parent = enter("wall_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("wall_child");
                    burn_cpu(50_000);
                }
            }

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "wall_parent").unwrap();
        let child = records.iter().find(|r| r.name == "wall_child").unwrap();

        // After fix: parent.self_ms ~ parent.total_ms (no cross-thread wall subtraction).
        assert!(
            parent.self_ms > child.self_ms * 0.5,
            "parent wall self ({:.3}ms) should NOT be reduced by cross-thread child wall ({:.3}ms). \
             parent.total={:.3}ms",
            parent.self_ms,
            child.self_ms,
            parent.total_ms,
        );
    }

    // ---------------------------------------------------------------
    // Async / migration tests
    // ---------------------------------------------------------------

    #[test]
    fn async_guard_same_thread() {
        reset();
        {
            let _outer = enter("outer");
            burn_cpu(5_000);
            {
                let _inner = enter("inner");
                burn_cpu(10_000);
            }
            burn_cpu(5_000);
        }
        let records = collect();
        let outer = records.iter().find(|r| r.name == "outer").unwrap();
        let inner = records.iter().find(|r| r.name == "inner").unwrap();
        assert!(
            outer.self_ms < outer.total_ms,
            "self should be less than total"
        );
        let diff = (inner.self_ms - inner.total_ms).abs();
        assert!(diff < inner.total_ms * 0.1, "inner is leaf: self ~ total");
    }

    #[test]
    fn async_guard_migrated_wall_time() {
        reset();
        let guard = enter("migrating_fn");
        burn_cpu(10_000);

        std::thread::scope(|s| {
            s.spawn(move || {
                burn_cpu(10_000);
                drop(guard);
            });
        });

        let records = collect_all();
        let rec = records.iter().find(|r| r.name == "migrating_fn");
        assert!(
            rec.is_some(),
            "migrated guard should still produce a record"
        );
        assert!(
            rec.unwrap().total_ms > 0.5,
            "wall time should reflect work on both threads"
        );
    }

    #[test]
    fn async_guard_orphan_cleanup() {
        reset();
        {
            let _parent = enter("parent");
            burn_cpu(5_000);

            let child = enter("child");
            burn_cpu(5_000);

            std::thread::scope(|s| {
                s.spawn(move || {
                    burn_cpu(5_000);
                    drop(child);
                });
            });

            burn_cpu(5_000);
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "parent").unwrap();
        assert_eq!(parent.calls, 1, "parent should have exactly 1 call");
        assert!(parent.total_ms > 0.0, "parent wall time should be positive");
        assert!(parent.self_ms > 0.0, "parent self time should be positive");
    }

    #[test]
    fn async_guard_nested_migration() {
        reset();
        {
            let _parent = enter("gp_parent");
            burn_cpu(5_000);
            {
                let _child = enter("gp_child");
                burn_cpu(5_000);

                let grandchild = enter("gp_grandchild");
                burn_cpu(5_000);

                std::thread::scope(|s| {
                    s.spawn(move || {
                        drop(grandchild);
                    });
                });

                burn_cpu(5_000);
            }
            burn_cpu(5_000);
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "gp_parent").unwrap();
        let child = records.iter().find(|r| r.name == "gp_child").unwrap();
        assert_eq!(parent.calls, 1);
        assert_eq!(child.calls, 1);
        assert!(parent.self_ms > 0.0, "parent not corrupted");
        assert!(child.self_ms > 0.0, "child not corrupted");
        assert!(
            parent.self_ms < parent.total_ms,
            "parent has child time subtracted"
        );
    }

    #[test]
    fn async_guard_alloc_restore_on_orphan() {
        // When a child guard migrates, its stack entry's saved_alloc is
        // orphaned. During the parent's drop, the orphan drain restores
        // those saved counters to ALLOC_COUNTERS before the parent's own
        // saved_alloc is restored. This ensures the grandparent scope
        // sees consistent alloc state after the parent completes.
        reset();

        // Set a known alloc baseline before any guards.
        crate::alloc::ALLOC_COUNTERS.with(|cell| {
            cell.set(crate::alloc::AllocSnapshot {
                alloc_count: 42,
                alloc_bytes: 4200,
                free_count: 0,
                free_bytes: 0,
            });
        });

        {
            let _parent = enter("alloc_parent");
            // enter() saved {42, 4200} and zeroed counters.
            // Simulate allocations in parent scope.
            crate::alloc::ALLOC_COUNTERS.with(|cell| {
                cell.set(crate::alloc::AllocSnapshot {
                    alloc_count: 10,
                    alloc_bytes: 1000,
                    free_count: 0,
                    free_bytes: 0,
                });
            });

            let child = enter("alloc_child");
            // enter() saved {10, 1000} and zeroed counters.

            std::thread::scope(|s| {
                s.spawn(move || {
                    drop(child);
                });
            });
            // child's stack entry is now orphaned with saved_alloc = {10, 1000}.
        }
        // After parent drops: orphan drain restores {10, 1000}, then parent
        // restores its own saved {42, 4200}. ALLOC_COUNTERS should be {42, 4200}.
        let restored = crate::alloc::ALLOC_COUNTERS.with(|cell| cell.get());
        assert_eq!(
            restored.alloc_count, 42,
            "grandparent alloc_count should be restored after orphan drain"
        );
        assert_eq!(
            restored.alloc_bytes, 4200,
            "grandparent alloc_bytes should be restored after orphan drain"
        );
    }

    #[cfg(feature = "cpu-time")]
    #[test]
    fn async_guard_cpu_time_skipped_on_migration() {
        reset();
        let guard = enter("cpu_migrated");
        burn_cpu(20_000);

        std::thread::scope(|s| {
            s.spawn(move || {
                burn_cpu(20_000);
                drop(guard);
            });
        });

        let records = collect_all();
        let rec = records.iter().find(|r| r.name == "cpu_migrated").unwrap();
        assert!(rec.total_ms > 0.0, "wall time captured");
        assert!(
            rec.cpu_self_ms == 0.0,
            "cpu_self_ms should be exactly 0 for migrated guard, got {:.3}",
            rec.cpu_self_ms
        );
    }
}
