//! Thread-local timing collector with RAII guards.
//!
//! Each instrumented function calls `enter(name)` which pushes a `StackEntry`
//! onto a thread-local call stack and returns an RAII `Guard`. When the guard
//! drops (on any exit path), it pops the stack entry, computes elapsed time,
//! propagates children time to the parent, and merges into per-function
//! aggregates (`FnAgg`) via linear scan.
//!
//! `collect()` returns pre-aggregated per-function summaries sorted by
//! self-time descending. `reset()` clears all state for the current thread.
//!
//! Flush strategy: when streaming is enabled (init() called), each completed
//! frame (depth-0 guard drop) is written directly to disk as one NDJSON line.
//! The function name table is written as a trailer at shutdown.
//!
//! When streaming is not enabled (tests, no init()), frames buffer in
//! `Arc<Mutex<Vec<Vec<FrameFnSummary>>>>` registered in `THREAD_FRAMES`,
//! and `shutdown()` writes them in bulk (legacy path).
//!
//! Per-function aggregates live in `Arc<Mutex<Vec<FnAgg>>>` registered in
//! a global `THREAD_RECORDS` Vec. `shutdown()` iterates all Arcs to collect
//! data from every thread, including thread-pool workers whose TLS
//! destructors may never fire. Always writes NDJSON.
//!
//! Thread-locality: stack and records are thread-local. Each thread produces an
//! independent call tree by default. For cross-thread attribution (e.g. rayon
//! scopes, spawned threads), use `fork()` / `adopt()` to propagate timing context
//! so that child thread elapsed time is correctly subtracted from the parent's
//! self-time. `SpanContext` auto-finalizes on Drop.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) mod fork_adopt;
pub(crate) mod name_table;
pub(crate) mod ndjson;
mod signal;

// Re-export submodule items for crate-level access
pub use fork_adopt::{adopt, fork, AdoptGuard, SpanContext};
use name_table::{intern_name, pack_name_depth, unpack_depth};
pub(crate) use name_table::{lookup_name, unpack_name_id};
use ndjson::{flush_impl, stream_frame, write_ndjson, write_stream_trailer, StreamState};

/// Thread-safe, initialize-once cell using `Once` + `UnsafeCell`.
///
/// Equivalent to `OnceLock` (stabilized in 1.70) but works on Rust 1.59+.
/// The `Once` primitive guarantees single-writer semantics; after initialization
/// the value is read-only, so there are no data races.
pub(crate) struct SyncOnceCell<T> {
    once: Once,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: `Once` synchronizes initialization. After `call_once` completes,
// the inner value is only read, never mutated, so `Sync` is sound.
unsafe impl<T: Send + Sync> Sync for SyncOnceCell<T> {}

impl<T> SyncOnceCell<T> {
    pub(crate) const fn new() -> Self {
        Self {
            once: Once::new(),
            value: UnsafeCell::new(None),
        }
    }

    pub(crate) fn get_or_init(&self, f: impl FnOnce() -> T) -> &T {
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
/// All threads within a single process share this ID, so that NDJSON files
/// can be correlated during report consolidation.
static RUN_ID: SyncOnceCell<String> = SyncOnceCell::new();

fn run_id() -> &'static str {
    RUN_ID.get_or_init(|| format!("{}_{}", std::process::id(), timestamp_ms()))
}

/// Process-start epoch for relative timestamps.
static EPOCH: SyncOnceCell<Instant> = SyncOnceCell::new();

/// Explicit runs directory set by the CLI via `set_runs_dir()`.
///
/// When set, `runs_dir()` returns this path instead of the env-var /
/// home-directory fallback. This ensures `flush()` and `shutdown()` write
/// to the same project-local directory that `shutdown_to()` uses.
///
/// Uses `SyncOnceCell<Mutex<_>>` instead of `static Mutex` because
/// `Mutex::new` is not `const fn` on Rust < 1.63 (runtime MSRV is 1.59).
///
/// TESTING: Process-global. Tests that call `set_runs_dir()` or
/// `clear_runs_dir()` must use `#[serial]` to prevent concurrent mutation.
/// Prefer `flush_to(dir)` or `shutdown_impl_inner(dir)` when you only need
/// to direct output to a specific directory.
static RUNS_DIR: SyncOnceCell<Mutex<Option<PathBuf>>> = SyncOnceCell::new();

fn runs_dir_lock() -> &'static Mutex<Option<PathBuf>> {
    RUNS_DIR.get_or_init(|| Mutex::new(None))
}

/// Guards against double-shutdown (e.g. signal handler + normal exit).
///
/// `shutdown_impl_inner` is a no-op once this flag is set.
static SHUTDOWN_DONE: AtomicBool = AtomicBool::new(false);

/// Whether frame streaming to disk is active.
///
/// Set by `init()`. When true, depth-0 frame boundaries write directly to
/// disk via STREAM_FILE instead of buffering in the FRAMES thread-local.
/// When false (tests, no init() call), FRAMES is used as before.
///
/// TESTING: Process-global. Tests that store to this flag must use
/// `#[serial]` -- any concurrent guard drop will take the streaming path
/// and write to STREAM_FILE instead of buffering in FRAMES.
static STREAMING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether per-frame data should be streamed to disk.
///
/// When false (default), depth-0 frame boundaries skip `stream_frame()`,
/// and shutdown writes a single aggregate frame via `synthesize_frame_from_agg`.
/// Set to true by `init()` when `PIANO_STREAM_FRAMES=1` is in the environment
/// (i.e., `piano profile --frames` was used).
static STREAM_FRAMES: AtomicBool = AtomicBool::new(false);

/// Global streaming file handle, lazily opened on first frame.
///
/// All threads serialize frame writes through this single mutex. For typical
/// profiling workloads (tens to hundreds of frame boundaries per second),
/// contention is negligible. For rayon micro-benchmarks with thousands of
/// short iterations per second, consider the overhead.
///
/// TESTING: Process-global. Tests that open or close the stream file must
/// use `#[serial]` to avoid corrupting concurrent tests' output.
static STREAM_FILE: SyncOnceCell<Mutex<Option<StreamState>>> = SyncOnceCell::new();

fn stream_file() -> &'static Mutex<Option<StreamState>> {
    STREAM_FILE.get_or_init(|| Mutex::new(None))
}

fn epoch() -> Instant {
    *EPOCH.get_or_init(|| {
        crate::tsc::calibrate();
        crate::tsc::calibrate_bias();
        #[cfg(feature = "cpu-time")]
        crate::cpu_clock::calibrate_bias();
        let tsc_val = crate::tsc::read();
        let now = Instant::now();
        crate::tsc::set_epoch_tsc(tsc_val);
        now
    })
}

/// Calibrate guard instrumentation overhead. Called once from enter_cold
/// after epoch is initialized. Uses AtomicBool try-set to avoid deadlock
/// (Once would deadlock on re-entrant calls from the calibration loop).
#[cfg(feature = "cpu-time")]
fn calibrate_guard_cost_once() {
    static DONE: AtomicBool = AtomicBool::new(false);
    if DONE.load(Ordering::Relaxed) {
        return;
    }
    if DONE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    calibrate_guard_cost();
}

/// Measure the total CPU cost of one enter()/drop() cycle with empty body,
/// then compute guard_overhead = guard_cost - bias.
#[cfg(feature = "cpu-time")]
fn calibrate_guard_cost() {
    const N: usize = 10_000;
    let start = crate::cpu_clock::cpu_now_ns();
    for _ in 0..N {
        let _g = enter("__piano_cal__");
    }
    let end = crate::cpu_clock::cpu_now_ns();
    let guard_cost = (end - start) as f64 / N as f64;
    let overhead = (guard_cost - crate::cpu_clock::bias_f64()).max(0.0);
    crate::cpu_clock::store_guard_overhead(overhead);

    // Clean up calibration records from RECORDS_BUF to avoid polluting output.
    RECORDS_BUF.with(|buf| {
        buf.borrow_mut().retain(|e| e.name != "__piano_cal__");
    });
    // Clean up invocation records in test builds.
    #[cfg(any(test, feature = "_test_internals"))]
    INVOCATIONS.with(|inv| inv.borrow_mut().clear());
}

/// Aggregated timing data for a single function.
#[derive(Debug, Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
pub struct FrameFnSummary {
    pub name: &'static str,
    pub calls: u64,
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
#[non_exhaustive]
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
#[derive(Clone, Copy)]
pub(crate) struct StackEntry {
    /// TSC value captured during enter(). Stored here (in addition to Guard)
    /// so TlsFlushGuard::drop can recover timing for in-flight functions
    /// whose Guards never dropped (process::exit).
    pub(crate) start_tsc: u64,
    pub(crate) children_ns: u64,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_children_ns: u64,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_start_ns: u64,
    /// Saved ALLOC_COUNTERS from before this scope, restored on Guard::drop.
    pub(crate) saved_alloc: crate::alloc::AllocSnapshot,
    /// Packed identity: `[unused:32][name_id:16][depth:16]`.
    pub(crate) packed: u64,
}

/// In-flight per-function aggregate. Replaces per-invocation storage to bound
/// memory growth: instead of storing one record per invocation, we merge into
/// one entry per unique function name via linear scan with pointer identity.
#[derive(Clone)]
struct FnAgg {
    name: &'static str,
    calls: u64,
    total_ms: f64,
    self_ms: f64,
    #[cfg(feature = "cpu-time")]
    cpu_self_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
}

impl FnAgg {
    /// Merge another aggregate's fields into self.
    fn absorb(&mut self, other: &FnAgg) {
        self.calls += other.calls;
        self.total_ms += other.total_ms;
        self.self_ms += other.self_ms;
        #[cfg(feature = "cpu-time")]
        {
            self.cpu_self_ns += other.cpu_self_ns;
        }
        self.alloc_count += other.alloc_count;
        self.alloc_bytes += other.alloc_bytes;
        self.free_count += other.free_count;
        self.free_bytes += other.free_bytes;
    }
}

type ThreadRecordArc = Arc<Mutex<Vec<FnAgg>>>;
type ThreadFrameArc = Arc<Mutex<Vec<Vec<FrameFnSummary>>>>;

/// Global registry of per-thread record storage.
/// Each thread registers its Arc on first access. collect_all() iterates all Arcs.
static THREAD_RECORDS: SyncOnceCell<Mutex<Vec<ThreadRecordArc>>> = SyncOnceCell::new();

fn thread_records() -> &'static Mutex<Vec<ThreadRecordArc>> {
    THREAD_RECORDS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Global registry of per-thread frame storage.
/// Each thread registers its Arc on first depth-0 frame push. collect_frames() iterates all Arcs.
static THREAD_FRAMES: SyncOnceCell<Mutex<Vec<(usize, ThreadFrameArc)>>> = SyncOnceCell::new();

fn thread_frames() -> &'static Mutex<Vec<(usize, ThreadFrameArc)>> {
    THREAD_FRAMES.get_or_init(|| Mutex::new(Vec::new()))
}

/// Monotonic counter assigning a unique index to each thread that registers
/// with the profiling runtime. Thread 0 is the first thread to access TLS.
static NEXT_THREAD_INDEX: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    pub(crate) static STACK: UnsafeCell<Vec<StackEntry>> = UnsafeCell::new(Vec::new());
    /// Debug-only borrow counter to detect reentrant STACK access (replaces
    /// RefCell's runtime borrow checking at zero cost in release builds).
    /// Positive = number of shared borrows, -1 = exclusive borrow.
    #[cfg(debug_assertions)]
    static STACK_BORROW_COUNT: Cell<isize> = Cell::new(0);
}

/// Access STACK with an exclusive (&mut) borrow, with debug-mode reentrance detection.
///
/// In debug builds, asserts that no other borrow is active (same guarantee RefCell
/// provides, at zero cost in release). In release builds, compiles to a plain
/// UnsafeCell::get dereference.
///
/// SAFETY contract: the closure `f` must not call any function that accesses STACK
/// (enter, drop_cold, fork, adopt, reset, or PianoFuture::poll). All current call
/// sites satisfy this — they do local Vec operations and return.
#[inline(always)]
pub(crate) fn with_stack_mut<R>(f: impl FnOnce(&mut Vec<StackEntry>) -> R) -> R {
    STACK.with(|stack| {
        #[cfg(debug_assertions)]
        {
            STACK_BORROW_COUNT.with(|c| {
                assert!(
                    c.get() == 0,
                    "piano-runtime: reentrant STACK borrow detected (bug)"
                );
                c.set(-1);
            });
        }
        let r = f(unsafe { &mut *stack.get() });
        #[cfg(debug_assertions)]
        {
            STACK_BORROW_COUNT.with(|c| c.set(0));
        }
        r
    })
}

/// Access STACK with a shared (&) borrow, with debug-mode reentrance detection.
#[inline(always)]
pub(crate) fn with_stack_ref<R>(f: impl FnOnce(&Vec<StackEntry>) -> R) -> R {
    STACK.with(|stack| {
        #[cfg(debug_assertions)]
        {
            STACK_BORROW_COUNT.with(|c| {
                assert!(
                    c.get() >= 0,
                    "piano-runtime: reentrant STACK borrow detected (bug)"
                );
                c.set(c.get() + 1);
            });
        }
        let r = f(unsafe { &*stack.get() });
        #[cfg(debug_assertions)]
        {
            STACK_BORROW_COUNT.with(|c| c.set(c.get() - 1));
        }
        r
    })
}

thread_local! {
    /// Sequential thread index for this thread, assigned on first TLS access.
    static THREAD_INDEX: Cell<usize> = Cell::new(
        NEXT_THREAD_INDEX.fetch_add(1, Ordering::Relaxed)
    );
    static RECORDS: Arc<Mutex<Vec<FnAgg>>> = {
        let arc = Arc::new(Mutex::new(Vec::new()));
        thread_records().lock().unwrap_or_else(|e| e.into_inner()).push(Arc::clone(&arc));
        arc
    };
    static REGISTERED: RefCell<Vec<&'static str>> = RefCell::new(Vec::new());
    #[cfg(any(test, feature = "_test_internals"))]
    static INVOCATIONS: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    /// Per-function summaries accumulated within the current frame (cleared on frame boundary).
    static FRAME_BUFFER: RefCell<Vec<FrameFnSummary>> = RefCell::new(Vec::new());
    /// Completed per-frame summaries. Arc-wrapped and registered globally so
    /// collect_frames() can iterate all threads, same pattern as RECORDS/THREAD_RECORDS.
    static FRAMES: Arc<Mutex<Vec<Vec<FrameFnSummary>>>> = {
        let arc = Arc::new(Mutex::new(Vec::new()));
        let tid = THREAD_INDEX.with(|c| c.get());
        thread_frames().lock().unwrap_or_else(|e| e.into_inner()).push((tid, Arc::clone(&arc)));
        arc
    };
    /// Fast local buffer for FnAgg entries. Avoids the Mutex lock on RECORDS
    /// for every drop_cold call. Flushed to RECORDS at depth-0 boundaries.
    static RECORDS_BUF: RefCell<Vec<FnAgg>> = RefCell::new(Vec::new());
}

/// Sentinel whose Drop drains thread-local buffers into their Arc-backed
/// global registries. On platforms where TLS destructors run before atexit
/// handlers (glibc >= 2.18, macOS), this preserves buffered data that the
/// atexit handler can then collect from the global registries.
///
/// Also drains in-flight STACK entries (functions whose Guards never dropped
/// due to process::exit) into RECORDS_BUF and FRAME_BUFFER before the
/// buffer-to-Arc drain, recovering timing data for those functions.
///
/// Destruction order: the initializer force-touches STACK, RECORDS_BUF,
/// RECORDS, FRAME_BUFFER, and FRAMES so their destructors are registered
/// first. Reverse-order destruction means this guard is destroyed first,
/// while all dependencies are still alive.
struct TlsFlushGuard;

/// Drain in-flight STACK entries into RECORDS_BUF and FRAME_BUFFER, then
/// flush both buffers into their Arc-backed global registries.
///
/// Called from `shutdown_impl_inner()` to recover timing data when guards
/// haven't dropped (SIGTERM / process::exit), and from `TlsFlushGuard::drop`
/// during TLS destruction. Idempotent: clears STACK after draining, so a
/// second call is a no-op.
///
/// Uses `try_with` (not `with_stack_mut`) because this may run during TLS
/// destruction when `STACK_BORROW_COUNT` is already destroyed.
fn drain_inflight_stack() {
    // Drain in-flight STACK entries for functions whose Guards never
    // dropped (process::exit). Read current TSC as the end time.
    // Must happen before RECORDS_BUF/FRAME_BUFFER drains below,
    // because we merge into those buffers here.
    // NOTE: Cannot use with_stack_mut here — during TLS destruction,
    // STACK_BORROW_COUNT (also TLS) may already be destroyed.
    let _ = STACK.try_with(|stack| {
        // SAFETY: When called from TlsFlushGuard::drop, TLS destruction is
        // single-threaded so no concurrent borrows are possible. When called
        // from a signal handler, this may alias a &mut from an interrupted
        // enter()/drop_cold() — accepted per signal.rs rationale (best-effort
        // recovery over data loss).
        let s = unsafe { &mut *stack.get() };
        if s.is_empty() {
            return;
        }
        let end_tsc = crate::tsc::read();
        #[cfg(feature = "cpu-time")]
        let cpu_end_ns = crate::cpu_clock::cpu_now_ns();

        // Read current ALLOC_COUNTERS for the innermost scope's alloc delta.
        // Cell<AllocSnapshot> has no destructor, so TLS is still accessible.
        let current_alloc = crate::alloc::ALLOC_COUNTERS
            .try_with(|cell| cell.get())
            .unwrap_or_default();

        // Process all in-flight entries. Skip entries with start_tsc == 0
        // (synthetic adopt anchors that are not real timed functions).
        // Note: forward-order iteration does not propagate elapsed_ns to parent
        // children_ns, so self_ns for outer in-flight entries may be overstated
        // when multiple nested functions are in-flight. This is acceptable for
        // best-effort recovery.
        //
        // Alloc deltas: enter() saves counters into saved_alloc and zeroes them,
        // so entry[i]'s accumulated allocs = entry[i+1].saved_alloc (what the
        // child saw as the parent's accumulation), or current_alloc for the
        // innermost entry. StackEntry is Copy, so index-based access works
        // without allocation.
        let len = s.len();
        for i in 0..len {
            let entry = s[i];
            if entry.start_tsc == 0 {
                continue;
            }
            let name = lookup_name(unpack_name_id(entry.packed));
            let raw_ticks = end_tsc.wrapping_sub(entry.start_tsc);
            let corrected_ticks = raw_ticks.saturating_sub(crate::tsc::bias_ticks());
            let elapsed_ns = crate::tsc::ticks_to_ns(corrected_ticks);
            let children_ns = entry.children_ns;
            let self_ns = elapsed_ns.saturating_sub(children_ns);

            // cpu_start_ns == 0 means enter() never completed (process::exit
            // between enter_cold and the cpu_start_ns patch). Report 0.
            // Raw CPU elapsed: no per-call bias subtraction. Amortized correction
            // is applied at aggregation (raw_total - calls * bias), matching drop_cold.
            #[cfg(feature = "cpu-time")]
            let cpu_raw_elapsed = if entry.cpu_start_ns == 0 {
                0
            } else {
                cpu_end_ns.saturating_sub(entry.cpu_start_ns)
            };
            #[cfg(feature = "cpu-time")]
            let cpu_self_ns = cpu_raw_elapsed.saturating_sub(entry.cpu_children_ns);

            let scope_alloc = if i + 1 < len {
                s[i + 1].saved_alloc
            } else {
                current_alloc
            };

            let _ = RECORDS_BUF.try_with(|buf| {
                merge_into_fnagg_vec(
                    &mut buf.borrow_mut(),
                    name,
                    elapsed_ns,
                    children_ns,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns,
                    scope_alloc.alloc_count,
                    scope_alloc.alloc_bytes,
                    scope_alloc.free_count,
                    scope_alloc.free_bytes,
                );
            });

            let _ = FRAME_BUFFER.try_with(|buf| {
                merge_into_frame_buf(
                    &mut buf.borrow_mut(),
                    name,
                    self_ns,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns,
                    scope_alloc.alloc_count,
                    scope_alloc.alloc_bytes,
                    scope_alloc.free_count,
                    scope_alloc.free_bytes,
                );
            });
        }
        s.clear();
    });
    // Drain RECORDS_BUF -> RECORDS Arc (survives via THREAD_RECORDS).
    let _ = RECORDS_BUF.try_with(|buf| {
        let mut buf = buf.borrow_mut();
        if buf.is_empty() {
            return;
        }
        let _ = RECORDS.try_with(|records| {
            let mut recs = records.lock().unwrap_or_else(|e| e.into_inner());
            for local in buf.drain(..) {
                if let Some(entry) = recs.iter_mut().find(|e| std::ptr::eq(e.name, local.name)) {
                    entry.absorb(&local);
                } else {
                    recs.push(local);
                }
            }
        });
    });
    // Drain FRAME_BUFFER -> FRAMES Arc (survives via THREAD_FRAMES).
    let _ = FRAME_BUFFER.try_with(|buf| {
        let mut b = buf.borrow_mut();
        if b.is_empty() {
            return;
        }
        let _ = FRAMES.try_with(|frames| {
            frames
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(std::mem::take(&mut *b));
        });
    });
}

impl Drop for TlsFlushGuard {
    // Skipped by cargo-mutants (see mutants.toml). Only exercised during TLS
    // destruction via process::exit(); tested by tests/process_exit.rs at the
    // workspace level, but not reachable from piano-runtime unit tests because
    // process::exit() would kill the test runner.
    fn drop(&mut self) {
        drain_inflight_stack();
    }
}

thread_local! {
    /// Initialized after dependency TLS to guarantee it is destroyed first.
    /// See `TlsFlushGuard` doc comment for the destruction order contract.
    static TLS_FLUSH_GUARD: TlsFlushGuard = {
        // Force-init dependencies so their destructors register before ours.
        // STACK must be first: it is read during drop to recover in-flight entries.
        STACK.with(|_| ());
        RECORDS_BUF.with(|_| ());
        RECORDS.with(|_| ());
        FRAME_BUFFER.with(|_| ());
        FRAMES.with(|_| ());
        TlsFlushGuard
    };
}

/// Merge a single invocation into a Vec<FnAgg> via linear scan on interned name pointer.
#[allow(clippy::too_many_arguments)]
fn merge_into_fnagg_vec(
    buf: &mut Vec<FnAgg>,
    name: &'static str,
    elapsed_ns: u64,
    children_ns: u64,
    #[cfg(feature = "cpu-time")] cpu_self_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
) {
    let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
    let self_ms = elapsed_ns.saturating_sub(children_ns) as f64 / 1_000_000.0;
    if let Some(entry) = buf.iter_mut().find(|e| std::ptr::eq(e.name, name)) {
        entry.calls += 1;
        entry.total_ms += elapsed_ms;
        entry.self_ms += self_ms;
        #[cfg(feature = "cpu-time")]
        {
            entry.cpu_self_ns += cpu_self_ns;
        }
        entry.alloc_count += alloc_count;
        entry.alloc_bytes += alloc_bytes;
        entry.free_count += free_count;
        entry.free_bytes += free_bytes;
    } else {
        buf.push(FnAgg {
            name,
            calls: 1,
            total_ms: elapsed_ms,
            self_ms,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns,
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        });
    }
}

/// Merge a single invocation into a Vec<FrameFnSummary> via linear scan on interned name pointer.
#[allow(clippy::too_many_arguments)]
fn merge_into_frame_buf(
    buf: &mut Vec<FrameFnSummary>,
    name: &'static str,
    self_ns: u64,
    #[cfg(feature = "cpu-time")] cpu_self_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
) {
    if let Some(entry) = buf.iter_mut().find(|e| std::ptr::eq(e.name, name)) {
        entry.calls += 1;
        entry.self_ns += self_ns;
        #[cfg(feature = "cpu-time")]
        {
            entry.cpu_self_ns += cpu_self_ns;
        }
        entry.alloc_count += alloc_count;
        entry.alloc_bytes += alloc_bytes;
        entry.free_count += free_count;
        entry.free_bytes += free_bytes;
    } else {
        buf.push(FrameFnSummary {
            name,
            calls: 1,
            self_ns,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns,
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        });
    }
}

/// Drain RECORDS_BUF into the Mutex-guarded RECORDS.
/// Called at depth-0 boundaries and before reading RECORDS.
fn flush_records_buf() {
    // try_with: if TLS is already destroyed (process::exit on glibc/macOS),
    // skip gracefully. The TlsFlushGuard already drained buffers into the
    // Arc-backed global registries during TLS destruction.
    let _ = RECORDS_BUF.try_with(|buf| {
        let mut buf = buf.borrow_mut();
        if buf.is_empty() {
            return;
        }
        let _ = RECORDS.try_with(|records| {
            let mut recs = records.lock().unwrap_or_else(|e| e.into_inner());
            for local in buf.drain(..) {
                if let Some(entry) = recs.iter_mut().find(|e| std::ptr::eq(e.name, local.name)) {
                    entry.absorb(&local);
                } else {
                    recs.push(local);
                }
            }
        });
    });
}

/// RAII timing guard. Records elapsed time on drop.
///
/// 8 bytes: fits in one register on both x86_64 (rax) and
/// aarch64 (x0), eliminating all memory stores from the
/// measurement window.
///
/// Uses a raw hardware counter (`rdtsc` / `cntvct_el0`) instead of
/// `Instant::now()` to minimize clock-read cost. The tsc-to-nanosecond
/// conversion happens in `drop_cold`, outside the measurement window.
#[must_use = "dropping the guard immediately records ~0ms; bind it with `let _guard = ...`"]
#[non_exhaustive]
pub struct Guard {
    start_tsc: u64,
}

// Guard must be Send so async runtimes can move futures containing guards
// across worker threads.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<Guard>();
    }
    // Verify Guard is exactly 8 bytes (fits in 1 register).
    fn _assert_size() {
        let _ = core::mem::transmute::<Guard, [u8; 8]>;
    }
};

/// Bookkeeping half of Guard::drop(): alloc restore, stack pop,
/// recording. Kept out-of-line so the inlined drop is just a counter read + call.
#[inline(never)]
fn drop_cold(guard: &Guard, end_tsc: u64, #[cfg(feature = "cpu-time")] cpu_end_ns: u64) {
    // Wrap in ALLOC_COUNTERS.try_with so we hold the Cell reference across
    // all infra operations. Read scope allocs FIRST, do all work (merge,
    // flush, stream), then restore parent counters LAST — overwriting any
    // infra allocs that accumulated in the cell.
    let _ = crate::alloc::ALLOC_COUNTERS.try_with(|alloc_cell| {
        let scope_alloc = alloc_cell.get();

        with_stack_mut(|s| {
            let entry = match s.pop() {
                Some(e) => e,
                None => {
                    eprintln!("piano-runtime: guard dropped without matching stack entry (bug)");
                    return;
                }
            };

            let name = lookup_name(unpack_name_id(entry.packed));

            let raw_ticks = end_tsc.wrapping_sub(guard.start_tsc);
            let corrected_ticks = raw_ticks.saturating_sub(crate::tsc::bias_ticks());
            let elapsed_ns = crate::tsc::ticks_to_ns(corrected_ticks);
            let children_ns = entry.children_ns;
            let self_ns = elapsed_ns.saturating_sub(children_ns);

            // Raw CPU elapsed: no per-call bias subtraction. Amortized correction
            // is applied at aggregation (raw_total - calls * bias), allowing
            // positive/negative quantization noise to cancel for sub-ns precision.
            #[cfg(feature = "cpu-time")]
            let cpu_raw_elapsed = cpu_end_ns.saturating_sub(entry.cpu_start_ns);
            #[cfg(feature = "cpu-time")]
            let cpu_self_ns = cpu_raw_elapsed.saturating_sub(entry.cpu_children_ns);

            if let Some(parent) = s.last_mut() {
                parent.children_ns += elapsed_ns;
                #[cfg(feature = "cpu-time")]
                {
                    // Add guard_overhead: the portion of per-call instrumentation
                    // cost that falls inside the parent's CPU bracket but outside
                    // the child's raw elapsed. Without this, N child calls inflate
                    // the parent's cpu_self by N * guard_overhead.
                    //
                    // Truncation to u64 loses ~0.5ns/call (<0.1% of typical ~50ns
                    // guard_overhead). Acceptable for now; aggregation-time f64
                    // correction would need a children_calls counter in FnAgg.
                    parent.cpu_children_ns +=
                        cpu_raw_elapsed + crate::cpu_clock::guard_overhead_ns();
                }
            }

            let _ = RECORDS_BUF.try_with(|buf| {
                merge_into_fnagg_vec(
                    &mut buf.borrow_mut(),
                    name,
                    elapsed_ns,
                    children_ns,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns,
                    scope_alloc.alloc_count,
                    scope_alloc.alloc_bytes,
                    scope_alloc.free_count,
                    scope_alloc.free_bytes,
                );
            });

            #[cfg(any(test, feature = "_test_internals"))]
            {
                let start_ns =
                    crate::tsc::ticks_to_epoch_ns(guard.start_tsc, crate::tsc::epoch_tsc());
                let _ = INVOCATIONS.try_with(|inv| {
                    inv.borrow_mut().push(InvocationRecord {
                        name,
                        start_ns,
                        elapsed_ns,
                        self_ns,
                        #[cfg(feature = "cpu-time")]
                        cpu_self_ns,
                        alloc_count: scope_alloc.alloc_count,
                        alloc_bytes: scope_alloc.alloc_bytes,
                        free_count: scope_alloc.free_count,
                        free_bytes: scope_alloc.free_bytes,
                        depth: unpack_depth(entry.packed),
                    });
                });
            }

            let _ = FRAME_BUFFER.try_with(|buf| {
                merge_into_frame_buf(
                    &mut buf.borrow_mut(),
                    name,
                    self_ns,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns,
                    scope_alloc.alloc_count,
                    scope_alloc.alloc_bytes,
                    scope_alloc.free_count,
                    scope_alloc.free_bytes,
                );
            });

            let remaining_all_base = s.iter().all(|e| unpack_depth(e.packed) == 0);
            let is_frame_boundary = unpack_depth(entry.packed) == 0 || remaining_all_base;

            if is_frame_boundary {
                flush_records_buf();
            }
            if unpack_depth(entry.packed) == 0 {
                let _ = FRAME_BUFFER.try_with(|buf| {
                    let mut b = buf.borrow_mut();
                    // Apply amortized CPU bias correction before streaming.
                    #[cfg(feature = "cpu-time")]
                    {
                        let bias = crate::cpu_clock::bias_f64();
                        for fe in b.iter_mut() {
                            let corrected = fe.cpu_self_ns as f64 - fe.calls as f64 * bias;
                            fe.cpu_self_ns = if corrected > 0.0 { corrected as u64 } else { 0 };
                        }
                    }
                    if !b.is_empty() {
                        stream_frame(&b);
                    }
                    b.clear();
                });
            }

            alloc_cell.set(entry.saved_alloc);
        });
    });
}

impl Drop for Guard {
    /// Inlined so the counter read (`rdtsc`/`cntvct_el0`) happens at the drop
    /// site as a single inline instruction.
    /// `compiler_fence` prevents the compiler from hoisting Guard field loads
    /// before the counter read.
    #[inline(always)]
    fn drop(&mut self) {
        let end_tsc = crate::tsc::read();
        #[cfg(feature = "cpu-time")]
        let cpu_end_ns = crate::cpu_clock::cpu_now_ns();

        // Prevent the compiler from moving Guard field reads (needed by
        // drop_cold) before the counter read above.
        compiler_fence(Ordering::SeqCst);

        drop_cold(
            self,
            end_tsc,
            #[cfg(feature = "cpu-time")]
            cpu_end_ns,
        );
    }
}

/// Bookkeeping half of enter(): epoch, alloc save, stack push, name interning.
#[inline(never)]
fn enter_cold(name: &'static str) {
    let _ = epoch();

    #[cfg(feature = "cpu-time")]
    calibrate_guard_cost_once();

    // Wrap in ALLOC_COUNTERS.try_with so we hold the Cell reference across
    // all infra operations. Snapshot parent state FIRST, then do infra work
    // (TLS init, interning, Vec push), then zero the counters — discarding
    // any infra allocs that accumulated in the cell.
    let _ = crate::alloc::ALLOC_COUNTERS.try_with(|alloc_cell| {
        let saved_alloc = alloc_cell.get();

        TLS_FLUSH_GUARD.with(|_| ());

        let name_id = intern_name(name);

        // cpu_start_ns is set to 0 here (placeholder). The real value is
        // captured in enter() after all bookkeeping, right at the function
        // body boundary.
        with_stack_mut(|s| {
            let depth = s.len() as u16;
            let packed = pack_name_depth(name_id, depth);
            s.push(StackEntry {
                start_tsc: 0,
                children_ns: 0,
                #[cfg(feature = "cpu-time")]
                cpu_children_ns: 0,
                #[cfg(feature = "cpu-time")]
                cpu_start_ns: 0,
                saved_alloc,
                packed,
            });
        });

        alloc_cell.set(crate::alloc::AllocSnapshot::new());
    });
}

/// Start timing a function. Returns a Guard that records the measurement on drop.
///
/// Inlined so the counter read (`rdtsc`/`cntvct_el0`) happens at the call
/// site as a single inline instruction — no function call, no vDSO overhead.
///
/// Guard is 8 bytes: fits in one register, zero memory stores inside the
/// measurement window. cpu_start_ns is captured as the last thing before
/// returning, right at the function body boundary, minimizing
/// instrumentation overhead inside the CPU time bracket.
#[inline(always)]
pub fn enter(name: &'static str) -> Guard {
    enter_cold(name);
    let start_tsc = crate::tsc::read();
    // Capture cpu_start_ns AFTER all bookkeeping (enter_cold, tsc::read).
    // This is the tightest possible placement — right at the body boundary.
    #[cfg(feature = "cpu-time")]
    let cpu_start_ns = crate::cpu_clock::cpu_now_ns();
    // Write start_tsc (and cpu_start_ns) to StackEntry for process::exit
    // recovery and PianoFuture save/restore.
    with_stack_mut(|s| {
        if let Some(entry) = s.last_mut() {
            entry.start_tsc = start_tsc;
            #[cfg(feature = "cpu-time")]
            {
                entry.cpu_start_ns = cpu_start_ns;
            }
        }
    });
    Guard { start_tsc }
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
        if !reg.iter().any(|&r| std::ptr::eq(r, name)) {
            reg.push(name);
        }
    });
}

/// Convert pre-aggregated FnAgg entries into FunctionRecord output, adding
/// zero-entry placeholders for registered names not yet present.
/// Result is sorted by self_ms descending.
fn aggregate(agg: &[FnAgg], registered: &[&str]) -> Vec<FunctionRecord> {
    let mut result: Vec<FunctionRecord> = agg
        .iter()
        .map(|e| FunctionRecord {
            name: e.name.to_owned(),
            calls: e.calls,
            total_ms: e.total_ms,
            self_ms: e.self_ms,
            #[cfg(feature = "cpu-time")]
            cpu_self_ms: {
                // Amortized bias correction: raw_total - calls * bias.
                // Positive/negative quantization noise cancels over many
                // calls, achieving sub-ns mean precision. Clamped to 0.0
                // for single-call fast functions where raw < bias.
                let raw = e.cpu_self_ns as f64;
                let corrected = raw - e.calls as f64 * crate::cpu_clock::bias_f64();
                if corrected > 0.0 {
                    corrected / 1_000_000.0
                } else {
                    0.0
                }
            },
        })
        .collect();

    // Add zero-entry placeholders for registered names not yet seen.
    for &name in registered {
        if !agg.iter().any(|e| std::ptr::eq(e.name, name)) {
            result.push(FunctionRecord {
                name: name.to_owned(),
                calls: 0,
                total_ms: 0.0,
                self_ms: 0.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ms: 0.0,
            });
        }
    }

    result.sort_by(|a, b| {
        b.self_ms
            .partial_cmp(&a.self_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result
}

/// Return pre-aggregated per-function summaries sorted by self_ms descending.
/// Reads only from the current thread's record storage.
pub fn collect() -> Vec<FunctionRecord> {
    flush_records_buf();
    RECORDS.with(|records| {
        let recs = records.lock().unwrap_or_else(|e| e.into_inner());
        REGISTERED.with(|reg| aggregate(&recs, &reg.borrow()))
    })
}

/// Return all raw per-invocation records (not aggregated).
#[cfg(any(test, feature = "_test_internals"))]
pub fn collect_invocations() -> Vec<InvocationRecord> {
    INVOCATIONS.with(|inv| inv.borrow().clone())
}

/// Collect frame data from ALL threads via the global THREAD_FRAMES registry.
///
/// Iterates per-thread frame Arcs under the registry lock, collecting all
/// completed frames.
///
/// No `flush_records_buf()` call is needed here: FRAMES is populated at
/// depth-0 boundaries -- the same point where RECORDS_BUF is flushed into
/// RECORDS. By the time a frame appears in FRAMES, its records have already
/// been flushed.
/// Collect frame data from ALL threads, preserving thread identity.
pub fn collect_frames_with_tid() -> Vec<(usize, Vec<FrameFnSummary>)> {
    let registry = thread_frames().lock().unwrap_or_else(|e| e.into_inner());
    let mut all_frames = Vec::new();
    for (tid, arc) in registry.iter() {
        let frames = arc.lock().unwrap_or_else(|e| e.into_inner());
        for frame in frames.iter() {
            all_frames.push((*tid, frame.clone()));
        }
    }
    all_frames
}

/// Collect frame data from ALL threads (without thread identity).
///
/// Backward-compatible wrapper around `collect_frames_with_tid()`.
pub fn collect_frames() -> Vec<Vec<FrameFnSummary>> {
    collect_frames_with_tid()
        .into_iter()
        .map(|(_, frame)| frame)
        .collect()
}

/// Collect records from ALL threads via the global registry.
/// This is the primary collection method for cross-thread profiling — it
/// captures data from thread-pool workers whose TLS destructors may never fire.
///
/// Iterates per-thread records under the registry lock, merging FnAgg
/// entries in-place.
///
/// Note: `REGISTERED` (the set of known function names) is read from the
/// calling thread's TLS only. Function names registered on other threads
/// will not appear in the output unless they were also recorded via `enter()`.
/// In the normal flow the AST rewriter injects all `register()` calls into
/// `main()`, so calling `collect_all()` from `main()` (via `shutdown()`)
/// sees every registered name.
pub fn collect_all() -> Vec<FunctionRecord> {
    let merged = collect_all_fnagg();
    let registered: Vec<&str> = REGISTERED
        .try_with(|reg| reg.borrow().clone())
        .unwrap_or_default();
    aggregate(&merged, &registered)
}

/// Clear all collected timing data for the current thread.
pub fn reset() {
    with_stack_mut(|s| s.clear());
    RECORDS_BUF.with(|buf| buf.borrow_mut().clear());
    RECORDS.with(|records| {
        records.lock().unwrap_or_else(|e| e.into_inner()).clear();
    });
    REGISTERED.with(|reg| reg.borrow_mut().clear());
    #[cfg(any(test, feature = "_test_internals"))]
    INVOCATIONS.with(|inv| inv.borrow_mut().clear());
    FRAME_BUFFER.with(|buf| buf.borrow_mut().clear());
    FRAMES.with(|frames| frames.lock().unwrap_or_else(|e| e.into_inner()).clear());
    // NAME_TABLE and NAME_CACHE are intentionally NOT cleared here.
    // The intern table is append-only: IDs are stable process-wide and may be
    // cached in other threads' NAME_CACHE. Clearing either would invalidate
    // those cached IDs, causing stale or incorrect name lookups.
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
    // Clear every thread's frame Arc.
    let frame_arcs: Vec<ThreadFrameArc> = {
        let registry = thread_frames().lock().unwrap_or_else(|e| e.into_inner());
        registry.iter().map(|(_, arc)| arc.clone()).collect()
    };
    for arc in &frame_arcs {
        arc.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
    // Reset the thread index counter so new threads start from 0.
    NEXT_THREAD_INDEX.store(0, Ordering::Relaxed);
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

/// Configure the directory where run files should be written.
///
/// Called by CLI-injected code at the start of `main()` so that both
/// `flush()` and `shutdown()` write to the project-local directory.
/// `PIANO_RUNS_DIR` env var still takes priority (for testing and user
/// overrides).
pub fn set_runs_dir(dir: &Path) {
    *runs_dir_lock().lock().unwrap_or_else(|e| e.into_inner()) = Some(dir.to_path_buf());
}

/// Clear the configured runs directory.
///
/// Exposed for testing. Not part of the public API.
#[cfg(test)]
pub fn clear_runs_dir() {
    *runs_dir_lock().lock().unwrap_or_else(|e| e.into_inner()) = None;
}

/// Return the directory where run files should be written.
///
/// Priority: `PIANO_RUNS_DIR` env var > `set_runs_dir()` > `~/.piano/runs/`.
fn runs_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("PIANO_RUNS_DIR") {
        return Some(PathBuf::from(dir));
    }
    if let Some(dir) = runs_dir_lock()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
    {
        return Some(dir);
    }
    dirs_fallback().map(|home| home.join(".piano").join("runs"))
}

/// Non-blocking variant of `runs_dir()` for use in signal handlers.
///
/// Uses `try_lock()` instead of `lock()` to avoid deadlocking when the
/// signal fires while `set_runs_dir()` holds the mutex. Falls back to
/// the env var and home directory if the lock is contended.
fn runs_dir_nonblocking() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("PIANO_RUNS_DIR") {
        return Some(PathBuf::from(dir));
    }
    if let Some(dir) = runs_dir_lock().try_lock().ok().and_then(|g| g.clone()) {
        return Some(dir);
    }
    dirs_fallback().map(|home| home.join(".piano").join("runs"))
}

/// Best-effort home directory detection (no deps).
fn dirs_fallback() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// Flush collected timing data to disk in NDJSON format.
///
/// If frame data is present, writes one line per frame. If only aggregate
/// data exists (no frames), synthesizes a single frame from the aggregates.
///
/// Normally you don't need to call this — `shutdown()` flushes all threads
/// at the end of main. This function exists for explicit mid-program flushes
/// of the current thread's data.
///
/// Note: flush() clones other threads' frame data but only clears the local
/// thread. If shutdown() (or another flush()) runs later, other threads'
/// frames will be written again, producing duplicates. The normal single-
/// shutdown path is unaffected.
pub fn flush() {
    if STREAMING_ENABLED.load(Ordering::Relaxed) {
        // Streaming mode: frames are already on disk. Just sync.
        let mut state = stream_file().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut s) = *state {
            let _ = s.file.flush();
            let _ = s.file.get_ref().sync_all();
            drop(state);
            reset();
            return;
        }
        // No stream file -- fall through to the non-streaming path.
        // This can happen if STREAMING_ENABLED was set but no frames
        // completed yet (stream file is lazily opened on first frame).
        drop(state);
    }

    // Non-streaming path (existing behavior, unchanged)
    let dir = match runs_dir() {
        Some(d) => d,
        None => return,
    };

    flush_impl(&dir);
}

/// Write collected profiling data to the specified directory.
///
/// Like `flush()` but takes an explicit directory, bypassing the global
/// `runs_dir()` resolution. Used by tests to avoid races on the
/// process-global `RUNS_DIR` / `PIANO_RUNS_DIR` env var.
///
/// Streaming mode is intentionally not handled here -- test-only callers
/// never call `init()`, so streaming is never active.
#[cfg(test)]
pub fn flush_to(dir: &std::path::Path) {
    flush_impl(dir);
}

/// Returns true when the given env var result represents an opt-in ("1").
#[inline]
fn env_is_opt_in(val: Result<String, std::env::VarError>) -> bool {
    val.as_deref() == Ok("1")
}

/// Initialize the runtime: install handlers for data recovery.
///
/// Called at the start of instrumented main(). Registers:
/// - SIGTERM/SIGINT signal handlers (Unix only) for kill/Ctrl-C
/// - C-level atexit handler for `std::process::exit()` calls
pub fn init() {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    signal::install_handlers();
    signal::register_atexit();
    STREAMING_ENABLED.store(true, Ordering::Relaxed);
    if env_is_opt_in(std::env::var("PIANO_STREAM_FRAMES")) {
        STREAM_FRAMES.store(true, Ordering::Relaxed);
    }
}

/// Flush all collected timing data from ALL threads and write to disk.
///
/// Collects frames from all threads via the global THREAD_FRAMES registry,
/// and aggregate records via THREAD_RECORDS. Always writes NDJSON.
/// Injected at the end of main() by the AST rewriter.
///
/// Guarded by `SHUTDOWN_DONE` to prevent double-writes (e.g. if a signal
/// handler already flushed data before normal exit).
pub fn shutdown() {
    if SHUTDOWN_DONE.swap(true, Ordering::SeqCst) {
        return;
    }
    let dir = match runs_dir() {
        Some(d) => d,
        None => return,
    };
    if shutdown_impl_inner(&dir) {
        eprintln!("piano: profiling data could not be written (see errors above)");
        std::process::exit(70);
    }
}

/// Like `shutdown`, but writes run files to the specified directory.
///
/// Stores `dir` via `set_runs_dir` and delegates to `shutdown()`, so
/// directory resolution goes through a single code path. `PIANO_RUNS_DIR`
/// env var still takes priority (checked inside `runs_dir()`).
pub fn shutdown_to(dir: &Path) {
    set_runs_dir(dir);
    shutdown();
}

/// Collect merged FnAgg records from all threads (raw aggregates, not FunctionRecord).
///
/// Shared core for `collect_all()` and `flush()` — flushes the calling thread's
/// buffer, then iterates per-thread records under the registry lock, merging
/// FnAgg entries via linear scan.
fn collect_all_fnagg() -> Vec<FnAgg> {
    flush_records_buf();
    let registry = thread_records().lock().unwrap_or_else(|e| e.into_inner());
    let mut merged: Vec<FnAgg> = Vec::new();
    for arc in registry.iter() {
        let records = arc.lock().unwrap_or_else(|e| e.into_inner());
        for entry in records.iter() {
            if let Some(dst) = merged.iter_mut().find(|e| std::ptr::eq(e.name, entry.name)) {
                dst.absorb(entry);
            } else {
                merged.push(entry.clone());
            }
        }
    }
    merged
}

/// Like `collect_all_fnagg` but without `flush_records_buf()`.
///
/// Used in shutdown phase 2 where all data is already in the RECORDS Arcs:
/// - Normal exit: depth-0 guard drops flushed RECORDS_BUF during drop_cold.
/// - Abnormal exit (process::exit / SIGTERM): drain_inflight_stack (phase 1)
///   merged STACK -> RECORDS_BUF -> RECORDS Arc.
///
/// Calling flush_records_buf here would be phase leakage -- reaching into
/// TLS during a globals-only phase.
fn collect_all_fnagg_no_flush() -> Vec<FnAgg> {
    let registry = thread_records().lock().unwrap_or_else(|e| e.into_inner());
    let mut merged: Vec<FnAgg> = Vec::new();
    for arc in registry.iter() {
        let records = arc.lock().unwrap_or_else(|e| e.into_inner());
        for entry in records.iter() {
            if let Some(dst) = merged.iter_mut().find(|e| std::ptr::eq(e.name, entry.name)) {
                dst.absorb(entry);
            } else {
                merged.push(entry.clone());
            }
        }
    }
    merged
}

/// Synthesize a single NDJSON frame from aggregate FnAgg data.
/// Used as fallback when no depth-0 frames were recorded (e.g., program crashed
/// mid-function, or all work happened in contexts that don't produce frames).
fn synthesize_frame_from_agg(agg: &[FnAgg]) -> Vec<FrameFnSummary> {
    agg.iter()
        .map(|e| FrameFnSummary {
            name: e.name,
            calls: e.calls,
            self_ns: (e.self_ms * 1_000_000.0).max(0.0) as u64,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: e.cpu_self_ns,
            alloc_count: e.alloc_count,
            alloc_bytes: e.alloc_bytes,
            free_count: e.free_count,
            free_bytes: e.free_bytes,
        })
        .collect()
}

/// Core write logic shared by `shutdown()` and the signal handler.
///
/// Three strict phases with no leakage between them:
///   Phase 1 -- Drain: TLS -> globals (drain_inflight_stack)
///   Phase 2 -- Collect: read globals only, returns values
///   Phase 3 -- Write: data-driven decisions, no mode flags
///
/// Returns `true` if any write failed. Does NOT check `SHUTDOWN_DONE` --
/// callers are responsible for the guard.
fn shutdown_impl_inner(dir: &std::path::Path) -> bool {
    // Phase 1: Drain (impure, TLS -> globals).
    // Moves STACK -> RECORDS_BUF -> RECORDS Arc,
    // and FRAME_BUFFER -> FRAMES Arc.
    // After this, TLS is irrelevant.
    drain_inflight_stack();

    // Phase 2: Collect (globals-only, returns values).
    // No flush_records_buf -- drain already emptied RECORDS_BUF.
    // No mode flags consulted.
    let frames = collect_frames_with_tid();
    let records = collect_all_fnagg_no_flush();

    // Phase 3: Write (data-driven decisions).
    let mut write_failed = false;

    // Stream file: append remaining frames + trailer if handle exists.
    // This is a file-handle fact, not a mode-flag check.
    {
        let mut state = stream_file().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut s) = *state {
            ndjson::write_shutdown_frames(s, &frames);
            if let Err(e) = write_stream_trailer(s) {
                eprintln!(
                    "piano: failed to write trailer to {}: {e}",
                    s.path.display()
                );
                write_failed = true;
            }
            // Close the stream file.
            *state = None;
            // Stream file is the artifact. Done.
            return write_failed;
        }
    }

    // No stream file: write fresh NDJSON from collected data.
    // Frames first, fall back to synthesized from records.
    let output_frames = if !frames.is_empty() {
        frames
    } else if !records.is_empty() {
        vec![(0, synthesize_frame_from_agg(&records))]
    } else {
        return write_failed;
    };

    let mut seen = HashSet::new();
    let mut fn_names: Vec<&str> = Vec::new();
    for (_, frame) in &output_frames {
        for s in frame {
            if seen.insert(s.name) {
                fn_names.push(s.name);
            }
        }
    }
    let path = dir.join(format!("{}.ndjson", timestamp_ms()));
    if let Err(e) = write_ndjson(&output_frames, &fn_names, &path) {
        eprintln!(
            "piano: failed to write profiling data to {}: {e}",
            path.display()
        );
        write_failed = true;
    }
    write_failed
}

/// CPU-bound workload for testing: hash a buffer `iterations` times.
/// Uses wrapping arithmetic to prevent optimization while staying deterministic.
#[cfg(test)]
pub(crate) fn burn_cpu(iterations: u64) {
    let mut buf = [0x42u8; 256];
    for i in 0..iterations {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[cfg(test)]
mod tests {
    use super::ndjson::open_stream_file;
    use super::*;
    use serial_test::serial;
    use std::thread;

    #[test]
    #[serial]
    fn flush_writes_valid_output_to_env_dir() {
        reset();
        {
            let _g = enter("flush_test");
            burn_cpu(5_000);
        }

        let tmp = std::env::temp_dir().join(format!("piano_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        flush_to(&tmp);

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
    #[serial]
    fn flush_to_writes_to_explicit_dir() {
        reset();
        {
            let _g = enter("flush_to_test");
            burn_cpu(5_000);
        }

        let tmp = std::env::temp_dir().join(format!("piano_flush_to_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        flush_to(&tmp);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "ndjson" || ext == "json")
            })
            .collect();
        assert!(!files.is_empty(), "flush_to should write output files");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("flush_to_test"),
            "should contain function name"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn init_can_be_called_multiple_times() {
        // Calling init() multiple times is safe (handler overwrites are idempotent).
        // We call the sub-components directly instead of init() to avoid
        // setting STREAMING_ENABLED, which would race with parallel tests.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            signal::install_handlers();
            signal::install_handlers();
            signal::install_handlers();
        }
        signal::register_atexit();
        signal::register_atexit();
        signal::register_atexit();
    }

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
    fn self_time_numerical_precision() {
        reset();
        {
            let _outer = enter("prec_outer");
            burn_cpu(5_000);
            {
                let _inner = enter("prec_inner");
                burn_cpu(20_000);
            }
            burn_cpu(5_000);
        }
        let records = collect();
        let outer = records.iter().find(|r| r.name == "prec_outer").unwrap();
        let inner = records.iter().find(|r| r.name == "prec_inner").unwrap();

        // Inner is a leaf: self ≈ total.
        assert!(
            (inner.self_ms - inner.total_ms).abs() < inner.total_ms * 0.15,
            "inner: self_ms={:.3} total_ms={:.3}",
            inner.self_ms,
            inner.total_ms,
        );

        // Precision check: outer.self_ms ≈ outer.total_ms - inner.total_ms
        // within 30% tolerance (accounts for TSC conversion and scheduling jitter).
        let expected_outer_self = outer.total_ms - inner.total_ms;
        let error = (outer.self_ms - expected_outer_self).abs();
        assert!(
            error < expected_outer_self * 0.30,
            "outer.self_ms ({:.3}) should be within 30% of (total_ms - inner.total_ms) = {:.3}, error = {:.3}",
            outer.self_ms, expected_outer_self, error,
        );
    }

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
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
        // Filter to our test's functions since collect() reads thread-local
        // RECORDS which may contain stale data if reset_all() ran mid-test.
        let records: Vec<_> = records
            .into_iter()
            .filter(|r| r.name == "fast" || r.name == "slow")
            .collect();
        assert_eq!(
            records.len(),
            2,
            "expected 2 records, got {}: {:?}",
            records.len(),
            records.iter().map(|r| &r.name).collect::<Vec<_>>()
        );
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
    #[serial]
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
    #[serial]
    fn output_contains_run_id() {
        reset();
        {
            let _g = enter("rid_test");
            burn_cpu(1_000);
        }
        let tmp = std::env::temp_dir().join(format!("piano_rid_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        flush_to(&tmp);
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
        // Regression test: merge_into_fnagg_vec clamps (elapsed - children).max(0.0).
        // Verify the clamp propagates through aggregate() into FunctionRecord.
        let agg = vec![FnAgg {
            name: "drifted",
            calls: 1,
            total_ms: 10.0,
            self_ms: 0.0, // already clamped by merge_into_fnagg_vec
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];
        let result = aggregate(&agg, &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].self_ms, 0.0,
            "negative self-time should be clamped to zero"
        );
    }

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
    fn write_ndjson_format() {
        reset();
        for _ in 0..2 {
            let _outer = enter("ndjson_update");
            burn_cpu(5_000);
            {
                let _inner = enter("ndjson_physics");
                burn_cpu(5_000);
            }
        }

        let tmp = std::env::temp_dir().join(format!("piano_ndjson_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        flush_to(&tmp);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        assert!(!files.is_empty(), "should write .ndjson file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // First line is v4 header (no functions — those are in the trailer)
        assert!(lines[0].contains("\"format_version\":4"));
        assert!(!lines[0].contains("\"functions\""));

        // header + 2 frames + trailer = 4 lines
        assert!(
            lines.len() >= 4,
            "header + 2 frames + trailer, got {}",
            lines.len()
        );
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));
        // Last line is trailer with function names
        assert!(lines.last().unwrap().contains("\"functions\""));

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn frame_boundary_aggregation() {
        reset();
        // Simulate 3 frames: depth-0 function called 3 times
        for _frame in 0..3u32 {
            let _outer = enter("fba_update");
            burn_cpu(5_000);
            {
                let _inner = enter("fba_physics");
                burn_cpu(5_000);
            }
        }
        let frames = collect_frames();
        // Filter to frames containing our test functions (collect_frames is now global).
        let my_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fba_update"))
            .collect();
        assert_eq!(my_frames.len(), 3, "should have 3 frames with 'fba_update'");
        for frame in &my_frames {
            let update = frame.iter().find(|s| s.name == "fba_update").unwrap();
            assert_eq!(update.calls, 1);
            let physics = frame.iter().find(|s| s.name == "fba_physics").unwrap();
            assert_eq!(physics.calls, 1);
        }
    }

    #[test]
    #[serial]
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
        // Each depth-0 return is a frame boundary, so we get 2 single-function frames.
        // Filter by our test functions since collect_frames is now global.
        let frames = collect_frames();
        let my_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "parse" || s.name == "resolve"))
            .collect();
        assert_eq!(my_frames.len(), 2, "each depth-0 return creates a frame");

        // Aggregate collect() should still work
        let records: Vec<_> = collect()
            .into_iter()
            .filter(|r| r.name == "parse" || r.name == "resolve")
            .collect();
        assert_eq!(records.len(), 2);
    }

    #[test]
    #[serial]
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
    #[serial]
    fn shutdown_writes_ndjson_with_all_thread_data() {
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

        // Use shutdown_impl_inner directly to avoid the SHUTDOWN_DONE
        // guard, which is process-global and unreliable in parallel tests.
        let tmp = std::env::temp_dir().join(format!("piano_shutdown_{}", timestamp_ms()));
        std::fs::create_dir_all(&tmp).unwrap();
        let _ = shutdown_impl_inner(&tmp);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        assert!(!files.is_empty(), "shutdown should write NDJSON");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("shutdown_thread_work"),
            "should contain thread work: {content}"
        );
        assert!(
            content.contains("shutdown_main_work"),
            "should contain main work: {content}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    #[ignore] // reset_all() clears ALL threads; must run in full isolation (--ignored)
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

    // ---------------------------------------------------------------
    // Async / migration tests
    // ---------------------------------------------------------------

    #[test]
    #[serial]
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
    #[serial]
    fn set_runs_dir_used_by_flush() {
        // set_runs_dir() should configure where flush() writes data,
        // without requiring PIANO_RUNS_DIR env var or ~/.piano/ fallback.
        reset();
        {
            let _g = enter("set_dir_fn");
            burn_cpu(5_000);
        }

        let tmp = std::env::temp_dir().join(format!("piano_setdir_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        // Configure runs dir via set_runs_dir, NOT env var.
        set_runs_dir(&tmp);
        flush();

        // Clear the global so other tests aren't affected.
        clear_runs_dir();

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !files.is_empty(),
            "flush() should write to set_runs_dir() path, got no files in {tmp:?}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn shutdown_to_sets_runs_dir_for_flush() {
        // Verify that shutdown_impl_inner writes to the given directory.
        // flush() is also called but its output dir depends on the
        // process-global runs_dir() which is unreliable in parallel tests
        // (env var and set_runs_dir races). We verify shutdown writes
        // correctly and that flush() doesn't panic.
        reset();

        let tmp = std::env::temp_dir().join(format!("piano_shutdown_to_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        set_runs_dir(&tmp);

        // Generate some data and flush mid-program.
        {
            let _g = enter("mid_flush_fn");
            burn_cpu(5_000);
        }
        flush();

        // Generate more data.
        {
            let _g = enter("shutdown_fn");
            burn_cpu(5_000);
        }

        // Write remaining data to the explicit dir.
        let _ = shutdown_impl_inner(&tmp);
        clear_runs_dir();

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        // shutdown_impl_inner writes directly to tmp (not via runs_dir()),
        // so at least 1 file is guaranteed. flush() may also write here
        // if runs_dir() wasn't overridden by a parallel test's env var.
        assert!(
            !files.is_empty(),
            "shutdown_impl_inner should write to {tmp:?}, got 0 files"
        );

        // Verify the shutdown file contains our function.
        let has_shutdown_fn = files.iter().any(|f| {
            std::fs::read_to_string(f.path())
                .unwrap_or_default()
                .contains("shutdown_fn")
        });
        assert!(
            has_shutdown_fn,
            "shutdown output should contain 'shutdown_fn'"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn stack_entry_size() {
        let size = core::mem::size_of::<StackEntry>();
        // Without cpu-time: 56 bytes (name resolved via packed name_id; includes start_tsc
        // for process::exit recovery).
        // With cpu-time: 72 bytes (two extra u64 fields for cpu_children_ns, cpu_start_ns).
        #[cfg(not(feature = "cpu-time"))]
        assert_eq!(size, 56, "StackEntry without cpu-time must be 56 bytes");
        #[cfg(feature = "cpu-time")]
        assert_eq!(size, 72, "StackEntry with cpu-time must be 72 bytes");
    }

    #[test]
    #[serial]
    fn shutdown_impl_reports_write_errors_to_stderr() {
        reset();
        // Produce some data so shutdown_impl_inner has something to write.
        {
            let _g = enter("write_err_test");
        }

        // Point at a path that cannot be a directory (a file, not a dir).
        let tmp = std::env::temp_dir().join(format!("piano_write_err_{}", std::process::id()));
        // Create a file where shutdown_impl_inner expects a directory.
        std::fs::write(&tmp, b"not a directory").unwrap();

        // shutdown_impl_inner should try to write and fail, printing to stderr.
        // We can't easily capture stderr in a unit test, so instead verify
        // that the function does not panic and returns normally.
        let failed = shutdown_impl_inner(&tmp);
        assert!(
            failed,
            "shutdown_impl_inner should return true when writes fail"
        );

        // Clean up.
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    #[serial]
    fn records_aggregate_in_flight() {
        reset();
        for _ in 0..10_000 {
            let _g = enter("hot_fn");
        }
        flush_records_buf();
        RECORDS.with(|records| {
            let recs = records.lock().unwrap_or_else(|e| e.into_inner());
            // Filter to our function — reset_all() from another test may have
            // cleared RECORDS mid-loop, splitting the aggregation into pieces.
            let hot: Vec<_> = recs.iter().filter(|e| e.name == "hot_fn").collect();
            assert_eq!(
                hot.len(),
                1,
                "expected 1 aggregated entry for hot_fn, got {}",
                hot.len()
            );
            assert_eq!(hot[0].calls, 10_000);
        });
    }

    #[test]
    #[serial]
    fn frame_buffer_aggregates_in_flight() {
        reset();
        {
            let _outer = enter("fbuf_agg_outer");
            for _ in 0..10_000 {
                let _g = enter("fbuf_agg_inner");
            }
        }
        let frames = collect_frames();
        // Filter to frames from this test (collect_frames is now global).
        let my_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fbuf_agg_outer"))
            .collect();
        assert_eq!(my_frames.len(), 1);
        assert!(
            my_frames[0].len() <= 2,
            "expected <= 2 fn summaries, got {}",
            my_frames[0].len()
        );
        let inner = my_frames[0]
            .iter()
            .find(|f| f.name == "fbuf_agg_inner")
            .expect("fbuf_agg_inner not found");
        assert_eq!(inner.calls, 10_000);
    }

    #[test]
    fn frame_fn_summary_calls_holds_above_u32_max() {
        let above_u32_max: u64 = u32::MAX as u64 + 1;
        let summary = FrameFnSummary {
            name: "hot_fn",
            calls: above_u32_max,
            self_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        };
        assert_eq!(
            summary.calls, above_u32_max,
            "FrameFnSummary.calls must hold values above u32::MAX"
        );
    }

    #[test]
    #[serial]
    fn frames_on_disk_before_shutdown() {
        // Verify the end-to-end wiring: drop_cold at depth 0 calls
        // stream_frame(), which writes to disk when streaming is enabled.
        //
        // We do the frame work on a dedicated thread, using a channel to
        // coordinate: the spawned thread enables streaming just before its
        // own guard drop, and disables it immediately after. This keeps
        // the STREAMING_ENABLED=true window to a single guard drop on a
        // single thread, avoiding interference with parallel tests.

        let tmp = std::env::temp_dir().join(format!("piano_immediate_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        set_runs_dir(&tmp);

        // Pre-open the stream file so the spawned thread doesn't race
        // with other threads on the lazy initialization.
        {
            let mut state = stream_file().lock().unwrap();
            if state.is_none() {
                *state = Some(open_stream_file(&tmp).unwrap());
            }
        }

        let handle = thread::spawn(|| {
            reset();
            // Create the guard, do work, then enable streaming just
            // before the guard drops. The drop is the ONLY operation
            // that runs with streaming enabled.
            let g = enter("immediate_fn");
            burn_cpu(5_000);
            STREAMING_ENABLED.store(true, Ordering::SeqCst);
            STREAM_FRAMES.store(true, Ordering::SeqCst);
            drop(g);
            STREAM_FRAMES.store(false, Ordering::SeqCst);
            STREAMING_ENABLED.store(false, Ordering::SeqCst);
        });
        handle.join().unwrap();

        // Force flush the BufWriter so we can read the content.
        {
            let mut state = stream_file().lock().unwrap();
            if let Some(ref mut s) = *state {
                s.file.flush().unwrap();
            }
        }

        // File should exist with header + at least 1 frame, NO trailer.
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        assert_eq!(files.len(), 1);

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert!(
            lines.len() >= 2,
            "header + at least 1 frame, got {} lines",
            lines.len()
        );
        assert!(lines[0].contains("\"format_version\":4"));
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[1].contains("\"id\":"));

        // Cleanup
        *stream_file().lock().unwrap() = None;
        clear_runs_dir();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn synthesize_frame_from_agg_precision() {
        reset();

        let agg = vec![
            FnAgg {
                name: "synth_fn_a",
                calls: 5,
                total_ms: 100.0,
                self_ms: 75.5,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 50_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FnAgg {
                name: "synth_fn_b",
                calls: 1,
                total_ms: 10.0,
                self_ms: 10.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 8_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
        ];

        let frames = synthesize_frame_from_agg(&agg);

        assert_eq!(frames.len(), 2, "should have 2 function summaries");

        let a = frames.iter().find(|f| f.name == "synth_fn_a").unwrap();
        assert_eq!(a.calls, 5);
        assert!(
            (a.self_ns as i64 - 75_500_000i64).unsigned_abs() <= 1,
            "synth_fn_a.self_ns = {}, expected ~75_500_000",
            a.self_ns,
        );
        assert_eq!(a.alloc_count, 0);
        assert_eq!(a.alloc_bytes, 0);
        assert_eq!(a.free_count, 0);
        assert_eq!(a.free_bytes, 0);

        let b = frames.iter().find(|f| f.name == "synth_fn_b").unwrap();
        assert_eq!(b.calls, 1);
        assert!(
            (b.self_ns as i64 - 10_000_000i64).unsigned_abs() <= 1,
            "synth_fn_b.self_ns = {}, expected ~10_000_000",
            b.self_ns,
        );
    }

    #[test]
    #[serial]
    fn double_flush_idempotency() {
        reset();
        {
            let _g = enter("dflush_fn");
            burn_cpu(1_000);
        }
        // drop_cold already flushed RECORDS_BUF at the depth-0 boundary.
        // collect() calls flush_records_buf() again.
        let records = collect();
        let rec = records.iter().find(|r| r.name == "dflush_fn").unwrap();
        assert_eq!(rec.calls, 1, "double flush should not double-count calls");

        // Also verify via raw RECORDS that there's exactly one entry.
        flush_records_buf();
        RECORDS.with(|records| {
            let recs = records.lock().unwrap_or_else(|e| e.into_inner());
            let count = recs.iter().filter(|e| e.name == "dflush_fn").count();
            assert_eq!(
                count, 1,
                "RECORDS should have exactly one dflush_fn entry, got {count}"
            );
        });
    }

    #[test]
    #[serial]
    #[cfg(debug_assertions)]
    fn reentrant_stack_access_panics_in_debug() {
        // Verify that the debug-mode borrow guard on STACK detects reentrant
        // access. This ensures UnsafeCell usage stays sound even if someone
        // accidentally introduces a nested STACK access.
        reset();
        let result = std::panic::catch_unwind(|| {
            with_stack_mut(|_outer| {
                // Attempt a nested access while the outer borrow is active.
                // This should panic due to the STACK_BORROW_COUNT guard.
                with_stack_ref(|_inner| {});
            });
        });
        assert!(
            result.is_err(),
            "nested STACK access should panic in debug mode"
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: FnAgg::absorb arithmetic
    // ---------------------------------------------------------------

    #[test]
    fn absorb_adds_calls() {
        let mut dst = FnAgg {
            name: "f",
            calls: 3,
            total_ms: 10.0,
            self_ms: 5.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 100,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        };
        let src = FnAgg {
            name: "f",
            calls: 2,
            total_ms: 4.0,
            self_ms: 3.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 50,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        };
        dst.absorb(&src);
        assert_eq!(dst.calls, 5, "calls: 3 + 2 = 5");
        assert!(
            (dst.total_ms - 14.0).abs() < f64::EPSILON,
            "total_ms: 10.0 + 4.0 = 14.0, got {}",
            dst.total_ms
        );
        assert!(
            (dst.self_ms - 8.0).abs() < f64::EPSILON,
            "self_ms: 5.0 + 3.0 = 8.0, got {}",
            dst.self_ms
        );
        #[cfg(feature = "cpu-time")]
        assert_eq!(dst.cpu_self_ns, 150, "cpu_self_ns: 100 + 50 = 150");
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: merge_into_fnagg_vec arithmetic
    // ---------------------------------------------------------------

    #[test]
    fn merge_into_fnagg_vec_existing_entry() {
        // First insert creates the entry.
        let name: &'static str = "mifv_fn";
        let mut buf: Vec<FnAgg> = Vec::new();
        merge_into_fnagg_vec(
            &mut buf,
            name,
            2_000_000, // 2ms elapsed
            500_000,   // 0.5ms children
            #[cfg(feature = "cpu-time")]
            100,
            0,
            0,
            0,
            0,
        );
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].calls, 1);

        // Second merge into the same name should ADD, not multiply or subtract.
        merge_into_fnagg_vec(
            &mut buf,
            name,
            3_000_000, // 3ms elapsed
            1_000_000, // 1ms children
            #[cfg(feature = "cpu-time")]
            200,
            0,
            0,
            0,
            0,
        );
        assert_eq!(buf.len(), 1, "same name should merge, not create new entry");
        assert_eq!(buf[0].calls, 2, "calls: 1 + 1 = 2");

        let expected_total = 2_000_000.0 / 1_000_000.0 + 3_000_000.0 / 1_000_000.0;
        assert!(
            (buf[0].total_ms - expected_total).abs() < 0.001,
            "total_ms should be sum: expected {expected_total}, got {}",
            buf[0].total_ms
        );

        let expected_self = (2_000_000u64.saturating_sub(500_000)) as f64 / 1_000_000.0
            + (3_000_000u64.saturating_sub(1_000_000)) as f64 / 1_000_000.0;
        assert!(
            (buf[0].self_ms - expected_self).abs() < 0.001,
            "self_ms should be sum: expected {expected_self}, got {}",
            buf[0].self_ms
        );

        #[cfg(feature = "cpu-time")]
        assert_eq!(buf[0].cpu_self_ns, 300, "cpu_self_ns: 100 + 200 = 300");
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: merge_into_frame_buf arithmetic
    // ---------------------------------------------------------------

    #[test]
    fn merge_into_frame_buf_existing_entry() {
        let name: &'static str = "mifb_fn";
        let mut buf: Vec<FrameFnSummary> = Vec::new();
        merge_into_frame_buf(
            &mut buf,
            name,
            1000,
            #[cfg(feature = "cpu-time")]
            50,
            10,
            256,
            3,
            128,
        );
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].calls, 1);

        // Merge again with different values to verify += (not -= or *=).
        merge_into_frame_buf(
            &mut buf,
            name,
            2000,
            #[cfg(feature = "cpu-time")]
            70,
            5,
            512,
            2,
            64,
        );
        assert_eq!(buf.len(), 1, "same name should merge");
        assert_eq!(buf[0].calls, 2, "calls: 1 + 1 = 2");
        assert_eq!(buf[0].self_ns, 3000, "self_ns: 1000 + 2000 = 3000");
        assert_eq!(buf[0].alloc_count, 15, "alloc_count: 10 + 5 = 15");
        assert_eq!(buf[0].alloc_bytes, 768, "alloc_bytes: 256 + 512 = 768");
        assert_eq!(buf[0].free_count, 5, "free_count: 3 + 2 = 5");
        assert_eq!(buf[0].free_bytes, 192, "free_bytes: 128 + 64 = 192");
        #[cfg(feature = "cpu-time")]
        assert_eq!(buf[0].cpu_self_ns, 120, "cpu_self_ns: 50 + 70 = 120");
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: drop_cold frame boundary logic
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn drop_cold_parent_children_ns_accumulates() {
        // Kills: collector.rs:915 replace += with -= and *= in drop_cold
        // When an inner function drops, its elapsed time must be ADDED to
        // parent.children_ns. If -= or *= is used instead, the parent's
        // self_ms would be inflated or wrong.
        reset();
        {
            let _outer = enter("dc_outer");
            burn_cpu(5_000);
            {
                let _inner1 = enter("dc_inner1");
                burn_cpu(10_000);
            }
            {
                let _inner2 = enter("dc_inner2");
                burn_cpu(10_000);
            }
        }
        let records = collect();
        let outer = records.iter().find(|r| r.name == "dc_outer").unwrap();
        // With correct += on children_ns, outer.self_ms should be much less
        // than outer.total_ms (both children subtracted).
        // With -= or *=, self_ms would be >= total_ms.
        assert!(
            outer.self_ms < outer.total_ms * 0.6,
            "outer self ({:.3}) should be much less than total ({:.3}) \
             because two children's time was subtracted",
            outer.self_ms,
            outer.total_ms,
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: aggregate division
    // ---------------------------------------------------------------

    #[cfg(feature = "cpu-time")]
    #[test]
    fn aggregate_divides_cpu_ns_to_ms() {
        // Kills: collector.rs:1087 replace / with % and *
        let agg = vec![FnAgg {
            name: "agg_div_fn",
            calls: 1,
            total_ms: 10.0,
            self_ms: 8.0,
            cpu_self_ns: 5_000_000, // 5ms
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];
        let result = aggregate(&agg, &[]);
        assert_eq!(result.len(), 1);
        let rec = &result[0];
        // 5_000_000 / 1_000_000.0 = 5.0ms
        // 5_000_000 % 1_000_000 = 0 (wrong)
        // 5_000_000 * 1_000_000.0 = 5e12 (wrong)
        assert!(
            (rec.cpu_self_ms - 5.0).abs() < 0.001,
            "cpu_self_ms should be 5.0 (ns / 1M), got {}",
            rec.cpu_self_ms
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: timestamp_ms
    // ---------------------------------------------------------------

    #[test]
    fn timestamp_ms_returns_plausible_value() {
        // Kills: collector.rs:1217 replace timestamp_ms -> u128 with 0 and 1
        let ts = timestamp_ms();
        // Any reasonable timestamp after 2020 is > 1_577_836_800_000 ms
        assert!(
            ts > 1_577_836_800_000,
            "timestamp_ms should return current time, got {ts}"
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: run_id
    // ---------------------------------------------------------------

    #[test]
    fn run_id_is_nonempty_and_stable() {
        // Kills: collector.rs:80 replace run_id -> &'static str with "" and "xyzzy"
        let id = run_id();
        assert!(!id.is_empty(), "run_id should not be empty");
        // run_id contains the PID and a timestamp separated by underscore.
        assert!(
            id.contains('_'),
            "run_id should contain underscore separator: {id}"
        );
        // Verify it contains the process ID.
        let pid = std::process::id().to_string();
        assert!(
            id.starts_with(&pid),
            "run_id should start with PID ({pid}): {id}"
        );
        // Stability: calling again returns the same value.
        assert_eq!(run_id(), id, "run_id should be stable across calls");
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: runs_dir_nonblocking and dirs_fallback
    // ---------------------------------------------------------------

    #[test]
    fn dirs_fallback_returns_home() {
        // Kills: collector.rs:1275 replace dirs_fallback with None and Some(Default)
        // In test environments HOME is typically set.
        if let Ok(home) = std::env::var("HOME") {
            let result = dirs_fallback();
            assert_eq!(
                result,
                Some(PathBuf::from(&home)),
                "dirs_fallback should return HOME"
            );
        }
    }

    #[test]
    #[serial]
    fn runs_dir_nonblocking_returns_some() {
        // Kills: collector.rs:1264 replace runs_dir_nonblocking with None and Some(Default)
        // With a runs dir set, runs_dir_nonblocking should return it.
        let tmp = std::env::temp_dir().join("piano_nonblocking_test");
        set_runs_dir(&tmp);
        let result = runs_dir_nonblocking();
        clear_runs_dir();
        assert_eq!(
            result,
            Some(tmp),
            "runs_dir_nonblocking should return the configured dir"
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: shutdown_impl_inner negation
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn shutdown_impl_inner_returns_false_on_success() {
        // Kills: collector.rs:1546, 1551, 1578 delete ! in shutdown_impl_inner
        // A successful write should return false (no failure).
        reset();
        {
            let _g = enter("shutdown_ok_test");
            burn_cpu(1_000);
        }
        let tmp = std::env::temp_dir().join(format!("piano_shutdown_ok_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let failed = shutdown_impl_inner(&tmp);
        assert!(
            !failed,
            "shutdown_impl_inner should return false on successful write"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn shutdown_synthesizes_frame_from_agg_when_no_frames_exist() {
        // Kills: delete ! in `if !agg.is_empty()` inside shutdown_impl_inner.
        // Simulates process::exit mid-function: RECORDS has data but FRAMES
        // is empty. shutdown_impl_inner should synthesize a frame from
        // aggregates and write an NDJSON file.
        reset_all();

        // Directly inject an FnAgg into RECORDS (bypassing drop_cold, which
        // would also commit a frame). This mirrors what happens when
        // TlsFlushGuard::drop drains RECORDS_BUF on process::exit while a
        // function is still on the stack.
        RECORDS.with(|records| {
            records.lock().unwrap().push(FnAgg {
                name: "mid_exit_fn",
                calls: 1,
                total_ms: 5.0,
                self_ms: 5.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 5_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            });
        });

        // Verify precondition: frames are empty, agg is non-empty.
        assert!(
            collect_frames().is_empty(),
            "precondition: no frames should exist"
        );
        let agg = collect_all_fnagg();
        assert!(!agg.is_empty(), "precondition: aggregate data should exist");

        let tmp = std::env::temp_dir().join(format!("piano_synth_frame_{}", timestamp_ms()));
        std::fs::create_dir_all(&tmp).unwrap();
        let failed = shutdown_impl_inner(&tmp);
        assert!(!failed, "shutdown should succeed");

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("ndjson"))
            .collect();
        assert!(
            !files.is_empty(),
            "shutdown should write an NDJSON file when agg data exists but no frames"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn collect_all_fnagg_no_flush_reads_globals_only() {
        // Phase 2 of shutdown must not reach into TLS.
        // collect_all_fnagg_no_flush should read THREAD_RECORDS
        // without calling flush_records_buf.
        reset_all();

        // Put data directly into RECORDS Arc (simulating post-drain state).
        RECORDS.with(|records| {
            records.lock().unwrap().push(FnAgg {
                name: "global_fn",
                calls: 3,
                total_ms: 10.0,
                self_ms: 8.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 8_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            });
        });

        // Put data in RECORDS_BUF (TLS) -- this should NOT be picked up.
        RECORDS_BUF.with(|buf| {
            buf.borrow_mut().push(FnAgg {
                name: "tls_only_fn",
                calls: 1,
                total_ms: 2.0,
                self_ms: 2.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 2_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            });
        });

        let result = collect_all_fnagg_no_flush();
        assert_eq!(result.len(), 1, "should only contain global data");
        assert_eq!(result[0].name, "global_fn");
        assert_eq!(result[0].calls, 3);

        // Verify RECORDS_BUF was NOT drained.
        RECORDS_BUF.with(|buf| {
            assert_eq!(buf.borrow().len(), 1, "RECORDS_BUF should be untouched");
        });
    }

    #[test]
    #[serial]
    fn shutdown_streaming_no_file_preserves_frames_bug_533() {
        // Bug #533: when STREAMING_ENABLED=true but no stream file exists,
        // shutdown_impl_inner falls through to aggregate-only synthesis,
        // losing any frames in the FRAMES Arc.
        //
        // Phase-separated shutdown fixes this: the write phase checks
        // FRAMES first regardless of streaming mode.
        reset_all();

        // Simulate post-drain state: frames exist in FRAMES Arc.
        FRAMES.with(|frames| {
            frames.lock().unwrap().push(vec![FrameFnSummary {
                name: "bug_533_fn",
                calls: 1,
                self_ns: 5_000_000,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 5_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]);
        });

        // Enable streaming but do NOT open a stream file.
        // This is the #533 scenario: streaming was configured but
        // no frame ever completed to trigger file creation.
        STREAMING_ENABLED.store(true, Ordering::Relaxed);

        let tmp = std::env::temp_dir().join(format!("piano_bug533_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let failed = shutdown_impl_inner(&tmp);

        // Reset streaming flag for other tests.
        STREAMING_ENABLED.store(false, Ordering::Relaxed);

        assert!(!failed, "shutdown should succeed");

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        assert!(
            !files.is_empty(),
            "shutdown should write NDJSON even with streaming enabled but no stream file"
        );

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        assert!(
            content.contains("bug_533_fn"),
            "NDJSON should contain the frame data: {content}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn shutdown_streaming_synthesizes_frame_from_agg_when_no_stream_opened() {
        // Kills: delete ! in `if !agg.is_empty()` in the streaming branch
        // of shutdown_impl_inner. Same scenario as the non-streaming test
        // but with STREAMING_ENABLED=true and no stream file opened.
        reset_all();

        // Inject aggregate data without committing any frames.
        RECORDS.with(|records| {
            records.lock().unwrap().push(FnAgg {
                name: "streaming_mid_exit_fn",
                calls: 1,
                total_ms: 3.0,
                self_ms: 3.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 3_000_000,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            });
        });

        // Enable streaming without opening a stream file.
        STREAMING_ENABLED.store(true, Ordering::SeqCst);

        let tmp = std::env::temp_dir().join(format!("piano_stream_synth_{}", timestamp_ms()));
        std::fs::create_dir_all(&tmp).unwrap();
        let failed = shutdown_impl_inner(&tmp);

        // Restore before assertions so cleanup runs even on failure.
        STREAMING_ENABLED.store(false, Ordering::SeqCst);

        assert!(!failed, "shutdown should succeed");

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("ndjson"))
            .collect();
        assert!(
            !files.is_empty(),
            "streaming shutdown should write NDJSON when agg data exists but no stream file"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ---------------------------------------------------------------
    // Issue #523: stream_frames flag suppresses per-frame streaming
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn stream_frames_off_skips_disk_writes_and_shutdown_synthesizes_aggregate() {
        // When STREAMING_ENABLED=true but STREAM_FRAMES=false (default),
        // depth-0 frame boundaries must NOT write to disk. Shutdown should
        // fall through to synthesize_frame_from_agg and produce a valid
        // aggregate-only NDJSON file.
        reset_all();
        let tmp =
            std::env::temp_dir().join(format!("piano_no_stream_frames_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        set_runs_dir(&tmp);

        // Enable streaming but keep STREAM_FRAMES off (the new default).
        STREAMING_ENABLED.store(true, Ordering::SeqCst);
        STREAM_FRAMES.store(false, Ordering::SeqCst);

        let handle = std::thread::spawn(|| {
            reset();
            // Two depth-0 calls — would normally produce 2 NDJSON frame lines.
            {
                let _g = enter("no_frames_fn");
                burn_cpu(5_000);
            }
            {
                let _g = enter("no_frames_fn");
                burn_cpu(5_000);
            }
        });
        handle.join().unwrap();

        // No stream file should have been opened.
        assert!(
            stream_file().lock().unwrap().is_none(),
            "stream file should not be opened when STREAM_FRAMES is false"
        );

        // Shutdown should produce an aggregate-only NDJSON file.
        let failed = shutdown_impl_inner(&tmp);

        // Restore before assertions.
        STREAMING_ENABLED.store(false, Ordering::SeqCst);
        clear_runs_dir();

        assert!(!failed, "shutdown should succeed");

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("ndjson"))
            .collect();
        assert_eq!(files.len(), 1, "should write exactly one NDJSON file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();

        // Should be: header + 1 aggregate frame + trailer = 3 lines.
        assert_eq!(
            lines.len(),
            3,
            "expected header + 1 aggregate frame + trailer, got {lines:?}"
        );
        assert!(lines[0].contains("\"format_version\":4"), "header");
        // Frame line uses fn_id references, function names are in the trailer.
        assert!(
            lines[1].contains("\"fns\":["),
            "aggregate frame should contain function entries"
        );
        assert!(
            lines[1].contains("\"calls\":2"),
            "aggregate should sum both calls, got: {}",
            lines[1]
        );
        assert!(lines[2].contains("\"functions\""), "trailer");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ---------------------------------------------------------------
    // Issue #486: FnAgg must carry alloc data
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn fnagg_carries_alloc_data_through_synthesize() {
        // FnAgg entries with alloc data should produce non-zero alloc
        // fields in synthesize_frame_from_agg output.
        let agg = vec![FnAgg {
            name: "synth_alloc_fn",
            calls: 3,
            total_ms: 50.0,
            self_ms: 40.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 30_000_000,
            alloc_count: 100,
            alloc_bytes: 4096,
            free_count: 50,
            free_bytes: 2048,
        }];

        let frames = synthesize_frame_from_agg(&agg);
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.alloc_count, 100, "alloc_count should come from FnAgg");
        assert_eq!(f.alloc_bytes, 4096, "alloc_bytes should come from FnAgg");
        assert_eq!(f.free_count, 50, "free_count should come from FnAgg");
        assert_eq!(f.free_bytes, 2048, "free_bytes should come from FnAgg");
    }

    #[test]
    #[serial]
    fn fnagg_absorb_merges_alloc_fields() {
        let mut a = FnAgg {
            name: "absorb_fn",
            calls: 1,
            total_ms: 10.0,
            self_ms: 8.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 5_000_000,
            alloc_count: 10,
            alloc_bytes: 1024,
            free_count: 5,
            free_bytes: 512,
        };
        let b = FnAgg {
            name: "absorb_fn",
            calls: 2,
            total_ms: 20.0,
            self_ms: 15.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 10_000_000,
            alloc_count: 20,
            alloc_bytes: 2048,
            free_count: 8,
            free_bytes: 768,
        };
        a.absorb(&b);
        assert_eq!(a.alloc_count, 30);
        assert_eq!(a.alloc_bytes, 3072);
        assert_eq!(a.free_count, 13);
        assert_eq!(a.free_bytes, 1280);
    }

    #[test]
    #[serial]
    fn merge_into_fnagg_vec_accumulates_alloc() {
        let name: &'static str = "mifav_alloc_fn";
        let mut buf: Vec<FnAgg> = Vec::new();
        merge_into_fnagg_vec(
            &mut buf,
            name,
            1_000_000,
            0,
            #[cfg(feature = "cpu-time")]
            500_000,
            10,
            256,
            3,
            128,
        );
        assert_eq!(buf[0].alloc_count, 10);
        assert_eq!(buf[0].alloc_bytes, 256);
        assert_eq!(buf[0].free_count, 3);
        assert_eq!(buf[0].free_bytes, 128);

        // Second merge: alloc fields should accumulate
        merge_into_fnagg_vec(
            &mut buf,
            name,
            2_000_000,
            0,
            #[cfg(feature = "cpu-time")]
            700_000,
            5,
            512,
            2,
            64,
        );
        assert_eq!(buf[0].alloc_count, 15, "alloc_count: 10 + 5");
        assert_eq!(buf[0].alloc_bytes, 768, "alloc_bytes: 256 + 512");
        assert_eq!(buf[0].free_count, 5, "free_count: 3 + 2");
        assert_eq!(buf[0].free_bytes, 192, "free_bytes: 128 + 64");
    }

    #[test]
    #[serial]
    fn drop_cold_records_alloc_in_fnagg() {
        // After a function completes, its alloc data should appear in
        // FnAgg aggregates (not just FrameFnSummary).
        reset();
        {
            let _g = enter("dc_alloc_fn");
            crate::alloc::track_alloc(1024);
            crate::alloc::track_alloc(512);
            crate::alloc::track_dealloc(256);
        }
        flush_records_buf();
        let agg = RECORDS.with(|records| records.lock().unwrap().clone());
        let entry = agg.iter().find(|e| e.name == "dc_alloc_fn").unwrap();
        assert_eq!(entry.alloc_count, 2, "should record 2 allocations");
        assert_eq!(entry.alloc_bytes, 1536, "should record 1536 alloc bytes");
        assert_eq!(entry.free_count, 1, "should record 1 free");
        assert_eq!(entry.free_bytes, 256, "should record 256 free bytes");
    }

    #[test]
    #[serial]
    fn recursive_non_allocating_has_zero_allocs() {
        // A recursive function that does no allocation should not pick up
        // phantom allocs from STACK Vec growth during enter_cold().
        reset();

        // Simulate a recursive call chain deep enough to trigger Vec growth.
        // Vec starts empty, capacity doubles: 0 -> 1 -> 2 -> 4 -> 8 -> 16...
        // 20 levels ensures multiple reallocations.
        fn recurse(depth: u32) {
            let _g = enter("recurse_fn");
            if depth > 0 {
                recurse(depth - 1);
            }
        }
        recurse(20);

        flush_records_buf();
        let agg = RECORDS.with(|records| records.lock().unwrap().clone());
        let entry = agg.iter().find(|e| e.name == "recurse_fn").unwrap();
        assert_eq!(
            entry.alloc_count, 0,
            "non-allocating recursive function should have 0 alloc_count, got {}",
            entry.alloc_count
        );
        assert_eq!(
            entry.alloc_bytes, 0,
            "non-allocating recursive function should have 0 alloc_bytes, got {}",
            entry.alloc_bytes
        );
    }

    #[test]
    #[serial]
    fn drain_inflight_stack_recovers_forgotten_guards() {
        // Simulate SIGTERM: enter two functions, forget their guards (so they
        // never drop), then call drain_inflight_stack to recover the data.
        reset();

        register("outer_fn");
        register("inner_fn");

        let g_outer = enter("outer_fn");
        burn_cpu(5_000);
        let g_inner = enter("inner_fn");
        burn_cpu(5_000);

        // Forget guards to simulate SIGTERM (guards never drop).
        std::mem::forget(g_inner);
        std::mem::forget(g_outer);

        drain_inflight_stack();

        let all = collect_all();
        assert!(
            all.len() >= 2,
            "expected at least 2 functions recovered, got {}",
            all.len()
        );
        let outer = all
            .iter()
            .find(|r| r.name == "outer_fn")
            .expect("outer_fn not found");
        let inner = all
            .iter()
            .find(|r| r.name == "inner_fn")
            .expect("inner_fn not found");

        assert!(outer.calls > 0, "outer_fn should have calls > 0");
        assert!(inner.calls > 0, "inner_fn should have calls > 0");
        assert!(outer.self_ms > 0.0, "outer_fn should have positive self_ms");
        assert!(inner.self_ms > 0.0, "inner_fn should have positive self_ms");

        // Verify cpu_self_ms is positive for normally-entered functions.
        // Catches mutations that invert the cpu_start_ns == 0 guard in
        // drain_inflight_stack (the guard handles partial enter(); normal
        // entries must produce non-zero CPU time).
        #[cfg(feature = "cpu-time")]
        {
            assert!(
                outer.cpu_self_ms > 0.0,
                "outer_fn should have positive cpu_self_ms, got {}",
                outer.cpu_self_ms
            );
            assert!(
                inner.cpu_self_ms > 0.0,
                "inner_fn should have positive cpu_self_ms, got {}",
                inner.cpu_self_ms
            );
        }
    }

    #[test]
    #[serial]
    fn drain_inflight_stack_alloc_attribution() {
        // Verify that drain_inflight_stack assigns the correct alloc counts
        // to each in-flight entry: entry[i] gets entry[i+1].saved_alloc,
        // and the innermost entry gets current ALLOC_COUNTERS.
        reset();

        register("drain_outer");
        register("drain_inner");

        // enter("drain_outer") saves+zeroes ALLOC_COUNTERS into outer's saved_alloc.
        let g_outer = enter("drain_outer");

        // Simulate 3 allocations while outer is running (before inner enters).
        // These accumulate in ALLOC_COUNTERS and will be saved into inner's
        // saved_alloc when enter("drain_inner") is called.
        crate::alloc::track_alloc(100);
        crate::alloc::track_alloc(100);
        crate::alloc::track_alloc(100);

        // enter("drain_inner") saves current counters (3 allocs, 300 bytes)
        // into inner's saved_alloc, then zeroes ALLOC_COUNTERS.
        let g_inner = enter("drain_inner");

        // Simulate 5 allocations while inner is running.
        // These remain in ALLOC_COUNTERS (current_alloc at drain time).
        for _ in 0..5 {
            crate::alloc::track_alloc(200);
        }

        std::mem::forget(g_inner);
        std::mem::forget(g_outer);

        drain_inflight_stack();
        flush_records_buf();

        let agg = RECORDS.with(|records| records.lock().unwrap().clone());
        let outer = agg
            .iter()
            .find(|e| e.name == "drain_outer")
            .expect("drain_outer not in RECORDS");
        let inner = agg
            .iter()
            .find(|e| e.name == "drain_inner")
            .expect("drain_inner not in RECORDS");

        // outer (i=0) should get s[1].saved_alloc = inner's saved snapshot
        // = 3 allocs / 300 bytes (what accumulated between outer enter and inner enter).
        assert_eq!(
            outer.alloc_count, 3,
            "outer should have 3 allocs from s[i+1].saved_alloc, got {}",
            outer.alloc_count
        );
        assert_eq!(
            outer.alloc_bytes, 300,
            "outer should have 300 alloc bytes, got {}",
            outer.alloc_bytes
        );

        // inner (i=1, innermost) should get current_alloc = 5 allocs / 1000 bytes.
        assert_eq!(
            inner.alloc_count, 5,
            "inner should have 5 allocs from current_alloc, got {}",
            inner.alloc_count
        );
        assert_eq!(
            inner.alloc_bytes, 1000,
            "inner should have 1000 alloc bytes, got {}",
            inner.alloc_bytes
        );
    }

    // ---------------------------------------------------------------
    // Issue #527: env_is_opt_in parsing (kills == → != mutant)
    // ---------------------------------------------------------------

    #[test]
    fn env_opt_in_returns_true_for_one() {
        assert!(env_is_opt_in(Ok("1".into())));
    }

    #[test]
    fn env_opt_in_returns_false_for_missing_var() {
        assert!(!env_is_opt_in(Err(std::env::VarError::NotPresent)));
    }

    #[test]
    fn env_opt_in_returns_false_for_other_values() {
        assert!(!env_is_opt_in(Ok("0".into())));
        assert!(!env_is_opt_in(Ok("true".into())));
        assert!(!env_is_opt_in(Ok("".into())));
    }
}
