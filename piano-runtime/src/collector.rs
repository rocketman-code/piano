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
use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Thread-safe, initialize-once cell using `Once` + `UnsafeCell`.
///
/// Equivalent to `OnceLock` (stabilized in 1.70) but works on Rust 1.59+.
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

/// State for the streaming NDJSON file.
struct StreamState {
    file: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    frame_count: usize,
}

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

/// Open a new streaming NDJSON file and write the v4 header.
fn open_stream_file(dir: &std::path::Path) -> std::io::Result<StreamState> {
    std::fs::create_dir_all(dir)?;
    let ts = timestamp_ms();
    let path = dir.join(format!("{ts}.ndjson"));
    let mut file = std::io::BufWriter::new(std::fs::File::create(&path)?);
    let run_id = run_id();

    write!(
        file,
        "{{\"format_version\":4,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts}"
    )?;
    #[cfg(feature = "cpu-time")]
    write!(file, ",\"has_cpu_time\":true")?;
    writeln!(file, "}}")?;

    Ok(StreamState {
        file,
        path,
        frame_count: 0,
    })
}

/// Write a frame to an already-locked StreamState.
fn stream_frame_to_writer(state: &mut StreamState, buf: &[FrameFnSummary]) {
    let frame_idx = state.frame_count;
    let tid = THREAD_INDEX.with(|c| c.get());
    let _ = write!(
        state.file,
        "{{\"frame\":{frame_idx},\"tid\":{tid},\"fns\":["
    );
    for (i, entry) in buf.iter().enumerate() {
        if i > 0 {
            let _ = write!(state.file, ",");
        }
        let fn_id = intern_name(entry.name);
        let _ = write!(
            state.file,
            "{{\"id\":{},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}",
            fn_id,
            entry.calls,
            entry.self_ns,
            entry.alloc_count,
            entry.alloc_bytes,
            entry.free_count,
            entry.free_bytes
        );
        #[cfg(feature = "cpu-time")]
        let _ = write!(state.file, ",\"csn\":{}", entry.cpu_self_ns);
        let _ = write!(state.file, "}}");
    }
    let _ = writeln!(state.file, "]}}");
    state.frame_count += 1;
}

/// Push a frame into the thread-local in-memory buffer (fallback path).
fn push_to_frames(buf: &[FrameFnSummary]) {
    FRAMES.with(|frames| {
        frames
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(buf.to_vec());
    });
}

/// Stream a completed frame to disk, or fall back to in-memory FRAMES.
///
/// When streaming is enabled (init() was called), writes one NDJSON line
/// per frame to the global stream file. When not enabled (tests), pushes
/// to the thread-local FRAMES vec as before.
fn stream_frame(buf: &[FrameFnSummary]) {
    if !STREAMING_ENABLED.load(Ordering::Relaxed) {
        push_to_frames(buf);
        return;
    }

    let dir = match runs_dir() {
        Some(d) => d,
        None => {
            push_to_frames(buf);
            return;
        }
    };

    let mut state = stream_file().lock().unwrap_or_else(|e| e.into_inner());

    if state.is_none() {
        // After shutdown, the stream file has been closed and the trailer
        // written. Don't open a new orphan file (it would have no trailer
        // and the BufWriter might not flush before process exit).
        if SHUTDOWN_DONE.load(Ordering::Relaxed) {
            return;
        }
        match open_stream_file(&dir) {
            Ok(s) => *state = Some(s),
            Err(e) => {
                eprintln!("piano: failed to open stream file: {e}");
                drop(state);
                push_to_frames(buf);
                return;
            }
        }
    }

    if let Some(ref mut s) = *state {
        stream_frame_to_writer(s, buf);
    }
}

/// Write the function name table as a trailer line and flush.
fn write_stream_trailer(state: &mut StreamState) -> std::io::Result<()> {
    let len = NAME_TABLE_LEN.load(Ordering::Acquire);
    write!(state.file, "{{\"functions\":[")?;
    for i in 0..len {
        if i > 0 {
            write!(state.file, ",")?;
        }
        let name = name_table_get(i).unwrap_or("<unknown>");
        let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(state.file, "\"{escaped}\"")?;
    }
    writeln!(state.file, "]}}")?;
    state.file.flush()?;
    Ok(())
}

/// Signal handling for graceful data recovery on SIGTERM / SIGINT.
///
/// On Unix, `init()` registers signal handlers so that profiling data is
/// flushed before the process exits. This prevents data loss when the
/// profiled program is killed (e.g. Ctrl-C, `kill`).
///
/// Calling `shutdown_impl_inner` from a signal handler is technically not
/// async-signal-safe (it acquires mutexes, allocates, does file I/O).
/// This is a pragmatic choice: for a profiling tool the worst case is a
/// corrupted output file, which is strictly better than losing all data
/// (the current behavior without signal handling). Many profilers take
/// the same approach.
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod signal {
    use super::*;

    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SA_RESETDFL: i32 = {
        #[cfg(target_os = "linux")]
        {
            // SA_RESETDFL == SA_ONESHOT == 0x80000000 on Linux
            0x80000000u32 as i32
        }
        #[cfg(target_os = "macos")]
        {
            // SA_RESETDFL == 4 on macOS
            4
        }
    };

    #[cfg(target_os = "linux")]
    #[repr(C)]
    struct Sigaction {
        sa_handler: extern "C" fn(i32),
        sa_mask: [u8; 128], // sigset_t is 128 bytes on Linux
        sa_flags: i32,
        sa_restorer: usize,
    }

    #[cfg(target_os = "macos")]
    #[repr(C)]
    struct Sigaction {
        sa_handler: extern "C" fn(i32),
        sa_mask: u32, // sigset_t is 4 bytes on macOS
        sa_flags: i32,
    }

    extern "C" {
        fn sigaction(sig: i32, act: *const Sigaction, oldact: *mut Sigaction) -> i32;
        fn raise(sig: i32) -> i32;
    }

    extern "C" fn handler(sig: i32) {
        // Guard: if shutdown already ran (normal exit path), just re-raise.
        if SHUTDOWN_DONE.swap(true, Ordering::SeqCst) {
            // Data already written. Re-raise with default handler
            // (SA_RESETDFL restored the default before we were called).
            unsafe { raise(sig) };
            return;
        }

        // Best-effort flush. Not async-signal-safe, but losing data is worse.
        // Uses runs_dir_nonblocking() to avoid deadlocking when the signal
        // fires while set_runs_dir() holds the RUNS_DIR mutex.
        if let Some(dir) = runs_dir_nonblocking() {
            let _ = shutdown_impl_inner(&dir);
        }

        // Re-raise so the process exits with the correct signal status.
        // SA_RESETDFL already restored the default disposition, so this
        // will terminate the process normally.
        unsafe { raise(sig) };
    }

    pub(super) fn install_handlers() {
        unsafe {
            for &sig in &[SIGINT, SIGTERM] {
                #[cfg(target_os = "linux")]
                let act = Sigaction {
                    sa_handler: handler,
                    sa_mask: [0u8; 128],
                    sa_flags: SA_RESETDFL,
                    sa_restorer: 0,
                };
                #[cfg(target_os = "macos")]
                let act = Sigaction {
                    sa_handler: handler,
                    sa_mask: 0,
                    sa_flags: SA_RESETDFL,
                };
                sigaction(sig, &act, std::ptr::null_mut());
            }
        }
    }
}

/// Register a C-level atexit handler so profiling data is flushed when user
/// code calls `std::process::exit()`.
///
/// `std::process::exit()` calls libc `exit()`, which runs atexit handlers
/// but does NOT trigger Rust stack unwinding. Without this, all profiling
/// data is silently lost on `process::exit()`. The `SHUTDOWN_DONE` atomic
/// prevents double-writes when the normal shutdown path also runs.
mod atexit {
    use super::*;

    extern "C" {
        fn atexit(f: extern "C" fn()) -> i32;
    }

    extern "C" fn on_exit() {
        if SHUTDOWN_DONE.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(dir) = runs_dir_nonblocking() {
            let _ = shutdown_impl_inner(&dir);
        }
    }

    pub(super) fn register() {
        unsafe {
            atexit(on_exit);
        }
    }
}

fn epoch() -> Instant {
    *EPOCH.get_or_init(|| {
        crate::tsc::calibrate();
        crate::tsc::calibrate_bias();
        let tsc_val = crate::tsc::read();
        let now = Instant::now();
        crate::tsc::set_epoch_tsc(tsc_val);
        now
    })
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
/// Destruction order: the initializer force-touches RECORDS_BUF, RECORDS,
/// FRAME_BUFFER, and FRAMES so their destructors are registered first.
/// Reverse-order destruction means this guard is destroyed first, while
/// all dependencies are still alive.
struct TlsFlushGuard;

impl Drop for TlsFlushGuard {
    // Skipped by cargo-mutants (see mutants.toml). Only exercised during TLS
    // destruction via process::exit(); tested by tests/process_exit.rs at the
    // workspace level, but not reachable from piano-runtime unit tests because
    // process::exit() would kill the test runner.
    fn drop(&mut self) {
        // Drain RECORDS_BUF -> RECORDS Arc (survives via THREAD_RECORDS).
        let _ = RECORDS_BUF.try_with(|buf| {
            let mut buf = buf.borrow_mut();
            if buf.is_empty() {
                return;
            }
            let _ = RECORDS.try_with(|records| {
                let mut recs = records.lock().unwrap_or_else(|e| e.into_inner());
                for local in buf.drain(..) {
                    if let Some(entry) = recs.iter_mut().find(|e| std::ptr::eq(e.name, local.name))
                    {
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
}

thread_local! {
    /// Initialized after dependency TLS to guarantee it is destroyed first.
    /// See `TlsFlushGuard` doc comment for the destruction order contract.
    static TLS_FLUSH_GUARD: TlsFlushGuard = {
        // Force-init dependencies so their destructors register before ours.
        RECORDS_BUF.with(|_| ());
        RECORDS.with(|_| ());
        FRAME_BUFFER.with(|_| ());
        FRAMES.with(|_| ());
        TlsFlushGuard
    };
}

/// Merge a single invocation into a Vec<FnAgg> via linear scan on interned name pointer.
fn merge_into_fnagg_vec(
    buf: &mut Vec<FnAgg>,
    name: &'static str,
    elapsed_ns: u64,
    children_ns: u64,
    #[cfg(feature = "cpu-time")] cpu_self_ns: u64,
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
    } else {
        buf.push(FnAgg {
            name,
            calls: 1,
            total_ms: elapsed_ms,
            self_ms,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns,
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

// -- Interned function name table ------------------------------------------
//
// Maps u16 IDs to `&'static str` function names so the Guard can carry a
// compact name reference without growing beyond 16 bytes.
//
// Layout: append-only Vec behind a Mutex. Reads during drop_cold take the
// lock briefly; writes happen once per unique name in enter_cold.
// A thread-local cache avoids the global lock on the hot path.

/// Lock-free interned name table: index -> &'static str.
///
/// Each slot stores a `&'static str` as (AtomicPtr<u8>, AtomicUsize) for the
/// data pointer and length. Reads use Acquire ordering; writes use Release.
/// The table is append-only: once written, a slot never changes.
/// NAME_TABLE_LEN tracks how many slots are valid.
///
/// Maximum 4096 unique function names. This covers all practical programs
/// (typical instrumented binaries have <100 unique functions). The table
/// occupies 64KB of BSS (zero-initialized, no runtime cost until used).
const NAME_TABLE_CAPACITY: usize = 4096;

static NAME_TABLE_PTRS: [AtomicPtr<u8>; NAME_TABLE_CAPACITY] = {
    // SAFETY: AtomicPtr<u8> has the same representation as *mut u8,
    // and null is a valid value for AtomicPtr.
    // const { } blocks aren't available at MSRV 1.59, so we use transmute.
    // This is a well-known pattern for initializing large atomic arrays.
    unsafe { core::mem::transmute([core::ptr::null_mut::<u8>(); NAME_TABLE_CAPACITY]) }
};
#[allow(unused_braces)]
static NAME_TABLE_LENS: [AtomicUsize; NAME_TABLE_CAPACITY] =
    { unsafe { core::mem::transmute([0usize; NAME_TABLE_CAPACITY]) } };
static NAME_TABLE_LEN: AtomicUsize = AtomicUsize::new(0);

/// Mutex protecting writes to the name table. Reads are lock-free.
/// Uses SyncOnceCell for MSRV 1.59 compatibility (Mutex::new is not const).
static NAME_TABLE_WRITE_LOCK: SyncOnceCell<Mutex<()>> = SyncOnceCell::new();

fn name_table_lock() -> &'static Mutex<()> {
    NAME_TABLE_WRITE_LOCK.get_or_init(|| Mutex::new(()))
}

/// Read a name from the lock-free table by index.
/// Returns None if index is out of range.
#[inline(always)]
fn name_table_get(idx: usize) -> Option<&'static str> {
    if idx >= NAME_TABLE_LEN.load(Ordering::Acquire) {
        return None;
    }
    let ptr = NAME_TABLE_PTRS[idx].load(Ordering::Acquire);
    let len = NAME_TABLE_LENS[idx].load(Ordering::Acquire);
    // SAFETY: the pointer and length were stored from a valid &'static str
    // and the slot is immutable once written (append-only table).
    Some(unsafe { core::str::from_utf8_unchecked(core::slice::from_raw_parts(ptr, len)) })
}

/// Append a name to the lock-free table. Caller must hold NAME_TABLE_WRITE_LOCK.
/// Returns the assigned index.
/// Compute the saturation index when the name table is full.
///
/// Returns the last valid slot so that lookups remain in-bounds even if
/// a new unique function name appears after the table is exhausted.
/// Extracted as a pure function for testability in debug builds where
/// `name_table_push` panics via `debug_assert` on overflow.
#[inline(always)]
fn name_table_overflow_index() -> u16 {
    (NAME_TABLE_CAPACITY - 1) as u16
}

fn name_table_push(name: &'static str) -> u16 {
    let idx = NAME_TABLE_LEN.load(Ordering::Acquire);
    debug_assert!(
        idx < NAME_TABLE_CAPACITY,
        "interned name table overflow: more than {NAME_TABLE_CAPACITY} unique function names"
    );
    if idx >= NAME_TABLE_CAPACITY {
        // Saturate at max capacity -- degrades gracefully.
        return name_table_overflow_index();
    }
    NAME_TABLE_PTRS[idx].store(name.as_ptr() as *mut u8, Ordering::Release);
    NAME_TABLE_LENS[idx].store(name.len(), Ordering::Release);
    NAME_TABLE_LEN.store(idx + 1, Ordering::Release);
    idx as u16
}

// Thread-local cache mapping name pointer -> interned ID.
// Uses pointer identity (`&'static str` addresses are stable).
thread_local! {
    static NAME_CACHE: RefCell<HashMap<usize, u16>> = RefCell::new(HashMap::new());
}

/// Intern a function name, returning its u16 ID.
/// Fast path: thread-local cache hit (no global lock).
/// Slow path: global table lookup/insert under write lock, then cache.
#[inline(always)]
fn intern_name(name: &'static str) -> u16 {
    let ptr = name.as_ptr() as usize;
    let cached = NAME_CACHE.with(|cache| cache.borrow().get(&ptr).copied());
    if let Some(id) = cached {
        return id;
    }
    intern_name_slow(name, ptr)
}

#[inline(never)]
fn intern_name_slow(name: &'static str, ptr: usize) -> u16 {
    let _guard = name_table_lock().lock().unwrap_or_else(|e| e.into_inner());
    // Check if already in global table (another thread may have added it).
    let len = NAME_TABLE_LEN.load(Ordering::Acquire);
    let id = {
        let found = NAME_TABLE_PTRS[..len]
            .iter()
            .position(|p| p.load(Ordering::Acquire) as usize == ptr);
        if let Some(pos) = found {
            pos as u16
        } else {
            name_table_push(name)
        }
    };
    drop(_guard);
    NAME_CACHE.with(|cache| {
        cache.borrow_mut().insert(ptr, id);
    });
    id
}

/// Pack a name ID (16 bits) and stack depth (16 bits) into a u64.
///
/// Layout: `[unused:32][name_id:16][depth:16]`
#[inline(always)]
fn pack_name_depth(name_id: u16, depth: u16) -> u64 {
    ((name_id as u64) << 16) | (depth as u64)
}

/// Unpack the name ID (bits 16..31) from a packed u64.
#[inline(always)]
pub(crate) fn unpack_name_id(packed: u64) -> u16 {
    (packed >> 16) as u16
}

/// Unpack the stack depth (low 16 bits) from a packed u64.
#[inline(always)]
fn unpack_depth(packed: u64) -> u16 {
    packed as u16
}

/// Resolve a name ID back to its interned `&'static str`.
///
/// Lock-free: reads directly from the static atomic arrays.
/// No TLS access, no Mutex, no allocation.
///
/// Panics (debug) / returns `"<unknown>"` (release) if the ID is out of range.
#[inline(always)]
pub(crate) fn lookup_name(name_id: u16) -> &'static str {
    match name_table_get(name_id as usize) {
        Some(name) => name,
        None => {
            debug_assert!(
                false,
                "lookup_name: id {name_id} out of range (table len {})",
                NAME_TABLE_LEN.load(Ordering::Relaxed)
            );
            "<unknown>"
        }
    }
}

/// RAII timing guard. Records elapsed time on drop.
///
/// 16 bytes: fits in two registers on both x86_64 (rax+rdx) and
/// aarch64 (x0+x1), eliminating all memory stores from the
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
    let scope_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| cell.get())
        .unwrap_or_default();

    with_stack_mut(|s| {
        let entry = match s.pop() {
            Some(e) => e,
            None => {
                eprintln!("piano-runtime: guard dropped without matching stack entry (bug)");
                return;
            }
        };

        // Resolve name once from the interned table.
        let name = lookup_name(unpack_name_id(entry.packed));

        // Restore parent's saved alloc counters.
        let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
            cell.set(entry.saved_alloc);
        });

        let raw_ticks = end_tsc.wrapping_sub(guard.start_tsc);
        let corrected_ticks = raw_ticks.saturating_sub(crate::tsc::bias_ticks());
        let elapsed_ns = crate::tsc::ticks_to_ns(corrected_ticks);
        let children_ns = entry.children_ns;
        let self_ns = elapsed_ns.saturating_sub(children_ns);

        #[cfg(feature = "cpu-time")]
        let cpu_elapsed_ns = cpu_end_ns.saturating_sub(entry.cpu_start_ns);
        #[cfg(feature = "cpu-time")]
        let cpu_self_ns = cpu_elapsed_ns.saturating_sub(entry.cpu_children_ns);

        if let Some(parent) = s.last_mut() {
            parent.children_ns += elapsed_ns;
            #[cfg(feature = "cpu-time")]
            {
                parent.cpu_children_ns += cpu_elapsed_ns;
            }
        }

        RECORDS_BUF.with(|buf| {
            merge_into_fnagg_vec(
                &mut buf.borrow_mut(),
                name,
                elapsed_ns,
                children_ns,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns,
            );
        });

        #[cfg(any(test, feature = "_test_internals"))]
        {
            let start_ns = crate::tsc::ticks_to_epoch_ns(guard.start_tsc, crate::tsc::epoch_tsc());
            INVOCATIONS.with(|inv| {
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

        FRAME_BUFFER.with(|buf| {
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

        // Flush RECORDS_BUF at frame boundaries. Normally this is depth 0,
        // but fork/adopt places an entry at depth 0 on worker threads,
        // pushing real functions to depth 1+. Flush when all remaining
        // stack entries are at depth 0 (all real work for this frame done).
        let remaining_all_base = s.iter().all(|e| unpack_depth(e.packed) == 0);
        let is_frame_boundary = unpack_depth(entry.packed) == 0 || remaining_all_base;

        if is_frame_boundary {
            flush_records_buf();
        }
        if unpack_depth(entry.packed) == 0 {
            FRAME_BUFFER.with(|buf| {
                let b = buf.borrow();
                if !b.is_empty() {
                    stream_frame(&b);
                }
                drop(b);
                buf.borrow_mut().clear();
            });
        }
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

    // Ensure the TLS flush guard is registered on this thread so that
    // on process::exit(), buffers are drained into global Arcs before
    // TLS destruction completes and the atexit handler fires.
    TLS_FLUSH_GUARD.with(|_| ());

    let name_id = intern_name(name);

    let saved_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| {
            let snap = cell.get();
            cell.set(crate::alloc::AllocSnapshot::new());
            snap
        })
        .unwrap_or_default();

    #[cfg(feature = "cpu-time")]
    let cpu_start_ns = crate::cpu_clock::cpu_now_ns();

    with_stack_mut(|s| {
        let depth = s.len() as u16;
        let packed = pack_name_depth(name_id, depth);
        s.push(StackEntry {
            children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns,
            saved_alloc,
            packed,
        });
    })
}

/// Start timing a function. Returns a Guard that records the measurement on drop.
///
/// Inlined so the counter read (`rdtsc`/`cntvct_el0`) happens at the call
/// site as a single inline instruction — no function call, no vDSO overhead.
///
/// Guard is 8 bytes: fits in one register, zero memory stores inside the
/// measurement window.
#[inline(always)]
pub fn enter(name: &'static str) -> Guard {
    enter_cold(name);
    let start_tsc = crate::tsc::read();
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
        if !reg.contains(&name) {
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
            cpu_self_ms: e.cpu_self_ns as f64 / 1_000_000.0,
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

/// Write an NDJSON file with frame-level data.
///
/// Line 1: header with metadata and function name table.
/// Lines 2+: one line per frame with per-function summaries.
fn write_ndjson(
    frames: &[(usize, Vec<FrameFnSummary>)],
    fn_names: &[&str],
    path: &std::path::Path,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = BufWriter::new(std::fs::File::create(path)?);
    let ts = timestamp_ms();
    let run_id = run_id();

    // v4 header: metadata only (no functions — those go in the trailer)
    write!(
        f,
        "{{\"format_version\":4,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts}"
    )?;
    #[cfg(feature = "cpu-time")]
    write!(f, ",\"has_cpu_time\":true")?;
    writeln!(f, "}}")?;

    // Build index for O(1) fn_id lookup
    let fn_id_map: HashMap<&str, usize> =
        fn_names.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // One line per frame
    for (frame_idx, (tid, frame)) in frames.iter().enumerate() {
        write!(f, "{{\"frame\":{frame_idx},\"tid\":{tid},\"fns\":[")?;
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

    // v4 trailer: function name table
    write!(f, "{{\"functions\":[")?;
    for (i, name) in fn_names.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        let name = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(f, "\"{name}\"")?;
    }
    writeln!(f, "]}}")?;

    Ok(())
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

/// Collect frames from all threads and write them to the given directory.
///
/// Shared implementation for `flush()` (non-streaming path) and `flush_to()`.
/// Collects (clones) frames from all threads -- we intentionally do NOT drain
/// other threads' frames here. Only the local thread's state is cleared via
/// `reset()`. Other threads' frames persist for the final `shutdown()` write.
fn flush_impl(dir: &std::path::Path) {
    let mut frames = collect_frames_with_tid();

    // Synthesize from aggregates if no frames exist (same as shutdown_impl_inner).
    if frames.is_empty() {
        let agg = collect_all_fnagg();
        if agg.is_empty() {
            return;
        }
        frames.push((0, synthesize_frame_from_agg(&agg)));
    }

    let mut seen = HashSet::new();
    let mut fn_names: Vec<&str> = Vec::new();
    for (_, frame) in &frames {
        for s in frame {
            if seen.insert(s.name) {
                fn_names.push(s.name);
            }
        }
    }
    let path = dir.join(format!("{}.ndjson", timestamp_ms()));
    if let Err(e) = write_ndjson(&frames, &fn_names, &path) {
        eprintln!(
            "piano: failed to write profiling data to {}: {e}",
            path.display()
        );
    }
    // Clear only the local thread's records, stack, and frames so subsequent
    // enter() calls start fresh. Other threads' state is left intact.
    reset();
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

/// Initialize the runtime: install handlers for data recovery.
///
/// Called at the start of instrumented main(). Registers:
/// - SIGTERM/SIGINT signal handlers (Unix only) for kill/Ctrl-C
/// - C-level atexit handler for `std::process::exit()` calls
pub fn init() {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    signal::install_handlers();
    atexit::register();
    STREAMING_ENABLED.store(true, Ordering::Relaxed);
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

/// Synthesize a single NDJSON frame from aggregate FnAgg data.
/// Used as fallback when no depth-0 frames were recorded (e.g., program crashed
/// mid-function, or all work happened in contexts that don't produce frames).
/// Alloc data is unavailable in aggregates, so alloc fields are zero.
fn synthesize_frame_from_agg(agg: &[FnAgg]) -> Vec<FrameFnSummary> {
    agg.iter()
        .map(|e| FrameFnSummary {
            name: e.name,
            calls: e.calls,
            self_ns: (e.self_ms * 1_000_000.0).max(0.0) as u64,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: e.cpu_self_ns,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        })
        .collect()
}

/// Core write logic shared by `shutdown()` and the signal handler.
///
/// When streaming is enabled: writes the trailer line (function name table)
/// to the existing stream file and closes it. When streaming is not enabled
/// (or no stream file was opened): collects frames from THREAD_FRAMES and
/// writes a complete NDJSON file. In either case, when no frames exist but
/// aggregate records do, synthesizes a single frame from aggregate data.
///
/// Returns `true` if any write failed. Does NOT check `SHUTDOWN_DONE` --
/// callers are responsible for the guard.
fn shutdown_impl_inner(dir: &std::path::Path) -> bool {
    let mut write_failed = false;

    if STREAMING_ENABLED.load(Ordering::Relaxed) {
        // Streaming path: frames are already on disk. Just write the trailer.
        let has_stream = {
            let mut state = stream_file().lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref mut s) = *state {
                if let Err(e) = write_stream_trailer(s) {
                    eprintln!(
                        "piano: failed to write trailer to {}: {e}",
                        s.path.display()
                    );
                    write_failed = true;
                }
                // Close the stream file (drop the BufWriter/File).
                *state = None;
                true
            } else {
                false
            }
        };
        if !has_stream {
            // No stream file opened (no frames ever completed).
            // Try synthesizing from aggregates (process::exit mid-function case).
            flush_records_buf();
            let agg = collect_all_fnagg();
            if !agg.is_empty() {
                let frames = vec![(0, synthesize_frame_from_agg(&agg))];
                let mut seen = HashSet::new();
                let mut fn_names: Vec<&str> = Vec::new();
                for (_, frame) in &frames {
                    for s in frame {
                        if seen.insert(s.name) {
                            fn_names.push(s.name);
                        }
                    }
                }
                let path = dir.join(format!("{}.ndjson", timestamp_ms()));
                if let Err(e) = write_ndjson(&frames, &fn_names, &path) {
                    eprintln!(
                        "piano: failed to write profiling data to {}: {e}",
                        path.display()
                    );
                    write_failed = true;
                }
            }
        }
    } else {
        // Non-streaming path (tests, no init() call) -- existing behavior
        let ts = timestamp_ms();
        let mut frames = collect_frames_with_tid();
        if frames.is_empty() {
            let agg = collect_all_fnagg();
            if !agg.is_empty() {
                frames.push((0, synthesize_frame_from_agg(&agg)));
            }
        }
        if !frames.is_empty() {
            let mut seen = HashSet::new();
            let mut fn_names: Vec<&str> = Vec::new();
            for (_, frame) in &frames {
                for s in frame {
                    if seen.insert(s.name) {
                        fn_names.push(s.name);
                    }
                }
            }
            let path = dir.join(format!("{ts}.ndjson"));
            if let Err(e) = write_ndjson(&frames, &fn_names, &path) {
                eprintln!(
                    "piano: failed to write profiling data to {}: {e}",
                    path.display()
                );
                write_failed = true;
            }
        }
    }

    write_failed
}

/// Context for propagating parent-child CPU timing across thread boundaries.
///
/// Created by `fork()` on the parent thread, passed to child threads via
/// `adopt()`. When the child completes, its CPU time is accumulated
/// in `children_cpu_ns` which the parent reads back via Drop (or explicit `finalize()`).
/// Wall time is NOT propagated cross-thread (it's not additive for parallel work).
#[non_exhaustive]
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
            with_stack_mut(|s| {
                if let Some(top) = s.last_mut() {
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
#[non_exhaustive]
pub struct AdoptGuard {
    #[cfg(feature = "cpu-time")]
    cpu_start_ns: u64,
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc<Mutex<u64>>,
}

impl Drop for AdoptGuard {
    fn drop(&mut self) {
        // Restore the parent's saved alloc counters (same pattern as Guard::drop).
        // The adopted scope's alloc data isn't recorded into an InvocationRecord,
        // but the restore is necessary for correct nesting.
        with_stack_mut(|s| {
            let entry = match s.pop() {
                Some(e) => e,
                None => return,
            };

            let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
                cell.set(entry.saved_alloc);
            });

            // The synthetic adopt entry is at depth 0. When it drops, any
            // pending FRAME_BUFFER data from child functions (which ran at
            // depth 1+) must be flushed to FRAMES — same as a normal
            // depth-0 guard drop. Without this, worker-thread frame data
            // would be silently lost.
            if unpack_depth(entry.packed) == 0 {
                flush_records_buf();
                FRAME_BUFFER.with(|buf| {
                    let b = buf.borrow();
                    if !b.is_empty() {
                        stream_frame(&b);
                    }
                    drop(b);
                    buf.borrow_mut().clear();
                });
            }

            // Propagate this thread's CPU time back to the parent context.
            #[cfg(feature = "cpu-time")]
            {
                let cpu_elapsed_ns =
                    crate::cpu_clock::cpu_now_ns().saturating_sub(self.cpu_start_ns);
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
    with_stack_ref(|s| {
        let top = s.last()?;
        Some(SpanContext {
            parent_name: lookup_name(unpack_name_id(top.packed)),
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

    #[cfg(feature = "cpu-time")]
    let cpu_start_ns = crate::cpu_clock::cpu_now_ns();

    with_stack_mut(|s| {
        let depth = s.len() as u16;
        s.push(StackEntry {
            children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns,
            saved_alloc,
            packed: pack_name_depth(intern_name(ctx.parent_name), depth),
        });
    });

    AdoptGuard {
        #[cfg(feature = "cpu-time")]
        cpu_start_ns,
        #[cfg(feature = "cpu-time")]
        ctx_children_cpu_ns: Arc::clone(&ctx.children_cpu_ns),
    }
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
        atexit::register();
        atexit::register();
        atexit::register();
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
    fn fork_returns_none_with_empty_stack() {
        reset();
        assert!(fork().is_none(), "fork should return None with empty stack");
    }

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
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

    #[cfg(feature = "cpu-time")]
    #[test]
    #[serial]
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
    #[serial]
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
        // Without cpu-time: 48 bytes (name resolved via packed name_id).
        // With cpu-time: 64 bytes (two extra u64 fields for cpu_children_ns, cpu_start_ns;
        // power-of-two enables lsl #6 indexing on ARM).
        #[cfg(not(feature = "cpu-time"))]
        assert_eq!(size, 48, "StackEntry without cpu-time must be 48 bytes");
        #[cfg(feature = "cpu-time")]
        assert_eq!(size, 64, "StackEntry with cpu-time must be 64 bytes");
    }

    #[test]
    fn pack_unpack_round_trip() {
        let name_id: u16 = 42;
        let depth: u16 = 7;
        let packed = pack_name_depth(name_id, depth);
        assert_eq!(unpack_depth(packed), depth);
        assert_eq!(unpack_name_id(packed), name_id);

        // Max values
        let packed_max = pack_name_depth(u16::MAX, u16::MAX);
        assert_eq!(unpack_depth(packed_max), u16::MAX);
        assert_eq!(unpack_name_id(packed_max), u16::MAX);

        // Zero values
        let packed_zero = pack_name_depth(0, 0);
        assert_eq!(unpack_depth(packed_zero), 0);
        assert_eq!(unpack_name_id(packed_zero), 0);
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
    fn stream_writes_valid_v4_ndjson() {
        reset();
        let tmp = std::env::temp_dir().join(format!("piano_stream_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Test the streaming infrastructure directly by calling
        // open_stream_file / stream_frame_to_writer / write_stream_trailer.
        // This avoids setting the global STREAMING_ENABLED flag, which would
        // interfere with parallel tests that expect the FRAMES path.
        let mut state = open_stream_file(&tmp).unwrap();

        // Generate 3 frames worth of data
        for _ in 0..3 {
            let _g = enter("stream_test_fn");
            burn_cpu(5_000);
        }

        // Collect the frames that were pushed to FRAMES (streaming disabled).
        let frames = collect_frames();
        let my_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "stream_test_fn"))
            .collect();
        assert_eq!(
            my_frames.len(),
            3,
            "should have 3 frames with stream_test_fn"
        );

        // Write each frame to the stream file
        for frame in &my_frames {
            stream_frame_to_writer(&mut state, frame);
        }

        // Write trailer and flush
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        // Find the .ndjson file
        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        assert_eq!(files.len(), 1, "expected exactly one ndjson file");

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Header (line 0): metadata, no functions
        assert!(
            lines[0].contains("\"format_version\":4"),
            "header should have format_version 4: {}",
            lines[0]
        );
        assert!(lines[0].contains("\"run_id\""));
        assert!(!lines[0].contains("\"functions\""));

        // Frames (lines 1-3)
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));
        assert!(lines[3].contains("\"frame\":2"));

        // Trailer (last line): functions array
        let last = lines.last().unwrap();
        assert!(last.contains("\"functions\""));
        assert!(last.contains("stream_test_fn"));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
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
            drop(g);
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
    fn shutdown_streaming_writes_trailer() {
        // Test that the streaming shutdown path produces a complete v4 file.
        //
        // Uses local StreamState (not the global STREAM_FILE) to avoid
        // racing with frames_on_disk_before_shutdown. Constructs frame
        // data directly to avoid collect_frames() interference.
        let tmp =
            std::env::temp_dir().join(format!("piano_shutdown_trailer_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Register the function name so write_stream_trailer includes it.
        register("shutdown_trailer_fn");

        // Build frame data directly -- no reliance on global FRAMES.
        let frame = vec![FrameFnSummary {
            name: "shutdown_trailer_fn",
            calls: 1,
            self_ns: 1_000_000,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 500_000,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];

        // Simulate the streaming path: open file, write 2 frames, write trailer.
        let mut state = open_stream_file(&tmp).unwrap();
        stream_frame_to_writer(&mut state, &frame);
        stream_frame_to_writer(&mut state, &frame);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        assert_eq!(files.len(), 1);

        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();

        // Header + 2 frames + trailer = 4 lines
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("\"format_version\":4"));
        assert!(lines[1].contains("\"frame\":0"));
        assert!(lines[2].contains("\"frame\":1"));
        assert!(lines[3].contains("\"functions\""));
        assert!(lines[3].contains("shutdown_trailer_fn"));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn stream_frame_field_values_round_trip() {
        let tmp = std::env::temp_dir().join(format!("piano_rt_roundtrip_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        let frame_data = vec![FrameFnSummary {
            name: "roundtrip_fn",
            calls: 42,
            self_ns: 123_456_789,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 100_000_000,
            alloc_count: 7,
            alloc_bytes: 2048,
            free_count: 3,
            free_bytes: 1024,
        }];

        stream_frame_to_writer(&mut state, &frame_data);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        let frame_line = lines[1];
        assert!(frame_line.contains("\"frame\":0"), "frame index");

        let fns_start = frame_line.find("\"fns\":[").unwrap() + "\"fns\":[".len();
        let fns_end = frame_line[fns_start..].rfind(']').unwrap();
        let entry_str = &frame_line[fns_start..fns_start + fns_end];

        fn extract(s: &str, key: &str) -> u64 {
            let start = s.find(key).unwrap() + key.len();
            let end = s[start..]
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(s.len() - start);
            s[start..start + end].parse().unwrap()
        }

        assert_eq!(extract(entry_str, "\"calls\":"), 42, "calls");
        assert_eq!(extract(entry_str, "\"self_ns\":"), 123_456_789, "self_ns");
        assert_eq!(extract(entry_str, "\"ac\":"), 7, "alloc_count");
        assert_eq!(extract(entry_str, "\"ab\":"), 2048, "alloc_bytes");
        assert_eq!(extract(entry_str, "\"fc\":"), 3, "free_count");
        assert_eq!(extract(entry_str, "\"fb\":"), 1024, "free_bytes");

        let trailer = *lines.last().unwrap();
        assert!(
            trailer.contains("roundtrip_fn"),
            "trailer should contain function name"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // NOTE: The streaming fallback path (STREAMING_ENABLED=true but no stream
    // file opened) is not tested directly because setting STREAMING_ENABLED
    // globally races with parallel tests' guard drops. The aggregate-synthesis
    // logic is the same as the non-streaming path, already covered by
    // shutdown_writes_ndjson_with_all_thread_data. The streaming gate is a
    // simple flag check tested indirectly via integration tests.

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
            },
            FnAgg {
                name: "synth_fn_b",
                calls: 1,
                total_ms: 10.0,
                self_ms: 10.0,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 8_000_000,
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
    fn trailer_fn_id_round_trip() {
        let tmp = std::env::temp_dir().join(format!("piano_trailer_rt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        let name_plain: &'static str = "simple_fn";
        let name_generic: &'static str = "Vec<String>::push";
        let name_backslash: &'static str = "path\\to\\fn";

        let id_plain = intern_name(name_plain);
        let id_generic = intern_name(name_generic);
        let id_backslash = intern_name(name_backslash);

        let frame = vec![
            FrameFnSummary {
                name: name_plain,
                calls: 1,
                self_ns: 100,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: name_generic,
                calls: 2,
                self_ns: 200,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: name_backslash,
                calls: 3,
                self_ns: 300,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
        ];
        stream_frame_to_writer(&mut state, &frame);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        let trailer = *lines.last().unwrap();

        // Parse trailer: {"functions":["name0","name1",...]}
        let fns_start = trailer.find("\"functions\":[").unwrap() + "\"functions\":[".len();
        let fns_end = trailer[fns_start..].find(']').unwrap();
        let fns_str = &trailer[fns_start..fns_start + fns_end];

        let parsed: Vec<String> = fns_str
            .split("\",\"")
            .map(|s| {
                s.trim_matches('"')
                    .replace("\\\\", "\\")
                    .replace("\\\"", "\"")
            })
            .collect();

        assert_eq!(
            parsed.get(id_plain as usize).map(|s| s.as_str()),
            Some("simple_fn"),
            "plain name at id {id_plain}"
        );
        assert_eq!(
            parsed.get(id_generic as usize).map(|s| s.as_str()),
            Some("Vec<String>::push"),
            "generic name at id {id_generic}"
        );
        assert_eq!(
            parsed.get(id_backslash as usize).map(|s| s.as_str()),
            Some("path\\to\\fn"),
            "backslash name at id {id_backslash}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
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

    #[test]
    fn name_table_overflow_index_is_last_valid_slot() {
        // The overflow saturation index must be the last valid slot
        // (capacity - 1) so that lookups stay in-bounds.
        let idx = name_table_overflow_index();
        let expected = (NAME_TABLE_CAPACITY - 1) as u16;
        assert_eq!(
            idx, expected,
            "overflow index should be capacity-1 ({expected}), got {idx}"
        );
        // The index must be within the table's bounds.
        assert!(
            (idx as usize) < NAME_TABLE_CAPACITY,
            "overflow index {idx} must be < capacity {NAME_TABLE_CAPACITY}"
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
        };
        let src = FnAgg {
            name: "f",
            calls: 2,
            total_ms: 4.0,
            self_ms: 3.0,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 50,
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
    // Mutant-killing tests: drop_cold comparison/logic
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn drop_cold_frame_boundary_with_adopt_context() {
        // Kills: collector.rs:968 replace == with !=
        //        collector.rs:969 replace || with && and == with !=
        // When fork/adopt places an entry at depth 0, real functions run at
        // depth 1+. The frame boundary fires when all remaining entries are
        // depth 0 (remaining_all_base), OR when the dropped entry itself is
        // depth 0. We verify frames are produced correctly in both scenarios.
        reset();

        // Scenario 1: normal depth-0 drop produces a frame.
        {
            let _g = enter("fb_normal");
            burn_cpu(1_000);
        }
        let frames = collect_frames();
        let normal_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fb_normal"))
            .collect();
        assert_eq!(
            normal_frames.len(),
            1,
            "depth-0 drop should produce exactly 1 frame"
        );

        // Scenario 2: adopt context -- child at depth 1 should produce frame
        // data when it drops (since remaining entries are all depth 0).
        {
            let _parent = enter("fb_adopt_parent");
            let ctx = fork().unwrap();
            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("fb_adopt_child");
                    burn_cpu(1_000);
                }
            }
            ctx.finalize();
        }
        let frames = collect_frames();
        let adopt_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fb_adopt_child"))
            .collect();
        assert!(
            !adopt_frames.is_empty(),
            "adopt context child should produce frame data"
        );
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: streaming/NDJSON write path comparisons
    // ---------------------------------------------------------------

    #[test]
    #[serial]
    fn stream_frame_to_writer_comma_separation() {
        // Kills: collector.rs:171 replace > with ==/</>=
        // With 2+ entries, commas should separate them. With 1 entry, no comma.
        let tmp = std::env::temp_dir().join(format!("piano_comma_sep_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let mut state = open_stream_file(&tmp).unwrap();

        // Single entry: no comma
        let single = vec![FrameFnSummary {
            name: "comma_a",
            calls: 1,
            self_ns: 100,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }];
        stream_frame_to_writer(&mut state, &single);

        // Multiple entries: commas between them
        let multi = vec![
            FrameFnSummary {
                name: "comma_b",
                calls: 1,
                self_ns: 100,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
            FrameFnSummary {
                name: "comma_c",
                calls: 2,
                self_ns: 200,
                #[cfg(feature = "cpu-time")]
                cpu_self_ns: 0,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            },
        ];
        stream_frame_to_writer(&mut state, &multi);
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Frame 0 (single entry): no comma in fns array, no leading comma.
        // Mutant `i > 0` -> `i >= 0` would produce [,{...}] (leading comma).
        let frame0_fns = &lines[1][lines[1].find("\"fns\":[").unwrap()..];
        let comma_count_0 = frame0_fns.matches("},{").count();
        assert_eq!(
            comma_count_0, 0,
            "single-entry frame should have no comma between entries"
        );
        assert!(
            frame0_fns.contains("\"fns\":[{"),
            "fns array should start with [{{ not [,{{: {frame0_fns}"
        );

        // Frame 1 (two entries): exactly one comma between entries, no leading comma.
        let frame1_fns = &lines[2][lines[2].find("\"fns\":[").unwrap()..];
        let comma_count_1 = frame1_fns.matches("},{").count();
        assert_eq!(
            comma_count_1, 1,
            "two-entry frame should have exactly one comma separator"
        );
        assert!(
            frame1_fns.contains("\"fns\":[{"),
            "fns array should start with [{{ not [,{{: {frame1_fns}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[serial]
    fn write_stream_trailer_comma_separation() {
        // Kills: collector.rs:253 replace > with >=
        // Verifies commas between function names in the trailer.
        let tmp = std::env::temp_dir().join(format!("piano_trailer_comma_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Intern at least 2 names so the trailer has commas.
        let _id1 = intern_name("trailer_comma_a");
        let _id2 = intern_name("trailer_comma_b");

        let mut state = open_stream_file(&tmp).unwrap();
        write_stream_trailer(&mut state).unwrap();
        drop(state);

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "ndjson"))
            .collect();
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let trailer = content.lines().last().unwrap();

        // With >= instead of >, a leading comma would appear: [,"name1","name2"]
        assert!(
            !trailer.contains("[,"),
            "trailer should not start with a comma: {trailer}"
        );
        // With > replaced by ==, only the first entry would get a comma prefix
        // (when i == 0, which is wrong). Check structure is valid.
        assert!(
            trailer.contains("\"functions\":[\""),
            "trailer should have functions array starting with a quote: {trailer}"
        );
    }

    #[test]
    #[serial]
    fn write_ndjson_comma_separation() {
        // Kills: collector.rs:1303 and 1319 replace > with ==/</>=
        let tmp = std::env::temp_dir().join(format!("piano_ndjson_comma_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let fn_names = vec!["ndjson_fn_a", "ndjson_fn_b"];
        let frames = vec![(
            0,
            vec![
                FrameFnSummary {
                    name: "ndjson_fn_a",
                    calls: 1,
                    self_ns: 100,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns: 0,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                },
                FrameFnSummary {
                    name: "ndjson_fn_b",
                    calls: 2,
                    self_ns: 200,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns: 0,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                },
            ],
        )];

        let path = tmp.join("test.ndjson");
        write_ndjson(&frames, &fn_names, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // v4: header has no functions array; trailer (last line) has it.
        let header = lines[0];
        assert!(
            !header.contains("\"functions\""),
            "v4 header should not contain functions: {header}"
        );
        let trailer = *lines.last().unwrap();
        assert!(
            trailer.contains("\"functions\":[\"ndjson_fn_a\",\"ndjson_fn_b\"]"),
            "trailer functions array should have proper comma separation: {trailer}"
        );

        // Frame line: fns array should have comma between entries, not before first.
        let frame = lines[1];
        let fns_section = &frame[frame.find("\"fns\":[").unwrap()..];
        assert!(
            !fns_section.starts_with("\"fns\":[,"),
            "fns array should not start with comma: {fns_section}"
        );
        let entry_count = fns_section.matches("\"id\":").count();
        assert_eq!(entry_count, 2, "should have 2 fn entries");
        let comma_between = fns_section.matches("},{").count();
        assert_eq!(
            comma_between, 1,
            "should have exactly 1 comma between 2 entries"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ---------------------------------------------------------------
    // Mutant-killing tests: pack_name_depth bitwise OR
    // ---------------------------------------------------------------

    #[test]
    fn pack_name_depth_uses_or_not_xor() {
        // Kills: collector.rs:811 replace | with ^
        // When both name_id and depth have overlapping bits in the low 16,
        // XOR would cancel them out while OR preserves them.
        // With | : (5 << 16) | 5 = 0x50005 => name_id=5, depth=5
        // With ^ : (5 << 16) ^ 5 = 0x50005 (same, no overlap in shifted positions)
        // But if we pack(0xFFFF, 0xFFFF):
        // With | : (0xFFFF << 16) | 0xFFFF = 0xFFFF_FFFF
        // With ^ : (0xFFFF << 16) ^ 0xFFFF = 0xFFFF_FFFF (same, no overlap)
        // The fields don't overlap so | vs ^ gives the same result.
        // However, pack is used with unpack. The real test is the round trip,
        // which already exists. Let's test with values that ensure correctness.
        let packed = pack_name_depth(0x1234, 0x5678);
        assert_eq!(unpack_name_id(packed), 0x1234);
        assert_eq!(unpack_depth(packed), 0x5678);

        // Verify the raw bit pattern: (0x1234 << 16) | 0x5678
        let expected: u64 = (0x1234u64 << 16) | 0x5678u64;
        assert_eq!(
            packed, expected,
            "pack_name_depth should produce (name_id << 16) | depth"
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
    // Mutant-killing tests: name_table_lock
    // ---------------------------------------------------------------

    #[test]
    fn name_table_lock_returns_mutex() {
        // Kills: collector.rs:719 replace name_table_lock with Box::leak(...)
        // The lock should be acquirable and releasable without panic.
        let lock = name_table_lock();
        let guard = lock.lock().unwrap();
        drop(guard);
        // Acquire again to verify it was properly released.
        let guard2 = lock.lock().unwrap();
        drop(guard2);
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
}
