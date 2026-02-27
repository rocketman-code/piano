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
use std::sync::atomic::{compiler_fence, Ordering};
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
/// All threads within a single process share this ID, so that JSON files
/// written by different threads can be correlated during report consolidation.
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
static RUNS_DIR: SyncOnceCell<Mutex<Option<PathBuf>>> = SyncOnceCell::new();

fn runs_dir_lock() -> &'static Mutex<Option<PathBuf>> {
    RUNS_DIR.get_or_init(|| Mutex::new(None))
}

fn epoch() -> Instant {
    *EPOCH.get_or_init(|| {
        crate::tsc::calibrate();
        let tsc_val = crate::tsc::read();
        let now = Instant::now();
        crate::tsc::set_epoch_tsc(tsc_val);
        now
    })
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
    pub(crate) children_ms: f64,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_children_ns: u64,
    #[cfg(feature = "cpu-time")]
    pub(crate) cpu_start_ns: u64,
    /// Saved ALLOC_COUNTERS from before this scope, restored on Guard::drop.
    pub(crate) saved_alloc: crate::alloc::AllocSnapshot,
    /// Packed identity: `[cookie:32][name_id:16][depth:16]`.
    ///
    /// Regular entries: matches the Guard's packed value (same thread pushed it).
    /// Phantom entries: the migrated Guard's packed identity (different thread
    /// cookie), used as a unique key for PHANTOM_REGISTRY / PHANTOM_ARCS lookup.
    pub(crate) packed: u64,
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

/// Per-phantom tracking for cross-thread forwarding and cleanup.
///
/// When `check()` pushes a phantom on thread B, it registers a `PhantomInfo`
/// in the global `PHANTOM_REGISTRY`. This enables:
/// - Forwarding: children_ms accumulated on B is readable by C via the Arc.
/// - Cleanup: `host_cookie` identifies B so the cleanup queue can target it.
struct PhantomInfo {
    /// Thread cookie of the thread hosting this phantom StackEntry.
    host_cookie: u64,
    /// Shared children_ms accumulator. The host thread writes to this Arc
    /// (via PHANTOM_ARCS) when children update the phantom; the next thread
    /// reads from it to seed its own phantom.
    children_arc: Arc<Mutex<f64>>,
}

/// Global registry mapping guard packed identity -> phantom info.
///
/// `check()` inserts when pushing a phantom. On re-migration, `check()` reads
/// the old entry (forwarding + cleanup scheduling) and inserts a new one.
/// `drop_cold` removes the final entry.
static PHANTOM_REGISTRY: SyncOnceCell<Mutex<HashMap<u64, PhantomInfo>>> = SyncOnceCell::new();

fn phantom_registry() -> &'static Mutex<HashMap<u64, PhantomInfo>> {
    PHANTOM_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Cleanup queue: `(host_cookie, guard_packed)` pairs. Thread B drains entries
/// where `host_cookie == B` on its next `enter_cold`, removing the stale phantom
/// from its stack.
static PHANTOM_CLEANUP: SyncOnceCell<Mutex<Vec<(u64, u64)>>> = SyncOnceCell::new();

fn phantom_cleanup() -> &'static Mutex<Vec<(u64, u64)>> {
    PHANTOM_CLEANUP.get_or_init(|| Mutex::new(Vec::new()))
}

/// Fast-path guard: skip the cleanup lock when the queue is empty.
static HAS_PHANTOM_CLEANUP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

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
    #[cfg(any(test, feature = "_test_internals"))]
    static INVOCATIONS: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    /// Invocations accumulated within the current frame (cleared on frame boundary).
    static FRAME_BUFFER: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    /// Completed per-frame summaries.
    static FRAMES: RefCell<Vec<Vec<FrameFnSummary>>> = RefCell::new(Vec::new());
    /// Per-thread phantom forwarding Arcs. When a phantom StackEntry exists on
    /// this thread (from Guard::check()), the corresponding Arc is stored here
    /// so child drops can write through to it.
    static PHANTOM_ARCS: RefCell<Vec<(u64, Arc<Mutex<f64>>)>> = RefCell::new(Vec::new());
}

/// Sequential per-thread identifier. Cheaper than `std::thread::current().id()`
/// and packable into a u64 alongside the stack depth.
static NEXT_THREAD_COOKIE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

thread_local! {
    static THREAD_COOKIE: u64 = NEXT_THREAD_COOKIE.fetch_add(1, Ordering::Relaxed);
}

// -- Interned function name table ------------------------------------------
//
// Maps u16 IDs to `&'static str` function names so the Guard can carry a
// compact name reference without growing beyond 16 bytes.
//
// Layout: append-only Vec behind a Mutex. Reads during drop_cold take the
// lock briefly; writes happen once per unique name in enter_cold.
// A thread-local cache avoids the global lock on the hot path.

/// Global interned name table: index -> &'static str.
static NAME_TABLE: SyncOnceCell<Mutex<Vec<&'static str>>> = SyncOnceCell::new();

fn name_table() -> &'static Mutex<Vec<&'static str>> {
    NAME_TABLE.get_or_init(|| Mutex::new(Vec::new()))
}

// Thread-local cache mapping name pointer -> interned ID.
// Uses pointer identity (`&'static str` addresses are stable).
thread_local! {
    static NAME_CACHE: RefCell<HashMap<usize, u16>> = RefCell::new(HashMap::new());
}

/// Intern a function name, returning its u16 ID.
/// Fast path: thread-local cache hit (no global lock).
/// Slow path: global table lookup/insert under lock, then cache.
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
    let mut table = name_table().lock().unwrap_or_else(|e| e.into_inner());
    // Check if already in global table (another thread may have added it).
    let id = if let Some(pos) = table.iter().position(|&n| n.as_ptr() as usize == ptr) {
        pos as u16
    } else {
        let len = table.len();
        debug_assert!(
            len <= u16::MAX as usize,
            "interned name table overflow: more than 65535 unique function names"
        );
        if len > u16::MAX as usize {
            // Table full (65536 entries, indices 0..=u16::MAX). Saturate
            // instead of wrapping. lookup_name handles out-of-bounds by
            // returning "<unknown>", so this degrades gracefully.
            return u16::MAX;
        }
        let id = len as u16;
        table.push(name);
        id
    };
    drop(table);
    NAME_CACHE.with(|cache| {
        cache.borrow_mut().insert(ptr, id);
    });
    id
}

/// Look up a function name by its interned ID.
/// Returns `"<unknown>"` if the ID is out of bounds (should never happen).
fn lookup_name(id: u16) -> &'static str {
    let table = name_table().lock().unwrap_or_else(|e| e.into_inner());
    table.get(id as usize).copied().unwrap_or("<unknown>")
}

/// Pack a thread cookie (32 bits), name ID (16 bits), and stack depth (16 bits)
/// into a single u64. Guard stays 16 bytes.
///
/// Layout: `[cookie:32][name_id:16][depth:16]`
///
/// Note: only the low 32 bits of the cookie are stored, limiting unique thread
/// identification to ~4 billion threads per process. This is a deliberate
/// tradeoff (from a previous 48-bit cookie) to make room for the 16-bit
/// name_id. If THREAD_COOKIE exceeds 2^32, `unpack_cookie` returns only the
/// low 32 bits, which could cause false migration detection in `drop_cold`.
#[inline(always)]
fn pack_cookie_name_depth(cookie: u64, name_id: u16, depth: u16) -> u64 {
    (cookie << 32) | ((name_id as u64) << 16) | (depth as u64)
}

/// Unpack the thread cookie (high 32 bits) from a packed u64.
#[inline(always)]
fn unpack_cookie(packed: u64) -> u64 {
    packed >> 32
}

/// Unpack the name ID (bits 16..31) from a packed u64.
#[inline(always)]
fn unpack_name_id(packed: u64) -> u16 {
    (packed >> 16) as u16
}

/// Unpack the stack depth (low 16 bits) from a packed u64.
#[inline(always)]
fn unpack_depth(packed: u64) -> u16 {
    packed as u16
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
pub struct Guard {
    start_tsc: u64,
    /// Bit layout: `[cookie:32][name_id:16][depth:16]`
    /// - cookie: identifies the thread that called enter()
    /// - name_id: index into the global interned name table
    /// - depth: stack depth at the time of enter()
    packed: u64,
}

// Guard must be Send so async runtimes can move futures containing guards
// across worker threads.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<Guard>();
    }
    // Verify Guard is exactly 16 bytes (fits in 2 registers).
    fn _assert_size() {
        let _ = core::mem::transmute::<Guard, [u8; 16]>;
    }
};

impl Guard {
    /// Check for thread migration after an .await point.
    ///
    /// If the Guard has migrated to a different thread, pushes a phantom
    /// StackEntry onto the new thread's stack so that children can update
    /// it via the normal parent fast path. Also registers the Guard as
    /// migrated so the original thread can clean up the orphaned entry.
    ///
    /// No-op if still on the same thread. Idempotent: safe to call
    /// multiple times (skips if phantom already exists on current stack).
    pub fn check(&self) {
        let current_cookie = THREAD_COOKIE.with(|c| *c);
        let enter_cookie = unpack_cookie(self.packed);
        if current_cookie == enter_cookie {
            return; // Same thread, no migration
        }

        // Push phantom on current thread's stack (idempotent).
        // Children will update it via the normal parent fast path.
        STACK.with(|stack| {
            let mut s = stack.borrow_mut();
            // Check if phantom already exists for this guard (exact packed match).
            let already_has = s
                .iter()
                .any(|e| e.packed == self.packed && e.name == "<phantom>");
            if already_has {
                return;
            }

            // Read forwarded children_ms from a previous thread's phantom and
            // schedule cleanup of the old host thread's phantom (multi-hop
            // migration: A -> B -> C).
            let (forwarded_children_ms, fwd_arc) = {
                // Lock registry: remove old entry (if any) and insert new one.
                let arc = Arc::new(Mutex::new(0.0));
                let old_info = {
                    let mut reg = phantom_registry().lock().unwrap_or_else(|e| e.into_inner());
                    let old = reg.remove(&self.packed);
                    reg.insert(
                        self.packed,
                        PhantomInfo {
                            host_cookie: current_cookie,
                            children_arc: Arc::clone(&arc),
                        },
                    );
                    old
                }; // registry lock dropped here

                // Process old entry (if any) without holding registry lock.
                let forwarded = if let Some(old) = old_info {
                    // Schedule cleanup of old host's phantom.
                    let mut cleanup = phantom_cleanup().lock().unwrap_or_else(|e| e.into_inner());
                    cleanup.push((old.host_cookie, self.packed));
                    HAS_PHANTOM_CLEANUP.store(true, Ordering::Relaxed);
                    drop(cleanup); // cleanup lock dropped

                    let val = *old.children_arc.lock().unwrap_or_else(|e| e.into_inner());
                    *arc.lock().unwrap_or_else(|e| e.into_inner()) = val;
                    val
                } else {
                    0.0
                };

                (forwarded, arc)
            };

            // Store the Arc for write-through from child drops.
            PHANTOM_ARCS.with(|arcs| {
                arcs.borrow_mut().push((self.packed, fwd_arc));
            });

            s.push(StackEntry {
                name: "<phantom>",
                children_ms: forwarded_children_ms,
                #[cfg(feature = "cpu-time")]
                cpu_children_ns: 0,
                #[cfg(feature = "cpu-time")]
                cpu_start_ns: 0,
                saved_alloc: crate::alloc::AllocSnapshot::new(),
                packed: self.packed,
            });
        });
    }
}

/// Bookkeeping half of Guard::drop(): thread check, alloc restore, stack pop,
/// recording. Kept out-of-line so the inlined drop is just a counter read + call.
#[inline(never)]
fn drop_cold(guard: &Guard, end_tsc: u64, #[cfg(feature = "cpu-time")] cpu_end_ns: u64) {
    let drop_cookie = THREAD_COOKIE.with(|c| *c);
    let enter_cookie = unpack_cookie(guard.packed);
    let migrated = drop_cookie != enter_cookie;

    if migrated {
        let name = lookup_name(unpack_name_id(guard.packed));
        let elapsed_ns = crate::tsc::elapsed_ns(guard.start_tsc, end_tsc);
        let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
        let start_ns = crate::tsc::ticks_to_epoch_ns(guard.start_tsc, crate::tsc::epoch_tsc());

        // Post-migration children (Case 2): find and pop our phantom
        // StackEntry. Its children_ms was updated by children via the
        // normal parent fast path, and seeded with forwarded children
        // from previous thread hops (multi-hop migration).
        let phantom_children_ms = STACK.with(|stack| {
            let mut s = stack.borrow_mut();
            if let Some(pos) = s
                .iter()
                .rposition(|e| e.packed == guard.packed && e.name == "<phantom>")
            {
                let phantom = s.remove(pos);
                phantom.children_ms
            } else {
                0.0
            }
        });

        // Clean up the phantom registry and PHANTOM_ARCS for this guard.
        {
            let mut reg = phantom_registry().lock().unwrap_or_else(|e| e.into_inner());
            reg.remove(&guard.packed);
        }
        PHANTOM_ARCS.with(|arcs| {
            arcs.borrow_mut().retain(|(pk, _)| *pk != guard.packed);
        });

        let children_ns = (phantom_children_ms * 1_000_000.0) as u64;
        let self_ns = elapsed_ns.saturating_sub(children_ns);

        RECORDS.with(|records| {
            records
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(RawRecord {
                    name,
                    elapsed_ms,
                    children_ms: phantom_children_ms,
                    #[cfg(feature = "cpu-time")]
                    cpu_self_ns: 0,
                });
        });

        let invocation = InvocationRecord {
            name,
            start_ns,
            elapsed_ns,
            self_ns,
            #[cfg(feature = "cpu-time")]
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
            depth: 0,
        };

        #[cfg(any(test, feature = "_test_internals"))]
        INVOCATIONS.with(|inv| {
            inv.borrow_mut().push(invocation.clone());
        });

        FRAME_BUFFER.with(|buf| {
            buf.borrow_mut().push(invocation);
        });
        return;
    }

    // Same thread -- existing logic with orphan drain prefix.
    let scope_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| cell.get())
        .unwrap_or_default();

    STACK.with(|stack| {
        // Drain orphaned entries left by migrated child guards.
        {
            let mut s = stack.borrow_mut();
            let guard_depth = unpack_depth(guard.packed);
            while s
                .last()
                .map_or(false, |e| unpack_depth(e.packed) > guard_depth)
            {
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

        let elapsed_ns = crate::tsc::elapsed_ns(guard.start_tsc, end_tsc);
        let elapsed_ms = elapsed_ns as f64 / 1_000_000.0;
        let children_ns = (entry.children_ms * 1_000_000.0) as u64;
        let self_ns = elapsed_ns.saturating_sub(children_ns);
        let start_ns = crate::tsc::ticks_to_epoch_ns(guard.start_tsc, crate::tsc::epoch_tsc());
        let children_ms = entry.children_ms;

        #[cfg(feature = "cpu-time")]
        let cpu_elapsed_ns = cpu_end_ns.saturating_sub(entry.cpu_start_ns);
        #[cfg(feature = "cpu-time")]
        let cpu_self_ns = cpu_elapsed_ns.saturating_sub(entry.cpu_children_ns);

        if let Some(parent) = stack.borrow_mut().last_mut() {
            parent.children_ms += elapsed_ms;
            // If parent is a phantom (cookie differs from this thread),
            // write through to its forwarding Arc so subsequent thread
            // hops can read the accumulated children_ms.
            if unpack_cookie(parent.packed) != drop_cookie {
                let children = parent.children_ms;
                let pk = parent.packed;
                PHANTOM_ARCS.with(|arcs| {
                    if let Some((_, arc)) = arcs.borrow().iter().find(|(k, _)| *k == pk) {
                        *arc.lock().unwrap_or_else(|e| e.into_inner()) = children;
                    }
                });
            }
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
            depth: unpack_depth(entry.packed),
        };

        #[cfg(any(test, feature = "_test_internals"))]
        INVOCATIONS.with(|inv| {
            inv.borrow_mut().push(invocation.clone());
        });

        FRAME_BUFFER.with(|buf| {
            buf.borrow_mut().push(invocation);
        });
        if unpack_depth(entry.packed) == 0 {
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

/// Drain stale phantom StackEntries scheduled for cleanup on this thread.
///
/// Called from `enter_cold` before pushing the new entry. The cleanup queue
/// is populated by `check()` on other threads when a guard re-migrates
/// (e.g., A -> B -> C: C schedules B's phantom for cleanup).
fn drain_phantom_cleanup(my_cookie: u64) {
    if !HAS_PHANTOM_CLEANUP.load(Ordering::Relaxed) {
        return;
    }
    let mut queue = phantom_cleanup().lock().unwrap_or_else(|e| e.into_inner());
    let mine: Vec<u64> = queue
        .iter()
        .filter(|(cookie, _)| *cookie == my_cookie)
        .map(|(_, packed)| *packed)
        .collect();
    queue.retain(|(cookie, _)| *cookie != my_cookie);
    if queue.is_empty() {
        HAS_PHANTOM_CLEANUP.store(false, Ordering::Relaxed);
    }
    drop(queue);

    if mine.is_empty() {
        return;
    }

    // Remove matching phantom StackEntries from this thread's stack.
    STACK.with(|stack| {
        stack
            .borrow_mut()
            .retain(|e| !(e.name == "<phantom>" && mine.contains(&e.packed)));
    });
    // Remove matching PHANTOM_ARCS entries.
    PHANTOM_ARCS.with(|arcs| {
        arcs.borrow_mut()
            .retain(|(packed, _)| !mine.contains(packed));
    });
}

/// Bookkeeping half of enter(): epoch, alloc save, stack push, name interning.
/// Returns a packed u64: `[cookie:32][name_id:16][depth:16]`.
#[inline(never)]
fn enter_cold(name: &'static str) -> u64 {
    let _ = epoch();

    let cookie = THREAD_COOKIE.with(|c| *c);

    // Drain stale phantoms before computing depth, so the new entry
    // gets the correct stack position.
    drain_phantom_cleanup(cookie);

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

    STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        let packed = pack_cookie_name_depth(cookie, name_id, depth);
        stack.borrow_mut().push(StackEntry {
            name,
            children_ms: 0.0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns,
            saved_alloc,
            packed,
        });
        packed
    })
}

/// Start timing a function. Returns a Guard that records the measurement on drop.
///
/// Inlined so the counter read (`rdtsc`/`cntvct_el0`) happens at the call
/// site as a single inline instruction — no function call, no vDSO overhead.
///
/// Guard is 16 bytes: fits in two registers on both x86_64 and aarch64,
/// so the compiler keeps it in registers across the call — zero memory
/// stores inside the measurement window.
#[inline(always)]
pub fn enter(name: &'static str) -> Guard {
    let packed = enter_cold(name);
    let start_tsc = crate::tsc::read();
    Guard { start_tsc, packed }
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
#[cfg(any(test, feature = "_test_internals"))]
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
    // into_values() requires Rust 1.54; we support 1.59 but keep the pattern uniform
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
///
/// Also drains any pending phantom cleanup entries targeting this thread
/// from the global queue, so stale phantoms don't leak across test runs.
pub fn reset() {
    STACK.with(|stack| stack.borrow_mut().clear());
    RECORDS.with(|records| {
        records.lock().unwrap_or_else(|e| e.into_inner()).clear();
    });
    REGISTERED.with(|reg| reg.borrow_mut().clear());
    #[cfg(any(test, feature = "_test_internals"))]
    INVOCATIONS.with(|inv| inv.borrow_mut().clear());
    FRAME_BUFFER.with(|buf| buf.borrow_mut().clear());
    FRAMES.with(|frames| frames.borrow_mut().clear());
    PHANTOM_ARCS.with(|arcs| arcs.borrow_mut().clear());
    // Drain any pending cleanup entries for this thread from the global queue.
    let cookie = THREAD_COOKIE.with(|c| *c);
    drain_phantom_cleanup(cookie);
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
    // Clear global phantom state.
    phantom_registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    {
        let mut cleanup = phantom_cleanup().lock().unwrap_or_else(|e| e.into_inner());
        cleanup.clear();
        HAS_PHANTOM_CLEANUP.store(false, Ordering::Relaxed);
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

/// Configure the directory where run files should be written.
///
/// Called by CLI-injected code at the start of `main()` so that both
/// `flush()` and `shutdown()` write to the project-local directory.
/// `PIANO_RUNS_DIR` env var still takes priority (for testing and user
/// overrides).
pub fn set_runs_dir(dir: &str) {
    *runs_dir_lock().lock().unwrap_or_else(|e| e.into_inner()) = Some(PathBuf::from(dir));
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
        "{{\"format_version\":3,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts}"
    )?;
    #[cfg(feature = "cpu-time")]
    write!(f, ",\"has_cpu_time\":true")?;
    write!(f, ",\"functions\":[")?;
    for (i, name) in fn_names.iter().enumerate() {
        if i > 0 {
            write!(f, ",")?;
        }
        let name = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(f, "\"{name}\"")?;
    }
    writeln!(f, "]}}")?;

    // Build index for O(1) fn_id lookup
    let fn_id_map: HashMap<&str, usize> =
        fn_names.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // One line per frame
    for (frame_idx, frame) in frames.iter().enumerate() {
        write!(f, "{{\"frame\":{frame_idx},\"fns\":[")?;
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
        let path = dir.join(format!("{ts}.ndjson"));
        let _ = write_ndjson(&frames, &fn_names, &path);
    }

    // Always write aggregated cross-thread data (JSON format).
    let records = collect_all();
    if !records.is_empty() {
        let path = dir.join(format!("{ts}.json"));
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
    cpu_start_ns: u64,
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

    #[cfg(feature = "cpu-time")]
    let cpu_start_ns = crate::cpu_clock::cpu_now_ns();

    let cookie = THREAD_COOKIE.with(|c| *c);
    STACK.with(|stack| {
        let depth = stack.borrow().len() as u16;
        stack.borrow_mut().push(StackEntry {
            name: ctx.parent_name,
            children_ms: 0.0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns,
            saved_alloc,
            packed: pack_cookie_name_depth(cookie, intern_name(ctx.parent_name), depth),
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
            "migrated guard should preserve function name 'migrating_fn'. Got: {:?}",
            records.iter().map(|r| &r.name).collect::<Vec<_>>()
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

    #[test]
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
        set_runs_dir(tmp.to_str().unwrap());
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
    fn shutdown_to_sets_runs_dir_for_flush() {
        // shutdown_to() should set the global runs dir so that any
        // earlier flush() calls (e.g. from panic hooks) would have
        // used the same directory. After shutdown_to(), the dir should
        // be set so future flushes also use it.
        reset();

        // Produce data, then call set_runs_dir + flush to simulate
        // the scenario where the CLI injects set_runs_dir at start
        // of main, and flush() is called mid-program.
        let tmp = std::env::temp_dir().join(format!("piano_shutdown_to_{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();

        // First: set_runs_dir early (like CLI injection at start of main)
        set_runs_dir(tmp.to_str().unwrap());

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

        // shutdown_to uses the same dir.
        shutdown_to(tmp.to_str().unwrap());
        clear_runs_dir();

        let files: Vec<_> = std::fs::read_dir(&tmp)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        // Should have files from both flush and shutdown_to.
        assert!(
            files.len() >= 2,
            "expected files from both flush() and shutdown_to(), got {} in {tmp:?}",
            files.len()
        );

        let _ = std::fs::remove_dir_all(&tmp);
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
        let rec = records
            .iter()
            .find(|r| r.name == "cpu_migrated")
            .expect("migrated guard should preserve name 'cpu_migrated'");
        assert!(rec.total_ms > 0.0, "wall time captured");
        assert!(
            rec.cpu_self_ms == 0.0,
            "cpu_self_ms should be exactly 0 for migrated guard, got {:.3}",
            rec.cpu_self_ms
        );
    }

    #[test]
    fn stack_entry_is_64_bytes() {
        assert_eq!(
            core::mem::size_of::<StackEntry>(),
            64,
            "StackEntry must be exactly 64 bytes to preserve lsl #6 indexing"
        );
    }

    #[test]
    fn guard_check_pushes_phantom_on_migration() {
        reset();
        let guard = enter("check_parent");
        std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                STACK.with(|stack| {
                    let s = stack.borrow();
                    assert_eq!(s.len(), 1, "phantom should be pushed");
                    assert_eq!(s[0].name, "<phantom>");
                });
                drop(guard);
            });
        });
    }

    #[test]
    fn guard_check_is_noop_on_same_thread() {
        reset();
        let guard = enter("same_thread");
        guard.check();
        STACK.with(|stack| {
            let s = stack.borrow();
            assert_eq!(s.len(), 1, "no phantom on same thread");
            assert_eq!(s[0].name, "same_thread");
        });
        drop(guard);
    }

    #[test]
    fn guard_check_is_idempotent() {
        reset();
        let guard = enter("idempotent");
        std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                guard.check();
                STACK.with(|stack| {
                    let s = stack.borrow();
                    assert_eq!(s.len(), 1, "only one phantom after two checks");
                });
                drop(guard);
            });
        });
    }

    #[test]
    fn migrated_parent_subtracts_post_migration_children() {
        reset();
        let parent_guard = enter("mig_parent");
        let invocations = std::thread::scope(|s| {
            s.spawn(move || {
                parent_guard.check();
                {
                    let _child = enter("mig_child");
                    burn_cpu(20_000);
                }
                drop(parent_guard);
                collect_invocations()
            })
            .join()
            .unwrap()
        });

        let parent_inv = invocations
            .iter()
            .find(|r| r.name == "mig_parent")
            .expect("migrated parent should preserve name 'mig_parent'");
        let child_inv = invocations
            .iter()
            .find(|r| r.name == "mig_child")
            .expect("child should produce an invocation");

        assert!(
            parent_inv.self_ns < parent_inv.elapsed_ns,
            "self_ns ({}) should be < elapsed_ns ({}) after subtracting child",
            parent_inv.self_ns,
            parent_inv.elapsed_ns,
        );
        assert!(
            child_inv.elapsed_ns > 500_000,
            "child should have substantial elapsed time, got {}",
            child_inv.elapsed_ns,
        );
    }

    #[test]
    fn migrated_record_has_children_subtracted_in_collect() {
        reset();
        let parent_guard = enter("rec_parent");
        std::thread::scope(|s| {
            s.spawn(move || {
                parent_guard.check();
                {
                    let _child = enter("rec_child");
                    burn_cpu(20_000);
                }
                drop(parent_guard);
            });
        });

        let records = collect_all();
        let parent_rec = records
            .iter()
            .find(|r| r.name == "rec_parent")
            .expect("migrated parent should preserve name 'rec_parent'");

        assert!(
            parent_rec.self_ms < parent_rec.total_ms,
            "self_ms ({:.3}) should be < total_ms ({:.3})",
            parent_rec.self_ms,
            parent_rec.total_ms,
        );
    }

    #[test]
    fn root_function_does_not_affect_migrated_guard() {
        reset();
        {
            let _root = enter("root_fn");
            burn_cpu(20_000);
        }

        let guard = std::thread::scope(|s| s.spawn(|| enter("other_thread")).join().unwrap());
        guard.check();
        drop(guard);

        let invocations = collect_invocations();
        let migrated = invocations
            .iter()
            .find(|r| r.name == "other_thread")
            .expect("migrated guard should preserve name 'other_thread'");

        assert_eq!(
            migrated.self_ns, migrated.elapsed_ns,
            "migrated guard with no children: self_ns ({}) should equal elapsed_ns ({})",
            migrated.self_ns, migrated.elapsed_ns,
        );
    }

    #[test]
    fn phantom_on_second_migration_captures_children() {
        // A->B->C migration: guard enters on A, migrates to B (phantom on B,
        // child on B updates it), migrates to C (phantom on C, child on C
        // updates it), drops on C. Both B's and C's phantom children should
        // be subtracted from the migrated record's self_ns via forwarding.
        reset();
        let guard = enter("bc_parent");
        let (guard, _b_invocations) = std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                {
                    let _child = enter("b_child");
                    burn_cpu(10_000);
                }
                let inv = collect_invocations();
                (guard, inv)
            })
            .join()
            .unwrap()
        });

        let c_invocations = std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                {
                    let _child = enter("c_child");
                    burn_cpu(10_000);
                }
                drop(guard);
                collect_invocations()
            })
            .join()
            .unwrap()
        });

        let b_child_ns = _b_invocations
            .iter()
            .find(|r| r.name == "b_child")
            .expect("b_child invocation")
            .elapsed_ns;

        let c_child_ns = c_invocations
            .iter()
            .find(|r| r.name == "c_child")
            .expect("c_child invocation")
            .elapsed_ns;

        let migrated = c_invocations
            .iter()
            .find(|r| r.name == "bc_parent")
            .expect("migrated guard should preserve name 'bc_parent'");

        // Both B's and C's children should be subtracted from self_ns.
        let children_ns = migrated.elapsed_ns - migrated.self_ns;
        assert!(
            migrated.self_ns < migrated.elapsed_ns,
            "self_ns ({}) should be < elapsed_ns ({}) with children on B and C",
            migrated.self_ns,
            migrated.elapsed_ns,
        );
        // Verify B's children were forwarded: children_ns should account
        // for both b_child and c_child (with tolerance for timing noise).
        let expected_children_min = (b_child_ns + c_child_ns) / 2;
        assert!(
            children_ns >= expected_children_min,
            "children_ns ({children_ns}) should include both b_child ({b_child_ns}) \
             and c_child ({c_child_ns}) (min threshold: {expected_children_min})",
        );
    }

    #[test]
    fn multiple_checks_on_same_thread_are_idempotent() {
        reset();
        let guard = enter("multi_check");
        std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                {
                    let _child1 = enter("child1");
                    burn_cpu(10_000);
                }
                guard.check();
                {
                    let _child2 = enter("child2");
                    burn_cpu(10_000);
                }
                STACK.with(|stack| {
                    let s = stack.borrow();
                    assert_eq!(s.len(), 1, "only one phantom on stack");
                    assert!(
                        s[0].children_ms > 0.0,
                        "phantom should have accumulated children_ms"
                    );
                });
                drop(guard);
            });
        });
    }

    #[test]
    fn migrated_guard_preserves_function_name() {
        // Migrated guards should report the actual function name,
        // not a generic "<migrated>" placeholder.
        reset();
        let guard = enter("real_fn_name");
        burn_cpu(10_000);

        std::thread::scope(|s| {
            s.spawn(move || {
                burn_cpu(10_000);
                drop(guard);
            });
        });

        let records = collect_all();
        let rec = records.iter().find(|r| r.name == "real_fn_name");
        assert!(
            rec.is_some(),
            "migrated guard should preserve function name 'real_fn_name'. Got: {:?}",
            records.iter().map(|r| &r.name).collect::<Vec<_>>()
        );
        assert!(
            rec.unwrap().total_ms > 0.0,
            "should have recorded wall time"
        );
    }

    #[test]
    fn migrated_guards_distinguish_multiple_functions() {
        // When multiple functions migrate, each should retain its own name
        // instead of collapsing into a single "<migrated>" bucket.
        reset();
        let guard_a = enter("fn_alpha");
        burn_cpu(5_000);

        let guard_b = std::thread::scope(|s| s.spawn(|| enter("fn_beta")).join().unwrap());
        burn_cpu(5_000);

        // Drop both guards on different threads than where they were created.
        std::thread::scope(|s| {
            s.spawn(move || {
                drop(guard_a);
            });
        });
        std::thread::scope(|s| {
            s.spawn(move || {
                drop(guard_b);
            });
        });

        let records = collect_all();
        let names: Vec<&str> = records.iter().map(|r| r.name.as_str()).collect();
        assert!(
            names.contains(&"fn_alpha"),
            "should have fn_alpha in records. Got: {names:?}"
        );
        assert!(
            names.contains(&"fn_beta"),
            "should have fn_beta in records. Got: {names:?}"
        );
        assert!(
            !names.contains(&"<migrated>"),
            "should NOT have <migrated> placeholder. Got: {names:?}"
        );
    }

    #[test]
    fn migrated_invocation_has_real_name() {
        // Verify InvocationRecord also carries the real function name.
        reset();
        let guard = enter("inv_migrated_fn");
        burn_cpu(10_000);

        let invocations = std::thread::scope(|s| {
            s.spawn(move || {
                burn_cpu(10_000);
                drop(guard);
                collect_invocations()
            })
            .join()
            .unwrap()
        });

        let inv = invocations.iter().find(|r| r.name == "inv_migrated_fn");
        assert!(
            inv.is_some(),
            "migrated invocation should have name 'inv_migrated_fn'. Got: {:?}",
            invocations.iter().map(|r| r.name).collect::<Vec<_>>()
        );
        assert!(
            inv.unwrap().elapsed_ns > 0,
            "should have recorded elapsed time"
        );
    }

    #[test]
    fn pack_unpack_round_trip() {
        let cookie = 42u64;
        let name_id = 1234u16;
        let depth = 567u16;
        let packed = pack_cookie_name_depth(cookie, name_id, depth);
        assert_eq!(unpack_cookie(packed), cookie);
        assert_eq!(unpack_name_id(packed), name_id);
        assert_eq!(unpack_depth(packed), depth);

        // Max values: verifies the full bit range.
        let packed_max = pack_cookie_name_depth(u32::MAX as u64, u16::MAX, u16::MAX);
        assert_eq!(unpack_cookie(packed_max), u32::MAX as u64);
        assert_eq!(unpack_name_id(packed_max), u16::MAX);
        assert_eq!(unpack_depth(packed_max), u16::MAX);

        // Zero values: verifies zero-packing.
        let packed_zero = pack_cookie_name_depth(0, 0, 0);
        assert_eq!(unpack_cookie(packed_zero), 0);
        assert_eq!(unpack_name_id(packed_zero), 0);
        assert_eq!(unpack_depth(packed_zero), 0);
    }

    #[test]
    fn phantom_cleaned_up_on_intermediate_thread() {
        // A -> B -> C migration: after the guard migrates from B to C, B's
        // phantom StackEntry should be cleaned up so that subsequent functions
        // on B get correct depth and frame boundaries.
        //
        // This test uses a long-lived thread B (via channels) so we can:
        // 1. Send the guard to B (check() pushes phantom)
        // 2. Send the guard to C (check() on C detects re-migration, drops on C)
        // 3. Run a new function on B and verify correct behavior
        use std::sync::mpsc;

        reset();

        let guard = enter("async_fn");

        // Thread B: stays alive across the full test.
        let (tx_guard_to_b, rx_guard_on_b) = mpsc::channel::<Guard>();
        let (tx_guard_from_b, rx_guard_from_b) = mpsc::channel::<Guard>();
        let (tx_verify, rx_verify) = mpsc::channel::<()>();
        let (tx_results, rx_results) = mpsc::channel::<(u16, usize)>(); // (depth, frame_count)

        let b_handle = std::thread::spawn(move || {
            // Phase 1: receive guard, push phantom.
            let guard = rx_guard_on_b.recv().unwrap();
            guard.check();
            // Send guard to main so it can go to C.
            tx_guard_from_b.send(guard).unwrap();

            // Phase 2: wait for signal that guard has been dropped on C.
            rx_verify.recv().unwrap();

            // Phase 3: run a new top-level function on B.
            // Do NOT call reset() here -- that would clear the stack
            // and mask the bug. Only clear invocations and frames so we
            // can observe fresh results.
            INVOCATIONS.with(|inv| inv.borrow_mut().clear());
            FRAMES.with(|frames| frames.borrow_mut().clear());
            FRAME_BUFFER.with(|buf| buf.borrow_mut().clear());

            {
                let _work = enter("b_later_work");
                burn_cpu(1_000);
            }

            // Collect results: depth and frame count.
            let invocations = collect_invocations();
            let frames = collect_frames();

            let work_rec = invocations
                .iter()
                .find(|r| r.name == "b_later_work")
                .expect("should have b_later_work record");

            tx_results.send((work_rec.depth, frames.len())).unwrap();
        });

        // Send guard to B.
        tx_guard_to_b.send(guard).unwrap();
        // Get guard back from B.
        let guard = rx_guard_from_b.recv().unwrap();

        // Send guard to C, where it drops.
        std::thread::scope(|s| {
            s.spawn(move || {
                guard.check();
                drop(guard);
            });
        });

        // Signal B that the guard has dropped.
        tx_verify.send(()).unwrap();

        // Get results from B.
        let (depth, frame_count) = rx_results.recv().unwrap();
        b_handle.join().unwrap();

        // After phantom cleanup, b_later_work should be depth 0 (top-level on B).
        assert_eq!(
            depth, 0,
            "b_later_work depth should be 0 after phantom cleanup (got {depth})"
        );
        // Frame boundary should have triggered (depth == 0).
        assert_eq!(
            frame_count, 1,
            "should have 1 frame after b_later_work completes (got {frame_count})"
        );
    }
}
