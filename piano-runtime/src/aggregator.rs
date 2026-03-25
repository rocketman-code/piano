//! In-flight per-function aggregation.
//!
//! Each thread maintains a `Vec<FnAgg>` that accumulates self-time, call
//! counts, and allocation deltas as Guards drop. At shutdown, the vecs
//! are drained and written as summary NDJSON (one line per function per
//! thread, instead of one line per call).
//!
//! Linear scan with name_id equality. Benchmarked at 5.5 ns/call for
//! 3 functions, 18 ns/call for 100 functions (Zipf distribution).
//!
//! Invariants:
//! - Per-thread (no cross-thread sharing during recording).
//! - Registered in a global AggRegistry for shutdown drain.
//! - All mutex locks heal poisoning.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

/// Per-function aggregated measurements.
#[derive(Debug, Clone)]
pub struct FnAgg {
    pub name_id: u32,
    pub calls: u64,
    pub self_ns: u64,
    pub inclusive_ns: u64,
    pub cpu_self_ns: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}

/// Registry of per-thread aggregation buffers for shutdown drain.
pub type AggRegistry = Mutex<Vec<Arc<Mutex<Vec<FnAgg>>>>>;

thread_local! {
    static THREAD_AGG: RefCell<Option<Arc<Mutex<Vec<FnAgg>>>>> =
        const { RefCell::new(None) };
}

/// Merge a completed measurement into the current thread's aggregation buffer.
///
/// Linear scan for matching name_id. If found, accumulates. If not, pushes.
/// Initializes the thread's buffer and registers it on first call.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn aggregate(
    name_id: u32,
    self_ns: u64,
    inclusive_ns: u64,
    cpu_self_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
    registry: &AggRegistry,
) {
    let _ = THREAD_AGG.try_with(|cell| {
        let mut borrow = match cell.try_borrow_mut() {
            Ok(b) => b,
            Err(_) => return, // reentrant, skip
        };

        let arc = borrow.get_or_insert_with(|| {
            let arc = Arc::new(Mutex::new(Vec::new()));
            registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Arc::clone(&arc));
            arc
        });

        let mut buf = arc.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(entry) = buf.iter_mut().find(|e| e.name_id == name_id) {
            entry.calls += 1;
            entry.self_ns += self_ns;
            entry.inclusive_ns += inclusive_ns;
            entry.cpu_self_ns += cpu_self_ns;
            entry.alloc_count += alloc_count;
            entry.alloc_bytes += alloc_bytes;
            entry.free_count += free_count;
            entry.free_bytes += free_bytes;
        } else {
            buf.push(FnAgg {
                name_id,
                calls: 1,
                self_ns,
                inclusive_ns,
                cpu_self_ns,
                alloc_count,
                alloc_bytes,
                free_count,
                free_bytes,
            });
        }
    });
}

/// Drain all registered aggregation buffers. Returns one Vec<FnAgg> per
/// thread, preserving per-thread identity. The writer assigns thread
/// indices (0, 1, 2...) from the position in the outer Vec.
pub fn drain_all_agg(registry: &AggRegistry) -> Vec<Vec<FnAgg>> {
    let reg = registry.lock().unwrap_or_else(|e| e.into_inner());
    let mut all = Vec::new();
    for buf_arc in reg.iter() {
        let mut buf = buf_arc.lock().unwrap_or_else(|e| e.into_inner());
        let drained: Vec<FnAgg> = buf.drain(..).collect();
        if !drained.is_empty() {
            all.push(drained);
        }
    }
    all
}

/// Drain the current thread's aggregation buffer only. For tests.
#[cfg(feature = "_test_internals")]
pub fn drain_thread_agg() -> Vec<FnAgg> {
    THREAD_AGG
        .try_with(|cell| {
            let borrow = cell.borrow();
            match borrow.as_ref() {
                Some(arc) => {
                    let mut buf = arc.lock().unwrap_or_else(|e| e.into_inner());
                    buf.drain(..).collect()
                }
                None => Vec::new(),
            }
        })
        .unwrap_or_default()
}
