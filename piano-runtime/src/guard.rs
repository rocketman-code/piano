//! Sync function instrumentation -- RAII sentinel.
//!
//! Guard is created when entering a profiled function and dropped when
//! exiting. On drop, it computes self-time via the TLS children-time
//! accumulator and aggregates into the per-thread FnAgg vec.
//!
//! Invariants:
//! - Guard is !Send. Alloc deltas are computed on the same thread as
//!   creation. Enforcement: PhantomData<*const ()>.
//! - Profiler bookkeeping allocs are excluded from user counts.
//!   Enforcement: ReentrancyGuard (RAII) wraps creation and drop.
//! - Guard never panics. All arithmetic uses saturating_sub.
//! - TLS children_ns is saved/restored on create/drop (RAII stack discipline).

use core::sync::atomic::{compiler_fence, Ordering};

use crate::aggregator;
use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::children;
use crate::cpu_clock::cpu_now_ns;
use crate::session::ProfileSession;
use crate::time::read;
use std::marker::PhantomData;

/// RAII sentinel for sync function instrumentation.
///
/// Created by `piano_runtime::enter(name_id)`. Dropped at function exit.
/// On drop: end timestamp, self-time computation, aggregate.
///
/// !Send because alloc counters are per-thread TLS.
pub struct Guard {
    /// None = inactive (profiling not initialized). Drop is a no-op.
    session: Option<&'static ProfileSession>,
    saved_children_ns: u64,
    name_id: u32,
    cpu_time_enabled: bool,
    cpu_start_ns: u64,
    start_ns: u64,
    alloc_count_start: u64,
    alloc_bytes_start: u64,
    free_count_start: u64,
    free_bytes_start: u64,
    _not_send: PhantomData<*const ()>,
}

/// Enter a profiled function. Returns a Guard that aggregates on drop.
///
/// Reads profiling context from &'static ProfileSession.
/// No function parameters needed. No closure captures created.
///
/// If profiling is not active (ProfileSession not initialized), returns an
/// inactive Guard whose drop is a no-op.
#[inline(always)]
pub fn enter(name_id: u32) -> Guard {
    let session = match ProfileSession::get() {
        Some(s) => s,
        None => return Guard::inactive(),
    };
    let mut guard = Guard::create(session, name_id);
    guard.stamp();
    guard
}

impl Guard {
    /// Inactive guard. Drop is a no-op.
    fn inactive() -> Self {
        Self {
            session: None,
            saved_children_ns: 0,
            name_id: 0,
            cpu_time_enabled: false,
            cpu_start_ns: 0,
            start_ns: 0,
            alloc_count_start: 0,
            alloc_bytes_start: 0,
            free_count_start: 0,
            free_bytes_start: 0,
            _not_send: PhantomData,
        }
    }

    /// Create a guard with all bookkeeping done but start_ns = 0.
    /// Caller must call stamp() after the struct is materialized.
    ///
    /// NOT inlined: keeps the heavy bookkeeping (TLS, alloc snapshot)
    /// out of the caller. Only stamp() (one TSC read) is inlined.
    #[inline(never)]
    fn create(session: &'static ProfileSession, name_id: u32) -> Self {
        let _reentrancy = ReentrancyGuard::enter();
        let saved_children_ns = children::save_and_zero();
        let snap = snapshot_alloc_counters();
        let cpu_start_ns = if session.cpu_time_enabled {
            cpu_now_ns()
        } else {
            0
        };
        drop(_reentrancy);

        Self {
            session: Some(session),
            saved_children_ns,
            name_id,
            cpu_time_enabled: session.cpu_time_enabled,
            cpu_start_ns,
            start_ns: 0,
            alloc_count_start: snap.alloc_count,
            alloc_bytes_start: snap.alloc_bytes,
            free_count_start: snap.free_count,
            free_bytes_start: snap.free_bytes,
            _not_send: PhantomData,
        }
    }

    /// Write the start timestamp. Called after the struct is materialized.
    #[inline(always)]
    pub fn stamp(&mut self) {
        compiler_fence(Ordering::SeqCst);
        self.start_ns = read();
    }
}

impl Drop for Guard {
    #[inline(always)]
    fn drop(&mut self) {
        let end_ticks = read();
        compiler_fence(Ordering::SeqCst);

        let session = match self.session {
            Some(s) => s,
            None => return,
        };
        let _reentrancy = ReentrancyGuard::enter();
        let cpu_end_ns = if self.cpu_time_enabled {
            cpu_now_ns()
        } else {
            0
        };
        let snap_end = snapshot_alloc_counters();

        let start_ns = session.calibration.now_ns(self.start_ns);
        let end_ns = session.calibration.now_ns(end_ticks);
        let inclusive_ns = end_ns.saturating_sub(start_ns);

        let my_children_ns = children::current_children_ns();
        let self_ns = inclusive_ns.saturating_sub(my_children_ns);
        let cpu_self_ns = cpu_end_ns.saturating_sub(self.cpu_start_ns);

        aggregator::aggregate(
            self.name_id,
            self_ns,
            inclusive_ns,
            cpu_self_ns,
            snap_end.alloc_count.saturating_sub(self.alloc_count_start),
            snap_end.alloc_bytes.saturating_sub(self.alloc_bytes_start),
            snap_end.free_count.saturating_sub(self.free_count_start),
            snap_end.free_bytes.saturating_sub(self.free_bytes_start),
            &session.agg_registry,
        );

        // Report inclusive time to parent scope.
        children::restore_and_report(self.saved_children_ns, inclusive_ns);
    }
}
