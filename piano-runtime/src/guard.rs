//! Sync function instrumentation -- RAII sentinel.
//!
//! Guard is created when entering a profiled function and dropped when
//! exiting. On drop, it computes timing and allocation deltas, pushes
//! a Measurement to the per-thread buffer, and restores the TLS parent
//! span_id.
//!
//! Invariants:
//! - Guard is !Send. Alloc deltas are computed on the same thread as
//!   creation. Enforcement: PhantomData<*const ()>.
//! - Profiler bookkeeping allocs are excluded from user counts.
//!   Enforcement: ReentrancyGuard (RAII) wraps creation and drop.
//! - Guard never panics. All arithmetic uses saturating_sub.
//!   Profiler never crashes the host.
//! - TLS parent_span_id is restored on drop (RAII stack discipline).

use core::sync::atomic::{compiler_fence, Ordering};

use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::buffer::{push_measurement, ensure_thread_file_sink};
use crate::cpu_clock::cpu_now_ns;
use crate::measurement::Measurement;
use crate::parent;
use crate::session::ProfileSession;
use crate::thread_id::current_thread_id;
use crate::time::read;
use std::marker::PhantomData;

/// RAII sentinel for sync function instrumentation.
///
/// Created by `piano_runtime::enter(name_id)`. Dropped at function exit.
/// On drop: end timestamp, alloc delta, push measurement, restore TLS parent.
///
/// !Send because alloc counters are per-thread TLS.
pub struct Guard {
    /// None = inactive (profiling not initialized). Drop is a no-op.
    session: Option<&'static ProfileSession>,
    span_id: u64,
    saved_parent: u64,
    name_id: u32,
    cpu_time_enabled: bool,
    cpu_start_ns: u64,
    start_ns: u64,
    alloc_count_start: u64,
    alloc_bytes_start: u64,
    free_count_start: u64,
    free_bytes_start: u64,
    thread_id: u64,
    _not_send: PhantomData<*const ()>,
}

/// Enter a profiled function. Returns a Guard that restores state on drop.
///
/// Reads profiling context from &'static ProfileSession + TLS parent_span_id.
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
            span_id: 0,
            saved_parent: 0,
            name_id: 0,
            cpu_time_enabled: false,
            cpu_start_ns: 0,
            start_ns: 0,
            alloc_count_start: 0,
            alloc_bytes_start: 0,
            free_count_start: 0,
            free_bytes_start: 0,
            thread_id: 0,
            _not_send: PhantomData,
        }
    }

    /// Create a guard with all bookkeeping done but start_ns = 0.
    /// Caller must call stamp() after the struct is materialized.
    fn create(session: &'static ProfileSession, name_id: u32) -> Self {
        if let Some(ref fs) = session.file_sink {
            ensure_thread_file_sink(fs, &session.registry);
        }

        let _reentrancy = ReentrancyGuard::enter();
        let span_id = session.next_span_id();
        let saved_parent = parent::set_parent(span_id);
        let snap = snapshot_alloc_counters();
        let cpu_start_ns = if session.cpu_time_enabled { cpu_now_ns() } else { 0 };
        let thread_id = current_thread_id(&session.thread_id_alloc);
        drop(_reentrancy);

        Self {
            session: Some(session),
            span_id,
            saved_parent,
            name_id,
            cpu_time_enabled: session.cpu_time_enabled,
            cpu_start_ns,
            start_ns: 0,
            alloc_count_start: snap.alloc_count,
            alloc_bytes_start: snap.alloc_bytes,
            free_count_start: snap.free_count,
            free_bytes_start: snap.free_bytes,
            thread_id,
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
        let session = match self.session {
            Some(s) => s,
            None => return,
        };

        let end_ticks = read();
        compiler_fence(Ordering::SeqCst);
        let _reentrancy = ReentrancyGuard::enter();
        let cpu_end_ns = if self.cpu_time_enabled { cpu_now_ns() } else { 0 };
        let snap_end = snapshot_alloc_counters();

        let m = Measurement {
            span_id: self.span_id,
            parent_span_id: self.saved_parent,
            name_id: self.name_id,
            start_ns: session.calibration.now_ns(self.start_ns),
            end_ns: session.calibration.now_ns(end_ticks),
            thread_id: self.thread_id,
            cpu_start_ns: self.cpu_start_ns,
            cpu_end_ns,
            alloc_count: snap_end.alloc_count.saturating_sub(self.alloc_count_start),
            alloc_bytes: snap_end.alloc_bytes.saturating_sub(self.alloc_bytes_start),
            free_count: snap_end.free_count.saturating_sub(self.free_count_start),
            free_bytes: snap_end.free_bytes.saturating_sub(self.free_bytes_start),
        };

        push_measurement(m, &session.registry);

        // Restore TLS parent span_id (RAII stack discipline).
        parent::restore_parent(self.saved_parent);
    }
}
