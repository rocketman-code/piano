//! Sync function instrumentation -- RAII sentinel.
//!
//! Guard is created when entering a profiled function and dropped when
//! exiting. On drop, it computes timing and allocation deltas and pushes
//! a Measurement to the per-thread buffer.
//!
//! Invariants:
//! - Guard is !Send. Alloc deltas are computed on the same thread as
//!   creation. Enforcement: PhantomData<*const ()>.
//! - Profiler bookkeeping allocs are excluded from user counts.
//!   Enforcement: ReentrancyGuard (RAII) wraps entire new() and drop().
//! - Guard never panics. All arithmetic uses saturating_sub.
//!   Profiler never crashes the host.

use core::sync::atomic::{compiler_fence, Ordering};

use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::buffer::push_measurement;
#[cfg(feature = "cpu-time")]
use crate::cpu_clock::cpu_now_ns;
use crate::measurement::Measurement;
use crate::thread_id::current_thread_id;
use crate::time::{read, now_ns};
use std::marker::PhantomData;

/// RAII sentinel for sync function instrumentation.
///
/// Created at function entry, dropped at function exit. On drop,
/// computes wall time and allocation deltas, builds a Measurement,
/// and pushes it to the per-thread buffer.
///
/// !Send because alloc counters are per-thread TLS. Moving a Guard
/// to another thread would compute deltas against the wrong counters.
pub struct Guard {
    span_id: u64,
    parent_span_id: u64,
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

impl Guard {
    /// Create a new Guard. Snapshots wall time and alloc counters.
    ///
    /// All bookkeeping allocations (Vec growth in snapshot, etc.) are
    /// excluded from user counts by the ReentrancyGuard.
    #[inline(always)]
    pub fn new(span_id: u64, parent_span_id: u64, name_id: u32, cpu_time_enabled: bool) -> Self {
        let _reentrancy = ReentrancyGuard::enter();
        let (alloc_count_start, alloc_bytes_start, free_count_start, free_bytes_start) = snapshot_alloc_counters();
        #[cfg(feature = "cpu-time")]
        let cpu_start_ns = if cpu_time_enabled { cpu_now_ns() } else { 0 };
        #[cfg(not(feature = "cpu-time"))]
        let cpu_start_ns = 0u64;
        let thread_id = current_thread_id();
        compiler_fence(Ordering::SeqCst);
        let start_ns = read();

        Self {
            span_id,
            parent_span_id,
            name_id,
            cpu_time_enabled,
            cpu_start_ns,
            start_ns,
            alloc_count_start,
            alloc_bytes_start,
            free_count_start,
            free_bytes_start,
            thread_id,
            _not_send: PhantomData,
        }
        // _reentrancy drops here, exiting reentrancy
    }
}

impl Drop for Guard {
    #[inline(always)]
    fn drop(&mut self) {
        let end_ticks = read();
        compiler_fence(Ordering::SeqCst);
        let _reentrancy = ReentrancyGuard::enter();
        #[cfg(feature = "cpu-time")]
        let cpu_end_ns = if self.cpu_time_enabled { cpu_now_ns() } else { 0 };
        #[cfg(not(feature = "cpu-time"))]
        let cpu_end_ns = 0u64;
        let (alloc_count_end, alloc_bytes_end, free_count_end, free_bytes_end) = snapshot_alloc_counters();

        let m = Measurement {
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name_id: self.name_id,
            start_ns: now_ns(self.start_ns),
            end_ns: now_ns(end_ticks),
            thread_id: self.thread_id,
            cpu_start_ns: self.cpu_start_ns,
            cpu_end_ns,
            alloc_count: alloc_count_end.saturating_sub(self.alloc_count_start),
            alloc_bytes: alloc_bytes_end.saturating_sub(self.alloc_bytes_start),
            free_count: free_count_end.saturating_sub(self.free_count_start),
            free_bytes: free_bytes_end.saturating_sub(self.free_bytes_start),
        };

        push_measurement(m);
        // _reentrancy drops here
    }
}
