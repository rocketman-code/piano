//! Async function instrumentation -- PianoFuture wrapper.
//!
//! PianoFuture wraps an inner Future and accumulates per-poll alloc,
//! CPU, and wall-time deltas. On completion or cancellation (drop),
//! it emits a single Measurement to the per-thread buffer.
//!
//! Invariants:
//! - Wall time starts on first poll, not construction. A future that
//!   is constructed but never polled emits no measurement.
//! - Wall time is accumulated per-poll (raw tick deltas around inner.poll()),
//!   so suspension time between polls is excluded from the measurement.
//! - Alloc deltas are accumulated across polls using snapshot_alloc_counters.
//!   Each poll takes a before/after snapshot and adds the delta.
//! - Bookkeeping allocs (snapshots, push_measurement) are excluded
//!   via ReentrancyGuard.
//! - cpu_start_ns is always 0 in the emitted Measurement. Accumulated
//!   CPU time goes into cpu_end_ns.
//! - Cancelled (dropped without completing) futures emit best-effort
//!   measurement. Panicking inner futures emit best-effort via Drop.
//! - Never double-emits (emitted flag).

use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{compiler_fence, Ordering};
use core::task::{Context, Poll};

use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::buffer::{push_measurement, Registry};
use crate::measurement::Measurement;
use crate::thread_id::current_thread_id;
use crate::time::{read, CalibrationData};
use crate::buffer::ThreadBuffer;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;

/// State passed from Ctx::enter_async() to PianoFuture::new().
/// Contains the span identity and configuration for the async function.
pub struct PianoFutureState {
    pub span_id: u64,
    pub parent_span_id: u64,
    pub name_id: u32,
    pub cpu_time_enabled: bool,
    pub calibration: CalibrationData,
    pub thread_id_alloc: Arc<AtomicU64>,
    pub registry: Arc<Registry>,
}

/// Future wrapper for async function instrumentation.
///
/// Wraps the async body, accumulates alloc and CPU deltas per-poll,
/// and emits a Measurement on completion or cancellation.
pub struct PianoFuture<F> {
    inner: F,
    span_id: u64,
    parent_span_id: u64,
    name_id: u32,
    cpu_time_enabled: bool,
    start_ticks: u64,
    thread_id: u64,
    alloc_count_acc: u64,
    alloc_bytes_acc: u64,
    free_count_acc: u64,
    free_bytes_acc: u64,
    cpu_acc_ns: u64,
    wall_ticks_acc: u64,
    emitted: bool,
    calibration: CalibrationData,
    thread_id_alloc: Arc<AtomicU64>,
    registry: Arc<Mutex<Vec<Arc<Mutex<ThreadBuffer>>>>>,
}

impl<F: Future> PianoFuture<F> {
    pub fn new(state: PianoFutureState, inner: F) -> Self {
        Self {
            inner,
            span_id: state.span_id,
            parent_span_id: state.parent_span_id,
            name_id: state.name_id,
            cpu_time_enabled: state.cpu_time_enabled,
            start_ticks: 0,
            thread_id: 0,
            alloc_count_acc: 0,
            alloc_bytes_acc: 0,
            free_count_acc: 0,
            free_bytes_acc: 0,
            cpu_acc_ns: 0,
            wall_ticks_acc: 0,
            emitted: false,
            calibration: state.calibration,
            thread_id_alloc: state.thread_id_alloc,
            registry: state.registry,
        }
    }
}

impl<F: Future> Future for PianoFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: We only project Pin to the `inner` field (which may be
        // !Unpin). All other fields are Unpin primitive types. We never
        // move `inner` out of self.
        let this = unsafe { self.get_unchecked_mut() };

        // --- Pre-poll bookkeeping (inside reentrancy guard) ---
        let snap_start;
        {
            let _reentrancy = ReentrancyGuard::enter();
            snap_start = snapshot_alloc_counters();

            if this.start_ticks == 0 {
                this.start_ticks = read();
                this.thread_id = current_thread_id(&this.thread_id_alloc);
            }
        }
        // _reentrancy dropped here -- user allocs during poll ARE tracked

        compiler_fence(Ordering::SeqCst);

        let cpu_poll_start = if this.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            0
        };

        // --- Poll inner future ---
        // SAFETY: We project Pin from self to inner. inner is the only
        // !Unpin field. We never move inner out of self, and we do not
        // access it again after this poll returns Ready.
        let poll_start_ticks = read();
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);
        this.wall_ticks_acc += read().wrapping_sub(poll_start_ticks);

        // --- Post-poll bookkeeping (inside reentrancy guard) ---
        let cpu_poll_end = if this.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            0
        };

        compiler_fence(Ordering::SeqCst);

        {
            let _reentrancy = ReentrancyGuard::enter();
            let snap_end = snapshot_alloc_counters();

            this.alloc_count_acc += snap_end.alloc_count.saturating_sub(snap_start.alloc_count);
            this.alloc_bytes_acc += snap_end.alloc_bytes.saturating_sub(snap_start.alloc_bytes);
            this.free_count_acc += snap_end.free_count.saturating_sub(snap_start.free_count);
            this.free_bytes_acc += snap_end.free_bytes.saturating_sub(snap_start.free_bytes);

            this.cpu_acc_ns += cpu_poll_end.saturating_sub(cpu_poll_start);
        }

        if result.is_ready() {
            this.emit();
        }

        result
    }
}

impl<F> PianoFuture<F> {
    fn emit(&mut self) {
        if self.emitted || self.start_ticks == 0 {
            return;
        }
        self.emitted = true;

        let m = Measurement {
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name_id: self.name_id,
            start_ns: self.calibration.now_ns(self.start_ticks),
            end_ns: self.calibration.now_ns(self.start_ticks).wrapping_add(self.calibration.ticks_to_ns(self.wall_ticks_acc)),
            thread_id: self.thread_id,
            cpu_start_ns: 0,
            cpu_end_ns: self.cpu_acc_ns,
            alloc_count: self.alloc_count_acc,
            alloc_bytes: self.alloc_bytes_acc,
            free_count: self.free_count_acc,
            free_bytes: self.free_bytes_acc,
        };

        push_measurement(m, &self.registry);
    }
}

impl<F> Drop for PianoFuture<F> {
    fn drop(&mut self) {
        self.emit();
    }
}

// PianoFuture is Send if the inner future is Send.
// SAFETY: All our fields are Send-safe (primitives, Arc, CalibrationData (Copy))
// + the inner future. The inner future determines sendability.
// This static assertion proves it:
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PianoFuture<core::future::Ready<()>>>();
    }
};
