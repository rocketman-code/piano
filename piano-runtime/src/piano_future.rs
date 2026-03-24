//! Async function instrumentation -- PianoFuture wrapper.
//!
//! PianoFuture wraps an inner Future and accumulates per-poll alloc
//! and CPU deltas. On completion or cancellation (drop), it computes
//! self-time via the TLS children-time accumulator and aggregates
//! into the per-thread FnAgg vec.
//!
//! Same exit path as Guard: children TLS save/restore + aggregate.
//! No Measurement struct. No push_measurement. No span tree.
//!
//! Invariants:
//! - Wall time starts on first poll, not construction.
//! - Each poll saves/restores TLS children_ns (nested guards contribute).
//! - Alloc deltas accumulated per-poll via snapshot_alloc_counters.
//! - Bookkeeping allocs excluded via ReentrancyGuard.
//! - Cancelled/panicking futures emit best-effort aggregate via Drop.
//! - Never double-emits (emitted flag).

use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{compiler_fence, Ordering};
use core::task::{Context, Poll};

use crate::aggregator;
use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::children;
use crate::session::ProfileSession;
use crate::time::read;

/// Future wrapper for async function instrumentation.
pub struct PianoFuture<F> {
    inner: F,
    session: Option<&'static ProfileSession>,
    name_id: u32,
    start_ticks: u64,
    saved_children_ns: u64,
    alloc_count_acc: u64,
    alloc_bytes_acc: u64,
    free_count_acc: u64,
    free_bytes_acc: u64,
    cpu_acc_ns: u64,
    children_ns_acc: u64,
    emitted: bool,
}

/// Wrap an async function body for profiling.
///
/// If profiling is not active, returns a transparent wrapper whose
/// poll delegates directly to the inner future with no overhead.
pub fn enter_async<F: Future>(name_id: u32, body: F) -> PianoFuture<F> {
    let session = match ProfileSession::get() {
        Some(s) => s,
        None => {
            return PianoFuture {
                inner: body,
                session: None,
                name_id: 0,
                start_ticks: 0,
                saved_children_ns: 0,
                alloc_count_acc: 0,
                alloc_bytes_acc: 0,
                free_count_acc: 0,
                free_bytes_acc: 0,
                cpu_acc_ns: 0,
                children_ns_acc: 0,
                emitted: true, // prevent emit on drop
            };
        }
    };

    // Save parent's children_ns accumulator at construction time.
    // This future's inclusive time will be reported to the parent on completion.
    let saved_children_ns = children::save_and_zero();

    PianoFuture {
        inner: body,
        session: Some(session),
        name_id,
        start_ticks: 0,
        saved_children_ns,
        alloc_count_acc: 0,
        alloc_bytes_acc: 0,
        free_count_acc: 0,
        free_bytes_acc: 0,
        cpu_acc_ns: 0,
        children_ns_acc: 0,
        emitted: false,
    }
}

impl<F: Future> Future for PianoFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: We project Pin to `inner` only. All other fields are
        // Unpin primitives. We never move `inner` out of self.
        let this = unsafe { self.get_unchecked_mut() };

        let session = match this.session {
            Some(s) => s,
            None => {
                // Inactive: transparent passthrough.
                // SAFETY: inner is pinned through self. We never move it.
                let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
                return inner.poll(cx);
            }
        };

        // Save TLS children_ns for this poll. Nested sync Guards and
        // PianoFutures will add their inclusive times to TLS during this poll.
        let poll_children_saved = children::save_and_zero();

        // Pre-poll bookkeeping
        let snap_start;
        {
            let _reentrancy = ReentrancyGuard::enter();
            snap_start = snapshot_alloc_counters();

            if this.start_ticks == 0 {
                this.start_ticks = read();
            }
        }

        compiler_fence(Ordering::SeqCst);

        let cpu_poll_start = if session.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            0
        };

        // Poll inner future
        // SAFETY: inner is pinned through self. We never move it.
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        // Post-poll bookkeeping
        let cpu_poll_end = if session.cpu_time_enabled {
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

        // Accumulate children's inclusive time from this poll.
        this.children_ns_acc += children::current_children_ns();

        // Restore TLS children_ns for the outer scope.
        // Don't report our time yet (we might have more polls).
        children::restore_and_report(poll_children_saved, 0);

        if result.is_ready() {
            this.emit(session);
        }

        result
    }
}

impl<F> PianoFuture<F> {
    fn emit(&mut self, session: &'static ProfileSession) {
        if self.emitted || self.start_ticks == 0 {
            return;
        }
        self.emitted = true;

        let end_ticks = read();
        let start_ns = session.calibration.now_ns(self.start_ticks);
        let end_ns = session.calibration.now_ns(end_ticks);
        let inclusive_ns = end_ns.saturating_sub(start_ns);
        let self_ns = inclusive_ns.saturating_sub(self.children_ns_acc);

        aggregator::aggregate(
            self.name_id,
            self_ns,
            inclusive_ns,
            self.cpu_acc_ns,
            self.alloc_count_acc,
            self.alloc_bytes_acc,
            self.free_count_acc,
            self.free_bytes_acc,
            &session.agg_registry,
        );

        // Report our inclusive time to the parent scope.
        children::restore_and_report(self.saved_children_ns, inclusive_ns);
    }
}

impl<F> Drop for PianoFuture<F> {
    fn drop(&mut self) {
        if let Some(session) = self.session {
            self.emit(session);
        }
    }
}

// PianoFuture is Send if the inner future is Send.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PianoFuture<core::future::Ready<()>>>();
    }
};
