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
use crate::alloc::{snapshot_alloc_counters, AllocDelta, ProfilerBookkeeping};
use crate::children;
use crate::cpu_clock::CpuNs;
use crate::session::ProfileSession;
use crate::time::{read, Ticks, WallNs};
use crate::NameId;

/// Future wrapper for async function instrumentation.
pub struct PianoFuture<F> {
    inner: F,
    session: Option<&'static ProfileSession>,
    name_id: NameId,
    start_ticks: Ticks,
    saved_children_ns: WallNs,
    alloc_acc: AllocDelta,
    cpu_acc: CpuNs,
    children_ns_acc: WallNs,
    emitted: bool,
}

/// Wrap an async function body for profiling.
///
/// If profiling is not active, returns a transparent wrapper whose
/// poll delegates directly to the inner future with no overhead.
pub fn enter_async<F: Future>(name_id: u32, body: F) -> PianoFuture<F> {
    let name_id = NameId(name_id);
    let session = match ProfileSession::get() {
        Some(s) => s,
        None => {
            return PianoFuture {
                inner: body,
                session: None,
                name_id: NameId(0),
                start_ticks: Ticks(0),
                saved_children_ns: WallNs::ZERO,
                alloc_acc: AllocDelta::ZERO,
                cpu_acc: CpuNs::ZERO,
                children_ns_acc: WallNs::ZERO,
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
        start_ticks: Ticks(0),
        saved_children_ns,
        alloc_acc: AllocDelta::ZERO,
        cpu_acc: CpuNs::ZERO,
        children_ns_acc: WallNs::ZERO,
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
            let _bookkeeping = ProfilerBookkeeping::enter();
            snap_start = snapshot_alloc_counters();

            if this.start_ticks.0 == 0 {
                this.start_ticks = read();
            }
        }

        compiler_fence(Ordering::SeqCst);

        let cpu_poll_start = if session.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            CpuNs::ZERO
        };

        // Poll inner future
        // SAFETY: inner is pinned through self. We never move it.
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        // Post-poll bookkeeping
        let cpu_poll_end = if session.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            CpuNs::ZERO
        };

        compiler_fence(Ordering::SeqCst);

        {
            let _bookkeeping = ProfilerBookkeeping::enter();
            let snap_end = snapshot_alloc_counters();

            this.alloc_acc += snap_end.delta_since(&snap_start);
            this.cpu_acc += cpu_poll_end.saturating_sub(cpu_poll_start);
        }

        // Accumulate children's inclusive time from this poll.
        this.children_ns_acc += children::current_children_ns();

        // Restore TLS children_ns for the outer scope.
        // Don't report our time yet (we might have more polls).
        children::restore_and_report(poll_children_saved, WallNs::ZERO);

        if result.is_ready() {
            this.emit(session);
        }

        result
    }
}

impl<F> PianoFuture<F> {
    fn emit(&mut self, session: &'static ProfileSession) {
        if self.emitted || self.start_ticks.0 == 0 {
            return;
        }
        self.emitted = true;

        let end_ticks = read();
        let bookkeeping = ProfilerBookkeeping::enter();
        let start_ns = session.calibration.now_ns(self.start_ticks);
        let end_ns = session.calibration.now_ns(end_ticks);
        let inclusive_ns = end_ns.saturating_sub(start_ns);
        let self_ns = inclusive_ns.saturating_sub(self.children_ns_acc);

        aggregator::aggregate(
            &bookkeeping,
            self.name_id.0,
            self_ns.0,
            inclusive_ns.0,
            self.cpu_acc.0,
            self.alloc_acc.alloc_count,
            self.alloc_acc.alloc_bytes,
            self.alloc_acc.free_count,
            self.alloc_acc.free_bytes,
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
