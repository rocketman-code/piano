//! Async function instrumentation -- PianoFuture wrapper.
//!
//! PianoFuture wraps an inner Future and accumulates per-poll wall, CPU,
//! alloc, and children deltas. On completion or cancellation (drop), it
//! computes self-time and aggregates into the per-thread FnAgg vec.
//!
//! Wall time uses per-poll accumulation (same as CPU and alloc), so
//! suspension time between polls is excluded from self_ns.
//!
//! Same exit path as Guard: children TLS save/restore + aggregate.
//!
//! Invariants:
//! - Wall time is accumulated per-poll, not measured as a span.
//! - Each poll saves/restores TLS children_ns (nested guards contribute).
//! - All four measurements (wall, cpu, alloc, children) are captured
//!   together in end_poll and destructured exhaustively.
//! - Bookkeeping allocs excluded via ProfilerBookkeeping proof token.
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
use crate::time::{read, WallNs};
use crate::types::{PollActive, PollDeltas};
use crate::NameId;

/// Future wrapper for async function instrumentation.
pub struct PianoFuture<F> {
    inner: F,
    session: Option<&'static ProfileSession>,
    name_id: NameId,
    /// None = never polled. Some = accumulated wall time across polls.
    wall_acc: Option<WallNs>,
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
    let name_id = NameId::from_raw(name_id);
    let session = match ProfileSession::get() {
        Some(s) => s,
        None => {
            return PianoFuture {
                inner: body,
                session: None,
                name_id: NameId::from_raw(0),
                wall_acc: None,
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
        wall_acc: None,
        saved_children_ns,
        alloc_acc: AllocDelta::ZERO,
        cpu_acc: CpuNs::ZERO,
        children_ns_acc: WallNs::ZERO,
        emitted: false,
    }
}

/// Capture pre-poll measurement snapshots (wall, cpu, alloc).
#[inline(always)]
fn begin_poll(session: &ProfileSession) -> PollActive {
    let alloc_start = {
        let _bookkeeping = ProfilerBookkeeping::enter();
        snapshot_alloc_counters()
    };
    let wall_start = read();
    let cpu_start = if session.cpu_time_enabled {
        crate::cpu_clock::cpu_now_ns()
    } else {
        CpuNs::ZERO
    };
    compiler_fence(Ordering::SeqCst);
    PollActive {
        wall_start,
        cpu_start,
        alloc_start,
    }
}

/// Capture post-poll snapshots and compute all per-poll deltas.
#[inline(always)]
fn end_poll(active: PollActive, session: &ProfileSession) -> PollDeltas {
    compiler_fence(Ordering::SeqCst);
    let cpu_end = if session.cpu_time_enabled {
        crate::cpu_clock::cpu_now_ns()
    } else {
        CpuNs::ZERO
    };
    let wall_end = read();
    let alloc = {
        let _bookkeeping = ProfilerBookkeeping::enter();
        let alloc_end = snapshot_alloc_counters();
        alloc_end.delta_since(&active.alloc_start)
    };
    let children = children::current_children_ns();

    let wall_start_ns = session.calibration.now_ns(active.wall_start);
    let wall_end_ns = session.calibration.now_ns(wall_end);

    PollDeltas {
        wall: wall_end_ns.saturating_sub(wall_start_ns),
        cpu: cpu_end.saturating_sub(active.cpu_start),
        alloc,
        children,
    }
}

impl<F: Future> Future for PianoFuture<F> {
    type Output = F::Output;

    #[inline(always)]
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

        // If inner panics, emit() in Drop can still produce output.
        if this.wall_acc.is_none() {
            this.wall_acc = Some(WallNs::ZERO);
        }

        let active = begin_poll(session);

        // SAFETY: inner is pinned through self. We never move it.
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        let PollDeltas {
            wall,
            cpu,
            alloc,
            children,
        } = end_poll(active, session);

        // end_poll reads children TLS; restore must happen AFTER.
        children::restore_and_report(poll_children_saved, WallNs::ZERO);

        if let Some(ref mut acc) = this.wall_acc {
            *acc += wall;
        }
        this.cpu_acc += cpu;
        this.alloc_acc += alloc;
        this.children_ns_acc += children;

        if result.is_ready() {
            this.emit(session);
        }

        result
    }
}

impl<F> PianoFuture<F> {
    fn emit(&mut self, session: &'static ProfileSession) {
        let inclusive_ns = match self.wall_acc {
            Some(w) if !self.emitted => w,
            _ => return,
        };
        self.emitted = true;

        let bookkeeping = ProfilerBookkeeping::enter();
        let self_ns = inclusive_ns.saturating_sub(self.children_ns_acc);

        aggregator::aggregate(
            &bookkeeping,
            self.name_id,
            self_ns,
            inclusive_ns,
            self.cpu_acc,
            self.alloc_acc,
            &session.agg_registry,
        );

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
