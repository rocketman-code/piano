//! Async function instrumentation -- PianoFuture wrapper.
//!
//! PianoFuture is the vehicle for the spec-derived AsyncExecution state:
//! it wraps an inner Future and drives the poll lifecycle operations.
//! Each poll runs begin_poll (pre-poll snapshots into the mid-poll state),
//! polls the inner future, then end_poll (deltas accumulated, back to
//! idle with is_ever_polled established). On Ready the aggregate is
//! emitted and the completion transition consumes the execution; on drop
//! the vehicle emits best-effort for cancelled or panicking futures.
//!
//! Invariants:
//! - Wall time is accumulated per-poll, not measured as a span, so
//!   suspension time between polls is excluded from self_ns.
//! - Children within each poll report their inclusive time; the future
//!   reports its own inclusive total upward exactly once, at emit.
//! - Bookkeeping allocs excluded via ProfilerBookkeeping (in the ops).
//! - Never double-emits: the execution value is taken out of the vehicle
//!   to emit, so a second emit has nothing to act on.
//! - While a poll is in flight the vehicle holds a primed fallback, so a
//!   panic unwinding through the inner poll still emits on drop.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::generated::piano_runtime::async_execution::{ops, AsyncExecution};
use crate::session::ProfileSession;
use crate::NameId;

/// Future wrapper for async function instrumentation.
pub struct PianoFuture<F> {
    inner: F,
    /// None = profiling inactive, or the execution was consumed at emit.
    exec: Option<AsyncExecution>,
}

/// Wrap an async function body for profiling.
///
/// If profiling is not active, returns a transparent wrapper whose
/// poll delegates directly to the inner future with no overhead.
pub fn enter_async<F: Future>(name_id: u32, body: F) -> PianoFuture<F> {
    let exec = ProfileSession::get().map(|_| ops::create_async(&NameId::from_raw(name_id)));
    PianoFuture { inner: body, exec }
}

impl<F: Future> Future for PianoFuture<F> {
    type Output = F::Output;

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: We project Pin to `inner` only. All other fields are
        // Unpin values. We never move `inner` out of self.
        let this = unsafe { self.get_unchecked_mut() };

        let exec = match this.exec.take() {
            Some(exec) => exec,
            None => {
                // Inactive: transparent passthrough.
                // SAFETY: inner is pinned through self. We never move it.
                let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
                return inner.poll(cx);
            }
        };

        let mid = crate::begin_poll(exec);
        // A panic unwinding through the inner poll drops `mid` on the
        // stack; the primed fallback keeps the drop-path emit alive.
        this.exec = Some(ops::primed_for_drop(&mid));

        // SAFETY: inner is pinned through self. We never move it.
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        let exec = ops::end_poll(mid);

        if result.is_ready() {
            crate::emit(&exec);
            let _completed = crate::mark_completed(exec);
            this.exec = None;
        } else {
            this.exec = Some(exec);
        }

        result
    }
}

impl<F> Drop for PianoFuture<F> {
    fn drop(&mut self) {
        if let Some(exec) = self.exec.take() {
            // Cancelled (idle, ever-polled) or panicked mid-poll (the
            // primed fallback): best-effort emit. Never-polled executions
            // emit nothing (wall_acc None inside the operation).
            crate::emit(&exec);
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
