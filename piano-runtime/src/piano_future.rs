//! Async function instrumentation -- PianoFuture wrapper.
//!
//! PianoFuture wraps an inner Future and accumulates per-poll alloc
//! and CPU deltas. On completion or cancellation (drop), it emits
//! a single Measurement to the per-thread buffer.
//!
//! Reads from &'static ProfileSession + TLS parent_span_id. No
//! function parameters. No closure captures.
//!
//! Invariants:
//! - Wall time starts on first poll, not construction. Never-polled
//!   futures emit no measurement.
//! - Parent span_id is captured at CONSTRUCTION (from TLS), not at
//!   poll time. This captures the correct caller context.
//! - Each poll sets TLS to this future's span_id (so nested calls
//!   see it as parent), then restores TLS after the poll.
//! - Alloc deltas accumulated per-poll via snapshot_alloc_counters.
//! - Bookkeeping allocs excluded via ReentrancyGuard.
//! - Cancelled/panicking futures emit best-effort measurement via Drop.
//! - Never double-emits (emitted flag).

use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{compiler_fence, Ordering};
use core::task::{Context, Poll};

use crate::alloc::{snapshot_alloc_counters, ReentrancyGuard};
use crate::buffer::push_measurement;
use crate::measurement::Measurement;
use crate::parent;
use crate::session::ProfileSession;
use crate::thread_id::current_thread_id;
use crate::time::read;

/// Future wrapper for async function instrumentation.
pub struct PianoFuture<F> {
    inner: F,
    session: Option<&'static ProfileSession>,
    span_id: u64,
    parent_span_id: u64,
    name_id: u32,
    start_ticks: u64,
    thread_id: u64,
    alloc_count_acc: u64,
    alloc_bytes_acc: u64,
    free_count_acc: u64,
    free_bytes_acc: u64,
    cpu_acc_ns: u64,
    emitted: bool,
}

/// Wrap an async function body for profiling. Returns a PianoFuture
/// that accumulates timing and allocation data across polls.
///
/// Reads parent_span_id from TLS at construction time (captures the
/// caller's context). No parameters needed. No closure captures.
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
                span_id: 0,
                parent_span_id: 0,
                name_id: 0,
                start_ticks: 0,
                thread_id: 0,
                alloc_count_acc: 0,
                alloc_bytes_acc: 0,
                free_count_acc: 0,
                free_bytes_acc: 0,
                cpu_acc_ns: 0,
                emitted: true, // prevent emit on drop
            };
        }
    };

    // Capture parent from TLS NOW (construction time).
    // This is the caller's span_id, which is the correct parent.
    let parent_span_id = parent::current_parent();
    let span_id = session.next_span_id();

    PianoFuture {
        inner: body,
        session: Some(session),
        span_id,
        parent_span_id,
        name_id,
        start_ticks: 0,
        thread_id: 0,
        alloc_count_acc: 0,
        alloc_bytes_acc: 0,
        free_count_acc: 0,
        free_bytes_acc: 0,
        cpu_acc_ns: 0,
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
                let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
                return inner.poll(cx);
            }
        };

        // Set TLS to this future's span_id for the duration of the poll.
        // Nested enter() calls will see this as their parent.
        let saved_tls = parent::set_parent(this.span_id);

        // Pre-poll bookkeeping
        let snap_start;
        {
            let _reentrancy = ReentrancyGuard::enter();
            snap_start = snapshot_alloc_counters();

            if this.start_ticks == 0 {
                this.start_ticks = read();
                this.thread_id = current_thread_id(&session.thread_id_alloc);
            }
        }

        compiler_fence(Ordering::SeqCst);

        let cpu_poll_start = if session.cpu_time_enabled {
            crate::cpu_clock::cpu_now_ns()
        } else {
            0
        };

        // Poll inner future
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

        // Restore TLS (this future is no longer "active")
        parent::restore_parent(saved_tls);

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

        let m = Measurement {
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name_id: self.name_id,
            start_ns: session.calibration.now_ns(self.start_ticks),
            end_ns: session.calibration.now_ns(read()),
            thread_id: self.thread_id,
            cpu_start_ns: 0,
            cpu_end_ns: self.cpu_acc_ns,
            alloc_count: self.alloc_count_acc,
            alloc_bytes: self.alloc_bytes_acc,
            free_count: self.free_count_acc,
            free_bytes: self.free_bytes_acc,
        };

        push_measurement(m, &session.registry);
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
// All our fields are Send-safe: primitives, Option<&'static ProfileSession>
// (ProfileSession is Sync, &'static T is Send when T is Sync).
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PianoFuture<core::future::Ready<()>>>();
    }
};
