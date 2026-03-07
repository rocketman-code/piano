#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

mod alloc;
mod collector;
mod cpu_clock;
mod piano_future;
#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod tsc;
#[cfg(not(feature = "_test_internals"))]
mod tsc;

// User-facing API: visible in docs
pub use alloc::PianoAllocator;
pub use collector::{
    collect, collect_all, collect_frames, enter, FrameFnSummary, FunctionRecord, Guard,
    InvocationRecord,
};

// Injection-only API: public for rewriter-generated code, hidden from docs
#[doc(hidden)]
pub use collector::{
    adopt, flush, fork, init, register, reset, set_runs_dir, shutdown, shutdown_to, AdoptGuard,
    SpanContext,
};
#[doc(hidden)]
pub use piano_future::PianoFuture;

#[cfg(test)]
pub use collector::clear_runs_dir;
#[cfg(any(test, feature = "_test_internals"))]
pub use collector::collect_invocations;
