#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

mod alloc;
mod collector;
mod cpu_clock;
mod tsc;

pub use alloc::{AllocAccumulator, PianoAllocator};
#[cfg(test)]
pub use collector::clear_runs_dir;
#[cfg(any(test, feature = "_test_internals"))]
pub use collector::collect_invocations;
pub use collector::{
    adopt, collect, collect_all, collect_frames, enter, flush, fork, init, register, reset,
    set_runs_dir, shutdown, shutdown_to, AdoptGuard, FrameFnSummary, FunctionRecord, Guard,
    InvocationRecord, SpanContext,
};
