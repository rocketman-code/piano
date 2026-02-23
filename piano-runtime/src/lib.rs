#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

mod alloc;
mod collector;

pub use alloc::PianoAllocator;
#[cfg(test)]
pub use collector::collect_invocations;
pub use collector::{
    adopt, collect, collect_all, collect_frames, enter, flush, fork, init, register, reset,
    shutdown, AdoptGuard, FrameFnSummary, FunctionRecord, Guard, InvocationRecord, SpanContext,
};
