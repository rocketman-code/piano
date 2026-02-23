#![allow(unsafe_code)]

mod alloc;
mod collector;

pub use alloc::PianoAllocator;
#[cfg(test)]
pub use collector::collect_invocations;
pub use collector::{
    AdoptGuard, FrameFnSummary, FunctionRecord, Guard, InvocationRecord, SpanContext, adopt,
    collect, collect_all, collect_frames, enter, flush, fork, init, register, reset, shutdown,
};
