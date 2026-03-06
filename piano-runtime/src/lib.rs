#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

mod alloc;
pub mod channel;
mod collector;
mod cpu_clock;
mod piano_future;
mod sync_util;
mod tsc;

// User-facing API: visible in docs
pub use alloc::PianoAllocator;
pub use collector::{
    collect, collect_all, collect_frames, enter, FrameFnSummary, FunctionRecord, Guard,
    InvocationRecord,
};

// Injection-only API: public for rewriter-generated code, hidden from docs
#[cfg(feature = "channels-async")]
#[doc(hidden)]
pub use channel::async_ch::{
    wrap_bounded as wrap_async_bounded, wrap_unbounded as wrap_async_unbounded,
};
#[cfg(feature = "channels-crossbeam")]
#[doc(hidden)]
pub use channel::crossbeam::{
    wrap_bounded as wrap_crossbeam_bounded, wrap_unbounded as wrap_crossbeam_unbounded,
};
#[cfg(feature = "channels-futures")]
#[doc(hidden)]
pub use channel::futures_ch::{
    wrap_bounded as wrap_futures_bounded, wrap_unbounded as wrap_futures_unbounded,
};
#[doc(hidden)]
pub use channel::std_mpsc::{
    wrap_channel as wrap_std_channel, wrap_sync_channel as wrap_std_sync_channel,
};
#[cfg(feature = "channels-tokio")]
#[doc(hidden)]
pub use channel::tokio_mpsc::{
    wrap_bounded as wrap_tokio_bounded, wrap_unbounded as wrap_tokio_unbounded,
};
#[doc(hidden)]
pub use channel::{register_channel, ChannelKind, ChannelStats};
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
