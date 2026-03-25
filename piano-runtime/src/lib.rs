#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod cpu_clock;
#[cfg(not(feature = "_test_internals"))]
mod cpu_clock;

pub(crate) mod children;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod aggregator;
#[cfg(not(feature = "_test_internals"))]
pub(crate) mod aggregator;

#[doc(hidden)]
pub mod session;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod time;
#[cfg(not(feature = "_test_internals"))]
mod time;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod guard;
#[cfg(not(feature = "_test_internals"))]
mod guard;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod output;
#[cfg(not(feature = "_test_internals"))]
mod output;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod shutdown;
#[cfg(not(feature = "_test_internals"))]
mod shutdown;

// Rewriter-referenced modules
#[doc(hidden)]
pub mod file_sink;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod alloc;
#[cfg(not(feature = "_test_internals"))]
mod alloc;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod piano_future;
#[cfg(not(feature = "_test_internals"))]
mod piano_future;

// Public API for rewriter-generated code
pub use alloc::PianoAllocator;
pub use guard::enter;
pub use piano_future::{enter_async, PianoFuture};
