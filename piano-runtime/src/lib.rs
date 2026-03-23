#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod cpu_clock;
#[cfg(not(feature = "_test_internals"))]
mod cpu_clock;

mod thread_id;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod parent;
#[cfg(not(feature = "_test_internals"))]
pub(crate) mod parent;

#[doc(hidden)]
pub mod session;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod time;
#[cfg(not(feature = "_test_internals"))]
mod time;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod buffer;
#[cfg(not(feature = "_test_internals"))]
mod buffer;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod guard;
#[cfg(not(feature = "_test_internals"))]
mod guard;

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod measurement;
#[cfg(not(feature = "_test_internals"))]
mod measurement;

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

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod span_id;
#[cfg(not(feature = "_test_internals"))]
mod span_id;

// Rewriter-referenced modules
#[doc(hidden)]
pub mod file_sink;

// Internal modules: rewriter uses crate-root re-exports (PianoAllocator, PianoFuture),
// not module paths. Private in production, pub for test access only.
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
