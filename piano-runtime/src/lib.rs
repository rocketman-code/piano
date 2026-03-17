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
pub mod time;
#[cfg(not(feature = "_test_internals"))]
mod time;

// Modules exposed conditionally for integration tests
#[cfg(any(test, feature = "_test_internals"))]
pub mod buffer;
#[cfg(not(any(test, feature = "_test_internals")))]
mod buffer;

#[cfg(any(test, feature = "_test_internals"))]
pub mod guard;
#[cfg(not(any(test, feature = "_test_internals")))]
mod guard;

#[cfg(any(test, feature = "_test_internals"))]
pub mod measurement;
#[cfg(not(any(test, feature = "_test_internals")))]
mod measurement;

#[cfg(any(test, feature = "_test_internals"))]
pub mod output;
#[cfg(not(any(test, feature = "_test_internals")))]
mod output;

#[cfg(any(test, feature = "_test_internals"))]
pub mod shutdown;
#[cfg(not(any(test, feature = "_test_internals")))]
mod shutdown;

#[cfg(any(test, feature = "_test_internals"))]
pub mod span_id;
#[cfg(not(any(test, feature = "_test_internals")))]
mod span_id;

// Rewriter-referenced modules: public because rewriter generates module paths
// (e.g. __piano_ctx: piano_runtime::ctx::Ctx, piano_runtime::file_sink::FileSink::new)
#[doc(hidden)]
pub mod ctx;
#[doc(hidden)]
pub mod file_sink;

// Internal modules: rewriter uses crate-root re-exports (PianoAllocator, PianoFuture),
// not module paths. Private in production, pub for test access only.
#[cfg(any(test, feature = "_test_internals"))]
pub mod alloc;
#[cfg(not(any(test, feature = "_test_internals")))]
mod alloc;

#[cfg(any(test, feature = "_test_internals"))]
pub mod piano_future;
#[cfg(not(any(test, feature = "_test_internals")))]
mod piano_future;

// User-facing API: visible in docs
pub use alloc::PianoAllocator;
#[doc(hidden)]
pub use piano_future::PianoFuture;
