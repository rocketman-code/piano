#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

mod cpu_clock;
mod thread_id;
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

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod tsc;
#[cfg(not(feature = "_test_internals"))]
mod tsc;

// Injection-only API: public for rewriter-generated code, hidden from docs
#[doc(hidden)]
pub mod ctx;
#[doc(hidden)]
pub mod file_sink;
#[doc(hidden)]
pub mod alloc;
#[doc(hidden)]
pub mod piano_future;

// User-facing API: visible in docs
pub use alloc::PianoAllocator;
#[doc(hidden)]
pub use piano_future::PianoFuture;
