#![allow(unsafe_code)]
#![allow(renamed_and_removed_lints)]
#![allow(clippy::missing_const_for_thread_local)]

#[cfg(feature = "_test_internals")]
#[doc(hidden)]
pub mod cpu_clock;
#[cfg(not(feature = "_test_internals"))]
mod cpu_clock;

pub(crate) mod children;
pub(crate) mod inflight;
pub(crate) mod types;

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

pub use types::NameId;

// Public API for rewriter-generated code
pub use alloc::PianoAllocator;
pub use guard::enter;
pub use piano_future::{enter_async, PianoFuture};

#[cfg(test)]
mod fixtures;
#[allow(dead_code)]
pub mod generated;
mod generated_root_ops;
#[cfg(test)]
mod invariants;
pub mod predicates;

pub use generated::piano_runtime::{
    alloc_delta::ops::{accumulate_delta, compute_delta},
    alloc_snapshot::ops::snapshot_counter,
    async_execution::ops::{create_async, end_poll},
    interrupted_entry::ops::interrupt_execution,
    sync_execution::ops::{enter_sync, exit_sync},
};
pub use generated::piano_runtime::{
    alloc_delta::AllocDelta,
    alloc_snapshot::AllocSnapshot,
    async_execution::{mid_poll::MidPoll, poll_completed::PollCompleted},
    cpu_ns::CpuNs,
    interrupted_entry::InterruptedEntry,
    serialized_line::SerializedLine,
    ticks::Ticks,
    wall_ns::WallNs,
};
pub use generated::NameTable;
pub use generated_root_ops::{
    aggregate, begin_poll, calibrate_to_ns, compute_self_time, cpu_accumulate, cpu_duration, emit,
    mark_completed, read_cpu_clock, read_tsc, report_child_inclusive, serialize_aggregate,
    serialize_header, serialize_interrupted, serialize_trailer, ticks_delta, wall_accumulate,
    wall_duration, write_line,
};
