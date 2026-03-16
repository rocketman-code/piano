//! Context propagation -- parent-child span relationships.
//!
//! Ctx is the user-facing handle for instrumented code. It carries
//! the current span_id so children know their parent. Pure value
//! propagation -- no global state, no TLS.
//!
//! Invariants:
//! - Parent-child relationships are correct by construction.
//!   Enforcement: enter() returns child Ctx with new span_id as
//!   current. Pure value propagation, no global state, no TLS.
//! - Ctx is Clone + Send (shared across spawned tasks/threads).
//! - Root Ctx has current_span_id = 0.
//! - Root Ctx::drop drains remaining buffers, writes trailer.
//!   Child Ctx::drop is a no-op (current_span_id != 0).

use crate::alloc::ReentrancyGuard;
use crate::buffer::{drain_buffers_for_file_sink, ensure_thread_file_sink};
use crate::guard::Guard;
use crate::output::{write_header, write_measurements, write_trailer};
use crate::piano_future::{PianoFuture, PianoFutureState};
use crate::shutdown;
use crate::time;
use crate::span_id::next_span_id;
use crate::file_sink::FileSink;
use std::future::Future;
use std::sync::Arc;

/// Profiling context. Carries the current span_id so children
/// know their parent. Threaded through instrumented code by the
/// rewriter.
///
/// Clone is cheap (two integers + one bool + one fat pointer + one Arc
/// clone ~9ns). Send allows passing to spawned threads and tasks.
#[derive(Clone)]
pub struct Ctx {
    current_span_id: u64,
    cpu_time_enabled: bool,
    file_sink: Option<Arc<FileSink>>,
    names: &'static [(u32, &'static str)],
}

impl Ctx {
    /// Create a root context. current_span_id = 0 (no parent).
    ///
    /// If file_sink is Some, writes the NDJSON header eagerly (name table
    /// is complete from the first byte) and registers for atexit cleanup.
    /// The names slice is stored for writing the trailer at shutdown.
    pub fn new(
        file_sink: Option<Arc<FileSink>>,
        cpu_time_enabled: bool,
        names: &'static [(u32, &'static str)],
    ) -> Self {
        time::init();
        if let Some(ref fs) = file_sink {
            if write_header(&mut *fs.lock(), names, time::bias_ns()).is_err() {
                fs.record_io_error();
            }

            shutdown::register(Arc::clone(fs), names);
        }
        Self {
            current_span_id: 0,
            cpu_time_enabled,
            file_sink,
            names,
        }
    }

    /// Enter a sync instrumented function. Returns (Guard, child Ctx).
    ///
    /// Allocates a new span_id. The Guard stores it (for the
    /// Measurement's span_id) and self.current_span_id as the
    /// parent_span_id. The returned child Ctx gets current_span_id
    /// set to the new span_id (so grandchildren use it as parent).
    pub fn enter(&self, name_id: u32) -> (Guard, Ctx) {
        if let Some(ref fs) = self.file_sink {
            ensure_thread_file_sink(fs);
        }
        let span_id = next_span_id();
        let guard = Guard::new(span_id, self.current_span_id, name_id, self.cpu_time_enabled);
        let child = Ctx {
            current_span_id: span_id,
            cpu_time_enabled: self.cpu_time_enabled,
            file_sink: self.file_sink.clone(),
            names: self.names,
        };
        (guard, child)
    }

    /// Enter an async instrumented function. Returns (PianoFutureState, child Ctx).
    ///
    /// Allocates a new span_id, creates a PianoFutureState for
    /// PianoFuture::new(), and returns a child Ctx. The child Ctx
    /// is captured by the async move block so nested calls use it.
    pub fn enter_async(&self, name_id: u32) -> (PianoFutureState, Ctx) {
        if let Some(ref fs) = self.file_sink {
            ensure_thread_file_sink(fs);
        }
        let span_id = next_span_id();
        let state = PianoFutureState {
            span_id,
            parent_span_id: self.current_span_id,
            name_id,
            cpu_time_enabled: self.cpu_time_enabled,
        };
        let child = Ctx {
            current_span_id: span_id,
            cpu_time_enabled: self.cpu_time_enabled,
            file_sink: self.file_sink.clone(),
            names: self.names,
        };
        (state, child)
    }

    /// Wrap an async body with PianoFuture instrumentation.
    /// Convenience method combining enter_async + PianoFuture::new.
    pub fn instrument_async<F: Future>(&self, name_id: u32, body: F) -> (PianoFuture<F>, Ctx) {
        let (state, child) = self.enter_async(name_id);
        (PianoFuture::new(state, body), child)
    }
}

impl Drop for Ctx {
    fn drop(&mut self) {
        // Only the root Ctx triggers shutdown (current_span_id == 0).
        // Child Ctxes from enter() have nonzero span_ids -- their drop
        // is a no-op. Double-drain is safe: Mutex serializes, second
        // drainer finds empty buffers and writes a harmless extra trailer.
        if self.current_span_id != 0 {
            return;
        }

        if let Some(ref file_sink) = self.file_sink {
            let remaining = drain_buffers_for_file_sink(file_sink);

            let _reentry = ReentrancyGuard::enter();
            {
                let mut file = file_sink.lock();
                if write_measurements(&mut *file, &remaining).is_err() {
                    file_sink.record_io_error();
                }
                if write_trailer(&mut *file, self.names, time::bias_ns()).is_err() {
                    file_sink.record_io_error();
                }
            }

            let errors = file_sink.io_error_count();
            if errors > 0 {
                eprintln!(
                    "piano: profiling data may be incomplete ({} write errors)",
                    errors
                );
            }
        }
    }
}
