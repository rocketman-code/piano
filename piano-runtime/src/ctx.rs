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
use crate::buffer::{drain_buffers_for_file_sink, ensure_thread_file_sink, Registry};
use crate::guard::Guard;
use crate::output::{write_header, write_measurements, write_trailer};
use crate::piano_future::{PianoFuture, PianoFutureState};
use crate::shutdown;
use crate::time::CalibrationData;
use crate::span_id::next_span_id;
use crate::file_sink::FileSink;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

/// Profiling context. Carries the current span_id so children
/// know their parent. Threaded through instrumented code by the
/// rewriter.
///
/// Clone is cheap (Copy calibration + Arc clones). Send allows
/// passing to spawned threads and tasks.
#[derive(Clone)]
pub struct Ctx {
    current_span_id: u64,
    cpu_time_enabled: bool,
    file_sink: Option<Arc<FileSink>>,
    names: &'static [(u32, &'static str)],
    calibration: CalibrationData,
    span_id_alloc: Arc<AtomicU64>,
    thread_id_alloc: Arc<AtomicU64>,
    registry: Arc<Registry>,
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
        let calibration = CalibrationData::calibrate();
        let span_id_alloc = Arc::new(AtomicU64::new(1));
        let thread_id_alloc = Arc::new(AtomicU64::new(1));
        let registry: Arc<Registry> = Arc::new(Mutex::new(Vec::new()));

        if let Some(ref fs) = file_sink {
            if write_header(&mut *fs.lock(), names, calibration.bias_ns()).is_err() {
                fs.record_io_error();
            }

            shutdown::register(Arc::clone(fs), names, Arc::clone(&registry));
        }
        Self {
            current_span_id: 0,
            cpu_time_enabled,
            file_sink,
            names,
            calibration,
            span_id_alloc,
            thread_id_alloc,
            registry,
        }
    }

    /// Enter a sync instrumented function. Returns (Guard, child Ctx).
    ///
    /// Thin #[inline(always)] wrapper: calls enter_inner() for all
    /// bookkeeping (~190 instructions, one shared copy), then calls
    /// guard.stamp() to record rdtsc after the struct is materialized.
    /// Per-call-site cost: ~22 bytes (call + stamp + ret).
    #[inline(always)]
    pub fn enter(&self, name_id: u32) -> (Guard, Ctx) {
        let (mut guard, ctx) = self.enter_inner(name_id);
        guard.stamp();
        (guard, ctx)
    }

    /// All bookkeeping for entering a sync function. Regular function
    /// (not inlined) -- one shared copy across all call sites.
    ///
    /// Returns a Guard with start_ns = 0. The caller (enter) stamps
    /// the actual rdtsc after this returns.
    fn enter_inner(&self, name_id: u32) -> (Guard, Ctx) {
        if let Some(ref fs) = self.file_sink {
            ensure_thread_file_sink(fs, &self.registry);
        }
        let span_id = next_span_id(&self.span_id_alloc);
        let guard = Guard::new_uninstrumented(
            span_id,
            self.current_span_id,
            name_id,
            self.cpu_time_enabled,
            self.calibration,
            &self.thread_id_alloc,
            Arc::clone(&self.registry),
        );
        let child = Ctx {
            current_span_id: span_id,
            cpu_time_enabled: self.cpu_time_enabled,
            file_sink: self.file_sink.clone(),
            names: self.names,
            calibration: self.calibration,
            span_id_alloc: Arc::clone(&self.span_id_alloc),
            thread_id_alloc: Arc::clone(&self.thread_id_alloc),
            registry: Arc::clone(&self.registry),
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
            ensure_thread_file_sink(fs, &self.registry);
        }
        let span_id = next_span_id(&self.span_id_alloc);
        let state = PianoFutureState {
            span_id,
            parent_span_id: self.current_span_id,
            name_id,
            cpu_time_enabled: self.cpu_time_enabled,
            calibration: self.calibration,
            thread_id_alloc: Arc::clone(&self.thread_id_alloc),
            registry: Arc::clone(&self.registry),
        };
        let child = Ctx {
            current_span_id: span_id,
            cpu_time_enabled: self.cpu_time_enabled,
            file_sink: self.file_sink.clone(),
            names: self.names,
            calibration: self.calibration,
            span_id_alloc: Arc::clone(&self.span_id_alloc),
            thread_id_alloc: Arc::clone(&self.thread_id_alloc),
            registry: Arc::clone(&self.registry),
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
            let remaining = drain_buffers_for_file_sink(file_sink, &self.registry);

            let _reentry = ReentrancyGuard::enter();
            {
                let mut file = file_sink.lock();
                if write_measurements(&mut *file, &remaining).is_err() {
                    file_sink.record_io_error();
                }
                if write_trailer(&mut *file, self.names, self.calibration.bias_ns()).is_err() {
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
