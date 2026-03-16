#![allow(unsafe_code)]

//! Per-thread measurement storage.
//!
//! Buffer is a pure storage mechanism. It accumulates measurements
//! and drains them on request. It does NOT make flush decisions --
//! that policy belongs to the output layer.
//!
//! Invariants:
//! - Measurements are never duplicated: drain() moves data out,
//!   double-drain returns empty. Enforced by Vec::drain iterator.
//! - Measurements are never lost within a buffer: push appends,
//!   drain collects all. No silent drops.
//! - Thread buffers survive thread exit: Arc in global registry
//!   keeps the buffer alive after TLS destruction.
//! - All mutex locks heal poisoning (unwrap_or_else into_inner).
//!   Profiler never crashes the host.

use crate::alloc::ReentrancyGuard;
use crate::file_sink::FileSink;
use crate::measurement::Measurement;
use crate::output::write_measurements;
use std::cell::{Cell, RefCell};
use std::sync::{Arc, Mutex, Once};

/// Flush threshold -- buffer drains to file when this many measurements
/// accumulate. 1024 entries = ~80KB per thread. Tuning parameter, not
/// a derived constant.
pub const FLUSH_THRESHOLD: usize = 1024;

/// Per-thread measurement storage. Accumulates completed Measurements
/// and drains them on request.
///
/// Wrapped in Arc<Mutex<>> for TLS + registry sharing. The Mutex is
/// uncontended during normal execution (single writer per thread).
pub struct ThreadBuffer {
    measurements: Vec<Measurement>,
    file_sink: Option<Arc<FileSink>>,
}

impl Default for ThreadBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadBuffer {
    pub fn new() -> Self {
        Self {
            measurements: Vec::new(),
            file_sink: None,
        }
    }

    /// Push a measurement into the buffer.
    pub fn push(&mut self, m: Measurement) {
        self.measurements.push(m);
    }

    /// Drain all measurements from the buffer, returning them.
    /// After drain, the buffer is empty but retains its capacity.
    pub fn drain(&mut self) -> Vec<Measurement> {
        self.measurements.drain(..).collect()
    }

    /// Number of measurements currently buffered.
    pub fn len(&self) -> usize {
        self.measurements.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.measurements.is_empty()
    }
}

// ---------------------------------------------------------------------------
// TLS storage + global registry
// ---------------------------------------------------------------------------

// Registry of all thread buffers for shutdown access.
// Uses Once + unsafe because Mutex::new() is not const on MSRV 1.59.
fn registry() -> &'static Mutex<Vec<Arc<Mutex<ThreadBuffer>>>> {
    static ONCE: Once = Once::new();
    static mut REGISTRY: *const Mutex<Vec<Arc<Mutex<ThreadBuffer>>>> = std::ptr::null();

    // SAFETY: ONCE.call_once guarantees single initialization. After init,
    // REGISTRY points to a heap-allocated Mutex that is never moved or freed
    // (intentional leak for 'static lifetime). The static mut is required
    // because OnceLock is unavailable on MSRV 1.59.
    unsafe {
        ONCE.call_once(|| {
            REGISTRY = Box::into_raw(Box::new(Mutex::new(Vec::new())));
        });
        &*REGISTRY
    }
}

thread_local! {
    static THREAD_BUFFER: RefCell<Option<Arc<Mutex<ThreadBuffer>>>> =
        const { RefCell::new(None) };

    /// Fast-path flag: once the file_sink is set on this thread's buffer,
    /// subsequent ensure_thread_file_sink calls skip the mutex lock entirely.
    static FILE_SINK_SET: Cell<bool> = const { Cell::new(false) };
}

/// Ensure the current thread's buffer has a reference to the file sink.
/// Called by Ctx::enter() to propagate the file handle to each thread.
/// Uses a Cell<bool> fast path -- after the first successful set,
/// subsequent calls are just a Cell::get (~0.3ns).
pub(crate) fn ensure_thread_file_sink(file_sink: &Arc<FileSink>) {
    // Fast path: already set on this thread
    if FILE_SINK_SET.try_with(|c| c.get()).unwrap_or(true) {
        return;
    }

    let _ = THREAD_BUFFER.try_with(|cell| {
        let mut borrow = match cell.try_borrow_mut() {
            Ok(b) => b,
            Err(_) => return,
        };

        let arc = borrow.get_or_insert_with(|| {
            let arc = Arc::new(Mutex::new(ThreadBuffer::new()));
            registry()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Arc::clone(&arc));
            arc
        });

        let mut buf = arc.lock().unwrap_or_else(|e| e.into_inner());
        if buf.file_sink.is_none() {
            buf.file_sink = Some(Arc::clone(file_sink));
        }
    });

    let _ = FILE_SINK_SET.try_with(|c| c.set(true));
}

/// Push a measurement to the current thread's buffer.
///
/// On first call per thread, initializes the buffer and registers it
/// in the global registry for shutdown access.
/// Silent no-op if TLS is destroyed (thread teardown -- measurement
/// is dropped, but the thread is dying anyway).
pub fn push_measurement(m: Measurement) {
    let _ = THREAD_BUFFER.try_with(|cell| {
        // try_borrow_mut: silent no-op on reentrant borrow.
        // Same degradation principle as TLS teardown — data loss
        // over host crash.
        let mut borrow = match cell.try_borrow_mut() {
            Ok(b) => b,
            Err(_) => return,
        };

        // Lazy init: create buffer and register on first push
        let arc = borrow.get_or_insert_with(|| {
            let arc = Arc::new(Mutex::new(ThreadBuffer::new()));
            registry()
                .lock()
                .unwrap_or_else(|e| e.into_inner()) // heal poisoned mutex
                .push(Arc::clone(&arc));
            arc
        });

        // Push and check threshold under buffer lock.
        // If threshold reached, drain and clone file_sink while locked,
        // then release buffer lock before acquiring file lock.
        let flush_data = {
            let mut buf = arc.lock().unwrap_or_else(|e| e.into_inner());
            buf.push(m);

            if buf.len() >= FLUSH_THRESHOLD {
                buf.file_sink.clone().map(|file_arc| (buf.drain(), file_arc))
            } else {
                None
            }
        }; // buffer mutex released here

        // Flush outside buffer lock (lock ordering: buffer then file, never simultaneous)
        if let Some((drained, file_arc)) = flush_data {
            let _reentry = ReentrancyGuard::enter();
            if write_measurements(&mut *file_arc.lock(), &drained).is_err() {
                file_arc.record_io_error();
            }
        }
    });
}

/// Drain the current thread's buffer only. Returns all measurements
/// buffered on this thread, leaving other threads' buffers untouched.
///
/// Used for per-thread cleanup and test isolation. Returns empty Vec
/// if no buffer has been initialized on this thread or if TLS is
/// destroyed (thread teardown).
pub fn drain_thread_buffer() -> Vec<Measurement> {
    THREAD_BUFFER
        .try_with(|cell| {
            let borrow = cell.borrow();
            match borrow.as_ref() {
                Some(arc) => {
                    let mut buf = arc.lock().unwrap_or_else(|e| e.into_inner());
                    buf.drain()
                }
                None => Vec::new(),
            }
        })
        .unwrap_or_default()
}

/// Drain buffers associated with the given file_sink.
/// Uses Arc::ptr_eq to identify ownership -- only drains buffers whose
/// file_sink points to the same allocation. Used by Ctx::drop.
///
/// Safe to call concurrently -- Mutex serializes drains.
/// Double-drain returns empty (no duplicates).
pub fn drain_buffers_for_file_sink(file_sink: &Arc<FileSink>) -> Vec<Measurement> {
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut all = Vec::new();
    for buf_arc in reg.iter() {
        let mut buf = buf_arc.lock().unwrap_or_else(|e| e.into_inner());
        if buf.file_sink.as_ref().map_or(false, |fs| Arc::ptr_eq(fs, file_sink)) {
            all.extend(buf.drain());
        }
    }
    all
}

/// Drain all registered thread buffers. Used by atexit handler as
/// last-resort cleanup -- drains everything regardless of file_sink.
///
/// Safe to call concurrently -- Mutex serializes drains.
/// Double-drain returns empty (no duplicates).
pub fn drain_all_buffers() -> Vec<Measurement> {
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    let mut all = Vec::new();
    for buf_arc in reg.iter() {
        let mut buf = buf_arc.lock().unwrap_or_else(|e| e.into_inner());
        all.extend(buf.drain());
    }
    all
}
