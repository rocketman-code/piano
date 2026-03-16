//! Output destination with I/O error counting.
//!
//! FileSink wraps a Mutex<File> and an AtomicU64 error counter.
//! All write sites check their Result and increment the counter
//! on failure. At shutdown, if errors > 0, a summary is printed
//! to stderr.
//!
//! Invariants:
//! - Error counter scoped per output destination (P1: no shared state).
//! - lock() heals mutex poisoning (same pattern as all other mutexes).
//! - Counter uses Relaxed ordering (sufficient for monotonic increment).

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// Output destination with I/O error counting.
pub struct FileSink {
    file: Mutex<File>,
    io_errors: AtomicU64,
}

impl FileSink {
    /// Create a new FileSink wrapping a File.
    pub fn new(file: File) -> Self {
        Self {
            file: Mutex::new(file),
            io_errors: AtomicU64::new(0),
        }
    }

    /// Lock the file for writing. Heals mutex poisoning.
    pub fn lock(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record an I/O error. Called by write sites on Err.
    pub fn record_io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the accumulated I/O error count.
    pub fn io_error_count(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }
}
