//! Output destination with I/O error counting.
//!
//! FileSink wraps a Mutex<BufWriter<File>> and an AtomicU64 error counter.
//! All write sites check their Result and increment the counter on failure.
//!
//! Invariants:
//! - Error counter scoped per output destination (no shared state).
//! - lock() heals mutex poisoning (same pattern as all other mutexes).
//! - Counter uses Relaxed ordering (sufficient for monotonic increment).

use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

pub struct FileSink {
    file: Mutex<BufWriter<File>>,
    io_errors: AtomicU64,
}

impl FileSink {
    pub fn new(file: File) -> Self {
        Self {
            file: Mutex::new(BufWriter::new(file)),
            io_errors: AtomicU64::new(0),
        }
    }

    /// Heals mutex poisoning via `into_inner`.
    pub fn lock(&self) -> MutexGuard<'_, BufWriter<File>> {
        self.file.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn record_io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn io_error_count(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }
}
