//! Output destination with I/O error counting.
//!
//! FileSink wraps a Mutex<File> and an AtomicU64 error counter.
//! All write sites check their Result and increment the counter
//! on failure. At shutdown, if errors > 0, a summary is printed
//! to stderr.
//!
//! Invariants:
//! - Error counter scoped per output destination (no shared state).
//! - lock() heals mutex poisoning (same pattern as all other mutexes).
//! - Counter uses Relaxed ordering (sufficient for monotonic increment).

use crate::alloc::ReentrancyGuard;
use crate::measurement::Measurement;
use crate::output::{write_measurements, write_trailer};
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

pub struct FileSink {
    file: Mutex<File>,
    io_errors: AtomicU64,
}

impl FileSink {
    pub fn new(file: File) -> Self {
        Self {
            file: Mutex::new(file),
            io_errors: AtomicU64::new(0),
        }
    }

    /// Heals mutex poisoning via `into_inner`.
    pub fn lock(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn record_io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn io_error_count(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }

    /// Write remaining measurements and trailer, then report any I/O errors
    /// to stderr. Used by both Ctx::drop and the atexit handler.
    pub(crate) fn flush_remaining(
        &self,
        measurements: &[Measurement],
        names: &[(u32, &str)],
        bias_ns: u64,
    ) {
        let _reentry = ReentrancyGuard::enter();
        {
            let mut file = self.lock();
            if write_measurements(&mut *file, measurements).is_err() {
                self.record_io_error();
            }
            if write_trailer(&mut *file, names, bias_ns).is_err() {
                self.record_io_error();
            }
        }

        let errors = self.io_error_count();
        if errors > 0 {
            // Not eprintln!: that panics if stderr write fails, crashing the host.
            let _ = std::io::Write::write_fmt(
                &mut std::io::stderr(),
                format_args!("piano: profiling data may be incomplete ({errors} write errors)\n"),
            );
        }
    }
}
