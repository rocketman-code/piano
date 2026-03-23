//! Profiling session -- immutable shared infrastructure.
//!
//! ProfileSession holds everything a Guard needs that doesn't change
//! per-call: calibration data, span_id allocator, thread_id allocator,
//! buffer registry, file sink, name table.
//!
//! Created once in main(), leaked as &'static. Guards read it without
//! parameters via get(), which checks thread-local first (one instruction),
//! then falls back to the global AtomicPtr for spawned threads.
//!
//! TLS-first lookup serves two purposes:
//! - Performance: Cell::get is faster than AtomicPtr::load (no ordering)
//! - Test isolation: each test thread sets its own TLS session, preventing
//!   concurrent init() calls from interfering with each other.
//!
//! Invariants:
//! - Leaked once per init(), never freed (OS reclaims on exit).
//! - Returns None before init and during TLS teardown (safe no-op).
//! - All fields are Send + Sync (Arcs, atomics, Copy types, &'static refs).

use crate::buffer::Registry;
use crate::file_sink::FileSink;
use crate::output::write_header;
use crate::shutdown;
use crate::time::CalibrationData;

use std::cell::Cell;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct ProfileSession {
    pub(crate) calibration: CalibrationData,
    pub(crate) cpu_time_enabled: bool,
    pub(crate) file_sink: Option<Arc<FileSink>>,
    pub(crate) names: &'static [(u32, &'static str)],
    pub(crate) span_id_alloc: AtomicU64,
    pub(crate) thread_id_alloc: AtomicU64,
    pub(crate) registry: Arc<Registry>,
}

// SAFETY: All fields are Send + Sync. AtomicU64 is Sync. Arc<T> is
// Send+Sync when T is Send+Sync. CalibrationData is Copy. &'static
// refs are Send+Sync. Interior mutability is behind Arc/Atomic.
unsafe impl Send for ProfileSession {}
unsafe impl Sync for ProfileSession {}

/// Global session pointer for spawned threads (fallback when TLS is empty).
static GLOBAL_SESSION: AtomicPtr<ProfileSession> = AtomicPtr::new(std::ptr::null_mut());

/// Per-thread session pointer. Checked first by get(). Set by init()
/// on the calling thread, and lazily cached from GLOBAL_SESSION on
/// spawned threads after their first get() call.
thread_local! {
    static THREAD_SESSION: Cell<*const ProfileSession> = const { Cell::new(std::ptr::null()) };
}

impl ProfileSession {
    /// Create a profiling session, write the NDJSON header, register
    /// for atexit/signal cleanup, and leak as &'static.
    ///
    /// Sets both thread-local (calling thread) and global (spawned threads).
    pub fn init(
        file_sink: Option<Arc<FileSink>>,
        cpu_time_enabled: bool,
        names: &'static [(u32, &'static str)],
    ) -> &'static Self {
        let calibration = CalibrationData::calibrate();
        let registry: Arc<Registry> = Arc::new(Mutex::new(Vec::new()));

        if let Some(ref fs) = file_sink {
            if write_header(&mut *fs.lock(), names, calibration.bias_ns()).is_err() {
                fs.record_io_error();
            }

            shutdown::register(Arc::clone(fs), names, Arc::clone(&registry));
        }

        let session = Box::new(Self {
            calibration,
            cpu_time_enabled,
            file_sink,
            names,
            span_id_alloc: AtomicU64::new(1),
            thread_id_alloc: AtomicU64::new(1),
            registry,
        });

        let ptr = Box::into_raw(session);

        // Set global for spawned threads
        GLOBAL_SESSION.store(ptr, Ordering::Release);

        // Set thread-local for the calling thread (fast path)
        let _ = THREAD_SESSION.try_with(|c| c.set(ptr));

        // SAFETY: ptr was just allocated via Box::into_raw.
        // It is never freed (intentional leak for 'static).
        unsafe { &*ptr }
    }

    /// Load the active session. TLS first (1 instruction), global fallback.
    ///
    /// On first call from a spawned thread, caches the global pointer in
    /// TLS so subsequent calls hit the fast path.
    #[inline(always)]
    pub fn get() -> Option<&'static Self> {
        let ptr = THREAD_SESSION
            .try_with(|c| {
                let p = c.get();
                if !p.is_null() {
                    return p;
                }
                // Spawned thread, first call: cache global → TLS
                let global = GLOBAL_SESSION.load(Ordering::Acquire);
                if !global.is_null() {
                    c.set(global);
                }
                global
            })
            .unwrap_or_else(|_| {
                // TLS destroyed (thread teardown): try global directly
                GLOBAL_SESSION.load(Ordering::Acquire)
            });

        if ptr.is_null() {
            None
        } else {
            // SAFETY: non-null ptr was set by init() via Box::into_raw.
            // It is never freed. The &'static lifetime is valid.
            Some(unsafe { &*ptr })
        }
    }

    /// Allocate the next span_id. Monotonically increasing, non-zero.
    #[inline(always)]
    pub fn next_span_id(&self) -> u64 {
        self.span_id_alloc.fetch_add(1, Ordering::Relaxed)
    }
}
