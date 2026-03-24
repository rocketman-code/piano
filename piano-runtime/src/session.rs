//! Profiling session -- immutable shared infrastructure.
//!
//! ProfileSession holds calibration data, file sink, name table,
//! and the aggregation registry. Created once in main(), leaked as
//! &'static. Guards and PianoFutures read it without parameters via
//! get(), which checks thread-local first (one instruction), then
//! falls back to the global AtomicPtr for spawned threads.
//!
//! Invariants:
//! - Leaked once per init(), never freed (OS reclaims on exit).
//! - Returns None before init and during TLS teardown (safe no-op).
//! - All fields are Send + Sync (Arcs, Copy types, &'static refs).

use crate::aggregator::AggRegistry;
use crate::file_sink::FileSink;
use crate::output::write_header;
use crate::shutdown;
use crate::time::CalibrationData;

use std::cell::Cell;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};

pub struct ProfileSession {
    pub(crate) calibration: CalibrationData,
    pub(crate) cpu_time_enabled: bool,
    pub(crate) file_sink: Option<Arc<FileSink>>,
    pub(crate) names: &'static [(u32, &'static str)],
    pub(crate) agg_registry: Arc<AggRegistry>,
}

// SAFETY: All fields are Send + Sync. Arc<T> is Send+Sync when T is
// Send+Sync. CalibrationData is Copy. &'static refs are Send+Sync.
unsafe impl Send for ProfileSession {}
unsafe impl Sync for ProfileSession {}

/// Global session pointer for spawned threads.
static GLOBAL_SESSION: AtomicPtr<ProfileSession> = AtomicPtr::new(std::ptr::null_mut());

thread_local! {
    static THREAD_SESSION: Cell<*const ProfileSession> = const { Cell::new(std::ptr::null()) };
}

impl ProfileSession {
    pub fn init(
        file_sink: Option<Arc<FileSink>>,
        cpu_time_enabled: bool,
        names: &'static [(u32, &'static str)],
    ) -> &'static Self {
        let calibration = CalibrationData::calibrate();
        let agg_registry: Arc<AggRegistry> = Arc::new(Mutex::new(Vec::new()));

        if let Some(ref fs) = file_sink {
            let mut file = fs.lock();
            if write_header(&mut *file, names, calibration.bias_ns()).is_err() {
                fs.record_io_error();
            }
            if std::io::Write::flush(&mut *file).is_err() {
                fs.record_io_error();
            }
            drop(file);

            shutdown::register(Arc::clone(fs), names, Arc::clone(&agg_registry));
        }

        let session = Box::new(Self {
            calibration,
            cpu_time_enabled,
            file_sink,
            names,
            agg_registry,
        });

        let ptr = Box::into_raw(session);
        GLOBAL_SESSION.store(ptr, Ordering::Release);
        let _ = THREAD_SESSION.try_with(|c| c.set(ptr));

        // SAFETY: ptr was just allocated via Box::into_raw.
        // It is never freed (intentional leak for 'static).
        unsafe { &*ptr }
    }

    #[inline(always)]
    pub fn get() -> Option<&'static Self> {
        let ptr = THREAD_SESSION
            .try_with(|c| {
                let p = c.get();
                if !p.is_null() {
                    return p;
                }
                let global = GLOBAL_SESSION.load(Ordering::Acquire);
                if !global.is_null() {
                    c.set(global);
                }
                global
            })
            .unwrap_or_else(|_| {
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
}
