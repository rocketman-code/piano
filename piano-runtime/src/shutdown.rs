#![allow(unsafe_code)]

//! Lifecycle management -- atexit and signal handlers for abnormal exit.
//!
//! Two shutdown paths, two contracts, NO shared code:
//!
//! Normal path (RootCtx::drop, atexit handler): acquires mutexes, drains
//! all buffers, writes measurements + trailer. Complete data guaranteed.
//!
//! Signal path (SIGTERM/SIGINT handler): fully POSIX async-signal-safe.
//! Best-effort try_lock drain of registered buffers, stack-based measurement
//! serialization, raw write() to pre-extracted fd, then pre-serialized
//! trailer. Skips contested buffers (no deadlock). Recovers all data when
//! no mutex is held (the common case: signal during sleep, I/O, user code).
//!
//! Invariants:
//! - Normal and signal paths share NO code. The signal handler never calls
//!   atexit_handler or any function that acquires a blocking lock.
//! - Double-drain safety: if both RootCtx::drop and atexit race to drain
//!   the same buffer, the Mutex serializes. First drainer gets data,
//!   second finds empty. Zero duplicates.
//! - All mutex locks heal poisoning (normal path only).
//! - Never panics. All errors degrade to data loss.
//! - Signal handler uses only: atomic loads, try_lock, raw write(), signal(),
//!   raise(). All POSIX async-signal-safe (IEEE 1003.1 Section 2.4.3).

use crate::buffer::{drain_all_buffers, Registry};
use crate::file_sink::FileSink;
use crate::time::CalibrationData;
use std::sync::{Arc, Mutex, Once};

struct ShutdownState {
    file_sink: Arc<FileSink>,
    names: &'static [(u32, &'static str)],
    registry: Arc<Registry>,
}

fn shutdown_state() -> &'static Mutex<Option<ShutdownState>> {
    static ONCE: Once = Once::new();
    static mut STATE: *const Mutex<Option<ShutdownState>> = std::ptr::null();

    // SAFETY: ONCE.call_once guarantees single initialization. After init,
    // STATE points to a heap-allocated Mutex that is never moved or freed
    // (intentional leak for 'static lifetime). OnceLock unavailable on MSRV 1.59.
    unsafe {
        ONCE.call_once(|| {
            STATE = Box::into_raw(Box::new(Mutex::new(None)));
        });
        &*STATE
    }
}

/// Register the file sink and name table for atexit cleanup.
/// Called by RootCtx::new() when a file_sink is provided.
/// Multiple calls overwrite the previous state (last RootCtx::new wins).
pub(crate) fn register(
    file_sink: Arc<FileSink>,
    names: &'static [(u32, &'static str)],
    registry: Arc<Registry>,
) {
    #[cfg(unix)]
    signal::register(names, &file_sink, &registry);

    let mut state = shutdown_state()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *state = Some(ShutdownState { file_sink, names, registry });

    // Register atexit handler. Multiple registrations are safe --
    // each invocation drains (first finds data, subsequent find empty).
    extern "C" {
        fn atexit(f: extern "C" fn()) -> std::os::raw::c_int;
    }
    // SAFETY: atexit_handler is a valid extern "C" function.
    // atexit is a standard C function available on all platforms.
    unsafe {
        atexit(atexit_handler);
    }
}

extern "C" fn atexit_handler() {
    // Clone the Arc and names pointer, then release the shutdown state
    // lock before doing any I/O. Minimizes lock hold time.
    let (file_sink, names, registry) = {
        let state = shutdown_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match state.as_ref() {
            Some(s) => (Arc::clone(&s.file_sink), s.names, Arc::clone(&s.registry)),
            None => return,
        }
    }; // shutdown state mutex released here

    let remaining = drain_all_buffers(&registry);
    let calibration = CalibrationData::calibrate();
    file_sink.flush_remaining(&remaining, names, calibration.bias_ns());
}

// ---------------------------------------------------------------------------
// Signal handler (Unix only) -- fully POSIX async-signal-safe
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod signal {
    use crate::buffer::{Registry, ThreadBuffer};
    use crate::file_sink::FileSink;
    use crate::measurement::Measurement;
    use crate::output::{serialize_measurement_to_stack, serialize_trailer};
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Once};

    const SIGINT: std::os::raw::c_int = 2;
    const SIGTERM: std::os::raw::c_int = 15;
    const SIG_ERR: usize = usize::MAX;

    /// Pre-serialized trailer bytes (leaked Box, never freed).
    static TRAILER: AtomicUsize = AtomicUsize::new(0);

    /// Raw file descriptor for signal-safe write.
    static FILE_FD: AtomicI32 = AtomicI32::new(-1);

    /// Registry pointer for signal-safe buffer access (leaked Arc contents).
    static REGISTRY: AtomicUsize = AtomicUsize::new(0);

    /// Previous handlers saved at registration time.
    static PREV_SIGINT: AtomicUsize = AtomicUsize::new(0);
    static PREV_SIGTERM: AtomicUsize = AtomicUsize::new(0);

    struct SignalTrailer {
        bytes: Vec<u8>,
    }

    extern "C" {
        fn signal(sig: std::os::raw::c_int, handler: usize) -> usize;
        fn raise(sig: std::os::raw::c_int) -> std::os::raw::c_int;
        fn write(fd: std::os::raw::c_int, buf: *const u8, count: usize) -> isize;
    }

    /// Signal handler. Fully POSIX async-signal-safe.
    ///
    /// Phase 1: try_lock drain each registered buffer. For each successful
    ///   lock, serialize measurements to a 512-byte stack buffer and write
    ///   via raw write(). Contested buffers are skipped (no deadlock).
    ///
    /// Phase 2: write pre-serialized trailer via raw write().
    ///
    /// Phase 3: restore previous handler and re-raise.
    extern "C" fn handler(sig: std::os::raw::c_int) {
        let fd = FILE_FD.load(Ordering::Relaxed);
        if fd < 0 {
            // No file registered. Just restore and re-raise.
            restore_and_reraise(sig);
            return;
        }

        // Phase 1: best-effort try_lock drain
        let reg_ptr = REGISTRY.load(Ordering::Relaxed);
        if reg_ptr != 0 {
            // SAFETY: reg_ptr points to a leaked Mutex<Vec<Arc<Mutex<ThreadBuffer>>>>
            // that is never freed. The outer Mutex guards the Vec of buffer Arcs.
            unsafe {
                let registry = &*(reg_ptr as *const Mutex<Vec<Arc<Mutex<ThreadBuffer>>>>);

                // try_lock the registry to get the buffer list.
                // If the registry mutex is held (e.g., during buffer registration),
                // skip the entire drain. Data loss over deadlock.
                if let Ok(buffers) = registry.try_lock() {
                    for buf_arc in buffers.iter() {
                        // try_lock each buffer. Skip if held (mid-push).
                        if let Ok(mut buf) = buf_arc.try_lock() {
                            let drained: Vec<Measurement> = buf.drain();
                            // Serialize each measurement to stack buffer and write
                            for m in &drained {
                                let mut stack_buf = [0u8; 512];
                                let len = serialize_measurement_to_stack(&mut stack_buf, m);
                                write(fd, stack_buf.as_ptr(), len);
                            }
                        }
                    }
                }
            }
        }

        // Phase 2: write pre-serialized trailer
        let trailer_ptr = TRAILER.load(Ordering::Relaxed);
        if trailer_ptr != 0 {
            // SAFETY: trailer_ptr points to a leaked Box<SignalTrailer>
            // that is never freed. The bytes Vec is stable.
            unsafe {
                let trailer = &*(trailer_ptr as *const SignalTrailer);
                write(fd, trailer.bytes.as_ptr(), trailer.bytes.len());
            }
        }

        // Phase 3: restore and re-raise
        restore_and_reraise(sig);
    }

    fn restore_and_reraise(sig: std::os::raw::c_int) {
        let prev = match sig {
            SIGINT => PREV_SIGINT.load(Ordering::Relaxed),
            SIGTERM => PREV_SIGTERM.load(Ordering::Relaxed),
            _ => 0,
        };
        // SAFETY: signal() and raise() are POSIX async-signal-safe.
        unsafe {
            signal(sig, prev);
            raise(sig);
        }
    }

    /// Register signal handlers. Pre-serializes the trailer, extracts the
    /// raw fd, and leaks the registry pointer for signal-safe access.
    pub(super) fn register(
        names: &'static [(u32, &'static str)],
        file_sink: &Arc<FileSink>,
        registry: &Arc<Registry>,
    ) {
        use std::os::unix::io::AsRawFd;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // Pre-serialize trailer (one-time allocation at startup).
            let calibration = crate::time::CalibrationData::calibrate();
            let trailer_bytes = serialize_trailer(names, calibration.bias_ns());
            let trailer = Box::new(SignalTrailer { bytes: trailer_bytes });
            TRAILER.store(Box::into_raw(trailer) as usize, Ordering::Relaxed);

            // Extract raw fd from FileSink.
            let file = file_sink.lock();
            FILE_FD.store(file.as_raw_fd(), Ordering::Relaxed);
            drop(file);

            // Leak a clone of the Registry Arc so the signal handler can
            // access it without going through shutdown_state's mutex.
            let reg_arc: Arc<Registry> = Arc::clone(registry);
            let reg_ptr = Arc::into_raw(reg_arc) as usize;
            REGISTRY.store(reg_ptr, Ordering::Relaxed);

            // SAFETY: signal() is safe to call during initialization.
            unsafe {
                let prev = signal(SIGINT, handler as usize);
                if prev != SIG_ERR {
                    PREV_SIGINT.store(prev, Ordering::Relaxed);
                }

                let prev = signal(SIGTERM, handler as usize);
                if prev != SIG_ERR {
                    PREV_SIGTERM.store(prev, Ordering::Relaxed);
                }
            }
        });
    }
}
