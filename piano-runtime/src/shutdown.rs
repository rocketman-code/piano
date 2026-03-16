#![allow(unsafe_code)]

//! Lifecycle management -- atexit and signal handlers for abnormal exit.
//!
//! Handles process::exit() and signal paths where Ctx::drop may not run.
//! The atexit handler drains all registered buffers, writes remaining
//! events, and writes the trailer. The signal handler (Unix only) saves
//! the previous handler, sets a flag, restores the previous handler, and
//! re-raises -- all signal-safe (no mutex, no alloc, no IO).
//!
//! Invariants:
//! - Double-drain safety: if both Ctx::drop and atexit race to drain
//!   the same buffer, the Mutex serializes. First drainer gets data,
//!   second finds empty. Zero duplicates.
//! - All mutex locks heal poisoning.
//! - Never panics. All errors degrade to data loss.
//! - Signal handler is signal-safe: atomic store, function pointer
//!   restore, raise. No mutex, no alloc, no IO.
//! - atexit handler does no mutex acquisition in the signal handler
//!   itself -- only in the registered atexit callback (safe context).

use crate::alloc::ReentrancyGuard;
use crate::buffer::drain_all_buffers;
use crate::file_sink::FileSink;
use crate::output::{write_measurements, write_trailer};
use crate::time;
use std::sync::{Arc, Mutex, Once};

struct ShutdownState {
    file_sink: Arc<FileSink>,
    names: &'static [(u32, &'static str)],
}

fn shutdown_state() -> &'static Mutex<Option<ShutdownState>> {
    static ONCE: Once = Once::new();
    static mut STATE: *const Mutex<Option<ShutdownState>> = std::ptr::null();

    // SAFETY: ONCE.call_once guarantees single initialization. After init,
    // STATE points to a heap-allocated Mutex that is never moved or freed
    // (intentional leak for 'static lifetime). Same pattern as buffer registry.
    unsafe {
        ONCE.call_once(|| {
            STATE = Box::into_raw(Box::new(Mutex::new(None)));
        });
        &*STATE
    }
}

/// Register the file sink and name table for atexit cleanup.
/// Called by Ctx::new() when a file_sink is provided.
/// Multiple calls overwrite the previous state (last Ctx::new wins).
pub(crate) fn register(
    file_sink: Arc<FileSink>,
    names: &'static [(u32, &'static str)],
) {
    let mut state = shutdown_state()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *state = Some(ShutdownState { file_sink, names });

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

    #[cfg(unix)]
    signal::register();
}

extern "C" fn atexit_handler() {
    // Clone the Arc and names pointer, then release the shutdown state
    // lock before doing any I/O. Minimizes lock hold time.
    let (file_sink, names) = {
        let state = shutdown_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match state.as_ref() {
            Some(s) => (Arc::clone(&s.file_sink), s.names),
            None => return,
        }
    }; // shutdown state mutex released here

    let remaining = drain_all_buffers();

    let _reentry = ReentrancyGuard::enter();
    {
        let mut file = file_sink.lock();
        if write_measurements(&mut *file, &remaining).is_err() {
            file_sink.record_io_error();
        }
        if write_trailer(&mut *file, names, time::bias_ns()).is_err() {
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

// ---------------------------------------------------------------------------
// Signal handler (Unix only)
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod signal {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Once;

    const SIGINT: std::os::raw::c_int = 2;
    const SIGTERM: std::os::raw::c_int = 15;
    const SIG_ERR: usize = usize::MAX;

    /// Flag set by the signal handler. Atomic store is signal-safe.
    /// The atexit handler drains unconditionally (does not check this),
    /// but the flag is observable for debugging and future extensions.
    static SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

    /// Previous handlers saved at registration time. Stored as usize
    /// because the value can be SIG_DFL (0), SIG_IGN (1), or a function
    /// pointer. AtomicUsize load is signal-safe (plain atomic read).
    static PREV_SIGINT: AtomicUsize = AtomicUsize::new(0);
    static PREV_SIGTERM: AtomicUsize = AtomicUsize::new(0);

    extern "C" {
        fn signal(
            sig: std::os::raw::c_int,
            handler: usize,
        ) -> usize;
        fn raise(sig: std::os::raw::c_int) -> std::os::raw::c_int;
    }

    /// Signal handler. All operations are signal-safe:
    /// - AtomicBool::store (atomic write, no alloc, no lock)
    /// - signal() to restore previous handler (POSIX async-signal-safe)
    /// - raise() to re-raise (POSIX async-signal-safe per IEEE 1003.1)
    extern "C" fn handler(sig: std::os::raw::c_int) {
        // Step a: set flag (signal-safe)
        SIGNAL_RECEIVED.store(true, Ordering::Relaxed);

        // Step b: restore the PREVIOUS handler, not SIG_DFL.
        // Restoring SIG_DFL would destroy the user's handler (role
        // constraint violation).
        let prev = match sig {
            SIGINT => PREV_SIGINT.load(Ordering::Relaxed),
            SIGTERM => PREV_SIGTERM.load(Ordering::Relaxed),
            _ => 0, // SIG_DFL (unreachable: we only register for SIGINT/SIGTERM)
        };
        // SAFETY: signal() is async-signal-safe per POSIX. We restore
        // the handler that was active before we installed ours.
        unsafe {
            signal(sig, prev);
        }

        // Step c: re-raise the signal. The re-raised signal invokes
        // the user's original handler (or SIG_DFL if there was none).
        // If the process terminates, atexit runs and drains buffers.
        // SAFETY: raise() is async-signal-safe per POSIX (IEEE 1003.1
        // Section 2.4.3).
        unsafe {
            raise(sig);
        }
    }

    /// Register signal handlers for SIGINT and SIGTERM. Called once
    /// via Once guard -- multiple Ctx::new() calls are idempotent.
    /// Saves the previous handlers so they can be restored on signal.
    pub(super) fn register() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // SAFETY: signal() is safe to call during initialization.
            // The returned previous handlers are saved in atomic statics
            // so the signal handler can read them signal-safely. SIG_ERR
            // indicates failure -- we silently skip (profiler must not
            // crash the host).
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
