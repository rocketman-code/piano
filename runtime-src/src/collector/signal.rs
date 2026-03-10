/// Signal handling for graceful data recovery on SIGTERM / SIGINT.
///
/// On Unix, `init()` registers signal handlers so that profiling data is
/// flushed before the process exits. This prevents data loss when the
/// profiled program is killed (e.g. Ctrl-C, `kill`).
///
/// Calling `shutdown_impl_inner` from a signal handler is technically not
/// async-signal-safe (it acquires mutexes, allocates, does file I/O).
/// This is a pragmatic choice: for a profiling tool the worst case is a
/// corrupted output file, which is strictly better than losing all data
/// (the current behavior without signal handling). Many profilers take
/// the same approach.
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod signal_impl {
    use std::sync::atomic::Ordering;

    use super::super::{runs_dir_nonblocking, shutdown_impl_inner, SHUTDOWN_DONE};

    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SA_RESETDFL: i32 = {
        #[cfg(target_os = "linux")]
        {
            // SA_RESETDFL == SA_ONESHOT == 0x80000000 on Linux
            0x80000000u32 as i32
        }
        #[cfg(target_os = "macos")]
        {
            // SA_RESETDFL == 4 on macOS
            4
        }
    };

    #[cfg(target_os = "linux")]
    #[repr(C)]
    struct Sigaction {
        sa_handler: extern "C" fn(i32),
        sa_mask: [u8; 128], // sigset_t is 128 bytes on Linux
        sa_flags: i32,
        sa_restorer: usize,
    }

    #[cfg(target_os = "macos")]
    #[repr(C)]
    struct Sigaction {
        sa_handler: extern "C" fn(i32),
        sa_mask: u32, // sigset_t is 4 bytes on macOS
        sa_flags: i32,
    }

    extern "C" {
        fn sigaction(sig: i32, act: *const Sigaction, oldact: *mut Sigaction) -> i32;
        fn raise(sig: i32) -> i32;
    }

    extern "C" fn handler(sig: i32) {
        // Guard: if shutdown already ran (normal exit path), just re-raise.
        if SHUTDOWN_DONE.swap(true, Ordering::SeqCst) {
            // Data already written. Re-raise with default handler
            // (SA_RESETDFL restored the default before we were called).
            unsafe { raise(sig) };
            return;
        }

        // Best-effort flush. Not async-signal-safe, but losing data is worse.
        // Uses runs_dir_nonblocking() to avoid deadlocking when the signal
        // fires while set_runs_dir() holds the RUNS_DIR mutex.
        if let Some(dir) = runs_dir_nonblocking() {
            let _ = shutdown_impl_inner(&dir);
        }

        // Re-raise so the process exits with the correct signal status.
        // SA_RESETDFL already restored the default disposition, so this
        // will terminate the process normally.
        unsafe { raise(sig) };
    }

    pub fn install_handlers() {
        unsafe {
            for &sig in &[SIGINT, SIGTERM] {
                #[cfg(target_os = "linux")]
                let act = Sigaction {
                    sa_handler: handler,
                    sa_mask: [0u8; 128],
                    sa_flags: SA_RESETDFL,
                    sa_restorer: 0,
                };
                #[cfg(target_os = "macos")]
                let act = Sigaction {
                    sa_handler: handler,
                    sa_mask: 0,
                    sa_flags: SA_RESETDFL,
                };
                sigaction(sig, &act, std::ptr::null_mut());
            }
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) use signal_impl::install_handlers;

/// Register a C-level atexit handler so profiling data is flushed when user
/// code calls `std::process::exit()`.
///
/// `std::process::exit()` calls libc `exit()`, which runs atexit handlers
/// but does NOT trigger Rust stack unwinding. Without this, all profiling
/// data is silently lost on `process::exit()`. The `SHUTDOWN_DONE` atomic
/// prevents double-writes when the normal shutdown path also runs.
mod atexit_impl {
    use std::sync::atomic::Ordering;

    use super::super::{runs_dir_nonblocking, shutdown_impl_inner, SHUTDOWN_DONE};

    extern "C" {
        fn atexit(f: extern "C" fn()) -> i32;
    }

    extern "C" fn on_exit() {
        if SHUTDOWN_DONE.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(dir) = runs_dir_nonblocking() {
            let _ = shutdown_impl_inner(&dir);
        }
    }

    pub fn register() {
        unsafe {
            atexit(on_exit);
        }
    }
}

pub(super) use atexit_impl::register as register_atexit;
