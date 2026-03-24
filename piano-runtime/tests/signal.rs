#![cfg(unix)]
#![allow(unsafe_code)]

// Signal handler tests. Separate binary for process isolation --
// signals are process-wide and cannot be scoped to a test.

use piano_runtime::ctx::RootCtx;
use piano_runtime::file_sink::FileSink;
use std::fs::{self, File};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn test_file(label: &str) -> (Arc<FileSink>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "piano_test_signal_{}_{}",
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let file = File::create(&path).expect("create temp file");
    (Arc::new(FileSink::new(file)), path)
}

extern "C" {
    fn signal(sig: std::os::raw::c_int, handler: usize) -> usize;
    fn raise(sig: std::os::raw::c_int) -> std::os::raw::c_int;
}

static USER_HANDLER_RAN: AtomicBool = AtomicBool::new(false);

extern "C" fn user_handler(_sig: std::os::raw::c_int) {
    USER_HANDLER_RAN.store(true, Ordering::Relaxed);
}

// INVARIANT TEST: piano's signal handler restores the previous handler.
// Install a custom handler, then create a Ctx (which registers piano's
// handler). Raise SIGINT. Piano's handler should: set flag, restore our
// handler, re-raise. Our handler should run.
#[test]
fn signal_handler_restores_previous() {
    // Reset flag
    USER_HANDLER_RAN.store(false, Ordering::Relaxed);

    // Install our custom handler for SIGINT
    // SAFETY: signal() is safe during initialization. user_handler is a
    // valid extern "C" function.
    unsafe {
        signal(2, user_handler as usize);
    }

    let (fs, path) = test_file("restore");

    // RootCtx::new -> shutdown::register -> signal::register
    // This installs piano's handler, saving user_handler as previous.
    {
        let _root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);

        // Raise SIGINT. Piano's handler runs:
        // 1. Sets AtomicBool flag
        // 2. Restores user_handler
        // 3. Re-raises SIGINT
        // user_handler runs, sets USER_HANDLER_RAN = true
        // SAFETY: raise() is safe to call.
        unsafe {
            raise(2);
        }

        assert!(
            USER_HANDLER_RAN.load(Ordering::Relaxed),
            "piano's signal handler must restore and invoke the previous handler"
        );
    }

    let _ = fs::remove_file(&path);
}
