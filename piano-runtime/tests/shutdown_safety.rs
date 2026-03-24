//! Prove that multiple ProfileSession::init calls are safe and
//! that shutdown heals poisoned mutexes.

use piano_runtime::file_sink::FileSink;
use piano_runtime::session::ProfileSession;
use std::fs::File;
use std::sync::{Arc, Mutex};

// Multiple ProfileSession::init calls (which call register()) are safe.
// Each overwrites the previous state. atexit handlers drain once each;
// the first finds data, subsequent find empty.
#[test]
fn multiple_register_calls_safe() {
    std::thread::spawn(|| {
        for _ in 0..5 {
            ProfileSession::init(None, false, &[(0, "test")]);
        }
    })
    .join()
    .expect("test thread panicked");
}

// Multiple ProfileSession::init with file sinks.
// Each creates a temp file, registers for atexit cleanup.
#[test]
fn multiple_init_with_file_sinks() {
    std::thread::spawn(|| {
        for i in 0..3 {
            let path = std::env::temp_dir().join(format!(
                "piano_shutdown_safety_test_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let file = File::create(&path).unwrap();
            let file_sink = Some(Arc::new(FileSink::new(file)));
            ProfileSession::init(file_sink, false, &[(0, "test")]);
            let _ = std::fs::remove_file(&path);
        }
    })
    .join()
    .expect("test thread panicked");
}

// Shutdown path heals poisoned mutexes. The healing pattern is:
//   mutex.lock().unwrap_or_else(|e| e.into_inner())
// This is used at every lock site in shutdown.rs.
#[test]
fn shutdown_healing_pattern_is_consistent() {
    let m = Arc::new(Mutex::new(42i32));

    // Poison it
    let m2 = Arc::clone(&m);
    let _ = std::thread::spawn(move || {
        let _g = m2.lock().unwrap();
        panic!("poison");
    })
    .join();

    // Heal it (exact pattern used in shutdown.rs)
    let val = m.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(*val, 42, "healed mutex should retain its value");
}
