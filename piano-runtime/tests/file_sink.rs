use piano_runtime::file_sink::FileSink;
use piano_runtime::session::ProfileSession;
use std::sync::Arc;

#[test]
fn error_counter_starts_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = FileSink::new(file);
    assert_eq!(sink.io_error_count(), 0);
}

#[test]
fn record_io_error_increments() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = FileSink::new(file);
    sink.record_io_error();
    sink.record_io_error();
    sink.record_io_error();
    assert_eq!(sink.io_error_count(), 3);
}

#[test]
fn lock_returns_writable_guard() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = FileSink::new(file);
    let mut guard = sink.lock();
    use std::io::Write;
    guard.write_all(b"test data").unwrap();
}

#[test]
fn lock_heals_poisoning() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = Arc::new(FileSink::new(file));
    let sink2 = Arc::clone(&sink);

    // Poison the mutex by panicking while holding the lock
    let _ = std::thread::spawn(move || {
        let _guard = sink2.lock();
        panic!("intentional panic to poison mutex");
    })
    .join();

    // lock() should still work (heals poisoning)
    let _guard = sink.lock();
}

#[test]
fn error_counter_thread_safe() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = Arc::new(FileSink::new(file));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let sink = Arc::clone(&sink);
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                sink.record_io_error();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(sink.io_error_count(), 1000);
}

// Header write failure increments io_errors.
#[test]
fn header_write_failure_increments_io_errors() {
    std::thread::spawn(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.piano");
        std::fs::File::create(&path).unwrap();
        let read_only = std::fs::File::open(&path).unwrap();
        let sink = Arc::new(FileSink::new(read_only));
        let sink_ref = Arc::clone(&sink);

        static NAMES: &[(u32, &str)] = &[(0, "test::func")];
        ProfileSession::init(Some(sink_ref), false, NAMES);

        assert!(
            sink.io_error_count() >= 1,
            "header write failure must increment io_errors; got: {}",
            sink.io_error_count()
        );
    })
    .join()
    .expect("test thread panicked");
}
