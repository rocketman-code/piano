use piano_runtime::file_sink::FileSink;
use std::sync::Arc;

// INVARIANT TEST: FileSink error counter starts at zero.
#[test]
fn error_counter_starts_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = FileSink::new(file);
    assert_eq!(sink.io_error_count(), 0);
}

// INVARIANT TEST: record_io_error increments counter.
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

// INVARIANT TEST: lock() returns a valid MutexGuard for writing.
#[test]
fn lock_returns_writable_guard() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("test.piano")).unwrap();
    let sink = FileSink::new(file);
    let mut guard = sink.lock();
    use std::io::Write;
    guard.write_all(b"test data").unwrap();
}

// INVARIANT TEST: lock() heals mutex poisoning.
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

// INVARIANT TEST: error counter is thread-safe (concurrent increments).
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
