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

// Production write path increments io_errors on write failure.
// Uses a read-only file so all write_* calls return Err, triggering
// record_io_error. The RootCtx::new -> write_header path is sufficient
// to prove the property, and Ctx::drop adds write_measurements +
// write_trailer failures on top.
#[test]
fn write_path_increments_io_errors_on_failure() {
    use piano_runtime::ctx::RootCtx;

    // Spawn a thread for TLS isolation.
    std::thread::spawn(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("readonly.piano");
        // Create the file, then reopen read-only so writes fail.
        std::fs::File::create(&path).unwrap();
        let read_only = std::fs::File::open(&path).unwrap();
        let sink = Arc::new(FileSink::new(read_only));
        let sink_ref = Arc::clone(&sink);

        {
            // RootCtx::new calls write_header -> fails -> record_io_error (1st)
            let root = RootCtx::new(Some(sink_ref), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            // enter + drop guard pushes a measurement to the buffer
            let (guard, _) = ctx.enter(1);
            drop(guard);
            drop(ctx);
            // RootCtx::drop: write_measurements -> fails -> record_io_error (2nd)
            // RootCtx::drop: write_trailer -> fails -> record_io_error (3rd)
        }

        // All three write sites checked their Result and incremented on Err.
        assert!(
            sink.io_error_count() >= 3,
            "all write sites must increment io_errors on Err; got: {}",
            sink.io_error_count()
        );
    })
    .join()
    .expect("test thread panicked");
}
