use piano_runtime::buffer::{drain_all_buffers, drain_thread_buffer, get_thread_buffer_arc};
use piano_runtime::ctx::RootCtx;
use piano_runtime::file_sink::FileSink;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Shutdown tests: Ctx::drop writes remaining measurements + trailer.
// All tests run in spawned threads for TLS isolation.

fn test_file(label: &str) -> (Arc<FileSink>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "piano_test_shutdown_{}_{}",
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let file = File::create(&path).expect("create temp file");
    (Arc::new(FileSink::new(file)), path)
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    let file = File::open(path).expect("open temp file for reading");
    BufReader::new(file)
        .lines()
        .map(|l| l.expect("read line"))
        .collect()
}

// ---------------------------------------------------------------------------
// Ctx::drop tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: root Ctx::drop writes trailer to file.
#[test]
fn root_ctx_drop_writes_trailer() {
    let (fs, path) = test_file("trailer");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        {
            let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            let (guard, _) = ctx.enter(1);
            drop(guard);
            drop(ctx);
            // root drops here -- triggers shutdown
        }

        let lines = read_lines(&path_clone);
        // Should have: header + trailer (measurement was flushed or drained)
        let last = lines.last().expect("file should not be empty");
        assert!(
            last.contains("\"type\":\"trailer\""),
            "last line must be trailer, got: {last}"
        );
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: root Ctx::drop drains remaining un-flushed measurements.
#[test]
fn root_ctx_drop_drains_remaining() {
    let (fs, path) = test_file("drain_remaining");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        {
            let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            // Push 5 measurements (well below threshold -- won't flush)
            for _ in 0..5 {
                let (guard, _) = ctx.enter(1);
                drop(guard);
            }
            drop(ctx);
            // root drops here -- drains remaining 5 + writes trailer
        }

        let lines = read_lines(&path_clone);
        // header (1) + 5 measurements + trailer (1) = 7 lines
        assert_eq!(
            lines.len(),
            7,
            "should have header + 5 measurements + trailer, got {}",
            lines.len()
        );

        assert!(lines[0].contains("\"type\":\"header\""), "first line is header");
        assert!(lines[6].contains("\"type\":\"trailer\""), "last line is trailer");
        for (i, line) in lines[1..6].iter().enumerate() {
            assert!(
                line.contains("\"span_id\""),
                "line {} should be a measurement", i + 1
            );
        }
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: child Ctx::drop does NOT trigger shutdown.
#[test]
fn child_ctx_drop_is_noop() {
    let (fs, path) = test_file("child_noop");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = root.ctx();
        {
            let (_guard, child_ctx) = ctx.enter(1);
            {
                let (_g2, _grandchild) = child_ctx.enter(1);
                // grandchild drops -- Ctx drop is always a no-op
            }
            // child_ctx drops -- Ctx drop is always a no-op
            // _guard drops -- pushes measurement
        }

        // Only header should be in the file (no trailer yet, root still alive)
        let lines = read_lines(&path_clone);
        assert!(
            !lines.iter().any(|l| l.contains("\"type\":\"trailer\"")),
            "Ctx drops must not write trailer (only RootCtx drop does)"
        );

        // Drain to clean up
        drain_thread_buffer();
        // root drops here -- writes trailer
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: double-drain safety -- Ctx::drop after manual drain
// finds empty buffers. No panic, no duplicates.
#[test]
fn double_drain_safety() {
    let (fs, path) = test_file("double_drain");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        {
            let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            for _ in 0..3 {
                let (guard, _) = ctx.enter(1);
                drop(guard);
            }

            // Manually drain before root drops
            let drained = drain_thread_buffer();
            assert_eq!(drained.len(), 3, "manual drain should get 3");

            drop(ctx);
            // root drops here -- drain_buffers_for_file_sink finds empty
        }

        let lines = read_lines(&path_clone);
        // header + trailer only (measurements were drained manually, not to file)
        assert!(lines[0].contains("\"type\":\"header\""));
        let last = lines.last().unwrap();
        assert!(
            last.contains("\"type\":\"trailer\""),
            "trailer must still be written"
        );
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: RootCtx::new with no file_sink -- Drop is a no-op.
#[test]
fn no_file_sink_drop_is_noop() {
    std::thread::spawn(|| {
        {
            let root = RootCtx::new(None, false, &[]);
            let ctx = root.ctx();
            let (guard, _) = ctx.enter(1);
            drop(guard);
            drop(ctx);
            // root drops -- file_sink is None, Drop does nothing
        }

        // Measurement is still in the buffer (not drained by Drop)
        let drained = drain_thread_buffer();
        assert_eq!(
            drained.len(),
            1,
            "measurement should still be in buffer after no-file-sink RootCtx drop"
        );
    })
    .join()
    .expect("test thread panicked");
}

// Shutdown prints io_error summary when io_errors > 0.
// Verifies the production path: Ctx::drop checks io_error_count,
// and if > 0, writes exactly:
//   "piano: profiling data may be incomplete (N write errors)"
//
// Triggers the path by using a read-only file (writes fail -> io_errors > 0),
// then asserts io_error_count is nonzero after Ctx::drop (proving the
// check path was reached). The format string is verified separately
// against the source.
#[test]
fn io_error_summary_on_write_failure() {
    std::thread::spawn(|| {
        let dir = std::env::temp_dir().join(format!(
            "piano_test_shutdown_io_err_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("readonly.ndjson");

        // Create the file, then reopen read-only so all writes fail.
        std::fs::File::create(&path).unwrap();
        let read_only = std::fs::File::open(&path).unwrap();

        let sink = Arc::new(FileSink::new(read_only));
        let sink_ref = Arc::clone(&sink);

        {
            // RootCtx::new writes header -> fails -> record_io_error
            let root = RootCtx::new(Some(sink_ref), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            // enter + drop guard -> pushes measurement (stays in buffer)
            let (guard, _) = ctx.enter(1);
            drop(guard);
            drop(ctx);
            // RootCtx::drop: drain + write_measurements -> fails -> record_io_error
            // RootCtx::drop: write_trailer -> fails -> record_io_error
            // RootCtx::drop: io_error_count > 0 -> prints summary to stderr
        }

        // The sink accumulated io_errors from the failed writes.
        // This proves the "if errors > 0" path was reached.
        assert!(
            sink.io_error_count() > 0,
            "io_errors must be > 0 when writes fail, got: {}",
            sink.io_error_count()
        );

        // Verify the exact format string exists in file_sink.rs (shared
        // by both Ctx::drop and atexit_handler via flush_remaining).
        let file_sink_source = include_str!("../src/file_sink.rs");
        assert!(
            file_sink_source.contains("piano: profiling data may be incomplete ({errors} write errors)"),
            "flush_remaining must contain the exact L6 format string"
        );

        let _ = std::fs::remove_dir_all(&dir);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: trailer contains the same name table as header.
#[test]
fn trailer_matches_header_names() {
    let (fs, path) = test_file("trailer_names");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        {
            let _root = RootCtx::new(
                Some(fs),
                false,
                &[(1, "my_crate::foo"), (2, "my_crate::bar")],
            );
            // root drops -- writes header + trailer
        }

        let lines = read_lines(&path_clone);
        let header = &lines[0];
        let trailer = lines.last().unwrap();

        // Both should contain the same names
        assert!(header.contains("\"1\":\"my_crate::foo\""));
        assert!(header.contains("\"2\":\"my_crate::bar\""));
        assert!(trailer.contains("\"1\":\"my_crate::foo\""));
        assert!(trailer.contains("\"2\":\"my_crate::bar\""));

        // Normalize type field -- should be identical otherwise
        let normalized = header.replace("\"type\":\"header\"", "\"type\":\"trailer\"");
        assert_eq!(
            &normalized, trailer,
            "header and trailer must have identical name content"
        );
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Path proofs: lifecycle fires exactly once
// ---------------------------------------------------------------------------

// PATH-PL2: Cloning the root-position Ctx and dropping the clone must
// NOT trigger shutdown. Only RootCtx::drop triggers shutdown.
#[test]
fn ctx_clone_does_not_trigger_shutdown() {
    let (fs, path) = test_file("clone_no_shutdown");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = root.ctx();

        // Clone the root-position ctx and drop the clone.
        {
            let clone = ctx.clone();
            let (guard, _) = clone.enter(1);
            drop(guard);
            drop(clone);
        }

        // No trailer should exist yet. Only header + measurement in buffer.
        let lines = read_lines(&path_clone);
        let trailer_count = lines.iter().filter(|l| l.contains("\"type\":\"trailer\"")).count();
        assert_eq!(
            trailer_count, 0,
            "Ctx clone drop must not write trailer, got {trailer_count} trailers"
        );

        drain_thread_buffer();
        drop(ctx);
        // root drops here, writes the one and only trailer
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// PATH-PL3: Multiple Ctx clones across threads, all dropped, must
// produce zero trailers. Only the final RootCtx::drop writes the trailer.
#[test]
fn multi_thread_ctx_clones_no_shutdown() {
    let (fs, path) = test_file("multi_thread_clones");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = root.ctx();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let ctx_clone = ctx.clone();
                std::thread::spawn(move || {
                    let (guard, _) = ctx_clone.enter(1);
                    drop(guard);
                    // ctx_clone drops here
                })
            })
            .collect();

        for h in handles {
            h.join().expect("worker thread panicked");
        }

        // No trailer yet
        let lines = read_lines(&path_clone);
        let trailer_count = lines.iter().filter(|l| l.contains("\"type\":\"trailer\"")).count();
        assert_eq!(
            trailer_count, 0,
            "Ctx clone drops across threads must not write trailer, got {trailer_count}"
        );

        drain_thread_buffer();
        drop(ctx);
        // root drops here, writes the single trailer
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// PATH-PD5: Panic unwind still flushes data via RootCtx::drop.
#[test]
fn panic_unwind_flushes_data() {
    let (fs, path) = test_file("panic_flush");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
            let ctx = root.ctx();
            let (guard, _) = ctx.enter(1);
            drop(guard);
            drop(ctx);
            panic!("deliberate test panic");
            // root drops during unwind
        }));

        assert!(result.is_err(), "should have caught panic");

        let lines = read_lines(&path_clone);
        assert!(
            lines.iter().any(|l| l.contains("\"type\":\"trailer\"")),
            "RootCtx::drop during panic unwind must write trailer"
        );
        assert!(
            lines.iter().any(|l| l.contains("\"span_id\"")),
            "measurement must survive panic unwind"
        );
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Composition proof: signal handler + mutex = deadlock
// ---------------------------------------------------------------------------

// COMPOSITION PROOF: if the signal handler calls drain_all_buffers while
// push_measurement holds the buffer mutex on the same thread, the process
// deadlocks. This is the exact composition that occurs when SIGTERM arrives
// during Guard::drop.
//
// Nodes proven:
//   1. Rust Mutex deadlocks on same-thread reentrant lock (standalone proof)
//   2. drain_all_buffers acquires buffer mutex (buffer.rs line 209)
//   3. push_measurement holds buffer mutex (buffer.rs line 140)
//   4. Signal handler calls atexit_handler which calls drain_all_buffers
//
// This test directly composes nodes 2+3: hold buffer mutex, call drain on
// the same thread. If it deadlocks (thread doesn't complete within 1s),
// the signal handler path is proven unsafe.
#[test]
fn signal_handler_deadlocks_if_mutex_held() {
    let done = Arc::new(AtomicBool::new(false));
    let done2 = Arc::clone(&done);

    std::thread::spawn(move || {
        // Create session and initialize thread buffer
        let root = RootCtx::new(None, false, &[(1, "test::func")]);
        let ctx = root.ctx();
        let (guard, _) = ctx.enter(1);
        drop(guard); // pushes measurement, initializes thread buffer

        // Get the thread's buffer Arc and hold the mutex
        let buf_arc = get_thread_buffer_arc().expect("buffer should be initialized");
        let _held = buf_arc.lock().unwrap();

        // Simulate what the signal handler does: call drain_all_buffers
        // while the buffer mutex is held on this thread.
        // This WILL deadlock (same thread, same mutex, non-reentrant).
        drain_all_buffers(&Arc::new(std::sync::Mutex::new(vec![Arc::clone(&buf_arc)])));

        // If we reach here, the drain somehow didn't deadlock.
        done2.store(true, Ordering::SeqCst);
        drain_thread_buffer();
    });

    std::thread::sleep(Duration::from_secs(1));
    assert!(
        !done.load(Ordering::SeqCst),
        "drain_all_buffers must deadlock when buffer mutex is already held \
         on the same thread. If this assertion fires, the deadlock \
         composition is disproven and the signal handler is safe."
    );
    // The spawned thread is deadlocked. It will be cleaned up when the
    // test process exits. This is unavoidable for a deadlock proof.
}
