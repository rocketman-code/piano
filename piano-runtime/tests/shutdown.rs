use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::ctx::Ctx;
use piano_runtime::file_sink::FileSink;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::sync::Arc;

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
            let ctx = Ctx::new(Some(fs), false, &[(1, "test::func")]);
            let (guard, _) = ctx.enter(1);
            drop(guard);
            // ctx drops here -- root (span_id 0) triggers shutdown
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
            let ctx = Ctx::new(Some(fs), false, &[(1, "test::func")]);
            // Push 5 measurements (well below threshold -- won't flush)
            for _ in 0..5 {
                let (guard, _) = ctx.enter(1);
                drop(guard);
            }
            // ctx drops here -- drains remaining 5 + writes trailer
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
        let ctx = Ctx::new(Some(fs), false, &[(1, "test::func")]);
        {
            let (_guard, child_ctx) = ctx.enter(1);
            {
                let (_g2, _grandchild) = child_ctx.enter(1);
                // grandchild drops -- should NOT write trailer (span_id != 0)
            }
            // child_ctx drops -- should NOT write trailer (span_id != 0)
            // _guard drops -- pushes measurement
        }

        // Only header should be in the file (no trailer yet, root still alive)
        let lines = read_lines(&path_clone);
        assert!(
            !lines.iter().any(|l| l.contains("\"type\":\"trailer\"")),
            "child/grandchild drops must not write trailer"
        );

        // Drain to clean up
        drain_thread_buffer();
        // root ctx drops here -- writes trailer
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
            let ctx = Ctx::new(Some(fs), false, &[(1, "test::func")]);
            for _ in 0..3 {
                let (guard, _) = ctx.enter(1);
                drop(guard);
            }

            // Manually drain before ctx drops
            let drained = drain_thread_buffer();
            assert_eq!(drained.len(), 3, "manual drain should get 3");

            // ctx drops here -- drain_buffers_for_file_sink finds empty
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

// INVARIANT TEST: Ctx::new with no file_sink -- Drop is a no-op.
#[test]
fn no_file_sink_drop_is_noop() {
    std::thread::spawn(|| {
        {
            let ctx = Ctx::new(None, false, &[]);
            let (guard, _) = ctx.enter(1);
            drop(guard);
            // ctx drops -- file_sink is None, Drop does nothing
        }

        // Measurement is still in the buffer (not drained by Drop)
        let drained = drain_thread_buffer();
        assert_eq!(
            drained.len(),
            1,
            "measurement should still be in buffer after no-file-sink Ctx drop"
        );
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
            let _ctx = Ctx::new(
                Some(fs),
                false,
                &[(1, "my_crate::foo"), (2, "my_crate::bar")],
            );
            // ctx drops -- writes header + trailer
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
