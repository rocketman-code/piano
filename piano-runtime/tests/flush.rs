use piano_runtime::buffer::{drain_thread_buffer, FLUSH_THRESHOLD};
use piano_runtime::ctx::RootCtx;
use piano_runtime::file_sink::FileSink;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::sync::Arc;

// Flush integration tests: buffer + output + ctx working together.
// All tests use real temp files (piano-runtime is zero-dep).
// All tests run in spawned threads for TLS isolation.

fn test_file(label: &str) -> (Arc<FileSink>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "piano_test_flush_{}_{}",
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let file = File::create(&path).expect("create temp file");
    (Arc::new(FileSink::new(file)), path)
}

fn count_lines(path: &std::path::Path) -> usize {
    let file = File::open(path).expect("open temp file for reading");
    BufReader::new(file).lines().count()
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    let file = File::open(path).expect("open temp file for reading");
    BufReader::new(file)
        .lines()
        .map(|l| l.expect("read line"))
        .collect()
}

// ---------------------------------------------------------------------------
// Threshold flush tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: pushing exactly FLUSH_THRESHOLD measurements triggers a
// flush -- file contains header (1 line) + 1024 measurement lines.
#[test]
fn flush_at_threshold() {
    let (fs, path) = test_file("at_threshold");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let _root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = _root.ctx();

        for _ in 0..FLUSH_THRESHOLD {
            let (guard, _) = ctx.enter(1);
            drop(guard);
        }

        // File should have header + flushed measurements
        let lines = count_lines(&path_clone);
        assert!(
            lines > FLUSH_THRESHOLD,
            "expected at least {} lines (1 header + {} measurements), got {}",
            1 + FLUSH_THRESHOLD,
            FLUSH_THRESHOLD,
            lines
        );

        // Drain remaining to avoid interference
        drain_thread_buffer();
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: pushing fewer than FLUSH_THRESHOLD measurements does not
// trigger a flush -- file contains only the header line.
#[test]
fn no_flush_below_threshold() {
    let (fs, path) = test_file("below_threshold");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let _root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = _root.ctx();

        // Push exactly FLUSH_THRESHOLD - 1 (should NOT trigger flush)
        for _ in 0..(FLUSH_THRESHOLD - 1) {
            let (guard, _) = ctx.enter(1);
            drop(guard);
        }

        let lines = count_lines(&path_clone);
        assert_eq!(
            lines, 1,
            "below threshold: file should contain only header (1 line), got {lines}"
        );

        // Drain to clean up (data is still in the buffer)
        let drained = drain_thread_buffer();
        assert_eq!(
            drained.len(),
            FLUSH_THRESHOLD - 1,
            "buffer should hold all un-flushed measurements"
        );
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: flushed data is valid NDJSON -- each line starts with {
// and ends with }.
#[test]
fn flushed_data_is_valid_ndjson() {
    let (fs, path) = test_file("valid_ndjson");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let _root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = _root.ctx();

        for _ in 0..FLUSH_THRESHOLD {
            let (guard, _) = ctx.enter(1);
            drop(guard);
        }

        let lines = read_lines(&path_clone);
        assert!(!lines.is_empty(), "file should not be empty");

        // First line is the header
        let header = &lines[0];
        assert!(header.starts_with('{'), "header must start with {{");
        assert!(header.ends_with('}'), "header must end with }}");
        assert!(
            header.contains("\"type\":\"header\""),
            "first line must be header"
        );

        // Measurement lines (all after header)
        for (i, line) in lines.iter().skip(1).enumerate() {
            assert!(
                line.starts_with('{'),
                "measurement line {i} must start with {{"
            );
            assert!(
                line.ends_with('}'),
                "measurement line {i} must end with }}"
            );
            assert!(
                line.contains("\"span_id\""),
                "measurement line {i} must contain span_id"
            );
        }

        drain_thread_buffer();
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: header contains the name table from RootCtx::new.
#[test]
fn header_contains_name_table() {
    let (fs, path) = test_file("name_table");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let _root = RootCtx::new(Some(fs), false, &[(1, "my_crate::foo"), (2, "my_crate::bar")]);

        let lines = read_lines(&path_clone);
        assert_eq!(lines.len(), 1, "should have exactly the header line");

        let header = &lines[0];
        assert!(header.contains("\"type\":\"header\""));
        assert!(header.contains("\"1\":\"my_crate::foo\""));
        assert!(header.contains("\"2\":\"my_crate::bar\""));
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}

// INVARIANT TEST: RootCtx::new with no file_sink does not write anything.
// (All existing tests use None -- this explicitly verifies no panic.)
#[test]
fn no_file_sink_no_flush() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();

        // Push more than threshold -- no file_sink, so no flush, no panic
        for _ in 0..(FLUSH_THRESHOLD + 100) {
            let (guard, _) = ctx.enter(1);
            drop(guard);
        }

        let drained = drain_thread_buffer();
        assert_eq!(
            drained.len(),
            FLUSH_THRESHOLD + 100,
            "all measurements should be in buffer (no flush without file_sink)"
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: multiple flushes work correctly -- push 2x threshold,
// file should contain header + 2048 measurement lines.
#[test]
fn multiple_flushes() {
    let (fs, path) = test_file("multi_flush");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        let _root = RootCtx::new(Some(fs), false, &[(1, "test::func")]);
        let ctx = _root.ctx();

        for _ in 0..(2 * FLUSH_THRESHOLD) {
            let (guard, _) = ctx.enter(1);
            drop(guard);
        }

        let lines = count_lines(&path_clone);
        assert!(
            lines > 2 * FLUSH_THRESHOLD,
            "expected at least {} lines (1 header + {} measurements), got {}",
            1 + 2 * FLUSH_THRESHOLD,
            2 * FLUSH_THRESHOLD,
            lines
        );

        drain_thread_buffer();
    })
    .join()
    .expect("test thread panicked");

    let _ = fs::remove_file(&path);
}
