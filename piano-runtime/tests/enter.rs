use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::file_sink::FileSink;
use piano_runtime::guard::enter;
use piano_runtime::parent::current_parent;
use piano_runtime::session::ProfileSession;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

// Level 1: enter() composes session + parent + guard.
// Every test runs in a spawned thread for TLS and session isolation.

fn test_file(label: &str) -> (Arc<FileSink>, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "piano_test_enter_{}_{}", label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_nanos()
    ));
    let file = std::fs::File::create(&path).unwrap();
    (Arc::new(FileSink::new(file)), path)
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    BufReader::new(file).lines().map(|l| l.unwrap()).collect()
}

#[test]
fn enter_inactive_is_noop() {
    // Before ProfileSession::init, enter() returns inactive guard.
    // Drop is a no-op. No panic. No TLS change.
    std::thread::spawn(|| {
        let _g = enter(0);
        // If we reach here without panic, the inactive guard works.
        assert_eq!(current_parent(), 0, "inactive guard must not touch TLS");
    })
    .join()
    .unwrap();
}

#[test]
fn enter_sets_and_restores_parent() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);
        assert_eq!(current_parent(), 0, "root before enter");

        {
            let _g = enter(0);
            let parent_during = current_parent();
            assert_ne!(parent_during, 0, "enter must set nonzero span_id as parent");
        }

        assert_eq!(current_parent(), 0, "parent restored after guard drop");
    })
    .join()
    .unwrap();
}

#[test]
fn nested_enter_builds_parent_chain() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        {
            let _g1 = enter(0);
            let span_a = current_parent();

            {
                let _g2 = enter(1);
                let span_b = current_parent();

                assert_ne!(span_a, span_b, "nested spans must have different IDs");
                assert!(span_b > span_a, "span IDs must be monotonically increasing");
            }

            assert_eq!(current_parent(), span_a, "inner guard restored to outer span");
        }

        assert_eq!(current_parent(), 0, "all guards dropped, back to root");
        drain_thread_buffer();
    })
    .join()
    .unwrap();
}

#[test]
fn enter_pushes_measurement_on_drop() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        {
            let _g = enter(0);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "guard drop must push exactly one measurement");

        let m = &drained[0];
        assert_ne!(m.span_id, 0, "span_id must be nonzero");
        assert_eq!(m.parent_span_id, 0, "parent must be root (0)");
        assert_eq!(m.name_id, 0, "name_id must match enter() argument");
        assert!(m.end_ns >= m.start_ns, "end must be >= start");
    })
    .join()
    .unwrap();
}

#[test]
fn enter_writes_to_file_on_flush() {
    let (fs, path) = test_file("flush");
    let path_clone = path.clone();

    std::thread::spawn(move || {
        ProfileSession::init(
            Some(fs),
            false,
            &[(0, "test::work")],
        );

        // Push enough measurements to trigger a flush (FLUSH_THRESHOLD = 1024)
        for _ in 0..1100 {
            let _g = enter(0);
        }

        // Some measurements should have flushed to disk
        let lines = read_lines(&path_clone);
        assert!(lines.len() > 1, "should have header + flushed measurements");
        assert!(
            lines[0].contains("\"type\":\"header\""),
            "first line must be header"
        );
        // At least 1024 measurements should have been flushed
        let measurement_count = lines.iter()
            .filter(|l| l.contains("\"span_id\""))
            .count();
        assert!(
            measurement_count >= 1024,
            "expected >= 1024 flushed measurements, got {measurement_count}"
        );

        drain_thread_buffer();
    })
    .join()
    .unwrap();

    let _ = std::fs::remove_file(&path);
}

#[test]
fn closure_works_without_capture() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        // The critical proof: enter() inside a closure creates NO captures.
        // This closure is 'static (no borrows from enclosing scope).
        let handle = std::thread::spawn(|| {
            let _g = enter(0);
            assert_ne!(current_parent(), 0);
        });
        handle.join().unwrap();

        // Closure in method chain: no captures
        let results: Vec<u64> = (0..3).map(|_| {
            let _g = enter(1);
            current_parent()
        }).collect();

        for r in &results {
            assert_ne!(*r, 0, "each closure call should have an active guard");
        }

        drain_thread_buffer();
    })
    .join()
    .unwrap();
}

#[test]
fn spawn_inside_map_works() {
    // THE reproducer that was impossible with parameter passing.
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        let handles: Vec<_> = (0..4).map(|_| {
            std::thread::spawn(|| {
                let _g = enter(0);
                std::hint::black_box(42)
            })
        }).collect();

        for h in handles {
            h.join().unwrap();
        }

        drain_thread_buffer();
    })
    .join()
    .unwrap();
}
