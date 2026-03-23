use piano_runtime::session::ProfileSession;

// Level 0 leaf: ProfileSession -- immutable shared infrastructure.
// Tests verify init, get, span_id allocation, and cross-thread access.

#[test]
fn get_returns_none_before_init() {
    // ProfileSession::get() returns None when no session exists.
    // Can't test in isolation (global state from other tests),
    // but we test that get() doesn't panic.
    let _ = ProfileSession::get();
}

#[test]
fn init_returns_static_ref() {
    std::thread::spawn(|| {
        let session = ProfileSession::init(None, false, &[]);
        // Must return a valid reference with working span_id allocation
        let id = session.next_span_id();
        assert_ne!(id, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn get_returns_some_after_init() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[(0, "test::func")]);
        let session = ProfileSession::get();
        assert!(session.is_some(), "get must return Some after init");
    })
    .join()
    .unwrap();
}

#[test]
fn span_ids_are_unique_and_nonzero() {
    std::thread::spawn(|| {
        let session = ProfileSession::init(None, false, &[]);
        let id1 = session.next_span_id();
        let id2 = session.next_span_id();
        let id3 = session.next_span_id();

        assert_ne!(id1, 0, "span IDs must be nonzero");
        assert_ne!(id2, 0);
        assert_ne!(id3, 0);
        assert_ne!(id1, id2, "span IDs must be unique");
        assert_ne!(id2, id3);
        assert!(id2 > id1, "span IDs must be monotonically increasing");
        assert!(id3 > id2);
    })
    .join()
    .unwrap();
}

#[test]
fn cross_thread_access() {
    std::thread::spawn(|| {
        ProfileSession::init(None, true, &[(0, "test::func")]);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    // Each thread loads the &'static session via get()
                    let s = ProfileSession::get().expect("session must be available");
                    let id = s.next_span_id();
                    assert_ne!(id, 0);
                    id
                })
            })
            .collect();

        let ids: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All IDs unique across threads
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "cross-thread span IDs must be unique");
    })
    .join()
    .unwrap();
}

#[test]
fn writes_header_when_file_sink_provided() {
    std::thread::spawn(|| {
        use piano_runtime::file_sink::FileSink;
        use std::io::{BufRead, BufReader};
        use std::sync::Arc;

        let path = std::env::temp_dir().join(format!(
            "piano_test_session_header_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = std::fs::File::create(&path).unwrap();
        let fs = Arc::new(FileSink::new(file));

        let _session = ProfileSession::init(
            Some(fs),
            false,
            &[(0, "test::work"), (1, "test::helper")],
        );

        let read_file = std::fs::File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(read_file)
            .lines()
            .map(|l| l.unwrap())
            .collect();

        assert!(!lines.is_empty(), "header must be written");
        assert!(
            lines[0].contains("\"type\":\"header\""),
            "first line must be header"
        );
        assert!(
            lines[0].contains("test::work"),
            "header must contain function names"
        );

        let _ = std::fs::remove_file(&path);
    })
    .join()
    .unwrap();
}
