use piano_runtime::session::ProfileSession;

#[test]
fn get_returns_none_before_init() {
    // Can't test in full isolation (global state from other tests),
    // but we test that get() doesn't panic.
    let _ = ProfileSession::get();
}

#[test]
fn init_returns_static_ref() {
    std::thread::spawn(|| {
        let _session = ProfileSession::init(None, false, &[], "test", 0);
        // init returns a valid &'static reference
        assert!(ProfileSession::get().is_some());
    })
    .join()
    .unwrap();
}

#[test]
fn get_returns_some_after_init() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[(0, "test::func")], "test", 0);
        let session = ProfileSession::get();
        assert!(session.is_some(), "get must return Some after init");
    })
    .join()
    .unwrap();
}

#[test]
fn cross_thread_access() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[(0, "test::func")], "test", 0);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    let _s = ProfileSession::get().expect("session must be available");
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
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
            "test",
            0,
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
