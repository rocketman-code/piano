use piano_runtime::buffer::{drain_thread_buffer, push_measurement, ThreadBuffer};
use piano_runtime::measurement::Measurement;

// All tests run in spawned threads for TLS isolation.
// All assertions use deltas, never absolute values (except fresh-thread properties).
//
// This file MUST NOT call drain_all_buffers. That function drains ALL
// registered thread buffers through the shared global registry, which
// races with other tests' spawned threads within the same binary.
// drain_all_buffers tests live in buffer_global.rs (separate binary,
// separate process, independent registry).

// ---------------------------------------------------------------------------
// ThreadBuffer unit tests (no TLS, no registry)
// ---------------------------------------------------------------------------

#[test]
fn thread_buffer_new_is_empty() {
    let buf = ThreadBuffer::new();
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
}

#[test]
fn thread_buffer_push_increments_len() {
    let mut buf = ThreadBuffer::new();
    buf.push(Measurement::default());
    assert_eq!(buf.len(), 1);
}

// INVARIANT TEST: drain moves data out, double-drain returns empty.
#[test]
fn thread_buffer_double_drain_returns_empty() {
    let mut buf = ThreadBuffer::new();
    buf.push(Measurement::default());
    buf.push(Measurement::default());

    let first = buf.drain();
    assert_eq!(first.len(), 2, "first drain should return all");
    assert!(buf.is_empty(), "buffer should be empty after drain");

    let second = buf.drain();
    assert!(second.is_empty(), "second drain must return empty (no duplicates)");
}

// INVARIANT TEST: push appends, drain collects all. No silent drops.
#[test]
fn thread_buffer_no_silent_drops() {
    let mut buf = ThreadBuffer::new();
    for i in 0..10 {
        buf.push(Measurement {
            span_id: i,
            ..Measurement::default()
        });
    }
    let drained = buf.drain();
    assert_eq!(drained.len(), 10, "all 10 measurements must survive");
    for (i, m) in drained.iter().enumerate() {
        assert_eq!(m.span_id, i as u64, "measurements must preserve order");
    }
}

// ---------------------------------------------------------------------------
// TLS integration tests (push_measurement, drain_thread_buffer)
// ---------------------------------------------------------------------------

// INVARIANT TEST: push_measurement accumulates, drain returns all.
#[test]
fn push_measurement_accumulates() {
    std::thread::spawn(|| {
        push_measurement(Measurement { span_id: 1, ..Measurement::default() });
        push_measurement(Measurement { span_id: 2, ..Measurement::default() });
        push_measurement(Measurement { span_id: 3, ..Measurement::default() });

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3, "all pushed measurements must be drainable");
        assert_eq!(drained[0].span_id, 1);
        assert_eq!(drained[1].span_id, 2);
        assert_eq!(drained[2].span_id, 3);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: drain_thread_buffer returns all, double-drain returns empty.
#[test]
fn drain_thread_buffer_no_duplicates() {
    std::thread::spawn(|| {
        push_measurement(Measurement { span_id: 1, ..Measurement::default() });
        push_measurement(Measurement { span_id: 2, ..Measurement::default() });
        push_measurement(Measurement { span_id: 3, ..Measurement::default() });

        let first = drain_thread_buffer();
        assert_eq!(first.len(), 3, "first drain should return all");

        let second = drain_thread_buffer();
        assert!(second.is_empty(), "second drain must return empty (no duplicates)");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cross-thread isolation -- each thread has its own buffer.
#[test]
fn cross_thread_buffer_isolation() {
    std::thread::spawn(|| {
        // Push on parent thread
        push_measurement(Measurement { span_id: 100, ..Measurement::default() });

        // Child thread should have independent buffer
        let child_drain = std::thread::spawn(|| {
            push_measurement(Measurement { span_id: 200, ..Measurement::default() });
            drain_thread_buffer()
        })
        .join()
        .expect("child panicked");

        assert_eq!(child_drain.len(), 1, "child should have its own measurement");
        assert_eq!(child_drain[0].span_id, 200);

        // Parent buffer should be unaffected by child's drain
        let parent_drain = drain_thread_buffer();
        assert_eq!(parent_drain.len(), 1, "parent buffer should be independent");
        assert_eq!(parent_drain[0].span_id, 100);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: push_measurement does not panic during TLS teardown.
// A sentinel type with a Drop impl calls push_measurement during thread
// destruction. join().unwrap() verifies no panic occurred regardless of
// whether try_with returned Ok (TLS alive) or Err (TLS gone).
#[test]
fn push_measurement_safe_during_tls_teardown() {
    struct PushOnDrop;
    impl Drop for PushOnDrop {
        fn drop(&mut self) {
            // This runs during TLS destruction. push_measurement must
            // not panic -- try_with returns Err if THREAD_BUFFER TLS
            // is already destroyed, and try_borrow_mut handles reentrant
            // borrow. Either way, no crash.
            push_measurement(Measurement {
                span_id: 999,
                ..Measurement::default()
            });
        }
    }

    thread_local! {
        static TEARDOWN_SENTINEL: std::cell::RefCell<Option<PushOnDrop>> =
            const { std::cell::RefCell::new(None) };
    }

    let handle = std::thread::spawn(|| {
        // Initialize THREAD_BUFFER first by pushing a real measurement
        push_measurement(Measurement {
            span_id: 1,
            ..Measurement::default()
        });

        // Then initialize the sentinel TLS. During thread teardown,
        // TLS destructors run in reverse initialization order, so
        // the sentinel may drop before or after THREAD_BUFFER depending
        // on platform. Either path must be safe.
        TEARDOWN_SENTINEL.with(|cell| {
            *cell.borrow_mut() = Some(PushOnDrop);
        });

        // Drain what we can before teardown
        drain_thread_buffer();
    });
    handle
        .join()
        .expect("thread panicked during TLS teardown -- push_measurement is not teardown-safe");
}

// INVARIANT TEST: drain_thread_buffer returns empty if no buffer initialized.
#[test]
fn drain_thread_buffer_empty_on_fresh_thread() {
    // Fresh spawned thread -- no push_measurement called.
    std::thread::spawn(|| {
        let drained = drain_thread_buffer();
        assert!(drained.is_empty(), "fresh thread should drain empty");
    })
    .join()
    .expect("test thread panicked");
}
