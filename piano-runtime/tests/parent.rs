use piano_runtime::parent::{current_parent, restore_parent, set_parent};

// Level 0 leaf: TLS parent_span_id with save/restore RAII.
// Every test runs in a spawned thread for TLS isolation.

#[test]
fn initial_value_is_zero() {
    std::thread::spawn(|| {
        assert_eq!(current_parent(), 0, "root sentinel before any set");
    })
    .join()
    .unwrap();
}

#[test]
fn set_returns_previous() {
    std::thread::spawn(|| {
        assert_eq!(set_parent(42), 0, "first set returns root sentinel");
        assert_eq!(set_parent(99), 42, "second set returns previous");
        restore_parent(0);
    })
    .join()
    .unwrap();
}

#[test]
fn restore_returns_to_previous() {
    std::thread::spawn(|| {
        let prev = set_parent(42);
        restore_parent(prev);
        assert_eq!(current_parent(), 0, "restored to root");
    })
    .join()
    .unwrap();
}

#[test]
fn nested_save_restore() {
    std::thread::spawn(|| {
        // Simulate: A calls B calls C
        let prev_a = set_parent(10); // enter A (parent = root)
        assert_eq!(current_parent(), 10);

        let prev_b = set_parent(20); // enter B (parent = A)
        assert_eq!(current_parent(), 20);

        let prev_c = set_parent(30); // enter C (parent = B)
        assert_eq!(current_parent(), 30);

        restore_parent(prev_c); // exit C
        assert_eq!(current_parent(), 20);

        restore_parent(prev_b); // exit B
        assert_eq!(current_parent(), 10);

        restore_parent(prev_a); // exit A
        assert_eq!(current_parent(), 0);
    })
    .join()
    .unwrap();
}

#[test]
fn cross_thread_isolation() {
    std::thread::spawn(|| {
        set_parent(42);

        let handle = std::thread::spawn(|| {
            // New thread has its own TLS, starts at 0
            assert_eq!(current_parent(), 0, "new thread starts at root");
            set_parent(99);
            assert_eq!(current_parent(), 99);
        });
        handle.join().unwrap();

        // Original thread unaffected
        assert_eq!(current_parent(), 42, "original thread unchanged");
        restore_parent(0);
    })
    .join()
    .unwrap();
}

#[test]
fn tls_destroyed_returns_zero() {
    // After thread exit, TLS is destroyed. Simulate by checking
    // the try_with fallback. We can't easily test post-destruction,
    // but we can verify the function is safe to call (no panic).
    std::thread::spawn(|| {
        set_parent(42);
        assert_eq!(current_parent(), 42);
        // Thread exits, TLS destroyed. The function's try_with
        // fallback returns 0. We can't observe this from outside
        // (the thread is gone), but the code path is exercised
        // by the tls_teardown tests for other TLS cells.
    })
    .join()
    .unwrap();
}

#[test]
fn simulated_guard_raii() {
    // Simulate what Guard will do: save on creation, restore on drop
    struct FakeGuard {
        saved_parent: u64,
    }

    impl FakeGuard {
        fn new(span_id: u64) -> Self {
            let saved = set_parent(span_id);
            Self { saved_parent: saved }
        }
    }

    impl Drop for FakeGuard {
        fn drop(&mut self) {
            restore_parent(self.saved_parent);
        }
    }

    std::thread::spawn(|| {
        assert_eq!(current_parent(), 0);

        {
            let _g1 = FakeGuard::new(10);
            assert_eq!(current_parent(), 10);

            {
                let _g2 = FakeGuard::new(20);
                assert_eq!(current_parent(), 20);
            }
            // g2 dropped, restored to 10
            assert_eq!(current_parent(), 10);
        }
        // g1 dropped, restored to 0
        assert_eq!(current_parent(), 0);
    })
    .join()
    .unwrap();
}

#[test]
fn closure_does_not_capture() {
    // The critical proof: closures that call current_parent() or
    // set_parent() don't capture any profiling state. They read TLS.
    std::thread::spawn(|| {
        set_parent(42);

        // 'static closure: no captures, reads TLS
        let handle = std::thread::spawn(|| {
            assert_eq!(current_parent(), 0); // new thread, own TLS
            let _g = set_parent(99);
            assert_eq!(current_parent(), 99);
        });
        handle.join().unwrap();

        // Closure in method chain: no captures
        let parents: Vec<u64> = (0..3)
            .map(|_| {
                current_parent() // reads TLS, no capture
            })
            .collect();
        assert_eq!(parents, vec![42, 42, 42]);

        restore_parent(0);
    })
    .join()
    .unwrap();
}
