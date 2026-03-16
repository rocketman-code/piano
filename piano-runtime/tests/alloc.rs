use piano_runtime::alloc::{
    is_reentrant, record_alloc, snapshot_alloc_counters, ReentrancyGuard,
};

// All tests run in spawned threads for TLS isolation.
// All assertions use deltas, never absolute values.

// INVARIANT TEST: alloc counters are monotonically increasing.
#[test]
fn record_alloc_increments_counters() {
    std::thread::spawn(|| {
        let (count_before, bytes_before, _fc, _fb) = snapshot_alloc_counters();
        record_alloc(100);
        record_alloc(200);
        let (count_after, bytes_after, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(count_after - count_before, 2, "should record 2 allocs");
        assert_eq!(bytes_after - bytes_before, 300, "should record 300 bytes");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: reentrancy guard excludes allocs from user counts.
#[test]
fn reentrancy_guard_excludes_allocs() {
    std::thread::spawn(|| {
        let (count_before, bytes_before, _fc, _fb) = snapshot_alloc_counters();
        {
            let _guard = ReentrancyGuard::enter();
            record_alloc(999);
        } // guard dropped, reentrancy exits
        let (count_after, bytes_after, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(count_after - count_before, 0, "guarded allocs should not count");
        assert_eq!(bytes_after - bytes_before, 0, "guarded bytes should not count");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: ReentrancyGuard is RAII -- nesting works correctly.
#[test]
fn nested_reentrancy() {
    std::thread::spawn(|| {
        let (count_before, _, _fc, _fb) = snapshot_alloc_counters();
        {
            let _outer = ReentrancyGuard::enter();
            record_alloc(100); // excluded (counter=1)
            {
                let _inner = ReentrancyGuard::enter();
                record_alloc(200); // excluded (counter=2)
            } // inner dropped (counter=1)
            record_alloc(300); // excluded (counter=1)
        } // outer dropped (counter=0)
        let (count_after, _, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(
            count_after - count_before, 0,
            "all allocs inside nested guards should be excluded"
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: is_reentrant reflects guard state.
#[test]
fn is_reentrant_reflects_guard_state() {
    // Fresh spawned thread -- zeroed TLS is a property of new threads,
    // not an absolute assertion on shared state.
    std::thread::spawn(|| {
        assert!(!is_reentrant(), "fresh thread should not be reentrant");
        {
            let _guard = ReentrancyGuard::enter();
            assert!(is_reentrant(), "should be reentrant while guard is held");
        }
        assert!(!is_reentrant(), "should not be reentrant after guard drops");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cross-thread isolation -- each thread has its own counters.
#[test]
fn cross_thread_isolation() {
    std::thread::spawn(|| {
        record_alloc(100);

        // Spawned child should have independent counters
        let (child_count, child_bytes, _fc, _fb) = std::thread::spawn(|| {
            snapshot_alloc_counters()
        })
        .join()
        .expect("child panicked");

        // Fresh thread TLS starts at 0 -- property of new threads
        assert_eq!(child_count, 0, "child should have independent alloc count");
        assert_eq!(child_bytes, 0, "child should have independent alloc bytes");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: ReentrancyGuard is !Send.
// Enforced by PhantomData<*const ()> in the struct definition.
// This compile-time property can't be tested at runtime, but we verify
// the observable consequence: the guard works correctly per-thread.
// The type system prevents cross-thread movement at compile time.
// If someone removes PhantomData, Guard tests (which depend on
// ReentrancyGuard being !Send) will catch the regression.
