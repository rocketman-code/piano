use piano_runtime::span_id::next_span_id;

// INVARIANT TEST: IDs are non-zero.
#[test]
fn ids_are_nonzero() {
    for _ in 0..100 {
        assert_ne!(next_span_id(), 0, "span_id must never be 0 (reserved for root)");
    }
}

// INVARIANT TEST: IDs are unique and monotonically increasing.
// Uses deltas (relative ordering), not absolute values.
#[test]
fn ids_are_monotonically_increasing() {
    let a = next_span_id();
    let b = next_span_id();
    let c = next_span_id();
    assert!(b > a, "span_ids must be strictly increasing");
    assert!(c > b, "span_ids must be strictly increasing");
}

// INVARIANT TEST: IDs are unique across threads.
#[test]
fn cross_thread_uniqueness() {
    use std::collections::HashSet;

    let handles: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                (0..1000).map(|_| next_span_id()).collect::<Vec<_>>()
            })
        })
        .collect();

    let mut all_ids = HashSet::new();
    for h in handles {
        let ids = h.join().expect("thread panicked");
        for id in ids {
            assert_ne!(id, 0, "span_id must never be 0");
            assert!(all_ids.insert(id), "duplicate span_id: {id}");
        }
    }
    assert_eq!(all_ids.len(), 4000);
}
