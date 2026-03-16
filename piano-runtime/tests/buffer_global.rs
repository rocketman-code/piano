use piano_runtime::buffer::{drain_all_buffers, push_measurement};
use piano_runtime::measurement::Measurement;

// drain_all_buffers tests live in this separate binary (separate process)
// because drain_all_buffers drains ALL registered thread buffers through the
// shared global registry. Running it in the same binary as per-thread tests
// steals other tests' measurements via the shared Arc<Mutex<ThreadBuffer>>.
//
// Process isolation gives this binary its own registry.

// INVARIANT TEST: thread buffers survive thread exit (Arc in registry),
// and drain_all_buffers double-drain returns empty (no duplicates).
//
// Combined into one test because multiple drain_all_buffers tests within
// the same binary race with each other through the shared registry.
#[test]
fn drain_all_buffers_invariants() {
    // Part 1: buffer survives thread exit via registry Arc
    let handle = std::thread::spawn(|| {
        push_measurement(Measurement {
            span_id: 42,
            ..Measurement::default()
        });
        // Thread exits without draining -- buffer remains in registry
    });
    handle.join().expect("thread panicked");

    let first = drain_all_buffers();
    assert!(
        first.iter().any(|m| m.span_id == 42),
        "measurement from dead thread must survive in registry"
    );

    // Part 2: double-drain returns empty (no duplicates)
    let second = drain_all_buffers();
    assert!(
        !second.iter().any(|m| m.span_id == 42),
        "second drain must not contain span_id 42 (no duplicates)"
    );
}
