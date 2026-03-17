use piano_runtime::buffer::{drain_all_buffers, push_measurement, Registry};
use piano_runtime::measurement::Measurement;
use std::sync::{Arc, Mutex};

// drain_all_buffers tests live in this separate binary (separate process)
// because drain_all_buffers drains ALL registered thread buffers through the
// shared registry. Running it in the same binary as per-thread tests
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
    let reg: Arc<Registry> = Arc::new(Mutex::new(Vec::new()));

    // Part 1: buffer survives thread exit via registry Arc
    let reg_clone = Arc::clone(&reg);
    let handle = std::thread::spawn(move || {
        push_measurement(Measurement {
            span_id: 42,
            ..Measurement::default()
        }, &reg_clone);
        // Thread exits without draining -- buffer remains in registry
    });
    handle.join().expect("thread panicked");

    let first = drain_all_buffers(&reg);
    assert!(
        first.iter().any(|m| m.span_id == 42),
        "measurement from dead thread must survive in registry"
    );

    // Part 2: double-drain returns empty (no duplicates)
    let second = drain_all_buffers(&reg);
    assert!(
        !second.iter().any(|m| m.span_id == 42),
        "second drain must not contain span_id 42 (no duplicates)"
    );
}
