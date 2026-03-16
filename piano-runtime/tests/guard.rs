use piano_runtime::alloc::{record_alloc, snapshot_alloc_counters, PianoAllocator};
use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::guard::Guard;
use std::alloc::System;

#[global_allocator]
static ALLOC: PianoAllocator<System> = PianoAllocator::new(System);

// All tests run in spawned threads for TLS isolation.
// All assertions use deltas, never absolute values.

// INVARIANT TEST: Guard produces a Measurement on drop.
#[test]
fn guard_produces_measurement_on_drop() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 42, false);
        } // guard drops, pushes measurement to buffer

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "guard should push exactly one measurement");

        let m = &drained[0];
        assert_eq!(m.span_id, 1);
        assert_eq!(m.parent_span_id, 0);
        assert_eq!(m.name_id, 42);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard captures wall time (end >= start).
#[test]
fn guard_captures_wall_time() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
            // Do some work to ensure nonzero wall time
            std::hint::black_box(vec![0u8; 1024]);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(
            m.end_ns >= m.start_ns,
            "end_ns ({}) must be >= start_ns ({})",
            m.end_ns,
            m.start_ns
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard captures allocation deltas.
#[test]
fn guard_captures_alloc_deltas() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
            // Record user allocations while guard is active
            record_alloc(100);
            record_alloc(200);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 2, "should capture 2 user allocations");
        assert_eq!(m.alloc_bytes, 300, "should capture 300 bytes");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Profiler bookkeeping allocs are excluded from user counts.
// Guard::new() and Guard::drop() both enter ReentrancyGuard, so any
// allocations that happen inside those calls are not counted.
#[test]
fn guard_excludes_own_bookkeeping_allocs() {
    std::thread::spawn(|| {
        let (count_before, bytes_before, _fc, _fb) = snapshot_alloc_counters();
        {
            let _g = Guard::new(1, 0, 0, false);
            // No explicit user allocs
        }
        let (count_after, bytes_after, _fc, _fb) = snapshot_alloc_counters();

        // The alloc counters should not have changed from user-visible allocs
        // (guard's internal bookkeeping is excluded by reentrancy)
        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 0, "no user allocs should be recorded");
        assert_eq!(m.alloc_bytes, 0, "no user bytes should be recorded");

        // The global counters also should show no user-visible change
        assert_eq!(
            count_after - count_before,
            0,
            "guard bookkeeping should not increment user alloc count"
        );
        assert_eq!(
            bytes_after - bytes_before,
            0,
            "guard bookkeeping should not increment user alloc bytes"
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard sets thread_id to the creating thread.
#[test]
fn guard_sets_thread_id() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(m.thread_id > 0, "thread_id must be nonzero");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: thread_id is consistent across multiple guards on same thread.
#[test]
fn guard_thread_id_consistent_on_same_thread() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
        }
        {
            let _g = Guard::new(2, 0, 0, false);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2);
        assert_eq!(
            drained[0].thread_id, drained[1].thread_id,
            "same thread must produce same thread_id"
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: different threads get different thread_ids.
#[test]
fn guard_thread_id_differs_across_threads() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
        }
        let parent_drain = drain_thread_buffer();
        let parent_tid = parent_drain[0].thread_id;

        let child_tid = std::thread::spawn(|| {
            {
                let _g = Guard::new(2, 0, 0, false);
            }
            let child_drain = drain_thread_buffer();
            child_drain[0].thread_id
        })
        .join()
        .expect("child panicked");

        assert_ne!(
            parent_tid, child_tid,
            "different threads must have different thread_ids"
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cpu_start_ns and cpu_end_ns are 0 (no CPU time support yet).
#[test]
fn guard_cpu_time_is_zero() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.cpu_start_ns, 0, "cpu_start_ns should be 0 (cpu_time_enabled is false)");
        assert_eq!(m.cpu_end_ns, 0, "cpu_end_ns should be 0 (cpu_time_enabled is false)");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: nested guards produce correct parent-child relationships.
#[test]
fn nested_guards_produce_measurements() {
    std::thread::spawn(|| {
        {
            let _outer = Guard::new(1, 0, 10, false);
            {
                let _inner = Guard::new(2, 1, 20, false);
                // inner drops first
            }
            // outer drops second
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2, "two guards should produce two measurements");

        // Inner drops first, so it's first in the buffer
        let inner = &drained[0];
        assert_eq!(inner.span_id, 2);
        assert_eq!(inner.parent_span_id, 1);
        assert_eq!(inner.name_id, 20);

        let outer = &drained[1];
        assert_eq!(outer.span_id, 1);
        assert_eq!(outer.parent_span_id, 0);
        assert_eq!(outer.name_id, 10);

        // Outer's wall time must be >= inner's wall time
        assert!(
            outer.wall_time_ns() >= inner.wall_time_ns(),
            "outer wall time ({}) must be >= inner wall time ({})",
            outer.wall_time_ns(),
            inner.wall_time_ns()
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: alloc deltas are scoped to the guard's lifetime.
#[test]
fn alloc_deltas_scoped_to_guard_lifetime() {
    std::thread::spawn(|| {
        // Allocs before the guard -- should NOT be counted
        record_alloc(999);

        {
            let _g = Guard::new(1, 0, 0, false);
            record_alloc(50);
            record_alloc(75);
        }

        // Allocs after the guard -- should NOT be counted
        record_alloc(888);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 2, "only allocs during guard lifetime count");
        assert_eq!(m.alloc_bytes, 125, "only bytes during guard lifetime count");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard captures free (deallocation) deltas.
#[test]
fn guard_captures_free_deltas() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, false);
            // Allocate and immediately drop to trigger dealloc tracking
            let v: Vec<u8> = Vec::with_capacity(100);
            let v = std::hint::black_box(v);
            drop(v);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(m.free_count >= 1, "should capture at least 1 free event");
        assert!(m.free_bytes >= 100, "should capture at least 100 freed bytes");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard captures CPU time when cpu_time_enabled is true.
#[cfg(feature = "cpu-time")]
#[test]
fn guard_captures_cpu_time_when_enabled() {
    std::thread::spawn(|| {
        {
            let _g = Guard::new(1, 0, 0, true);
            // Do some CPU work
            let mut sum: u64 = 0;
            for i in 0..10_000u64 {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);
        }

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(m.cpu_start_ns > 0, "cpu_start_ns must be nonzero when enabled");
        assert!(m.cpu_end_ns > 0, "cpu_end_ns must be nonzero when enabled");
        assert!(
            m.cpu_end_ns >= m.cpu_start_ns,
            "cpu_end_ns ({}) must be >= cpu_start_ns ({})",
            m.cpu_end_ns,
            m.cpu_start_ns
        );
    })
    .join()
    .expect("test thread panicked");
}
