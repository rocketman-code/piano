use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::alloc::{record_alloc, PianoAllocator};
use piano_runtime::guard::enter;
use piano_runtime::session::ProfileSession;
use std::alloc::System;

#[global_allocator]
static ALLOC: PianoAllocator<System> = PianoAllocator::new(System);

// All tests run in spawned threads for TLS isolation.

#[test]
fn guard_produces_aggregate_on_drop() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(42);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].name_id, 42);
        assert_eq!(agg[0].calls, 1);
    })
    .join()
    .unwrap();
}

#[test]
fn guard_captures_wall_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
            std::hint::black_box(vec![0u8; 1024]);
        }
        let agg = drain_thread_agg();
        assert!(agg[0].self_ns > 0, "self_ns must be nonzero");
        assert!(agg[0].inclusive_ns > 0, "inclusive_ns must be nonzero");
    })
    .join()
    .unwrap();
}

#[test]
fn guard_captures_alloc_deltas() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
            record_alloc(100);
            record_alloc(200);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg[0].alloc_count, 2);
        assert_eq!(agg[0].alloc_bytes, 300);
    })
    .join()
    .unwrap();
}

#[test]
fn guard_excludes_own_bookkeeping_allocs() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg[0].alloc_count, 0);
        assert_eq!(agg[0].alloc_bytes, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn multiple_calls_accumulate() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        for _ in 0..10 {
            let _g = enter(0);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].calls, 10);
    })
    .join()
    .unwrap();
}

#[test]
fn different_functions_get_separate_entries() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
        }
        {
            let _g = enter(1);
        }
        {
            let _g = enter(0);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 2);
        let f0 = agg.iter().find(|a| a.name_id == 0).unwrap();
        let f1 = agg.iter().find(|a| a.name_id == 1).unwrap();
        assert_eq!(f0.calls, 2);
        assert_eq!(f1.calls, 1);
    })
    .join()
    .unwrap();
}

#[test]
fn nested_guards_compute_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _outer = enter(0);
            std::hint::black_box(vec![0u8; 64]); // outer's own work
            {
                let _inner = enter(1);
                std::hint::black_box(vec![0u8; 64]); // inner's work
            }
        }
        let agg = drain_thread_agg();
        let outer = agg.iter().find(|a| a.name_id == 0).unwrap();
        let inner = agg.iter().find(|a| a.name_id == 1).unwrap();
        // Outer's self_ns should be LESS than its inclusive_ns
        // (because inner's time is subtracted)
        assert!(
            outer.self_ns < outer.inclusive_ns,
            "outer self ({}) must be < inclusive ({})",
            outer.self_ns,
            outer.inclusive_ns
        );
        // Inner's self_ns should equal its inclusive_ns (no children)
        assert_eq!(inner.self_ns, inner.inclusive_ns);
    })
    .join()
    .unwrap();
}

#[test]
fn alloc_deltas_scoped_to_guard_lifetime() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        record_alloc(999); // before guard
        {
            let _g = enter(0);
            record_alloc(50);
            record_alloc(75);
        }
        record_alloc(888); // after guard
        let agg = drain_thread_agg();
        assert_eq!(agg[0].alloc_count, 2, "only allocs during guard");
        assert_eq!(agg[0].alloc_bytes, 125);
    })
    .join()
    .unwrap();
}

#[test]
fn guard_captures_free_deltas() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
            let v: Vec<u8> = Vec::with_capacity(100);
            let v = std::hint::black_box(v);
            drop(v);
        }
        let agg = drain_thread_agg();
        assert!(agg[0].free_count >= 1);
        assert!(agg[0].free_bytes >= 100);
    })
    .join()
    .unwrap();
}

#[cfg(unix)]
#[test]
fn guard_captures_cpu_time_when_enabled() {
    std::thread::spawn(|| {
        ProfileSession::init(None, true, &[], "test", 0);
        {
            let _g = enter(0);
            let mut sum: u64 = 0;
            for i in 0..10_000u64 {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);
        }
        let agg = drain_thread_agg();
        assert!(
            agg[0].cpu_self_ns > 0,
            "cpu_self_ns must be nonzero when enabled"
        );
    })
    .join()
    .unwrap();
}

#[test]
fn guard_cpu_time_zero_when_disabled() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg[0].cpu_self_ns, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn guard_never_panics() {
    std::thread::spawn(|| {
        ProfileSession::init(None, true, &[], "test", 0);
        {
            let _g = enter(0);
        }
        drain_thread_agg();
    })
    .join()
    .expect("guard with cpu_time panicked");

    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let _g = enter(0);
        }
        drain_thread_agg();
    })
    .join()
    .expect("guard without cpu_time panicked");
}
