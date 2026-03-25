//! Code generation pattern tests.
//!
//! Each function below mirrors the EXACT code the rewriter produces.
//! If these compile and pass, the injection patterns are correct.

use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::session::ProfileSession;
use piano_runtime::PianoAllocator;
use std::alloc::System;

const PIANO_NAMES: &[(u32, &str)] = &[
    (0, "measured_sync"),
    (1, "measured_async"),
    (2, "inner_fn"),
    (3, "unsafe_measured"),
];

// --- Rewriter output patterns ---

fn measured_sync(x: i32) -> i32 {
    let __piano_guard = piano_runtime::enter(0);
    x + 1
}

async fn measured_async(x: i32) -> i32 {
    piano_runtime::enter_async(1, async move { x * 2 }).await
}

fn outer_with_inner(x: i32) -> i32 {
    let __piano_guard = piano_runtime::enter(0);
    fn inner_fn(y: i32) -> i32 {
        let __piano_guard = piano_runtime::enter(2);
        y * 3
    }
    inner_fn(x)
}

fn with_closure(items: &[i32]) -> Vec<i32> {
    let __piano_guard = piano_runtime::enter(0);
    items.iter().map(|x| x + 1).collect()
}

unsafe fn unsafe_measured(ptr: *const i32) -> i32 {
    let __piano_guard = piano_runtime::enter(3);
    unsafe { *ptr }
}

fn with_inner_attr(x: i32) -> i32 {
    #![allow(unused_variables, unused)]
    let __piano_guard = piano_runtime::enter(0);
    let unused = 42;
    x
}

// --- Compilation pattern tests ---

#[test]
fn sync_guard_injection_compiles_and_runs() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let result = measured_sync(10);
        assert_eq!(result, 11);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn async_whole_body_wrapping_compiles_and_runs() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(measured_async(5));
        assert_eq!(result, 10);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn inner_function_instrumented() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let result = outer_with_inner(4);
        assert_eq!(result, 12);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn closure_not_instrumented() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let result = with_closure(&[1, 2, 3]);
        assert_eq!(result, vec![2, 3, 4]);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn unsafe_fn_instrumented() {
    std::thread::spawn(|| {
        let val: i32 = 99;
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let result = unsafe { unsafe_measured(&val) };
        assert_eq!(result, 99);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn inner_attrs_before_guard() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);
        let result = with_inner_attr(5);
        assert_eq!(result, 5);
        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}

// --- Static verification tests ---

#[test]
fn name_table_format() {
    assert_eq!(PIANO_NAMES[0], (0, "measured_sync"));
    assert_eq!(PIANO_NAMES[1], (1, "measured_async"));
    assert_eq!(PIANO_NAMES[2], (2, "inner_fn"));
    assert_eq!(PIANO_NAMES[3], (3, "unsafe_measured"));
}

#[test]
fn allocator_wrapping_const() {
    const _: PianoAllocator<System> = PianoAllocator::new(System);
}

// --- Behavioral correctness ---

#[test]
fn nested_guards_compute_correct_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);

        {
            let _g1 = piano_runtime::enter(0);
            std::hint::black_box(vec![0u8; 64]);
            {
                let _g2 = piano_runtime::enter(1);
                std::hint::black_box(vec![0u8; 64]);
                {
                    let _g3 = piano_runtime::enter(2);
                    std::hint::black_box(vec![0u8; 64]);
                }
            }
        }

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 3, "3 functions = 3 aggregate entries");

        let f0 = agg.iter().find(|a| a.name_id == 0).unwrap();
        let f1 = agg.iter().find(|a| a.name_id == 1).unwrap();
        let f2 = agg.iter().find(|a| a.name_id == 2).unwrap();

        // Innermost has no children: self == inclusive
        assert_eq!(f2.self_ns, f2.inclusive_ns);
        // Middle's self < inclusive (inner subtracted)
        assert!(f1.self_ns < f1.inclusive_ns);
        // Outer's self < inclusive (middle+inner subtracted)
        assert!(f0.self_ns < f0.inclusive_ns);
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn siblings_both_aggregated() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);

        {
            let _g1 = piano_runtime::enter(0);
            {
                let _g2 = piano_runtime::enter(1);
            }
            {
                let _g3 = piano_runtime::enter(2);
            }
        }

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 3);
        for a in &agg {
            assert_eq!(a.calls, 1);
        }
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn async_preamble_has_nonzero_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);

        use std::future::Future;
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};
        fn noop_waker() -> Waker {
            fn no_op(_: *const ()) {}
            fn clone_fn(p: *const ()) -> RawWaker {
                RawWaker::new(p, &VTABLE)
            }
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
            unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
        }

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut = piano_runtime::enter_async(0, async move {
            let mut sum = 0u64;
            for i in 0..100_000 {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);
        });
        let pinned = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
        let _ = pinned.poll(&mut cx);
        drop(fut);

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert!(agg[0].self_ns > 0, "preamble must produce nonzero self_ns");
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn spawn_no_capture_required() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, PIANO_NAMES, "test", 0);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    let _g = piano_runtime::enter(0);
                    std::hint::black_box(42)
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        drain_thread_agg();
    })
    .join()
    .expect("test thread panicked");
}
