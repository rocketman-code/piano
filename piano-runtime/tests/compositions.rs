//! Composition proofs: verify that components sharing a resource
//! interact correctly.

use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::guard::enter;
use piano_runtime::piano_future::enter_async;
use piano_runtime::session::ProfileSession;
use std::future::Future;

fn block_on<F: std::future::Future>(mut f: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    let raw = RawWaker::new(
        std::ptr::null(),
        &RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {}),
    );
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
    let waker = unsafe { Waker::from_raw(raw) };
    let mut cx = Context::from_waker(&waker);
    let mut f = unsafe { std::pin::Pin::new_unchecked(&mut f) };
    match f.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("test future returned Pending"),
    }
}

// Nested PianoFutures compute correct self-time.
// Outer's self_ns should exclude inner's inclusive time.
#[test]
fn nested_piano_futures_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);

        block_on(async {
            let outer_fut = enter_async(0, async {
                let inner_fut = enter_async(1, async {
                    std::hint::black_box(vec![0u8; 64]);
                });
                block_on(inner_fut);
                std::hint::black_box(vec![0u8; 64]);
            });
            block_on(outer_fut);
        });

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 2);

        let outer = agg.iter().find(|a| a.name_id == 0).unwrap();
        let inner = agg.iter().find(|a| a.name_id == 1).unwrap();

        // Inner has no children: self == inclusive
        assert_eq!(inner.self_ns, inner.inclusive_ns);
        // Outer's self < inclusive (inner subtracted)
        assert!(
            outer.self_ns < outer.inclusive_ns,
            "outer self ({}) must be < inclusive ({})",
            outer.self_ns,
            outer.inclusive_ns
        );
    })
    .join()
    .unwrap();
}

// Guard inside PianoFuture: alloc deltas compose correctly.
#[test]
fn guard_inside_piano_future_alloc_deltas_compose() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);

        block_on(async {
            let fut = enter_async(0, async {
                {
                    let _g = enter(1);
                    piano_runtime::alloc::record_alloc(200);
                }
                piano_runtime::alloc::record_alloc(100);
            });
            block_on(fut);
        });

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 2);

        let sync_m = agg.iter().find(|a| a.name_id == 1).unwrap();
        let async_m = agg.iter().find(|a| a.name_id == 0).unwrap();

        assert_eq!(sync_m.alloc_count, 1);
        assert_eq!(sync_m.alloc_bytes, 200);
        // Async's alloc includes its own (100) + sync child (200)
        assert_eq!(async_m.alloc_count, 2);
        assert_eq!(async_m.alloc_bytes, 300);
    })
    .join()
    .unwrap();
}

fn make_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

// PianoFuture polled on thread A (Pending), completed on thread B.
// Alloc deltas from both polls must accumulate correctly.
#[test]
fn piano_future_thread_migration() {
    use piano_runtime::alloc::record_alloc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FlagFuture {
        ready: std::sync::Arc<AtomicBool>,
    }

    impl std::future::Future for FlagFuture {
        type Output = u64;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<u64> {
            if self.ready.load(Ordering::Relaxed) {
                record_alloc(200);
                std::task::Poll::Ready(42)
            } else {
                record_alloc(100);
                std::task::Poll::Pending
            }
        }
    }

    let flag = std::sync::Arc::new(AtomicBool::new(false));
    let flag2 = std::sync::Arc::clone(&flag);

    let (tx, rx) = std::sync::mpsc::sync_channel::<
        std::pin::Pin<Box<piano_runtime::PianoFuture<FlagFuture>>>,
    >(1);

    let handle_a = std::thread::spawn(move || {
        ProfileSession::init(None, false, &[], "test", 0);

        let pf = enter_async(0, FlagFuture { ready: flag2 });
        let mut boxed = Box::pin(pf);

        let waker = make_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let result = boxed.as_mut().poll(&mut cx);
        assert!(result.is_pending());

        tx.send(boxed).unwrap();
    });

    let handle_b = std::thread::spawn(move || {
        let mut boxed = rx.recv().unwrap();

        flag.store(true, std::sync::atomic::Ordering::Relaxed);

        let waker = make_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let result = boxed.as_mut().poll(&mut cx);
        assert!(matches!(result, std::task::Poll::Ready(42)));

        drop(boxed);

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);

        let m = &agg[0];
        assert_eq!(
            m.alloc_count, 2,
            "allocs from both polls (thread A + B) must accumulate"
        );
        assert_eq!(
            m.alloc_bytes, 300,
            "bytes from both polls (100 + 200) must accumulate"
        );
    });

    handle_a.join().expect("thread A panicked");
    handle_b.join().expect("thread B panicked");
}

#[test]
fn nested_guards_compute_correct_self_time() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);

        // outer calls inner. inner takes ~1ms. outer's self-time should
        // exclude inner's time (children TLS accumulator).
        {
            let _outer = enter(0);
            {
                let _inner = enter(1);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }

        let agg = drain_thread_agg();
        let outer = agg.iter().find(|a| a.name_id == 0).unwrap();
        let inner = agg.iter().find(|a| a.name_id == 1).unwrap();

        // inner's self_ns should be close to 1ms (the sleep).
        // outer's self_ns should be much less than inner's (just overhead).
        assert!(
            inner.self_ns > 500_000,
            "inner should have at least 0.5ms self-time, got {}ns",
            inner.self_ns
        );
        assert!(
            outer.self_ns < inner.self_ns,
            "outer self-time ({}) must be less than inner ({})",
            outer.self_ns,
            inner.self_ns
        );
        // outer's inclusive_ns should be >= inner's (it contains inner)
        assert!(
            outer.inclusive_ns >= inner.inclusive_ns,
            "outer inclusive ({}) must be >= inner inclusive ({})",
            outer.inclusive_ns,
            inner.inclusive_ns
        );
    })
    .join()
    .unwrap();
}
