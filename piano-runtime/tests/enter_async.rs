use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::piano_future::enter_async;
use piano_runtime::session::ProfileSession;

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
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => panic!("test future returned Pending"),
        }
    }
}

#[test]
fn async_enter_inactive_is_transparent() {
    std::thread::spawn(|| {
        let fut = enter_async(0, async { 42 });
        let result = block_on(fut);
        assert_eq!(result, 42);
    }).join().unwrap();
}

#[test]
fn async_enter_emits_aggregate_on_completion() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        let fut = enter_async(0, async { 42 });
        let _ = block_on(fut);
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].calls, 1);
        assert_eq!(agg[0].name_id, 0);
    }).join().unwrap();
}

#[test]
fn async_enter_emits_on_drop_if_cancelled() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        {
            let mut fut = enter_async(0, async {
                std::future::pending::<()>().await;
            });
            use std::future::Future;
            use std::task::{Context, RawWaker, RawWakerVTable, Waker};
            let raw = RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {}),
            );
            const VTABLE: RawWakerVTable =
                RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
            let waker = unsafe { Waker::from_raw(raw) };
            let mut cx = Context::from_waker(&waker);
            let pinned = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
            let _ = pinned.poll(&mut cx);
        }
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1, "cancelled future must emit aggregate");
    }).join().unwrap();
}

#[test]
fn never_polled_future_emits_nothing() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        { let _fut = enter_async(0, async { 42 }); }
        let agg = drain_thread_agg();
        assert!(agg.is_empty(), "never-polled future must emit nothing");
    }).join().unwrap();
}

#[test]
fn wall_time_starts_on_first_poll_not_construction() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);

        let fut = enter_async(0, async { 42 });
        // 10ms gap between construction and polling
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _ = block_on(fut);

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        // If timing started at construction, self_ns would be >= 10ms.
        // If timing started at first poll, self_ns should be < 1ms.
        assert!(
            agg[0].self_ns < 1_000_000,
            "self_ns ({} ns) should be < 1ms; wall time must start at first poll, not construction",
            agg[0].self_ns
        );
    }).join().unwrap();
}

#[test]
fn panicking_inner_future_emits_aggregate() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);

        struct PanickingFuture;
        impl std::future::Future for PanickingFuture {
            type Output = ();
            fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>)
                -> std::task::Poll<()> {
                panic!("intentional panic in poll");
            }
        }

        use std::future::Future;
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};
        let raw = RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {}),
        );
        const VTABLE: RawWakerVTable =
            RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
        let waker = unsafe { Waker::from_raw(raw) };
        let mut cx = Context::from_waker(&waker);

        let mut pf = enter_async(0, PanickingFuture);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let pinned = unsafe { std::pin::Pin::new_unchecked(&mut pf) };
            let _ = pinned.poll(&mut cx);
        }));
        assert!(result.is_err(), "poll should have panicked");

        drop(pf);
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1, "panicking future must emit aggregate via Drop");
    }).join().unwrap();
}

#[test]
fn multi_poll_alloc_accumulation() {
    std::thread::spawn(|| {
        use piano_runtime::alloc::record_alloc;

        ProfileSession::init(None, false, &[], "test", 0);

        struct AllocPerPoll {
            polled: bool,
        }
        impl std::future::Future for AllocPerPoll {
            type Output = ();
            fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>)
                -> std::task::Poll<()> {
                let this = self.get_mut();
                if this.polled {
                    record_alloc(200);
                    std::task::Poll::Ready(())
                } else {
                    record_alloc(100);
                    this.polled = true;
                    std::task::Poll::Pending
                }
            }
        }

        use std::future::Future;
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};
        let raw = RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {}),
        );
        const VTABLE: RawWakerVTable =
            RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
        let waker = unsafe { Waker::from_raw(raw) };
        let mut cx = Context::from_waker(&waker);

        let mut pf = enter_async(0, AllocPerPoll { polled: false });

        // First poll: Pending, 100 bytes
        let pinned = unsafe { std::pin::Pin::new_unchecked(&mut pf) };
        assert!(pinned.poll(&mut cx).is_pending());

        // Second poll: Ready, 200 bytes
        let pinned = unsafe { std::pin::Pin::new_unchecked(&mut pf) };
        assert!(pinned.poll(&mut cx).is_ready());

        drop(pf);
        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].alloc_count, 2, "allocs from both polls must accumulate");
        assert_eq!(agg[0].alloc_bytes, 300, "bytes from both polls must accumulate");
    }).join().unwrap();
}

#[test]
fn async_closure_no_capture() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        let handle = std::thread::spawn(|| {
            let fut = enter_async(0, async { 42 });
            block_on(fut)
        });
        assert_eq!(handle.join().unwrap(), 42);
        drain_thread_agg();
    }).join().unwrap();
}
