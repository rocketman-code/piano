use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::parent::current_parent;
use piano_runtime::piano_future::enter_async;
use piano_runtime::session::ProfileSession;

// Level 2: enter_async composes session + parent + PianoFuture.

fn block_on<F: std::future::Future>(mut f: F) -> F::Output {
    // Minimal single-threaded executor for testing.
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
        // No session initialized. enter_async should be transparent.
        let fut = enter_async(0, async { 42 });
        let result = block_on(fut);
        assert_eq!(result, 42, "inactive future must pass through return value");
    })
    .join()
    .unwrap();
}

#[test]
fn async_enter_captures_parent_at_construction() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        // Set up a parent via sync guard
        let _outer = piano_runtime::guard::enter(0);
        let outer_span = current_parent();

        // enter_async captures parent from TLS at construction time
        let fut = enter_async(1, async { 42 });
        let result = block_on(fut);

        assert_eq!(result, 42);

        // Check the measurement
        drop(_outer);
        let drained = drain_thread_buffer();
        // Two measurements: the async future + the outer sync guard
        assert_eq!(drained.len(), 2, "expected 2 measurements");

        // The async measurement's parent should be the outer sync span
        let async_m = drained.iter().find(|m| m.name_id == 1).expect("async measurement");
        assert_eq!(
            async_m.parent_span_id, outer_span,
            "async parent must be the sync span that was active at construction"
        );
    })
    .join()
    .unwrap();
}

#[test]
fn async_enter_emits_measurement_on_completion() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        let fut = enter_async(0, async { 42 });
        let _ = block_on(fut);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "completed future must emit one measurement");

        let m = &drained[0];
        assert_ne!(m.span_id, 0);
        assert_eq!(m.name_id, 0);
        assert!(m.end_ns >= m.start_ns);
    })
    .join()
    .unwrap();
}

#[test]
fn async_enter_emits_on_drop_if_cancelled() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        {
            // Create and poll once, then drop (simulate cancellation)
            let mut _fut = enter_async(0, async {
                std::future::pending::<()>().await;
            });
            // Poll once to start timing
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
            let mut pinned = unsafe { std::pin::Pin::new_unchecked(&mut _fut) };
            let _ = pinned.as_mut().poll(&mut cx); // returns Pending
            // _fut drops here (cancelled)
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "cancelled future must emit measurement on drop");
    })
    .join()
    .unwrap();
}

#[test]
fn never_polled_future_emits_nothing() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        {
            let _fut = enter_async(0, async { 42 });
            // Drop without polling
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 0, "never-polled future must emit nothing");
    })
    .join()
    .unwrap();
}

#[test]
fn async_closure_no_capture() {
    // The critical proof: enter_async inside a closure creates NO captures.
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[]);

        let handle = std::thread::spawn(|| {
            let fut = enter_async(0, async { 42 });
            block_on(fut)
        });
        assert_eq!(handle.join().unwrap(), 42);

        // In method chain
        let results: Vec<u32> = (0..3)
            .map(|_| {
                let fut = enter_async(1, async { 7 });
                block_on(fut)
            })
            .collect();
        assert_eq!(results, vec![7, 7, 7]);

        drain_thread_buffer();
    })
    .join()
    .unwrap();
}
