use piano_runtime::alloc::record_alloc;
use piano_runtime::buffer::{drain_thread_buffer, Registry};
use piano_runtime::guard::Guard;
use piano_runtime::piano_future::{PianoFuture, PianoFutureState};
use piano_runtime::time::CalibrationData;
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

// All tests run in spawned threads for TLS isolation.

fn noop_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    // SAFETY: The vtable functions are valid no-ops. The data pointer is
    // null and never dereferenced. The waker does nothing -- suitable
    // for manual polling in tests.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

fn test_registry() -> Arc<Registry> {
    Arc::new(Mutex::new(Vec::new()))
}

fn test_state(span_id: u64, parent_span_id: u64, name_id: u32) -> PianoFutureState {
    PianoFutureState {
        span_id,
        parent_span_id,
        name_id,
        cpu_time_enabled: false,
        calibration: CalibrationData::calibrate(),
        thread_id_alloc: Arc::new(AtomicU64::new(1)),
        registry: test_registry(),
    }
}

/// A future that returns Pending on the first poll, Ready(()) on the second.
struct PendingOnce {
    polled: bool,
}

impl PendingOnce {
    fn new() -> Self {
        Self { polled: false }
    }
}

impl Future for PendingOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.polled {
            Poll::Ready(())
        } else {
            this.polled = true;
            Poll::Pending
        }
    }
}

/// A future that records allocs on each poll. Pending first, Ready second.
struct AllocPerPoll {
    poll_count: u32,
    alloc_size: usize,
}

impl AllocPerPoll {
    fn new(alloc_size: usize) -> Self {
        Self {
            poll_count: 0,
            alloc_size,
        }
    }
}

impl Future for AllocPerPoll {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        record_alloc(this.alloc_size as u64);
        this.poll_count += 1;
        if this.poll_count >= 2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

// INVARIANT TEST: completed PianoFuture emits exactly one measurement.
#[test]
fn completion_emits_measurement() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 42),
            std::future::ready(()),
        );
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let result = pinned.poll(&mut cx);
        assert!(result.is_ready());

        // Must drop before draining -- drop checks emitted flag
        drop(pf);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "should emit exactly one measurement");
        let m = &drained[0];
        assert_eq!(m.span_id, 1);
        assert_eq!(m.parent_span_id, 0);
        assert_eq!(m.name_id, 42);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: never-polled future emits no measurement.
#[test]
fn never_polled_emits_nothing() {
    std::thread::spawn(|| {
        {
            let _pf = PianoFuture::new(
                test_state(1, 0, 0),
                std::future::ready(()),
            );
            // dropped without polling
        }

        let drained = drain_thread_buffer();
        assert!(drained.is_empty(), "never-polled future must emit nothing");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cancelled future (polled but not completed) emits measurement.
#[test]
fn cancellation_emits_measurement() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        {
            let mut pf = PianoFuture::new(test_state(1, 0, 10), PendingOnce::new());
            let pinned = unsafe { Pin::new_unchecked(&mut pf) };
            let result = pinned.poll(&mut cx);
            assert!(result.is_pending(), "first poll should be Pending");
            // pf dropped here -- cancellation
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "cancelled future should emit measurement");
        let m = &drained[0];
        assert_eq!(m.span_id, 1);
        assert_eq!(m.name_id, 10);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: wall time starts on first poll, not construction.
#[test]
fn wall_time_starts_on_first_poll() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 0),
            std::future::ready(()),
        );
        // Gap between construction and polling -- should not count
        std::hint::black_box(vec![0u8; 1024]);

        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(m.start_ns > 0, "start_ns should be set on first poll");
        assert!(m.end_ns >= m.start_ns, "end_ns must be >= start_ns");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: alloc deltas accumulated across multiple polls.
#[test]
fn alloc_deltas_accumulated_across_polls() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(test_state(1, 0, 0), AllocPerPoll::new(100));

        // First poll: records 100 bytes, returns Pending
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let result = pinned.poll(&mut cx);
        assert!(result.is_pending());

        // Second poll: records 100 more bytes, returns Ready
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let result = pinned.poll(&mut cx);
        assert!(result.is_ready());

        drop(pf);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 2, "should accumulate allocs from both polls");
        assert_eq!(m.alloc_bytes, 200, "should accumulate bytes from both polls");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: bookkeeping allocs excluded from user counts.
#[test]
fn bookkeeping_allocs_excluded() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 0),
            std::future::ready(()), // no user allocs
        );
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 0, "bookkeeping allocs must be excluded");
        assert_eq!(m.alloc_bytes, 0, "bookkeeping bytes must be excluded");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cpu_start_ns and cpu_end_ns are 0 (not yet supported).
#[test]
fn cpu_time_is_zero() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 0),
            std::future::ready(()),
        );
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.cpu_start_ns, 0);
        assert_eq!(m.cpu_end_ns, 0);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: alloc deltas scoped to poll lifetime, not construction.
#[test]
fn alloc_deltas_scoped_to_polls() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Allocs before construction -- should NOT be counted
        record_alloc(999);

        let mut pf = PianoFuture::new(test_state(1, 0, 0), async {
            record_alloc(50);
        });

        // Allocs between construction and poll -- should NOT be counted
        record_alloc(888);

        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        // Allocs after -- should NOT be counted
        record_alloc(777);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert_eq!(m.alloc_count, 1, "only allocs during poll count");
        assert_eq!(m.alloc_bytes, 50, "only bytes during poll count");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: PianoFuture is transparent -- returns inner value.
#[test]
fn transparent_return_value() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 0),
            std::future::ready(42u64),
        );
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        match pinned.poll(&mut cx) {
            Poll::Ready(val) => assert_eq!(val, 42, "must return inner value"),
            Poll::Pending => panic!("ready future should not be pending"),
        }

        drop(pf);
        drain_thread_buffer(); // cleanup
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: nested PianoFutures produce correct measurements.
#[test]
fn nested_piano_futures() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Outer wraps a future that contains an inner PianoFuture
        let mut outer = PianoFuture::new(
            test_state(1, 0, 10),
            async {
                let mut inner = PianoFuture::new(
                    test_state(2, 1, 20),
                    async {
                        record_alloc(50);
                    },
                );
                // SAFETY: inner lives on the stack frame of this async block,
                // which is pinned by outer. We poll it once and it completes.
                let pinned = unsafe { Pin::new_unchecked(&mut inner) };
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                assert!(pinned.poll(&mut cx).is_ready());
                drop(inner);
            },
        );

        let pinned = unsafe { Pin::new_unchecked(&mut outer) };
        let result = pinned.poll(&mut cx);
        assert!(result.is_ready());
        drop(outer);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2, "nested futures should produce 2 measurements");

        // Inner emits first (completes inside outer's poll)
        let inner_m = &drained[0];
        assert_eq!(inner_m.span_id, 2);
        assert_eq!(inner_m.parent_span_id, 1);
        assert_eq!(inner_m.name_id, 20);
        assert_eq!(inner_m.alloc_count, 1, "inner should capture its alloc");
        assert_eq!(inner_m.alloc_bytes, 50);

        // Outer emits second (inclusive -- captures inner's allocs too)
        let outer_m = &drained[1];
        assert_eq!(outer_m.span_id, 1);
        assert_eq!(outer_m.parent_span_id, 0);
        assert_eq!(outer_m.name_id, 10);
        // Outer's alloc count is inclusive (inner's allocs happened during outer's poll)
        assert!(
            outer_m.alloc_count >= inner_m.alloc_count,
            "outer alloc count ({}) must be >= inner ({})",
            outer_m.alloc_count,
            inner_m.alloc_count
        );
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Guard active during PianoFuture poll -- cross-type
// alloc deltas are independent and correct (inclusive nesting).
#[test]
fn guard_active_during_piano_future_poll() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let cal = CalibrationData::calibrate();
        let tid = AtomicU64::new(1);
        let reg = test_registry();

        // Create a sync Guard (outer)
        let mut guard = Guard::new_uninstrumented(1, 0, 10, false, cal, &tid, Arc::clone(&reg));
        guard.stamp();

        // Record some allocs attributed to the Guard's window
        record_alloc(100);

        // Poll a PianoFuture inside the Guard's lifetime
        let mut pf = PianoFuture::new(test_state(2, 1, 20), async {
            record_alloc(50);
        });
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        // Drop the Guard
        drop(guard);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2, "should produce 2 measurements");

        // PianoFuture emits first (dropped first)
        let pf_m = &drained[0];
        let guard_m = &drained[1];

        assert_eq!(pf_m.span_id, 2);
        assert_eq!(pf_m.name_id, 20);
        assert_eq!(pf_m.alloc_count, 1, "PianoFuture should capture its own alloc");
        assert_eq!(pf_m.alloc_bytes, 50);

        assert_eq!(guard_m.span_id, 1);
        assert_eq!(guard_m.name_id, 10);
        // Guard's delta is inclusive: 100 bytes (own) + 50 bytes (PianoFuture's)
        assert!(
            guard_m.alloc_bytes >= 150,
            "Guard alloc_bytes ({}) must include PianoFuture's allocs (>= 150)",
            guard_m.alloc_bytes
        );
    })
    .join()
    .expect("test thread panicked");
}

/// A future that panics on the first poll.
struct PanickingFuture;

impl Future for PanickingFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        panic!("intentional panic in poll");
    }
}

// INVARIANT TEST: PianoFuture wrapping a panicking inner future emits
// a measurement via Drop during unwinding. Best-effort data.
#[test]
fn panicking_inner_future_emits_measurement() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Record some allocs on a prior poll (PendingOnce then panic)
        // Use a simpler approach: just panic on first poll.
        let mut pf = PianoFuture::new(test_state(1, 0, 42), PanickingFuture);

        // catch_unwind requires the future to be UnwindSafe.
        // PianoFuture<PanickingFuture> is not AssertUnwindSafe by default,
        // but we can wrap the poll call.
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let pinned = unsafe { Pin::new_unchecked(&mut pf) };
            let _ = pinned.poll(&mut cx);
        }));

        assert!(result.is_err(), "poll should have panicked");

        // Drop the PianoFuture -- this should emit a measurement
        // because start_ns was set (first poll ran pre-poll bookkeeping
        // before the inner future panicked) and emitted is false.
        drop(pf);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1, "panicking future should emit one measurement via Drop");

        let m = &drained[0];
        assert_eq!(m.span_id, 1);
        assert_eq!(m.name_id, 42);
        assert!(m.start_ns > 0, "start_ns should be set from pre-poll bookkeeping");
        assert!(m.end_ns >= m.start_ns, "end_ns must be >= start_ns");
        // alloc_count is 0 for the panicking poll (post-poll accumulation
        // was skipped). This is the documented best-effort behavior.
        assert_eq!(m.alloc_count, 0, "alloc data is zero (post-poll skipped by panic)");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: PianoFuture captures thread_id on first poll,
// not at emission time.
#[test]
fn thread_id_captured_on_first_poll() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut pf = PianoFuture::new(
            test_state(1, 0, 0),
            std::future::ready(()),
        );
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        let drained = drain_thread_buffer();
        let m = &drained[0];
        assert!(m.thread_id > 0, "thread_id must be set from first poll");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: PianoFuture accumulates CPU time across polls.
#[cfg(unix)]
#[test]
fn piano_future_accumulates_cpu_time() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let state = PianoFutureState {
            span_id: 1,
            parent_span_id: 0,
            name_id: 10,
            cpu_time_enabled: true,
            calibration: CalibrationData::calibrate(),
            thread_id_alloc: Arc::new(AtomicU64::new(1)),
            registry: test_registry(),
        };

        // Use PendingOnce: first poll returns Pending, second returns Ready
        let mut pf = PianoFuture::new(state, PendingOnce::new());
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        assert!(pinned.poll(&mut cx).is_pending());

        // Do some CPU work between polls
        let mut sum: u64 = 0;
        for i in 0..10_000u64 {
            sum = sum.wrapping_add(i);
        }
        std::hint::black_box(sum);

        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        assert!(pinned.poll(&mut cx).is_ready());

        drop(pf);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1);
        let m = &drained[0];
        // cpu_start_ns is always 0 for PianoFuture (accumulated into cpu_end_ns)
        assert_eq!(m.cpu_start_ns, 0, "PianoFuture cpu_start_ns is always 0");
        assert!(m.cpu_end_ns > 0, "accumulated CPU time must be nonzero");
    })
    .join()
    .expect("test thread panicked");
}
