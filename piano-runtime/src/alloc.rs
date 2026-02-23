use std::alloc::{GlobalAlloc, Layout};
use std::time::Instant;

use crate::collector::STACK;

/// Thread-local re-entrancy guard to prevent infinite recursion.
/// When the tracking code itself allocates, we must not re-enter tracking.
thread_local! {
    static IN_ALLOC_TRACKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A global allocator wrapper that tracks allocation counts and bytes
/// per instrumented function scope, with zero timing distortion.
///
/// Wraps any inner `GlobalAlloc`. Measures its own bookkeeping overhead
/// and subtracts it from the current function's timing via `overhead_ns`.
pub struct PianoAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> PianoAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for PianoAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        track_alloc(layout.size() as u64);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        track_dealloc(layout.size() as u64);
    }
}

fn track_alloc(bytes: u64) {
    let ok = IN_ALLOC_TRACKING.try_with(|flag| {
        if flag.get() {
            return false;
        }
        flag.set(true);
        true
    });
    if ok != Ok(true) {
        return;
    }

    let t0 = Instant::now();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.alloc_count += 1;
            top.alloc_bytes += bytes;
        }
    });
    let overhead = t0.elapsed().as_nanos();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.overhead_ns += overhead;
        }
    });

    let _ = IN_ALLOC_TRACKING.try_with(|flag| flag.set(false));
}

fn track_dealloc(bytes: u64) {
    let ok = IN_ALLOC_TRACKING.try_with(|flag| {
        if flag.get() {
            return false;
        }
        flag.set(true);
        true
    });
    if ok != Ok(true) {
        return;
    }

    let t0 = Instant::now();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.free_count += 1;
            top.free_bytes += bytes;
        }
    });
    let overhead = t0.elapsed().as_nanos();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.overhead_ns += overhead;
        }
    });

    let _ = IN_ALLOC_TRACKING.try_with(|flag| flag.set(false));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collect_invocations, enter, reset};

    #[test]
    fn track_alloc_updates_stack_entry() {
        reset();
        {
            let _g = enter("alloc_test");
            track_alloc(1024);
            track_alloc(512);
            track_dealloc(256);
        }
        let invocations = collect_invocations();
        let rec = invocations.iter().find(|r| r.name == "alloc_test").unwrap();
        assert_eq!(rec.alloc_count, 2);
        assert_eq!(rec.alloc_bytes, 1536);
        assert_eq!(rec.free_count, 1);
        assert_eq!(rec.free_bytes, 256);
    }

    #[test]
    fn alloc_tracking_does_not_distort_timing() {
        reset();
        // Measure CPU work WITHOUT any alloc tracking
        let baseline_start = std::time::Instant::now();
        crate::collector::burn_cpu(50_000);
        let baseline_ns = baseline_start.elapsed().as_nanos() as u64;

        // Now measure WITH alloc tracking overhead injected
        {
            let _g = enter("timed_fn");
            crate::collector::burn_cpu(50_000);
            // Simulate 10K allocations worth of tracking overhead
            for _ in 0..10_000 {
                track_alloc(64);
            }
        }
        let invocations = collect_invocations();
        let rec = invocations.iter().find(|r| r.name == "timed_fn").unwrap();

        // self_ns should be close to baseline (overhead subtracted)
        // Allow 50% tolerance for measurement noise
        let ratio = rec.self_ns as f64 / baseline_ns as f64;
        assert!(
            (0.5..1.5).contains(&ratio),
            "self_ns ({}) should be close to baseline ({}) but ratio was {:.2}",
            rec.self_ns,
            baseline_ns,
            ratio
        );
    }
}
