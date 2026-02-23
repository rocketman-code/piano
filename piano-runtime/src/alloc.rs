use std::alloc::{GlobalAlloc, Layout};
use std::time::Instant;

use crate::collector::STACK;

// ---------------------------------------------------------------------------
// Destructor-free thread-local state for the global allocator.
//
// Rust 1.88 forbids global allocators from using `thread_local!` because the
// macro registers TLS destructors. We use raw POSIX TLS (`pthread_key_create`
// with a NULL destructor) to store a per-thread tracking state that is safe to
// access from within `GlobalAlloc::alloc` / `dealloc`.
//
// State values (stored as `*mut u8` cast from usize):
//   0  thread has not called `enter()` yet, or TLS key not ready — skip tracking
//   1  thread is active (STACK initialized), not currently inside tracking code
//   2  currently inside tracking code — re-entrancy guard
// ---------------------------------------------------------------------------
#[cfg(unix)]
mod raw_tls {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // `pthread_key_t` is `unsigned long` on macOS, `unsigned int` on Linux/glibc.
    // Use a type alias so the extern declarations match the platform ABI.
    #[cfg(target_os = "macos")]
    type PthreadKey = std::ffi::c_ulong;
    #[cfg(not(target_os = "macos"))]
    type PthreadKey = std::ffi::c_uint;

    unsafe extern "C" {
        fn pthread_key_create(
            key: *mut PthreadKey,
            destructor: Option<unsafe extern "C" fn(*mut u8)>,
        ) -> std::ffi::c_int;
        fn pthread_getspecific(key: PthreadKey) -> *mut u8;
        fn pthread_setspecific(key: PthreadKey, value: *mut u8) -> std::ffi::c_int;
    }

    /// Sentinel value: no key has been created yet.
    const KEY_UNINITIALIZED: usize = usize::MAX;
    /// Sentinel value: a thread is currently creating the key.
    const KEY_CREATING: usize = usize::MAX - 1;

    /// Stores the pthread key as a `usize`. `PthreadKey` is `c_ulong` (8 bytes)
    /// on macOS and `c_uint` (4 bytes) on Linux, so `usize` can hold either.
    /// `KEY_UNINITIALIZED` means no key yet; `KEY_CREATING` means init in progress.
    static KEY: AtomicUsize = AtomicUsize::new(KEY_UNINITIALIZED);
    static KEY_CREATING_FLAG: AtomicBool = AtomicBool::new(false);

    const STATE_INACTIVE: usize = 0;
    const STATE_READY: usize = 1;
    const STATE_TRACKING: usize = 2;

    fn ensure_key() -> Option<PthreadKey> {
        let k = KEY.load(Ordering::Acquire);
        if k != KEY_UNINITIALIZED && k != KEY_CREATING {
            return Some(k as PthreadKey);
        }
        // One-time initialization via compare-and-swap spinlock.
        if KEY_CREATING_FLAG
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let mut raw_key: PthreadKey = 0;
            // SAFETY: passing NULL destructor — no TLS destructor registered.
            let rc = unsafe { pthread_key_create(&mut raw_key, None) };
            if rc != 0 {
                KEY_CREATING_FLAG.store(false, Ordering::Release);
                return None;
            }
            KEY.store(raw_key as usize, Ordering::Release);
        } else {
            // Another thread is creating the key; spin until ready.
            loop {
                let v = KEY.load(Ordering::Acquire);
                if v != KEY_UNINITIALIZED && v != KEY_CREATING {
                    break;
                }
                std::hint::spin_loop();
            }
        }
        let stored = KEY.load(Ordering::Acquire);
        Some(stored as PthreadKey)
    }

    /// Mark the current thread as active (STACK is initialized).
    /// Called from `enter()` in the collector module.
    pub(crate) fn mark_thread_active() {
        let Some(key) = ensure_key() else { return };
        let current = unsafe { pthread_getspecific(key) } as usize;
        if current == STATE_INACTIVE {
            unsafe { pthread_setspecific(key, STATE_READY as *mut u8) };
        }
    }

    /// Try to acquire the tracking re-entrancy guard.
    /// Returns `true` if the caller should proceed with tracking.
    pub(crate) fn acquire_tracking() -> bool {
        let Some(key) = ensure_key() else {
            return false;
        };
        let current = unsafe { pthread_getspecific(key) } as usize;
        if current != STATE_READY {
            return false;
        }
        unsafe { pthread_setspecific(key, STATE_TRACKING as *mut u8) };
        true
    }

    /// Release the tracking re-entrancy guard.
    pub(crate) fn release_tracking() {
        let Some(key) = ensure_key() else { return };
        unsafe { pthread_setspecific(key, STATE_READY as *mut u8) };
    }
}

// Fallback for non-unix platforms: tracking is disabled (graceful degradation).
#[cfg(not(unix))]
mod raw_tls {
    pub(crate) fn mark_thread_active() {}
    pub(crate) fn acquire_tracking() -> bool {
        false
    }
    pub(crate) fn release_tracking() {}
}

pub(crate) use raw_tls::mark_thread_active;

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

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old_size = layout.size() as u64;
        let result = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !result.is_null() {
            track_dealloc(old_size);
            track_alloc(new_size as u64);
        }
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        track_alloc(layout.size() as u64);
        ptr
    }
}

fn track_alloc(bytes: u64) {
    if !raw_tls::acquire_tracking() {
        return;
    }

    let t0 = Instant::now();
    let _ = STACK.try_with(|stack| {
        if let Ok(mut s) = stack.try_borrow_mut()
            && let Some(top) = s.last_mut()
        {
            top.alloc_count += 1;
            top.alloc_bytes += bytes;
            top.overhead_ns += t0.elapsed().as_nanos();
        }
    });

    raw_tls::release_tracking();
}

fn track_dealloc(bytes: u64) {
    if !raw_tls::acquire_tracking() {
        return;
    }

    let t0 = Instant::now();
    let _ = STACK.try_with(|stack| {
        if let Ok(mut s) = stack.try_borrow_mut()
            && let Some(top) = s.last_mut()
        {
            top.free_count += 1;
            top.free_bytes += bytes;
            top.overhead_ns += t0.elapsed().as_nanos();
        }
    });

    raw_tls::release_tracking();
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
        // Allow 30% tolerance for measurement noise
        let ratio = rec.self_ns as f64 / baseline_ns as f64;
        assert!(
            (0.7..1.3).contains(&ratio),
            "self_ns ({}) should be close to baseline ({}) but ratio was {:.2}",
            rec.self_ns,
            baseline_ns,
            ratio
        );
    }
}
