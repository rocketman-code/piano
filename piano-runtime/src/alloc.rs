#![allow(unsafe_code)]

//! Allocation tracking -- per-thread counters, RAII reentrancy guard,
//! and the PianoAllocator that intercepts heap allocations.
//!
//! Invariants:
//! - Reentrancy guard is always correctly paired (enter/exit).
//!   Enforcement: RAII ReentrancyGuard type. No public enter/exit
//!   functions. The guard increments on creation, decrements on drop.
//!   Unpaired exit is structurally impossible.
//! - ReentrancyGuard is !Send (TLS counter is per-thread; moving the
//!   guard to another thread would decrement the wrong counter).
//!   Enforcement: PhantomData<*const ()>.
//! - Alloc counters are monotonically increasing, never reset.
//!   Enforcement: record_alloc only adds. No reset/clear functions.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::marker::PhantomData;

thread_local! {
    static ALLOC_COUNT: Cell<u64> = const { Cell::new(0) };
    static ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
    static FREE_COUNT: Cell<u64> = const { Cell::new(0) };
    static FREE_BYTES: Cell<u64> = const { Cell::new(0) };
    static REENTRANCY: Cell<u32> = const { Cell::new(0) };
}

/// Snapshot the current thread's allocation counters.
/// Returns (alloc_count, alloc_bytes, free_count, free_bytes).
pub fn snapshot_alloc_counters() -> (u64, u64, u64, u64) {
    ALLOC_COUNT.with(|ac| {
        ALLOC_BYTES.with(|ab| {
            FREE_COUNT.with(|fc| {
                FREE_BYTES.with(|fb| (ac.get(), ab.get(), fc.get(), fb.get()))
            })
        })
    })
}

/// Record an allocation on the current thread.
/// Called by PianoAllocator and directly in tests.
/// Skipped when reentrancy > 0 (profiler-internal allocs excluded).
pub fn record_alloc(size: u64) {
    let _ = REENTRANCY.try_with(|r| {
        if r.get() == 0 {
            let _ = ALLOC_COUNT.try_with(|c| c.set(c.get() + 1));
            let _ = ALLOC_BYTES.try_with(|b| b.set(b.get() + size));
        }
    });
}

/// Record a deallocation on the current thread.
/// Skipped when reentrancy > 0.
fn record_dealloc(size: u64) {
    let _ = REENTRANCY.try_with(|r| {
        if r.get() == 0 {
            let _ = FREE_COUNT.try_with(|c| c.set(c.get() + 1));
            let _ = FREE_BYTES.try_with(|b| b.set(b.get() + size));
        }
    });
}

/// Check if currently inside a reentrancy-guarded section.
pub fn is_reentrant() -> bool {
    REENTRANCY.with(|r| r.get() > 0)
}

/// RAII guard that prevents allocation tracking during profiler
/// bookkeeping. Increments a per-thread counter on creation,
/// decrements on drop. While counter > 0, record_alloc/record_dealloc
/// are no-ops.
///
/// !Send: TLS counter is per-thread; moving guard across threads
/// would decrement the wrong counter.
pub struct ReentrancyGuard {
    _not_send: PhantomData<*const ()>,
}

impl ReentrancyGuard {
    pub fn enter() -> Self {
        let _ = REENTRANCY.try_with(|r| r.set(r.get() + 1));
        Self {
            _not_send: PhantomData,
        }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        let _ = REENTRANCY.try_with(|r| {
            let prev = r.get();
            debug_assert!(prev > 0, "ReentrancyGuard dropped without matching enter");
            r.set(prev.saturating_sub(1));
        });
    }
}

/// A global allocator wrapper that tracks allocation counts and bytes.
///
/// Wraps any inner `GlobalAlloc`. Uses per-thread counters with
/// reentrancy protection so profiler-internal allocations don't
/// contaminate user counts.
///
/// The struct bound is on `GlobalAlloc` impls only (not the struct itself)
/// so that `const fn new` compiles on Rust < 1.61 where trait bounds on
/// const fn parameters are unstable.
pub struct PianoAllocator<A> {
    inner: A,
}

impl<A> PianoAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: PianoAllocator delegates all allocation operations to the inner
// allocator. The only addition is per-thread counter updates via record_alloc
// and record_dealloc, which are thread-local Cell operations (no shared
// mutable state, no UB). Failed allocations (null ptr) are not counted.
unsafe impl<A: GlobalAlloc> GlobalAlloc for PianoAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: layout validity is the caller's responsibility (GlobalAlloc contract).
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr was allocated by self.inner with the same layout (GlobalAlloc contract).
        unsafe { self.inner.dealloc(ptr, layout) };
        record_dealloc(layout.size() as u64);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: ptr was allocated by self.inner with layout, new_size >= 1 (GlobalAlloc contract).
        let result = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !result.is_null() {
            record_dealloc(layout.size() as u64);
            record_alloc(new_size as u64);
        }
        result
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: layout validity is the caller's responsibility (GlobalAlloc contract).
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size() as u64);
        }
        ptr
    }
}
