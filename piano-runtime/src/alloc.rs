#![allow(unsafe_code)]

//! Allocation tracking -- per-thread counters, RAII reentrancy guard,
//! and the PianoAllocator that intercepts heap allocations.
//!
//! Counts reflect the compiled binary's runtime behavior, not source-level
//! intent. The GlobalAlloc trait docs state "you must not rely on
//! allocations actually happening" -- LLVM can eliminate calls before
//! they reach the allocator. Counts may be lower than source-level
//! allocations suggest.
//!
//! `thread_local!` is safe inside GlobalAlloc: the trait methods receive
//! `&self` (shared reference), and `thread_local!` provides per-thread
//! isolation, so no synchronization issues arise.
//!
//! Invariants:
//! - Reentrancy guard is always correctly paired (enter/exit).
//!   Enforcement: RAII ReentrancyGuard type. No public enter/exit
//!   functions. The guard increments on creation, decrements on drop.
//!   Unpaired exit is structurally impossible.
//! - ReentrancyGuard is !Send (TLS counter is per-thread; moving the
//!   guard to another thread would decrement the wrong counter).
//!   Enforcement: `PhantomData<*const ()>`.
//! - Alloc counters are monotonically increasing, never reset.
//!   Enforcement: record_alloc only adds. No reset/clear functions.
//! - Allocator-observable profiler bookkeeping requires a proof token.
//!   Enforcement: ProfilerBookkeeping owns a ReentrancyGuard, and APIs that
//!   may allocate during bookkeeping require a live reference to it.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::marker::PhantomData;

pub use crate::generated::piano_runtime::alloc_delta::AllocDelta;
pub use crate::generated::piano_runtime::alloc_snapshot::AllocSnapshot;

/// Snapshot of per-thread allocation counters. Copy ensures no TLS destructor
/// (global allocator TLS with destructors is forbidden on older Rust versions).
impl Copy for AllocSnapshot {}

impl AllocSnapshot {
    #[cfg(not(feature = "_test_internals"))]
    pub(crate) fn from_counts(
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    ) -> Self {
        crate::generated::piano_runtime::alloc_snapshot::ops::from_counts(
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        )
    }

    #[cfg(feature = "_test_internals")]
    pub fn from_counts(
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    ) -> Self {
        crate::generated::piano_runtime::alloc_snapshot::ops::from_counts(
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        )
    }

    #[cfg(not(feature = "_test_internals"))]
    pub(crate) fn zero() -> Self {
        Self::from_counts(0, 0, 0, 0)
    }

    #[cfg(feature = "_test_internals")]
    pub fn zero() -> Self {
        Self::from_counts(0, 0, 0, 0)
    }

    pub fn delta_since(&self, start: &AllocSnapshot) -> AllocDelta {
        AllocDelta::from_counts(
            self.alloc_count().saturating_sub(start.alloc_count()),
            self.alloc_bytes().saturating_sub(start.alloc_bytes()),
            self.free_count().saturating_sub(start.free_count()),
            self.free_bytes().saturating_sub(start.free_bytes()),
        )
    }
}

/// Delta of allocation counters between two points in time.
impl Copy for AllocDelta {}

impl AllocDelta {
    #[cfg(not(feature = "_test_internals"))]
    pub(crate) fn from_counts(
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    ) -> Self {
        crate::generated::piano_runtime::alloc_delta::ops::from_counts(
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        )
    }

    #[cfg(feature = "_test_internals")]
    pub fn from_counts(
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    ) -> Self {
        crate::generated::piano_runtime::alloc_delta::ops::from_counts(
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        )
    }

    #[cfg(not(feature = "_test_internals"))]
    pub(crate) fn zero() -> Self {
        Self::from_counts(0, 0, 0, 0)
    }

    #[cfg(feature = "_test_internals")]
    pub fn zero() -> Self {
        Self::from_counts(0, 0, 0, 0)
    }
}

impl core::ops::AddAssign for AllocDelta {
    fn add_assign(&mut self, rhs: Self) {
        *self = AllocDelta::from_counts(
            self.alloc_count() + rhs.alloc_count(),
            self.alloc_bytes() + rhs.alloc_bytes(),
            self.free_count() + rhs.free_count(),
            self.free_bytes() + rhs.free_bytes(),
        );
    }
}

thread_local! {
    static ALLOC_COUNTERS: Cell<AllocSnapshot> = Cell::new(AllocSnapshot::zero());
    static REENTRANCY: Cell<u32> = const { Cell::new(0) };
}

/// Snapshot the current thread's allocation counters.
pub fn snapshot_alloc_counters() -> AllocSnapshot {
    ALLOC_COUNTERS
        .try_with(|c| c.get())
        .unwrap_or_else(|_| AllocSnapshot::zero())
}

/// Record an allocation on the current thread.
/// Called by PianoAllocator and directly in tests.
/// Skipped when reentrancy > 0 (profiler-internal allocs excluded).
pub fn record_alloc(size: u64) {
    let _ = REENTRANCY.try_with(|r| {
        if r.get() == 0 {
            let _ = ALLOC_COUNTERS.try_with(|c| {
                let s = c.get();
                c.set(AllocSnapshot::from_counts(
                    s.alloc_count() + 1,
                    s.alloc_bytes() + size,
                    s.free_count(),
                    s.free_bytes(),
                ));
            });
        }
    });
}

/// Record a deallocation on the current thread.
/// Skipped when reentrancy > 0.
fn record_dealloc(size: u64) {
    let _ = REENTRANCY.try_with(|r| {
        if r.get() == 0 {
            let _ = ALLOC_COUNTERS.try_with(|c| {
                let s = c.get();
                c.set(AllocSnapshot::from_counts(
                    s.alloc_count(),
                    s.alloc_bytes(),
                    s.free_count() + 1,
                    s.free_bytes() + size,
                ));
            });
        }
    });
}

/// Check if currently inside a reentrancy-guarded section.
#[cfg(feature = "_test_internals")]
pub fn is_reentrant() -> bool {
    REENTRANCY.try_with(|r| r.get() > 0).unwrap_or(false)
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
            // volatile write: prevents LLVM from reordering the decrement
            // past a compiler_fence. Without this, LLVM proves the Cell write
            // is non-aliasing and freely schedules it after rdtsc, adding
            // ~5 instructions to the measurement window.
            //
            // SAFETY: Cell<u32> is repr(transparent) around UnsafeCell<u32>.
            // We write through its raw pointer, which is what Cell::set does
            // internally. No other thread can access this TLS cell.
            unsafe {
                core::ptr::write_volatile(r.as_ptr(), prev.saturating_sub(1));
            }
        });
    }
}

/// Proof that profiler bookkeeping is running with allocation tracking
/// suspended on the current thread.
///
/// APIs that may allocate while maintaining profiler state should require this
/// token instead of relying on callers to remember a separate reentrancy guard.
pub struct ProfilerBookkeeping {
    _guard: ReentrancyGuard,
}

impl ProfilerBookkeeping {
    pub(crate) fn enter() -> Self {
        Self {
            _guard: ReentrancyGuard::enter(),
        }
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
