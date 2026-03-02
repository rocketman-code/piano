use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;

/// Allocation counters accumulated in the allocator hot path.
/// All fields are plain integers -> `Copy` -> `Cell<AllocSnapshot>` has no
/// destructor -> safe for use in global allocator TLS on all Rust versions.
#[derive(Clone, Copy, Default)]
pub(crate) struct AllocSnapshot {
    pub(crate) alloc_count: u64,
    pub(crate) alloc_bytes: u64,
    pub(crate) free_count: u64,
    pub(crate) free_bytes: u64,
}

thread_local! {
    /// Destructor-free counters that the allocator hot path increments.
    /// `enter()` saves and zeroes this; `Guard::drop()` reads and restores.
    pub(crate) static ALLOC_COUNTERS: Cell<AllocSnapshot> = Cell::new(AllocSnapshot::new());
}

impl AllocSnapshot {
    pub(crate) const fn new() -> Self {
        Self {
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }
    }
}

/// Accumulates allocation data across async thread migrations.
///
/// Declared after the Guard in instrumented async functions so that Rust's
/// reverse declaration drop order ensures this drops first, writing the
/// accumulated total to ALLOC_COUNTERS before the Guard reads it.
pub struct AllocAccumulator {
    cumulative: AllocSnapshot,
}

impl Default for AllocAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AllocAccumulator {
    #[inline]
    pub fn new() -> Self {
        Self {
            cumulative: AllocSnapshot::new(),
        }
    }

    /// Called BEFORE .await: captures this thread segment's allocs into
    /// cumulative and zeros counters for the next segment.
    #[inline]
    pub fn save(&mut self) {
        let current = ALLOC_COUNTERS.with(|cell| {
            let snap = cell.get();
            cell.set(AllocSnapshot::new());
            snap
        });
        self.cumulative.alloc_count += current.alloc_count;
        self.cumulative.alloc_bytes += current.alloc_bytes;
        self.cumulative.free_count += current.free_count;
        self.cumulative.free_bytes += current.free_bytes;
    }

    /// Called AFTER .await + check(): zeros counters on the (possibly new)
    /// thread so allocations from this point go into a fresh segment.
    #[inline]
    pub fn resume(&mut self) {
        ALLOC_COUNTERS.with(|cell| {
            cell.set(AllocSnapshot::new());
        });
    }
}

impl Drop for AllocAccumulator {
    fn drop(&mut self) {
        let _ = ALLOC_COUNTERS.try_with(|cell| {
            let final_seg = cell.get();
            cell.set(AllocSnapshot {
                alloc_count: self.cumulative.alloc_count + final_seg.alloc_count,
                alloc_bytes: self.cumulative.alloc_bytes + final_seg.alloc_bytes,
                free_count: self.cumulative.free_count + final_seg.free_count,
                free_bytes: self.cumulative.free_bytes + final_seg.free_bytes,
            });
        });
    }
}

/// A global allocator wrapper that tracks allocation counts and bytes
/// per instrumented function scope, with zero timing distortion.
///
/// Wraps any inner `GlobalAlloc`. Uses a destructor-free `Cell<AllocSnapshot>`
/// for thread-local bookkeeping, which is safe on all Rust versions
/// (including < 1.93.1 where TLS with destructors is forbidden for
/// global allocators).
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

unsafe impl<A: GlobalAlloc> GlobalAlloc for PianoAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            track_alloc(layout.size() as u64);
        }
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
        if !ptr.is_null() {
            track_alloc(layout.size() as u64);
        }
        ptr
    }
}

#[inline(always)]
fn track_alloc(bytes: u64) {
    // Cell::get/set are plain memory reads/writes -- no allocation, no
    // re-entrancy risk, no destructor. Safe from the global allocator.
    let _ = ALLOC_COUNTERS.try_with(|cell| {
        let mut snap = cell.get();
        snap.alloc_count += 1;
        snap.alloc_bytes += bytes;
        cell.set(snap);
    });
}

#[inline(always)]
fn track_dealloc(bytes: u64) {
    let _ = ALLOC_COUNTERS.try_with(|cell| {
        let mut snap = cell.get();
        snap.free_count += 1;
        snap.free_bytes += bytes;
        cell.set(snap);
    });
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
    fn alloc_tracking_nested_scopes() {
        reset();
        {
            let _outer = enter("outer_alloc");
            track_alloc(100);
            {
                let _inner = enter("inner_alloc");
                track_alloc(200);
                track_dealloc(50);
            }
            track_alloc(300);
            track_dealloc(75);
        }
        let invocations = collect_invocations();
        let outer = invocations
            .iter()
            .find(|r| r.name == "outer_alloc")
            .unwrap();
        let inner = invocations
            .iter()
            .find(|r| r.name == "inner_alloc")
            .unwrap();

        // Inner scope should only see its own allocations
        assert_eq!(inner.alloc_count, 1, "inner alloc_count");
        assert_eq!(inner.alloc_bytes, 200, "inner alloc_bytes");
        assert_eq!(inner.free_count, 1, "inner free_count");
        assert_eq!(inner.free_bytes, 50, "inner free_bytes");

        // Outer scope should see its own allocations (before + after inner)
        assert_eq!(outer.alloc_count, 2, "outer alloc_count");
        assert_eq!(outer.alloc_bytes, 400, "outer alloc_bytes");
        assert_eq!(outer.free_count, 1, "outer free_count");
        assert_eq!(outer.free_bytes, 75, "outer free_bytes");
    }

    #[test]
    fn alloc_accumulator_save_resume_drop_cycle() {
        ALLOC_COUNTERS.with(|cell| {
            cell.set(AllocSnapshot::new());
        });

        let total = {
            let mut acc = AllocAccumulator::new();

            // Segment 1: 5 allocs, 1000 bytes.
            ALLOC_COUNTERS.with(|cell| {
                cell.set(AllocSnapshot {
                    alloc_count: 5,
                    alloc_bytes: 1000,
                    free_count: 1,
                    free_bytes: 200,
                });
            });
            acc.save();

            let after = ALLOC_COUNTERS.with(|cell| cell.get());
            assert_eq!(after.alloc_count, 0);

            acc.resume();

            // Segment 2: 3 allocs, 2000 bytes.
            ALLOC_COUNTERS.with(|cell| {
                cell.set(AllocSnapshot {
                    alloc_count: 3,
                    alloc_bytes: 2000,
                    free_count: 2,
                    free_bytes: 500,
                });
            });

            drop(acc);
            ALLOC_COUNTERS.with(|cell| cell.get())
        };

        assert_eq!(total.alloc_count, 8, "5 + 3");
        assert_eq!(total.alloc_bytes, 3000, "1000 + 2000");
        assert_eq!(total.free_count, 3, "1 + 2");
        assert_eq!(total.free_bytes, 700, "200 + 500");
    }

    /// An allocator that always returns null, simulating allocation failure.
    struct FailingAlloc;

    unsafe impl GlobalAlloc for FailingAlloc {
        unsafe fn alloc(&self, _layout: Layout) -> *mut u8 {
            std::ptr::null_mut()
        }

        unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {}

        unsafe fn realloc(&self, _ptr: *mut u8, _layout: Layout, _new_size: usize) -> *mut u8 {
            std::ptr::null_mut()
        }

        unsafe fn alloc_zeroed(&self, _layout: Layout) -> *mut u8 {
            std::ptr::null_mut()
        }
    }

    #[test]
    fn failed_alloc_not_counted() {
        ALLOC_COUNTERS.with(|cell| cell.set(AllocSnapshot::new()));
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(64, 8).unwrap();
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(ptr.is_null());
        let snap = ALLOC_COUNTERS.with(|cell| cell.get());
        assert_eq!(snap.alloc_count, 0, "failed alloc should not be counted");
        assert_eq!(snap.alloc_bytes, 0, "failed alloc bytes should be zero");
    }

    #[test]
    fn failed_alloc_zeroed_not_counted() {
        ALLOC_COUNTERS.with(|cell| cell.set(AllocSnapshot::new()));
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(128, 8).unwrap();
        let ptr = unsafe { allocator.alloc_zeroed(layout) };
        assert!(ptr.is_null());
        let snap = ALLOC_COUNTERS.with(|cell| cell.get());
        assert_eq!(
            snap.alloc_count, 0,
            "failed alloc_zeroed should not be counted"
        );
        assert_eq!(
            snap.alloc_bytes, 0,
            "failed alloc_zeroed bytes should be zero"
        );
    }

    #[test]
    fn failed_realloc_not_counted() {
        ALLOC_COUNTERS.with(|cell| cell.set(AllocSnapshot::new()));
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(64, 8).unwrap();
        let ptr = unsafe { allocator.realloc(std::ptr::null_mut(), layout, 128) };
        assert!(ptr.is_null());
        let snap = ALLOC_COUNTERS.with(|cell| cell.get());
        assert_eq!(snap.alloc_count, 0, "failed realloc should not be counted");
        assert_eq!(
            snap.free_count, 0,
            "failed realloc should not count dealloc"
        );
    }

    #[test]
    fn alloc_count_holds_values_above_u32_max() {
        reset();
        let large: u64 = u32::MAX as u64 + 100;
        ALLOC_COUNTERS.with(|cell| {
            cell.set(AllocSnapshot {
                alloc_count: large,
                alloc_bytes: 0,
                free_count: large,
                free_bytes: 0,
            });
        });
        {
            let _g = enter("large_count");
            // Simulate one more allocation inside the scope
            track_alloc(64);
            track_dealloc(32);
        }
        let invocations = collect_invocations();
        let rec = invocations
            .iter()
            .find(|r| r.name == "large_count")
            .unwrap();
        assert_eq!(rec.alloc_count, 1, "should see only in-scope allocation");
        assert_eq!(rec.free_count, 1, "should see only in-scope deallocation");

        // Verify the TLS counter was restored to the large value
        let restored = ALLOC_COUNTERS.with(|cell| cell.get());
        assert_eq!(
            restored.alloc_count, large,
            "alloc_count should preserve values above u32::MAX"
        );
        assert_eq!(
            restored.free_count, large,
            "free_count should preserve values above u32::MAX"
        );
    }
}
