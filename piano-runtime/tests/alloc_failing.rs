use piano_runtime::alloc::{snapshot_alloc_counters, PianoAllocator};
use std::alloc::{GlobalAlloc, Layout};

/// An allocator that always returns null, simulating allocation failure.
struct FailingAlloc;

// SAFETY: FailingAlloc returns null from all allocation methods and is
// a no-op for dealloc. This satisfies GlobalAlloc: null return signals
// allocation failure, and the no-op dealloc is valid since no memory
// was ever allocated.
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

// All tests in spawned threads for TLS isolation, using deltas.

#[test]
fn failed_alloc_not_counted() {
    std::thread::spawn(|| {
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(64, 8).unwrap();
        let (count_before, bytes_before, _fc, _fb) = snapshot_alloc_counters();
        // SAFETY: layout is valid (from_size_align succeeded).
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(ptr.is_null());
        let (count_after, bytes_after, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(count_after - count_before, 0, "failed alloc should not be counted");
        assert_eq!(bytes_after - bytes_before, 0, "failed alloc bytes should be zero");
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn failed_alloc_zeroed_not_counted() {
    std::thread::spawn(|| {
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(128, 8).unwrap();
        let (count_before, bytes_before, _fc, _fb) = snapshot_alloc_counters();
        // SAFETY: layout is valid (from_size_align succeeded).
        let ptr = unsafe { allocator.alloc_zeroed(layout) };
        assert!(ptr.is_null());
        let (count_after, bytes_after, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(count_after - count_before, 0, "failed alloc_zeroed should not be counted");
        assert_eq!(bytes_after - bytes_before, 0, "failed alloc_zeroed bytes should be zero");
    })
    .join()
    .expect("test thread panicked");
}

#[test]
fn failed_realloc_not_counted() {
    std::thread::spawn(|| {
        let allocator = PianoAllocator::new(FailingAlloc);
        let layout = Layout::from_size_align(64, 8).unwrap();
        let (count_before, _, _fc, _fb) = snapshot_alloc_counters();
        // SAFETY: layout is valid. ptr is null (FailingAlloc never allocates).
        let ptr = unsafe { allocator.realloc(std::ptr::null_mut(), layout, 128) };
        assert!(ptr.is_null());
        let (count_after, _, _fc, _fb) = snapshot_alloc_counters();
        assert_eq!(count_after - count_before, 0, "failed realloc should not be counted");
    })
    .join()
    .expect("test thread panicked");
}
