//! Deterministic allocation count oracle tests.
//!
//! These tests verify that piano-runtime reports correct allocation counts
//! for known workloads using the real PianoAllocator. A warmup pass
//! initializes thread-local state so that subsequent measurements see
//! zero runtime overhead, making exact counts deterministic.
//!
//! Uses collect_frames() + FrameFnSummary which are always public (unlike
//! collect_invocations() which requires the _test_internals feature).
//!
//! Bug class coverage: #239, #250, #309, #315 (silent alloc data corruption/loss).
#![allow(clippy::incompatible_msrv)]

use piano_runtime::{FrameFnSummary, PianoAllocator};
use serial_test::serial;

#[global_allocator]
static ALLOC: PianoAllocator<std::alloc::System> = PianoAllocator::new(std::alloc::System);

/// Helper: find a named entry across all frames.
fn find_summary(frames: &[Vec<FrameFnSummary>], name: &str) -> FrameFnSummary {
    frames
        .iter()
        .flat_map(|f| f.iter())
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no frame summary for '{name}'"))
        .clone()
}

/// Warm up thread-local state so the first real measurement has no overhead.
/// The initial enter/exit allocates the TLS STACK Vec; after this, the
/// overhead is zero for all subsequent enter/exit calls on this thread.
fn warmup() {
    piano_runtime::reset();
    {
        let _g = piano_runtime::enter("__warmup__");
        piano_runtime::register("__warmup__");
        let v = Vec::<u8>::with_capacity(1);
        std::hint::black_box(v);
    }
    // Discard warmup data.
    let _ = piano_runtime::collect_frames();
    piano_runtime::reset();
}

#[test]
#[serial]
fn single_function_exact_alloc_count() {
    warmup();

    {
        let _g = piano_runtime::enter("alloc_fn");
        piano_runtime::register("alloc_fn");
        for _ in 0..5 {
            let v = Vec::<u8>::with_capacity(64);
            std::hint::black_box(v);
        }
    }

    let frames = piano_runtime::collect_frames();
    let rec = find_summary(&frames, "alloc_fn");
    assert_eq!(
        rec.alloc_count, 5,
        "expected exactly 5 allocations, got {}",
        rec.alloc_count
    );
    assert!(
        rec.alloc_bytes >= 320, // 5 * 64 minimum
        "expected at least 320 bytes, got {}",
        rec.alloc_bytes
    );

    piano_runtime::reset();
}

#[test]
#[serial]
fn nested_functions_alloc_attribution() {
    warmup();

    {
        let _outer = piano_runtime::enter("outer");
        piano_runtime::register("outer");
        piano_runtime::register("inner");

        // Outer allocates 4
        for _ in 0..4 {
            let v = Vec::<u8>::with_capacity(32);
            std::hint::black_box(v);
        }

        {
            let _inner = piano_runtime::enter("inner");
            // Inner allocates 7
            for _ in 0..7 {
                let v = Vec::<u8>::with_capacity(32);
                std::hint::black_box(v);
            }
        }
    }

    let frames = piano_runtime::collect_frames();
    let outer = find_summary(&frames, "outer");
    let inner = find_summary(&frames, "inner");

    // FrameFnSummary tracks self-only allocations per function.
    assert_eq!(
        inner.alloc_count, 7,
        "inner should have exactly 7 self allocs, got {}",
        inner.alloc_count
    );
    // Outer sees exactly its 4 user allocs (no bookkeeping leakage).
    assert_eq!(
        outer.alloc_count, 4,
        "outer should have exactly 4 self allocs, got {}",
        outer.alloc_count
    );

    piano_runtime::reset();
}

#[test]
#[serial]
fn zero_alloc_function_reports_zero() {
    warmup();

    {
        let _g = piano_runtime::enter("no_alloc");
        piano_runtime::register("no_alloc");
        // Pure computation, no heap allocation.
        let mut x = 0u64;
        for i in 0..1000 {
            x = x.wrapping_add(i);
        }
        std::hint::black_box(x);
    }

    let frames = piano_runtime::collect_frames();
    let rec = find_summary(&frames, "no_alloc");
    assert_eq!(
        rec.alloc_count, 0,
        "pure computation should have 0 allocs, got {}",
        rec.alloc_count
    );
    assert_eq!(
        rec.alloc_bytes, 0,
        "pure computation should have 0 alloc bytes, got {}",
        rec.alloc_bytes
    );

    piano_runtime::reset();
}

#[test]
#[serial]
fn async_alloc_count_across_yields() {
    warmup();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    // Warmup the tokio worker threads' TLS state.
    rt.block_on(async {
        for _ in 0..4 {
            tokio::task::spawn(async {
                piano_runtime::reset();
                {
                    let _g = piano_runtime::enter("__async_warmup__");
                    piano_runtime::register("__async_warmup__");
                    let v = Vec::<u8>::with_capacity(1);
                    std::hint::black_box(v);
                }
                let _ = piano_runtime::collect_frames();
                piano_runtime::reset();
            })
            .await
            .unwrap();
        }
    });

    piano_runtime::reset();

    rt.block_on(async {
        piano_runtime::PianoFuture::new(async {
            let _g = piano_runtime::enter("async_alloc");
            piano_runtime::register("async_alloc");

            // 3 allocs before yield
            for _ in 0..3 {
                let v = Vec::<u8>::with_capacity(64);
                std::hint::black_box(v);
            }

            tokio::task::yield_now().await;

            // 2 allocs after yield (possibly on different thread)
            for _ in 0..2 {
                let v = Vec::<u8>::with_capacity(64);
                std::hint::black_box(v);
            }
        })
        .await;
    });

    let frames = piano_runtime::collect_frames();
    let rec = find_summary(&frames, "async_alloc");
    assert_eq!(
        rec.alloc_count, 5,
        "expected 3+2=5 allocs across yield, got {}",
        rec.alloc_count
    );

    piano_runtime::reset();
}

#[test]
#[serial]
fn recursive_pure_math_reports_zero_allocs() {
    warmup();

    fn fib(n: u32) -> u64 {
        let _g = piano_runtime::enter("fib_pure");
        if n <= 1 {
            return n as u64;
        }
        fib(n - 1) + fib(n - 2)
    }

    piano_runtime::register("fib_pure");
    let result = fib(20);
    std::hint::black_box(result);

    let frames = piano_runtime::collect_frames();
    let rec = find_summary(&frames, "fib_pure");
    assert_eq!(
        rec.alloc_count, 0,
        "pure recursive function should have 0 alloc_count, got {}",
        rec.alloc_count
    );
    assert_eq!(
        rec.alloc_bytes, 0,
        "pure recursive function should have 0 alloc_bytes, got {}",
        rec.alloc_bytes
    );
    assert_eq!(
        rec.free_count, 0,
        "pure recursive function should have 0 free_count, got {}",
        rec.free_count
    );
    assert_eq!(
        rec.free_bytes, 0,
        "pure recursive function should have 0 free_bytes, got {}",
        rec.free_bytes
    );

    piano_runtime::reset();
}

/// CPU-bound workload for cpu-time tests.
#[cfg(feature = "cpu-time")]
fn burn_cpu(iterations: u64) {
    let mut buf = [0x42u8; 4096];
    for i in 0..iterations {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[test]
#[serial]
#[cfg(feature = "cpu-time")]
fn nested_cpu_children_ns_accumulation() {
    warmup();

    {
        let _outer = piano_runtime::enter("cpu_parent");
        piano_runtime::register("cpu_parent");
        burn_cpu(1_000);
        {
            let _inner = piano_runtime::enter("cpu_child");
            piano_runtime::register("cpu_child");
            burn_cpu(50_000);
        }
    }

    let frames = piano_runtime::collect_frames();
    let parent = find_summary(&frames, "cpu_parent");
    let child = find_summary(&frames, "cpu_child");

    // Parent's cpu_self_ns must exclude the child's CPU time.
    // If cpu_children_ns accumulation is wrong (e.g. *= instead of +=),
    // parent would claim all the CPU time.
    assert!(
        parent.cpu_self_ns < child.cpu_self_ns,
        "parent cpu_self_ns ({}) should be less than child cpu_self_ns ({})",
        parent.cpu_self_ns,
        child.cpu_self_ns
    );

    piano_runtime::reset();
}
