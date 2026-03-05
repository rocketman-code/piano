//! Recursion test coverage for piano-runtime.
//!
//! Verifies that enter/guard-drop correctly handles recursive call stacks:
//! - Direct recursion: each call pushes a new StackEntry, each drop pops it
//! - children_ms propagates up through recursive callers
//! - No stack overflow at reasonable depth (1000 levels)
//! - Mutual recursion (A calls B calls A) works correctly
#![allow(clippy::incompatible_msrv)]

/// CPU-bound workload: wrapping arithmetic over a buffer.
fn burn_cpu(iterations: u64) {
    let mut buf = [0x42u8; 4096];
    for i in 0..iterations {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

/// Instrumented recursive fibonacci that does real work at each level.
fn fib_instrumented(n: u32) -> u64 {
    let _g = piano_runtime::enter("fib");
    if n <= 1 {
        burn_cpu(1_000);
        return n as u64;
    }
    let result = fib_instrumented(n - 1) + fib_instrumented(n - 2);
    std::hint::black_box(result)
}

/// Simple recursive countdown that does work only at the leaf.
fn countdown(n: u32) {
    let _g = piano_runtime::enter("countdown");
    if n == 0 {
        burn_cpu(5_000);
        return;
    }
    countdown(n - 1);
}

/// Mutual recursion: even/odd parity check with instrumentation.
fn is_even(n: u32) -> bool {
    let _g = piano_runtime::enter("is_even");
    if n == 0 {
        return true;
    }
    is_odd(n - 1)
}

fn is_odd(n: u32) -> bool {
    let _g = piano_runtime::enter("is_odd");
    if n == 0 {
        return false;
    }
    is_even(n - 1)
}

#[test]
fn direct_recursion_call_count() {
    piano_runtime::reset();

    // fib(6) makes fib calls: fib(6)=1, fib(5)=1, fib(4)=2, fib(3)=3, fib(2)=5, fib(1)=8, fib(0)=5
    // Total calls = 25 (the tree has 25 nodes for fib(6))
    let result = fib_instrumented(6);
    assert_eq!(result, 8, "fib(6) should equal 8");

    let records = piano_runtime::collect();
    let fib = records.iter().find(|r| r.name == "fib").unwrap();

    // fib(6) produces exactly 25 calls (tree nodes in the recursive expansion)
    assert_eq!(
        fib.calls, 25,
        "fib(6) should produce 25 calls, got {}",
        fib.calls
    );

    // Self time should be positive (leaf nodes do real work)
    assert!(
        fib.self_ms > 0.0,
        "fib self_ms should be positive, got {}",
        fib.self_ms
    );

    // Total time should be >= self time (total includes children)
    assert!(
        fib.total_ms >= fib.self_ms,
        "total_ms ({}) should be >= self_ms ({})",
        fib.total_ms,
        fib.self_ms
    );

    piano_runtime::reset();
}

#[test]
fn recursive_children_ms_propagation() {
    piano_runtime::reset();

    // Use a wrapper that calls a recursive function, so we can verify
    // the wrapper's self_ms is near zero while children_ms captures
    // the recursive call's total time.
    {
        let _wrapper = piano_runtime::enter("wrapper");
        countdown(5);
    }

    let records = piano_runtime::collect();
    let wrapper = records.iter().find(|r| r.name == "wrapper").unwrap();
    let cd = records.iter().find(|r| r.name == "countdown").unwrap();

    eprintln!(
        "wrapper: total={:.4}ms self={:.4}ms | countdown: total={:.4}ms self={:.4}ms calls={}",
        wrapper.total_ms, wrapper.self_ms, cd.total_ms, cd.self_ms, cd.calls
    );

    // countdown is called 6 times (n=5,4,3,2,1,0), only leaf does work
    assert_eq!(
        cd.calls, 6,
        "countdown should be called 6 times, got {}",
        cd.calls
    );

    // The wrapper's total time should include the recursive calls
    assert!(
        wrapper.total_ms > 0.0,
        "wrapper total_ms should be positive"
    );

    // Wrapper does no work itself -- its self_ms should be much less than total_ms.
    // The vast majority of time is in countdown.
    assert!(
        wrapper.self_ms < wrapper.total_ms * 0.5,
        "wrapper self_ms ({:.4}) should be much less than total_ms ({:.4})",
        wrapper.self_ms,
        wrapper.total_ms
    );

    // countdown's self_ms should be positive (leaf does burn_cpu)
    assert!(
        cd.self_ms > 0.0,
        "countdown self_ms should be positive, got {}",
        cd.self_ms
    );

    piano_runtime::reset();
}

#[test]
fn deep_recursion_no_overflow() {
    piano_runtime::reset();

    const DEPTH: u32 = 1_000;

    fn deep(n: u32) {
        let _g = piano_runtime::enter("deep");
        if n == 0 {
            return;
        }
        deep(n - 1);
    }

    deep(DEPTH);

    let records = piano_runtime::collect();
    let deep_rec = records.iter().find(|r| r.name == "deep").unwrap();

    // Should have exactly DEPTH+1 calls (0..=DEPTH)
    assert_eq!(
        deep_rec.calls,
        (DEPTH + 1) as u64,
        "deep should be called {} times, got {}",
        DEPTH + 1,
        deep_rec.calls
    );

    piano_runtime::reset();
}

#[test]
fn mutual_recursion_correctness() {
    piano_runtime::reset();

    // is_even(10) -> is_odd(9) -> is_even(8) -> ... -> is_even(0) = true
    let result = is_even(10);
    assert!(result, "10 should be even");

    let records = piano_runtime::collect();
    let even = records.iter().find(|r| r.name == "is_even").unwrap();
    let odd = records.iter().find(|r| r.name == "is_odd").unwrap();

    // is_even called for n=10,8,6,4,2,0 -> 6 calls
    assert_eq!(
        even.calls, 6,
        "is_even should be called 6 times, got {}",
        even.calls
    );

    // is_odd called for n=9,7,5,3,1 -> 5 calls
    assert_eq!(
        odd.calls, 5,
        "is_odd should be called 5 times, got {}",
        odd.calls
    );

    piano_runtime::reset();
}

#[test]
fn mutual_recursion_odd_input() {
    piano_runtime::reset();

    let result = is_even(7);
    assert!(!result, "7 should be odd");

    let records = piano_runtime::collect();
    let even = records.iter().find(|r| r.name == "is_even").unwrap();
    let odd = records.iter().find(|r| r.name == "is_odd").unwrap();

    // is_even called for n=7,5,3,1 -> wait, is_even(7) -> is_odd(6) -> is_even(5) -> ...
    // is_even: n=7,5,3,1 -> 4 calls
    // is_odd: n=6,4,2,0 -> 4 calls
    assert_eq!(
        even.calls, 4,
        "is_even should be called 4 times, got {}",
        even.calls
    );
    assert_eq!(
        odd.calls, 4,
        "is_odd should be called 4 times, got {}",
        odd.calls
    );

    piano_runtime::reset();
}
