//! Calibration harness: busy-wait reference function and bias measurement.
//!
//! Run with: cargo test -p piano-runtime --features piano-runtime/_test_internals --test calibration -- --ignored --nocapture
#![allow(clippy::incompatible_msrv)] // tests run on dev toolchain, not published MSRV

use std::time::{Duration, Instant};

/// Spin-waits until `target` duration has elapsed, then returns actual elapsed time.
///
/// The returned value is ground truth -- it comes from the same `Instant` clock
/// Piano uses, with no instrumentation overhead between start and end.
fn busy_wait(target: Duration) -> Duration {
    let start = Instant::now();
    while start.elapsed() < target {}
    start.elapsed()
}

/// Validates that busy_wait's inner measurement agrees with an outer measurement.
///
/// For each target duration, runs 1000 ITERATIONS and checks that the median
/// delta between inner (returned by busy_wait) and outer (measured around the call)
/// is within 200ns -- the expected cost of two extra Instant::now() calls.
#[test]
#[ignore]
fn reference_function_accuracy() {
    let targets = [
        Duration::from_micros(100),
        Duration::from_micros(10),
        Duration::from_micros(1),
        Duration::from_nanos(100),
    ];

    const ITERATIONS: usize = 1000;
    const MAX_MEDIAN_DELTA_NS: u128 = 200;

    eprintln!(
        "{:<12} {:>12} {:>12} {:>12}",
        "target", "inner", "outer", "delta"
    );
    eprintln!("{}", "-".repeat(52));

    for target in targets {
        let mut samples: Vec<(Duration, Duration, u128)> = Vec::with_capacity(ITERATIONS);

        for _ in 0..ITERATIONS {
            let before = Instant::now();
            let inner = busy_wait(target);
            let outer = before.elapsed();

            let delta = if outer > inner {
                outer - inner
            } else {
                inner - outer
            };
            samples.push((inner, outer, delta.as_nanos()));
        }

        samples.sort_unstable_by_key(|s| s.2);
        let (inner, outer, median_delta) = samples[ITERATIONS / 2];

        eprintln!(
            "{:<12} {:>12} {:>12} {:>10}ns",
            format!("{target:?}"),
            format!("{inner:?}"),
            format!("{outer:?}"),
            median_delta,
        );

        assert!(
            median_delta <= MAX_MEDIAN_DELTA_NS,
            "median delta {median_delta}ns exceeds {MAX_MEDIAN_DELTA_NS}ns for target {target:?}"
        );
    }
}

/// Measures Piano's actual per-call overhead by amortization.
/// Runs enter()/drop() in a tight loop N times and divides total wall time
/// by N, giving sub-nanosecond precision regardless of clock granularity.
#[test]
#[ignore]
fn amortized_overhead() {
    piano_runtime::reset();
    const N: u64 = 1_000_000;

    // Measure Piano's enter/drop cost
    let start = Instant::now();
    for _ in 0..N {
        let _g = piano_runtime::enter("overhead_target");
    }
    let piano_total = start.elapsed();

    // Measure baseline: same loop but with a no-op to prevent the
    // compiler from optimizing away the loop itself
    let start = Instant::now();
    for _ in 0..N {
        std::hint::black_box(42u64);
    }
    let baseline_total = start.elapsed();

    let piano_ns = piano_total.as_nanos() as f64 / N as f64;
    let baseline_ns = baseline_total.as_nanos() as f64 / N as f64;
    let overhead_ns = piano_ns - baseline_ns;

    eprintln!();
    eprintln!("--- Amortized Per-Call Overhead ({N} iterations) ---");
    eprintln!("  piano:    {piano_ns:.1}ns/call");
    eprintln!("  baseline: {baseline_ns:.1}ns/call");
    eprintln!("  overhead: {overhead_ns:.1}ns/call");
    eprintln!();

    piano_runtime::reset();
}

/// Piano's reported elapsed for an empty function IS the bias.
///
/// black_box(()) takes ~0ns. Whatever Piano reports is pure instrumentation
/// bias -- the time captured by the clock window that isn't user code.
/// Amortization over N iterations gives sub-nanosecond precision.
#[cfg(feature = "_test_internals")]
#[test]
#[ignore]
fn bias_empty_fn() {
    const N: usize = 1_000_000;
    piano_runtime::reset();

    for _ in 0..N {
        let _g = piano_runtime::enter("empty");
        std::hint::black_box(());
    }

    let invocations = piano_runtime::collect_invocations();
    let piano_total_ns: u64 = invocations
        .iter()
        .filter(|r| r.name == "empty")
        .map(|r| r.elapsed_ns)
        .sum();

    let bias_per_call = piano_total_ns as f64 / N as f64;

    eprintln!();
    eprintln!("--- Empty Function Bias ({N} iterations) ---");
    eprintln!("  total reported: {piano_total_ns}ns");
    eprintln!("  per call: {bias_per_call:.2}ns");
    eprintln!("  (true elapsed of black_box(()) ~ 0ns, so reported ~ bias)");
    eprintln!();

    piano_runtime::reset();
}

/// After bias calibration, the reported time for an empty function should
/// be near zero. On aarch64 (~0ns uncorrected) this is marginal. On x86_64
/// (~12ns uncorrected on KVM) this validates the correction works.
#[cfg(feature = "_test_internals")]
#[test]
#[ignore]
fn bias_calibration_reduces_empty_fn() {
    const N: usize = 1_000_000;
    piano_runtime::reset();

    for _ in 0..N {
        let _g = piano_runtime::enter("bias_corrected");
        std::hint::black_box(());
    }

    let invocations = piano_runtime::collect_invocations();
    let piano_total_ns: u64 = invocations
        .iter()
        .filter(|r| r.name == "bias_corrected")
        .map(|r| r.elapsed_ns)
        .sum();

    let bias_per_call = piano_total_ns as f64 / N as f64;

    eprintln!();
    eprintln!("--- Bias-Corrected Empty Function ({N} iterations) ---");
    eprintln!("  total reported: {piano_total_ns}ns");
    eprintln!("  per call: {bias_per_call:.2}ns");
    eprintln!("  (should be near 0 after bias subtraction)");
    eprintln!();

    // After correction, per-call bias should be under 5ns on all platforms.
    // aarch64: ~0ns, x86_64 bare metal: ~0-3ns, x86_64 KVM: ~0-4ns.
    assert!(
        bias_per_call < 5.0,
        "bias-corrected per-call time {bias_per_call:.2}ns exceeds 5ns"
    );

    piano_runtime::reset();
}

/// After first enter(), guard overhead should be calibrated to a nonzero value.
/// Kills: replace calibrate_guard_cost_once with (), replace calibrate_guard_cost
/// with (), and line 188 arithmetic mutations that produce 0 (- to /, / to %).
#[cfg(all(feature = "cpu-time", feature = "_test_internals"))]
#[test]
#[serial_test::serial]
fn calibrate_guard_cost_produces_nonzero_overhead() {
    // reset() doesn't reset the DONE atomic, but guard overhead is a
    // global calibration -- just verify it's nonzero after any enter().
    piano_runtime::reset();
    let _g = piano_runtime::enter("trigger_calibration");
    drop(_g);

    let overhead = piano_runtime::load_guard_overhead_ns();
    assert!(
        overhead > 0,
        "guard_overhead should be > 0 after calibration, got {overhead}"
    );
    // Upper bound: guard overhead should be under 100us on any real hardware.
    // Kills line 188 mutations that produce huge values (- to +, / to *).
    assert!(
        overhead < 100_000,
        "guard_overhead {overhead}ns exceeds 100us -- calibration arithmetic bug"
    );

    piano_runtime::reset();
}

/// Aggregate bias correction must subtract (not add) calls * bias.
/// With a large injected bias, cpu_self_ms should clamp to 0 for trivial work.
/// Kills: line 993 - to + (would produce huge positive), * to + (insufficient correction).
#[cfg(all(feature = "cpu-time", feature = "_test_internals"))]
#[test]
#[serial_test::serial]
fn aggregate_bias_correction_subtracts_proportionally() {
    piano_runtime::reset();

    // Trigger calibration first, then override bias (saving original to restore).
    let _g = piano_runtime::enter("trigger_cal");
    drop(_g);
    let saved_bias = piano_runtime::load_cpu_bias_ns();
    piano_runtime::store_cpu_bias_ns(10_000); // 10us per call

    const N: usize = 10_000;
    for _ in 0..N {
        let _g = piano_runtime::enter("bias_sub_test");
        std::hint::black_box(42u64);
    }

    let records = piano_runtime::collect();
    let r = records
        .iter()
        .find(|r| r.name == "bias_sub_test")
        .expect("bias_sub_test should appear");

    // With 10us bias * 10000 calls = 100ms correction.
    // Actual CPU work is ~500us total. Correct subtraction clamps to 0.
    // - to + mutation: raw + 100ms = ~100ms, NOT clamped.
    // * to + mutation: raw - (10000 + 10000)ns = raw - 20us, still positive.
    assert!(
        r.cpu_self_ms < 0.1,
        "cpu_self_ms should be near 0 after large bias subtraction, got {:.3}ms",
        r.cpu_self_ms
    );

    piano_runtime::store_cpu_bias_ns(saved_bias);
    piano_runtime::reset();
}

/// Parent's cpu_self must exclude child CPU time via cpu_children_ns tracking.
/// Kills: line 762 += to *= (children always 0), + to * (guard_overhead=0 → 0).
#[cfg(all(feature = "cpu-time", feature = "_test_internals"))]
#[test]
#[serial_test::serial]
fn parent_cpu_self_excludes_child_cpu_time() {
    piano_runtime::reset();

    // Trigger calibration, then zero out overheads for deterministic test.
    let _g = piano_runtime::enter("trigger_cal");
    drop(_g);
    let saved_overhead = piano_runtime::load_guard_overhead_ns();
    let saved_bias = piano_runtime::load_cpu_bias_ns();
    piano_runtime::store_guard_overhead_ns(0);
    piano_runtime::store_cpu_bias_ns(0);

    {
        let _parent = piano_runtime::enter("parent_cpu_excl");
        for _ in 0..200 {
            let _child = piano_runtime::enter("child_cpu_excl");
            // Burn CPU in child.
            let mut acc = 0u64;
            for i in 0..5_000u64 {
                acc = acc.wrapping_add(i.wrapping_mul(7));
            }
            std::hint::black_box(acc);
        }
    }

    let invocations = piano_runtime::collect_invocations();
    let parent_cpu: u64 = invocations
        .iter()
        .filter(|r| r.name == "parent_cpu_excl")
        .map(|r| r.cpu_self_ns)
        .sum();
    let child_cpu: u64 = invocations
        .iter()
        .filter(|r| r.name == "child_cpu_excl")
        .map(|r| r.cpu_self_ns)
        .sum();

    eprintln!("parent_cpu_self={parent_cpu}ns, child_cpu_total={child_cpu}ns");

    // Parent does no work itself. Its cpu_self should be much less than
    // child total. If cpu_children tracking is broken (always 0), parent
    // cpu_self ≈ child_total, failing this assert.
    assert!(
        parent_cpu < child_cpu,
        "parent cpu_self ({parent_cpu}ns) should be less than child total ({child_cpu}ns) \
         -- cpu_children_ns tracking broken"
    );

    piano_runtime::store_guard_overhead_ns(saved_overhead);
    piano_runtime::store_cpu_bias_ns(saved_bias);
    piano_runtime::reset();
}

/// Regression test: cpu_self_ms must not inflate for high-call-count functions.
///
/// Before the amortized bias correction, cpu_self_ms was 11-17x higher than
/// wall self_ms for functions called many times (per-call saturating_sub created
/// a systematic 42ns quantum bias on Apple Silicon). After the fix, cpu_self_ms
/// should track wall self_ms within a small factor.
#[cfg(feature = "cpu-time")]
#[test]
#[serial_test::serial]
fn cpu_time_not_inflated_for_high_call_count() {
    piano_runtime::reset();

    const N: usize = 100_000;
    {
        let _outer = piano_runtime::enter("outer");
        for _ in 0..N {
            let _inner = piano_runtime::enter("high_call_leaf");
            // Trivial body: near-zero real work.
            std::hint::black_box(42u64);
        }
    }

    let records = piano_runtime::collect();
    let leaf = records
        .iter()
        .find(|r| r.name == "high_call_leaf")
        .expect("high_call_leaf should appear in records");

    assert_eq!(leaf.calls, N as u64);

    // cpu_self_ms should not exceed 5x wall self_ms.
    // Before the fix this ratio was 11-17x; after, it should be ~1x.
    let ratio = if leaf.self_ms > 0.001 {
        leaf.cpu_self_ms / leaf.self_ms
    } else {
        // self_ms near zero means cpu_self_ms should also be near zero.
        // If cpu_self_ms is inflated, this will catch it.
        leaf.cpu_self_ms * 1000.0 // scale up so any inflation fails the assert
    };

    eprintln!(
        "high_call_leaf: calls={}, self_ms={:.3}, cpu_self_ms={:.3}, ratio={:.1}x",
        leaf.calls, leaf.self_ms, leaf.cpu_self_ms, ratio
    );

    assert!(
        ratio < 5.0,
        "cpu_self_ms/self_ms ratio {ratio:.1}x exceeds 5x -- cpu-time inflation regression"
    );

    piano_runtime::reset();
}
