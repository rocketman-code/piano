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
