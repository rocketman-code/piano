//! TSC global-state mutation tests.
//!
//! These tests temporarily write sentinel / synthetic values into the global
//! TSC statics (QUOTIENT, MULTIPLIER, BIAS_TICKS, EPOCH_TSC) to verify
//! calibration and accessor correctness. They MUST live in a separate
//! integration test binary so that the temporary corruption cannot race with
//! unit tests that read these globals (e.g. PianoFuture tests calling
//! `bias_ticks()` or `ticks_to_ns()` via `drop_cold`).
//!
//! Within this binary the tests still serialize via `#[serial]` because they
//! mutate the same process-global atomics.
//!
//! Run with: cargo test -p piano-runtime --features _test_internals --test tsc_internals
#![cfg(feature = "_test_internals")]
#![allow(clippy::incompatible_msrv)]

use piano_runtime::tsc;
use serial_test::serial;

/// Helper: save and restore Q/M around a test that injects a synthetic ratio.
struct RatioGuard {
    saved_q: u64,
    saved_m: u64,
}

impl RatioGuard {
    fn new() -> Self {
        Self {
            saved_q: tsc::load_quotient(),
            saved_m: tsc::load_multiplier(),
        }
    }
}

impl Drop for RatioGuard {
    fn drop(&mut self) {
        tsc::store_quotient(self.saved_q);
        tsc::store_multiplier(self.saved_m);
    }
}

#[test]
#[serial]
fn ticks_to_ns_zero_ratio_returns_zero() {
    let _g = RatioGuard::new();

    // Pre-calibration state: Q=0, M=0 -> always returns 0
    tsc::store_quotient(0);
    tsc::store_multiplier(0);

    assert_eq!(tsc::ticks_to_ns(1000), 0);
    assert_eq!(tsc::ticks_to_ns(0), 0);
    assert_eq!(tsc::ticks_to_ns(u64::MAX), 0);
}

#[test]
#[serial]
fn elapsed_ns_with_zero_ratio_does_not_panic() {
    let _g = RatioGuard::new();

    tsc::store_quotient(0);
    tsc::store_multiplier(0);

    // Must not panic -- should return 0
    let result = tsc::elapsed_ns(0, 1000);
    assert_eq!(result, 0);
}

#[test]
#[serial]
fn ticks_to_ns_uses_calibrated_ratio() {
    let _g = RatioGuard::new();

    // 1000 ticks * (3/2) = 1500 ns
    tsc::store_ratio(3, 2);
    assert_eq!(tsc::ticks_to_ns(1000), 1500);
    assert_eq!(tsc::ticks_to_ns(0), 0);
}

#[test]
#[serial]
fn ticks_to_ns_non_integer_ratio() {
    let _g = RatioGuard::new();

    // 1000 ticks * (125/3) = 41666.667 -> floor = 41666
    tsc::store_ratio(125, 3);
    let result = tsc::ticks_to_ns(1000);
    // Fixed-point can be exact or +1; both are acceptable
    assert!(
        result == 41666 || result == 41667,
        "expected 41666 or 41667, got {result}"
    );
}

#[test]
#[serial]
fn ticks_to_ns_identity_ratio() {
    let _g = RatioGuard::new();

    // 1:1 ratio -> ticks == ns
    tsc::store_ratio(1, 1);
    assert_eq!(tsc::ticks_to_ns(12345), 12345);
    assert_eq!(tsc::ticks_to_ns(0), 0);
    assert_eq!(tsc::ticks_to_ns(u64::MAX), u64::MAX);
}

#[test]
#[serial]
fn elapsed_ns_delegates_to_ticks_to_ns() {
    let _g = RatioGuard::new();

    tsc::store_ratio(1, 1);
    assert_eq!(tsc::elapsed_ns(10, 110), tsc::ticks_to_ns(100));
}

#[test]
#[serial]
fn ticks_to_epoch_ns_returns_correct_value() {
    let _g = RatioGuard::new();

    // With ratio 1:1, ticks=1000, epoch=100 -> 900
    tsc::store_ratio(1, 1);
    let result = tsc::ticks_to_epoch_ns(1000, 100);
    assert_eq!(result, 900);

    // With ratio 3:2, ticks=1000, epoch=100 -> (900 * 3 / 2) = 1350
    tsc::store_ratio(3, 2);
    let result = tsc::ticks_to_epoch_ns(1000, 100);
    assert_eq!(result, 1350);

    assert!(
        result > 1,
        "ticks_to_epoch_ns must return meaningful value, not 0 or 1"
    );
}

#[test]
#[serial]
fn calibrate_sets_nonzero_conversion() {
    let _g = RatioGuard::new();

    // Zero out to detect that calibrate() actually stores values
    tsc::store_quotient(0);
    tsc::store_multiplier(0);

    tsc::calibrate();

    let q = tsc::load_quotient();
    let m = tsc::load_multiplier();

    // At least one of Q or M must be nonzero after calibration
    assert!(
        q > 0 || m > 0,
        "calibrate() must set a nonzero conversion (Q={q}, M={m})"
    );

    // Sanity: converting 1M ticks should give a reasonable nanosecond value
    let ns = tsc::ticks_to_ns(1_000_000);
    assert!(
        ns > 0,
        "ticks_to_ns(1M) must produce nonzero result after calibration"
    );
}

#[test]
#[serial]
fn calibrate_spins_for_target_duration() {
    let _g = RatioGuard::new();

    tsc::calibrate();

    let q = tsc::load_quotient();
    let m = tsc::load_multiplier();

    // On x86_64/aarch64, a proper calibration never gives exactly Q=1, M=0
    // because the TSC frequency differs from 1 GHz.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    assert!(
        q != 1 || m != 0,
        "calibrate() produced identity ratio -- loop likely did not spin"
    );
}

#[test]
#[serial]
fn calibrate_loop_spins_long_enough_for_accurate_ratio() {
    let _g = RatioGuard::new();

    tsc::calibrate();

    // Measure 10ms of wall time and verify TSC-based conversion is close.
    let tsc_before = tsc::read();
    let wall_before = std::time::Instant::now();
    let target = std::time::Duration::from_millis(10);
    while wall_before.elapsed() < target {}
    let tsc_after = tsc::read();
    let wall_actual_ns = wall_before.elapsed().as_nanos() as u64;

    let tsc_delta = tsc_after.wrapping_sub(tsc_before);
    let converted_ns = tsc::ticks_to_ns(tsc_delta);

    let ratio = converted_ns as f64 / wall_actual_ns as f64;
    assert!(
        ratio > 0.5 && ratio < 2.0,
        "TSC-to-ns conversion ratio {ratio:.3} is too far from 1.0 \
         (calibration likely didn't spin: converted={converted_ns}ns, wall={wall_actual_ns}ns)"
    );
}

#[test]
#[serial]
fn calibrate_quotient_is_reasonable() {
    let _g = RatioGuard::new();

    tsc::calibrate();

    let q = tsc::load_quotient();

    // Q = wall_ns / tsc_ticks = ns_per_tick.
    // aarch64 24 MHz: ~41 ns/tick. x86_64 3 GHz: ~0.33 ns/tick (Q=0).
    // Q should be at most a few hundred (even a 1 MHz counter gives Q=1000).
    assert!(q < 10_000, "Q={q} is unreasonably large");
}

#[test]
#[serial]
fn calibrate_bias_stores_nonzero_value() {
    let saved = tsc::load_bias_ticks();

    tsc::calibrate();

    // Set to a sentinel to detect that calibrate_bias actually writes
    tsc::store_bias_ticks(u64::MAX);

    tsc::calibrate_bias();

    let bias = tsc::load_bias_ticks();

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    assert_ne!(bias, u64::MAX, "calibrate_bias() did not write BIAS_TICKS");

    tsc::store_bias_ticks(saved);
}

#[test]
#[serial]
fn calibrate_bias_uses_correct_arithmetic() {
    let saved = tsc::load_bias_ticks();
    tsc::calibrate();
    tsc::calibrate_bias();

    let bias = tsc::load_bias_ticks();

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    assert!(
        bias < 10_000,
        "bias {bias} is unreasonably large (arithmetic mutation?)"
    );

    tsc::store_bias_ticks(saved);
}

#[test]
#[serial]
fn bias_ticks_returns_stored_value() {
    let saved = tsc::load_bias_ticks();

    tsc::store_bias_ticks(42);
    assert_eq!(
        tsc::load_bias_ticks(),
        42,
        "load_bias_ticks() must roundtrip"
    );
    assert_eq!(
        tsc::bias_ticks(),
        42,
        "bias_ticks() must return stored value"
    );

    tsc::store_bias_ticks(9999);
    assert_eq!(
        tsc::bias_ticks(),
        9999,
        "bias_ticks() must return stored value"
    );

    assert_ne!(tsc::bias_ticks(), 0);
    assert_ne!(tsc::bias_ticks(), 1);

    tsc::store_bias_ticks(saved);
}

#[test]
#[serial]
fn set_epoch_tsc_stores_value() {
    let saved = tsc::load_epoch_tsc();

    tsc::set_epoch_tsc(12345);
    assert_eq!(
        tsc::load_epoch_tsc(),
        12345,
        "set_epoch_tsc() must store the value"
    );

    tsc::set_epoch_tsc(0);
    assert_eq!(tsc::load_epoch_tsc(), 0);

    tsc::store_epoch_tsc(saved);
}

#[test]
#[serial]
fn epoch_tsc_returns_stored_value() {
    let saved = tsc::load_epoch_tsc();

    tsc::store_epoch_tsc(777);
    assert_eq!(
        tsc::epoch_tsc(),
        777,
        "epoch_tsc() must return stored value"
    );

    tsc::store_epoch_tsc(123456);
    assert_eq!(tsc::epoch_tsc(), 123456);

    assert_ne!(tsc::epoch_tsc(), 0);
    assert_ne!(tsc::epoch_tsc(), 1);

    tsc::store_epoch_tsc(saved);
}
