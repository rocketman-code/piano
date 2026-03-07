//! TSC global-state mutation tests.
//!
//! These tests temporarily write sentinel / synthetic values into the global
//! TSC statics (NUMER, DENOM, BIAS_TICKS, EPOCH_TSC) to verify calibration
//! and accessor correctness. They MUST live in a separate integration test
//! binary so that the temporary corruption cannot race with unit tests that
//! read these globals (e.g. PianoFuture tests calling `bias_ticks()` or
//! `ticks_to_ns()` via `drop_cold`).
//!
//! Within this binary the tests still serialize via `#[serial]` because they
//! mutate the same process-global atomics.
//!
//! Run with: cargo test -p piano-runtime --features _test_internals --test tsc_internals
#![cfg(feature = "_test_internals")]
#![allow(clippy::incompatible_msrv)]

use piano_runtime::tsc;
use serial_test::serial;

#[test]
#[serial]
fn elapsed_ns_with_zero_denom_does_not_panic() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    tsc::store_numer(1);
    tsc::store_denom(0);

    // Must not panic -- should return 0 for zero denominator
    let result = tsc::elapsed_ns(0, 1000);
    assert_eq!(result, 0);

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn ticks_to_ns_uses_calibrated_ratio() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    // 1000 ticks * (3/2) = 1500 ns
    tsc::store_numer(3);
    tsc::store_denom(2);

    assert_eq!(tsc::ticks_to_ns(1000), 1500);
    assert_eq!(tsc::ticks_to_ns(0), 0);

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn ticks_to_ns_zero_denom_returns_zero() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    tsc::store_numer(1);
    tsc::store_denom(0);

    assert_eq!(tsc::ticks_to_ns(1000), 0);

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn elapsed_ns_delegates_to_ticks_to_ns() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    tsc::store_numer(1);
    tsc::store_denom(1);

    assert_eq!(tsc::elapsed_ns(10, 110), tsc::ticks_to_ns(100));

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn ticks_to_epoch_ns_returns_correct_value() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    // With ratio 1:1, ticks=1000, epoch=100 -> 900
    tsc::store_numer(1);
    tsc::store_denom(1);

    let result = tsc::ticks_to_epoch_ns(1000, 100);
    assert_eq!(result, 900);

    // With ratio 3:2, ticks=1000, epoch=100 -> (900 * 3 / 2) = 1350
    tsc::store_numer(3);
    tsc::store_denom(2);

    let result = tsc::ticks_to_epoch_ns(1000, 100);
    assert_eq!(result, 1350);

    assert!(
        result > 1,
        "ticks_to_epoch_ns must return meaningful value, not 0 or 1"
    );

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn calibrate_sets_nonzero_ratio() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    // Zero out to detect that calibrate() actually stores values
    tsc::store_numer(0);
    tsc::store_denom(0);

    tsc::calibrate();

    let n = tsc::load_numer();
    let d = tsc::load_denom();

    assert!(n > 0, "NUMER must be nonzero after calibrate()");
    assert!(d > 0, "DENOM must be nonzero after calibrate()");

    let ratio = n as f64 / d as f64;
    assert!(
        ratio < 1000.0,
        "calibrated ratio {ratio} is unreasonably large (possible * instead of /)"
    );
    assert!(
        ratio > 0.001,
        "calibrated ratio {ratio} is unreasonably small"
    );

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn calibrate_spins_for_target_duration() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    tsc::calibrate();

    let n = tsc::load_numer();
    let d = tsc::load_denom();

    // On x86_64/aarch64, a proper calibration never gives exactly 1:1
    // because the TSC frequency differs from 1 GHz.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    assert!(
        n != 1 || d != 1,
        "calibrate() produced 1:1 ratio -- loop likely did not spin (< mutated?)"
    );

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn calibrate_loop_spins_long_enough_for_accurate_ratio() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

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

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
}

#[test]
#[serial]
fn calibrate_uses_division_not_multiplication() {
    let saved_n = tsc::load_numer();
    let saved_d = tsc::load_denom();

    tsc::calibrate();

    let n = tsc::load_numer();
    let d = tsc::load_denom();

    assert!(
        n < 100_000_000,
        "NUMER {n} is too large (division replaced with multiplication?)"
    );
    assert!(
        d < 100_000_000,
        "DENOM {d} is too large (division replaced with multiplication?)"
    );

    tsc::store_numer(saved_n);
    tsc::store_denom(saved_d);
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
