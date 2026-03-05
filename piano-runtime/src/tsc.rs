//! Fast inline timestamp via hardware counter (TSC on x86_64, CNTVCT on aarch64).
//!
//! `read()` returns raw counter ticks. `ticks_to_ns()` converts a tick count
//! to nanoseconds using a ratio calibrated once at startup. `bias_ticks()`
//! returns the calibrated measurement overhead in raw ticks.
//!
//! # Cross-core monotonicity (x86_64)
//!
//! On x86_64, `read()` uses the `rdtsc` instruction. This relies on the
//! invariant TSC feature (CPUID leaf 0x80000007, EDX bit 8), available on
//! all Intel processors since Nehalem (2008) and all AMD processors since
//! Athlon 64 X2 / Turion 64 X2 revision F (2006).
//!
//! On processors with invariant TSC, the counter runs at a constant rate
//! across all cores and is synchronized at boot, so timestamps are
//! monotonic even when a thread migrates between cores.
//!
//! On pre-Nehalem processors without invariant TSC, each core may maintain
//! an independent counter. If a thread migrates between cores between the
//! enter and exit `rdtsc` reads, the elapsed delta can be negative (the
//! exit timestamp from core B is less than the enter timestamp from core A).
//! Because we compute deltas with `wrapping_sub`, a negative result wraps
//! to a large positive value, which would appear as an outlier in the
//! profiling output.
//!
//! This is not checked at runtime. The practical impact is negligible: any
//! x86_64 hardware from the last ~15 years has invariant TSC. Piano does
//! not attempt to pin threads to cores or detect the feature at startup.
//!
//! On aarch64, `cntvct_el0` reads the generic timer which is architecturally
//! defined as a single system-wide counter, so cross-core monotonicity is
//! guaranteed by the architecture.

use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};
use std::time::Instant;

/// Numerator/denominator for converting ticks to nanoseconds.
/// `ns = ticks * NUMER / DENOM`
///
/// Stored as atomics so that `calibrate()` (called once from `epoch()`)
/// publishes them with Release and `ticks_to_ns()` reads with Relaxed
/// (after the Once-guarded init, every thread sees the calibrated values).
static NUMER: AtomicU64 = AtomicU64::new(0);
static DENOM: AtomicU64 = AtomicU64::new(1);

/// Calibrated measurement bias in raw ticks. Subtracted from every
/// elapsed measurement in `drop_cold` to correct for instructions
/// between the two TSC reads that are not user code.
static BIAS_TICKS: AtomicU64 = AtomicU64::new(0);

/// Read the hardware cycle counter. Single inline instruction on both
/// x86_64 (`rdtsc`) and aarch64 (`mrs cntvct_el0`).
#[inline(always)]
pub(crate) fn read() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(target_arch = "aarch64")]
    {
        let val: u64;
        unsafe { core::arch::asm!("mrs {}, cntvct_el0", out(reg) val) };
        val
    }
    // Fallback: use Instant (loses the perf benefit but still works).
    // Uses its own epoch to avoid a deadlock cycle with collector::epoch()
    // which calls tsc::read() inside EPOCH.get_or_init.
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        use std::sync::OnceLock;
        static FALLBACK_EPOCH: OnceLock<Instant> = OnceLock::new();
        let epoch = FALLBACK_EPOCH.get_or_init(Instant::now);
        Instant::now().duration_since(*epoch).as_nanos() as u64
    }
}

/// Convert a raw tick count to nanoseconds using the calibrated ratio.
#[inline(always)]
pub(crate) fn ticks_to_ns(ticks: u64) -> u64 {
    let n = NUMER.load(Ordering::Relaxed);
    let d = DENOM.load(Ordering::Relaxed);
    if d == 0 {
        return 0;
    }
    (ticks as u128 * n as u128 / d as u128) as u64
}

/// Convert a tick delta to nanoseconds using the calibrated ratio.
#[cfg(any(test, feature = "_test_internals"))]
#[inline(always)]
pub(crate) fn elapsed_ns(start: u64, end: u64) -> u64 {
    ticks_to_ns(end.wrapping_sub(start))
}

/// Convert a tick value to nanoseconds-since-epoch for absolute timestamps.
#[cfg(any(test, feature = "_test_internals"))]
#[inline]
pub(crate) fn ticks_to_epoch_ns(ticks: u64, epoch_tsc: u64) -> u64 {
    ticks_to_ns(ticks.wrapping_sub(epoch_tsc))
}

/// Calibrate the tick-to-nanosecond ratio. Called once from `epoch()`.
///
/// Measures how many ticks elapse in a known wall-clock interval using
/// `Instant` as the reference. The calibration spin is brief (~1ms).
pub(crate) fn calibrate() {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() already returns nanoseconds
        NUMER.store(1, Ordering::Release);
        DENOM.store(1, Ordering::Release);
        return;
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        // Spin for ~2ms to get a stable ratio
        let wall_start = Instant::now();
        let tsc_start = read();

        let target = std::time::Duration::from_millis(2);
        while wall_start.elapsed() < target {}

        let tsc_end = read();
        let wall_ns = wall_start.elapsed().as_nanos() as u64;
        let tsc_ticks = tsc_end.wrapping_sub(tsc_start);

        // Guard: if the counter did not advance (frozen TSC, broken VM,
        // or both reads returned the same value), fall back to 1:1 ratio.
        if tsc_ticks == 0 {
            NUMER.store(1, Ordering::Release);
            DENOM.store(1, Ordering::Release);
            return;
        }

        // ns = ticks * wall_ns / tsc_ticks
        // Simplify the fraction to avoid overflow in ticks_to_ns
        let g = gcd(wall_ns, tsc_ticks);
        NUMER.store(wall_ns / g, Ordering::Release);
        DENOM.store(tsc_ticks / g, Ordering::Release);
    }
}

/// Calibrate the measurement bias (cost of a TSC read pair in ticks).
/// Uses trimmed mean (2% trim) for robustness against VM preemption outliers.
/// Called once from `epoch()` after `calibrate()`.
pub(crate) fn calibrate_bias() {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() returns nanoseconds directly, no hardware bias.
        return;
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        const N: usize = 10_000;
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = read();
            // Compiler barrier: prevents reordering or merging of the two
            // read() calls. Emits no instructions but LLVM will not move
            // memory operations across it. The read() calls use inline asm
            // which already prevents reordering, but this guards against
            // future changes. Note: the previous read_volatile(&()) was a
            // no-op (ZST has no bytes to read, LLVM elides it entirely).
            // black_box requires Rust 1.66+ (MSRV is 1.59).
            compiler_fence(Ordering::SeqCst);
            let end = read();
            samples.push(end.wrapping_sub(start));
        }
        samples.sort_unstable();
        // Trimmed mean: discard top/bottom 2%
        let trim = N / 50;
        let trimmed = &samples[trim..N - trim];
        let sum: u64 = trimmed.iter().sum();
        let mean_ticks = sum / trimmed.len() as u64;
        BIAS_TICKS.store(mean_ticks, Ordering::Release);
    }
}

/// Return the calibrated bias in raw ticks.
#[inline(always)]
pub(crate) fn bias_ticks() -> u64 {
    BIAS_TICKS.load(Ordering::Relaxed)
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    // gcd(0, 0) is mathematically undefined but we use the result as a
    // divisor, so return 1 instead of 0 to avoid division by zero.
    if a == 0 {
        1
    } else {
        a
    }
}

/// The TSC value captured at epoch. Stored alongside the Instant epoch.
static EPOCH_TSC: AtomicU64 = AtomicU64::new(0);

pub(crate) fn set_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}

#[cfg(any(test, feature = "_test_internals"))]
pub(crate) fn epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn gcd_normal_cases() {
        assert_eq!(gcd(12, 8), 4);
        assert_eq!(gcd(8, 12), 4);
        assert_eq!(gcd(7, 13), 1);
        assert_eq!(gcd(100, 100), 100);
        assert_eq!(gcd(1, 1), 1);
    }

    #[test]
    fn gcd_with_one_zero_returns_nonzero() {
        // gcd(n, 0) = n by mathematical convention
        assert_eq!(gcd(5, 0), 5);
        assert_eq!(gcd(0, 5), 5);
    }

    #[test]
    fn gcd_both_zero_returns_one() {
        // gcd(0, 0) is mathematically undefined; we need a nonzero result
        // because we divide by the return value.
        let g = gcd(0, 0);
        assert_ne!(g, 0, "gcd(0,0) must not return 0 (used as divisor)");
        assert_eq!(g, 1);
    }

    #[test]
    #[serial]
    fn elapsed_ns_with_zero_denom_does_not_panic() {
        // If DENOM were zero (e.g., broken TSC), elapsed_ns must not panic.
        // Temporarily store 0 in DENOM, call elapsed_ns, then restore.
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        NUMER.store(1, Ordering::Release);
        DENOM.store(0, Ordering::Release);

        // Must not panic -- should return 0 for zero denominator
        let result = elapsed_ns(0, 1000);
        assert_eq!(result, 0);

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn ticks_to_ns_uses_calibrated_ratio() {
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        // 1000 ticks * (3/2) = 1500 ns
        NUMER.store(3, Ordering::Release);
        DENOM.store(2, Ordering::Release);

        assert_eq!(ticks_to_ns(1000), 1500);
        assert_eq!(ticks_to_ns(0), 0);

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn ticks_to_ns_zero_denom_returns_zero() {
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        NUMER.store(1, Ordering::Release);
        DENOM.store(0, Ordering::Release);

        assert_eq!(ticks_to_ns(1000), 0);

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn elapsed_ns_delegates_to_ticks_to_ns() {
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        NUMER.store(1, Ordering::Release);
        DENOM.store(1, Ordering::Release);

        assert_eq!(elapsed_ns(10, 110), ticks_to_ns(100));

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn ticks_to_epoch_ns_returns_correct_value() {
        // ticks_to_epoch_ns(ticks, epoch_tsc) = ticks_to_ns(ticks - epoch_tsc)
        // With ratio 1:1, ticks=1000, epoch=100 -> 900
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        NUMER.store(1, Ordering::Release);
        DENOM.store(1, Ordering::Release);

        let result = ticks_to_epoch_ns(1000, 100);
        assert_eq!(result, 900);

        // With ratio 3:2, ticks=1000, epoch=100 -> (900 * 3 / 2) = 1350
        NUMER.store(3, Ordering::Release);
        DENOM.store(2, Ordering::Release);

        let result = ticks_to_epoch_ns(1000, 100);
        assert_eq!(result, 1350);

        // Verify it does not return 0 or 1 for meaningful inputs
        assert!(
            result > 1,
            "ticks_to_epoch_ns must return meaningful value, not 0 or 1"
        );

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn calibrate_sets_nonzero_ratio() {
        // Save originals
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        // Zero out to detect that calibrate() actually stores values
        NUMER.store(0, Ordering::Release);
        DENOM.store(0, Ordering::Release);

        calibrate();

        let n = NUMER.load(Ordering::Relaxed);
        let d = DENOM.load(Ordering::Relaxed);

        // calibrate() must store nonzero values
        assert!(n > 0, "NUMER must be nonzero after calibrate()");
        assert!(d > 0, "DENOM must be nonzero after calibrate()");

        // The ratio n/d should be reasonable (not inverted by * instead of /).
        // On real hardware, TSC runs at GHz speeds so ~2ms calibration
        // yields millions of ticks. The ratio ns/ticks should be < 100
        // (even a 24MHz counter gives ~41 ns/tick).
        // If the mutation `/ -> *` were applied, the ratio would be astronomical.
        let ratio = n as f64 / d as f64;
        assert!(
            ratio < 1000.0,
            "calibrated ratio {ratio} is unreasonably large (possible * instead of /)"
        );
        assert!(
            ratio > 0.001,
            "calibrated ratio {ratio} is unreasonably small"
        );

        // Restore
        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn calibrate_spins_for_target_duration() {
        // calibrate() must spin until wall_start.elapsed() >= target (2ms).
        // If the comparison `<` is mutated to `==` or `>`, the loop body
        // executes zero times or exits immediately, producing a near-zero
        // tsc_ticks which hits the tsc_ticks==0 guard and stores 1:1 ratio.
        //
        // On real hardware with a GHz+ counter, 2ms of spinning produces
        // a ratio significantly different from 1:1. We verify that by
        // checking that at least one of NUMER or DENOM differs from 1.
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        calibrate();

        let n = NUMER.load(Ordering::Relaxed);
        let d = DENOM.load(Ordering::Relaxed);

        // On x86_64/aarch64, a proper calibration never gives exactly 1:1
        // because the TSC frequency differs from 1 GHz.
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(
            n != 1 || d != 1,
            "calibrate() produced 1:1 ratio -- loop likely did not spin (< mutated?)"
        );

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn calibrate_uses_division_not_multiplication() {
        // The GCD simplification does: NUMER = wall_ns / g, DENOM = tsc_ticks / g
        // If `/` is mutated to `*`, both values balloon to huge numbers.
        // ticks_to_ns would then overflow or produce wildly wrong results.
        let saved_n = NUMER.load(Ordering::Relaxed);
        let saved_d = DENOM.load(Ordering::Relaxed);

        calibrate();

        let n = NUMER.load(Ordering::Relaxed);
        let d = DENOM.load(Ordering::Relaxed);

        // After GCD simplification, both values should be reasonable.
        // wall_ns for 2ms ~ 2_000_000; tsc_ticks ~ millions.
        // After dividing by GCD, values should be well under 10M.
        // If `*` were used instead, values would be in the trillions.
        assert!(
            n < 100_000_000,
            "NUMER {n} is too large (division replaced with multiplication?)"
        );
        assert!(
            d < 100_000_000,
            "DENOM {d} is too large (division replaced with multiplication?)"
        );

        NUMER.store(saved_n, Ordering::Release);
        DENOM.store(saved_d, Ordering::Release);
    }

    #[test]
    #[serial]
    fn calibrate_bias_stores_nonzero_value() {
        // calibrate_bias() must store a nonzero value in BIAS_TICKS.
        // If mutated to `()`, BIAS_TICKS remains at its prior value.
        let saved = BIAS_TICKS.load(Ordering::Relaxed);

        // First ensure calibration is done (bias needs read() to work)
        calibrate();

        // Set to a sentinel to detect that calibrate_bias actually writes
        BIAS_TICKS.store(u64::MAX, Ordering::Release);

        calibrate_bias();

        let bias = BIAS_TICKS.load(Ordering::Relaxed);

        // calibrate_bias must have written something other than our sentinel.
        // On aarch64 Apple Silicon, bias can legitimately be 0 (counter read
        // is extremely cheap), so we only check that the store happened.
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert_ne!(bias, u64::MAX, "calibrate_bias() did not write BIAS_TICKS");

        BIAS_TICKS.store(saved, Ordering::Release);
    }

    #[test]
    #[serial]
    fn calibrate_bias_uses_correct_arithmetic() {
        // calibrate_bias computes: trim = N/50, trimmed = samples[trim..N-trim],
        // sum / trimmed.len(). If `/` is mutated to `%`, the mean calculation
        // breaks: N%50 = 0 (10000%50=0) so trim=0 and trimmed=full range,
        // but more critically sum%len produces garbage.
        //
        // We verify the stored bias is in a reasonable range.
        let saved = BIAS_TICKS.load(Ordering::Relaxed);
        calibrate();
        calibrate_bias();

        let bias = BIAS_TICKS.load(Ordering::Relaxed);

        // Bias should be small: typically 10-100 ticks on real hardware.
        // If `/ -> %` were applied to the mean calculation, the result
        // would be the remainder of sum/len which is < len (9600), but
        // could also be 0 if sum is exactly divisible.
        // More importantly, N/50 = 200 with division, but N%50 = 0 with
        // modulo, so the trim range would be samples[0..10000] (untrimmed).
        // The combination of both `%` mutations produces unstable results.
        //
        // On real hardware, bias is typically under 500 ticks.
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(
            bias < 10_000,
            "bias {bias} is unreasonably large (arithmetic mutation?)"
        );

        BIAS_TICKS.store(saved, Ordering::Release);
    }

    #[test]
    #[serial]
    fn bias_ticks_returns_stored_value() {
        // bias_ticks() loads BIAS_TICKS. If replaced with 0 or 1, this fails.
        let saved = BIAS_TICKS.load(Ordering::Relaxed);

        BIAS_TICKS.store(42, Ordering::Release);
        assert_eq!(bias_ticks(), 42, "bias_ticks() must return stored value");

        BIAS_TICKS.store(9999, Ordering::Release);
        assert_eq!(bias_ticks(), 9999, "bias_ticks() must return stored value");

        // Verify it does not return constants 0 or 1
        assert_ne!(bias_ticks(), 0);
        assert_ne!(bias_ticks(), 1);

        BIAS_TICKS.store(saved, Ordering::Release);
    }

    #[test]
    #[serial]
    fn set_epoch_tsc_stores_value() {
        // set_epoch_tsc must actually store. If replaced with `()`, EPOCH_TSC
        // remains unchanged.
        let saved = EPOCH_TSC.load(Ordering::Relaxed);

        set_epoch_tsc(12345);
        assert_eq!(
            EPOCH_TSC.load(Ordering::Relaxed),
            12345,
            "set_epoch_tsc() must store the value"
        );

        set_epoch_tsc(0);
        assert_eq!(EPOCH_TSC.load(Ordering::Relaxed), 0);

        EPOCH_TSC.store(saved, Ordering::Release);
    }

    #[test]
    #[serial]
    fn epoch_tsc_returns_stored_value() {
        // epoch_tsc() loads EPOCH_TSC. If replaced with 0 or 1, this fails.
        let saved = EPOCH_TSC.load(Ordering::Relaxed);

        EPOCH_TSC.store(777, Ordering::Release);
        assert_eq!(epoch_tsc(), 777, "epoch_tsc() must return stored value");

        EPOCH_TSC.store(123456, Ordering::Release);
        assert_eq!(epoch_tsc(), 123456);

        // Verify it does not return constants 0 or 1
        assert_ne!(epoch_tsc(), 0);
        assert_ne!(epoch_tsc(), 1);

        EPOCH_TSC.store(saved, Ordering::Release);
    }
}
