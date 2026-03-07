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
pub fn read() -> u64 {
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
pub fn ticks_to_ns(ticks: u64) -> u64 {
    let n = NUMER.load(Ordering::Relaxed);
    let d = DENOM.load(Ordering::Relaxed);
    if d == 0 {
        return 0;
    }
    (ticks as u128 * n as u128 / d as u128) as u64
}

/// Convert a tick delta to nanoseconds using the calibrated ratio.
#[cfg(feature = "_test_internals")]
#[inline(always)]
pub fn elapsed_ns(start: u64, end: u64) -> u64 {
    ticks_to_ns(end.wrapping_sub(start))
}

/// Convert a tick value to nanoseconds-since-epoch for absolute timestamps.
#[cfg(any(test, feature = "_test_internals"))]
#[inline]
pub fn ticks_to_epoch_ns(ticks: u64, epoch_tsc: u64) -> u64 {
    ticks_to_ns(ticks.wrapping_sub(epoch_tsc))
}

/// Calibrate the tick-to-nanosecond ratio. Called once from `epoch()`.
///
/// Measures how many ticks elapse in a known wall-clock interval using
/// `Instant` as the reference. The calibration spin is brief (~1ms).
pub fn calibrate() {
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
pub fn calibrate_bias() {
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
pub fn bias_ticks() -> u64 {
    BIAS_TICKS.load(Ordering::Relaxed)
}

/// Load/store accessors for TSC calibration state.
///
/// Used by the `tsc_internals` integration test (which runs in a separate
/// binary to avoid corrupting globals visible to other in-process tests).
#[cfg(feature = "_test_internals")]
pub fn load_numer() -> u64 {
    NUMER.load(Ordering::Relaxed)
}
#[cfg(feature = "_test_internals")]
pub fn load_denom() -> u64 {
    DENOM.load(Ordering::Relaxed)
}
#[cfg(feature = "_test_internals")]
pub fn store_numer(val: u64) {
    NUMER.store(val, Ordering::Release);
}
#[cfg(feature = "_test_internals")]
pub fn store_denom(val: u64) {
    DENOM.store(val, Ordering::Release);
}
#[cfg(feature = "_test_internals")]
pub fn store_bias_ticks(val: u64) {
    BIAS_TICKS.store(val, Ordering::Release);
}
#[cfg(feature = "_test_internals")]
pub fn load_bias_ticks() -> u64 {
    BIAS_TICKS.load(Ordering::Relaxed)
}
#[cfg(feature = "_test_internals")]
pub fn store_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}
#[cfg(feature = "_test_internals")]
pub fn load_epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
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

pub fn set_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}

#[cfg(any(test, feature = "_test_internals"))]
pub fn epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // Tests that mutate global TSC state (NUMER, DENOM, BIAS_TICKS, EPOCH_TSC)
    // live in tests/tsc_internals.rs -- a separate binary that cannot corrupt
    // globals visible to other in-process unit tests.
}
