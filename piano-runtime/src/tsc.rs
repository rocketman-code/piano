//! Fast inline timestamp via hardware counter (TSC on x86_64, CNTVCT on aarch64).
//!
//! `read()` returns raw counter ticks. `elapsed_ns()` converts a tick delta
//! to nanoseconds using a ratio calibrated once at startup.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Numerator/denominator for converting ticks to nanoseconds.
/// `ns = ticks * NUMER / DENOM`
///
/// Stored as atomics so that `calibrate()` (called once from `epoch()`)
/// publishes them with Release and `elapsed_ns()` reads with Relaxed
/// (after the Once-guarded init, every thread sees the calibrated values).
static NUMER: AtomicU64 = AtomicU64::new(0);
static DENOM: AtomicU64 = AtomicU64::new(1);

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

/// Convert a tick delta to nanoseconds using the calibrated ratio.
#[inline(always)]
pub(crate) fn elapsed_ns(start: u64, end: u64) -> u64 {
    let ticks = end.wrapping_sub(start);
    let n = NUMER.load(Ordering::Relaxed);
    let d = DENOM.load(Ordering::Relaxed);
    // Use u128 to avoid overflow on large tick counts
    (ticks as u128 * n as u128 / d as u128) as u64
}

/// Convert a tick value to nanoseconds-since-epoch for absolute timestamps.
#[inline]
pub(crate) fn ticks_to_epoch_ns(ticks: u64, epoch_tsc: u64) -> u64 {
    elapsed_ns(epoch_tsc, ticks)
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

        // ns = ticks * wall_ns / tsc_ticks
        // Simplify the fraction to avoid overflow in elapsed_ns
        let g = gcd(wall_ns, tsc_ticks);
        NUMER.store(wall_ns / g, Ordering::Release);
        DENOM.store(tsc_ticks / g, Ordering::Release);
    }
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

/// The TSC value captured at epoch. Stored alongside the Instant epoch.
static EPOCH_TSC: AtomicU64 = AtomicU64::new(0);

pub(crate) fn set_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}

pub(crate) fn epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
}
