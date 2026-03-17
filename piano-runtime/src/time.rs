#![allow(unsafe_code)]

//! Profiling clock and timing subsystem.
//!
//! On x86_64 and aarch64, uses hardware cycle counters (RDTSC / CNTVCT)
//! for ~2-5ns reads (vs ~15-30ns for Instant::now()). On other platforms,
//! falls back to Instant::now().
//!
//! Calibration runs once at startup via `CalibrationData::calibrate()`:
//! 1. calibrate() -- measures tick-to-nanosecond ratio via 2ms spin
//! 2. calibrate_bias() -- 10K back-to-back read pairs, trimmed mean (2% trim)
//! 3. cpu_clock::calibrate_bias() -- amortized CPU-time read cost
//!
//! All calibration state lives in the `CalibrationData` struct, threaded
//! by value through Ctx -> Guard / PianoFuture. No global atomics.

use std::sync::atomic::{compiler_fence, Ordering};
use std::sync::Once;
use std::time::Instant;

/// Immutable calibration snapshot. Copy-able, no atomics on access.
///
/// Created once per process by `CalibrationData::calibrate()`, then
/// threaded through Ctx -> Guard / PianoFuture by value. Replaces
/// the global AtomicU64 loads on every timing conversion with plain
/// field reads.
#[derive(Clone, Copy)]
pub struct CalibrationData {
    quotient: u64,
    multiplier: u64,
    bias_ticks: u64,
    epoch_tsc: u64,
    cpu_bias_f64_bits: u64,
    guard_overhead_f64_bits: u64,
}

impl CalibrationData {
    /// Run time + CPU bias calibration (once per process).
    ///
    /// Uses a Once + static inside the function so repeated calls
    /// return the cached Copy value after the first.
    pub fn calibrate() -> Self {
        static ONCE: Once = Once::new();
        static mut CACHED: CalibrationData = CalibrationData {
            quotient: 0,
            multiplier: 0,
            bias_ticks: 0,
            epoch_tsc: 0,
            cpu_bias_f64_bits: 0,
            guard_overhead_f64_bits: 0,
        };

        // SAFETY: ONCE.call_once guarantees single initialization. After init,
        // CACHED is only read (never written). The static mut is required
        // because OnceLock is unavailable on MSRV 1.59.
        unsafe {
            ONCE.call_once(|| {
                let (quotient, multiplier, epoch_tsc) = calibrate();
                let bias_ticks = calibrate_bias();
                let cpu_bias_f64_bits = crate::cpu_clock::calibrate_bias();

                CACHED = CalibrationData {
                    quotient,
                    multiplier,
                    bias_ticks,
                    epoch_tsc,
                    cpu_bias_f64_bits,
                    guard_overhead_f64_bits: 0,
                };
            });
            CACHED
        }
    }

    /// Construct with explicit values (for integration tests).
    #[cfg(feature = "_test_internals")]
    pub fn new_test(
        quotient: u64,
        multiplier: u64,
        bias_ticks: u64,
        epoch_tsc: u64,
        cpu_bias_f64_bits: u64,
        guard_overhead_f64_bits: u64,
    ) -> Self {
        CalibrationData {
            quotient,
            multiplier,
            bias_ticks,
            epoch_tsc,
            cpu_bias_f64_bits,
            guard_overhead_f64_bits,
        }
    }

    /// Convert raw ticks to nanoseconds using calibrated fixed-point ratio.
    #[inline(always)]
    pub fn ticks_to_ns(&self, ticks: u64) -> u64 {
        if self.quotient == 0 && self.multiplier == 0 {
            return 0;
        }
        let hi = ((ticks as u128 * self.multiplier as u128) >> 64) as u64;
        ticks.wrapping_mul(self.quotient).wrapping_add(hi)
    }

    /// Convert a raw tick value to epoch-relative nanoseconds.
    #[inline(always)]
    pub fn now_ns(&self, raw_ticks: u64) -> u64 {
        self.ticks_to_ns(raw_ticks.wrapping_sub(self.epoch_tsc))
    }

    /// Return the calibrated measurement bias in nanoseconds.
    #[inline(always)]
    pub fn bias_ns(&self) -> u64 {
        self.ticks_to_ns(self.bias_ticks)
    }

    /// Return the calibrated CPU-time bias as f64 nanoseconds.
    #[cfg(unix)]
    #[inline(always)]
    pub fn cpu_bias_f64(&self) -> f64 {
        f64::from_bits(self.cpu_bias_f64_bits)
    }

    /// Return the guard instrumentation overhead as f64 nanoseconds.
    #[cfg(unix)]
    #[inline(always)]
    pub fn guard_overhead_f64(&self) -> f64 {
        f64::from_bits(self.guard_overhead_f64_bits)
    }
}

/// Read the hardware cycle counter. Returns raw ticks.
/// On x86_64: rdtsc (~2-5ns).
/// On aarch64: mrs cntvct_el0 (~2-5ns).
/// Fallback: Instant-based epoch-relative nanoseconds.
#[inline(always)]
pub fn read() -> u64 {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: _rdtsc is a stable intrinsic that reads the timestamp counter.
    // No memory safety implications — returns a u64 counter value.
    unsafe {
        core::arch::x86_64::_rdtsc()
    }

    #[cfg(target_arch = "aarch64")]
    {
        let val: u64;
        // SAFETY: mrs cntvct_el0 reads the generic timer counter register.
        // Architecturally guaranteed available at EL0 on all aarch64.
        unsafe { core::arch::asm!("mrs {}, cntvct_el0", out(reg) val) };
        val
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: Instant-based. Uses Once + static mut for MSRV 1.59
        // (OnceLock requires 1.70). Uses its own epoch to avoid deadlock
        // with calibrate() which also reads the clock.
        static FALLBACK_ONCE: Once = Once::new();
        static mut FALLBACK_EPOCH: Option<Instant> = None;
        // SAFETY: FALLBACK_ONCE.call_once guarantees single initialization.
        // After init, FALLBACK_EPOCH is only read (never written).
        unsafe {
            FALLBACK_ONCE.call_once(|| {
                FALLBACK_EPOCH = Some(Instant::now());
            });
            Instant::now()
                .saturating_duration_since(FALLBACK_EPOCH.unwrap_or_else(Instant::now))
                .as_nanos() as u64
        }
    }
}

/// Compute ceil(remainder * 2^64 / divisor) as u64.
/// Fixed-point multiplier for the remainder term of tick-to-ns conversion.
fn compute_multiplier(remainder: u64, divisor: u64) -> u64 {
    if remainder == 0 {
        return 0;
    }
    let product = (remainder as u128) << 64;
    let div = product / divisor as u128;
    let rem = product % divisor as u128;
    if rem > 0 {
        (div + 1) as u64
    } else {
        div as u64
    }
}

/// Calibrate the tick-to-nanosecond ratio. Spins for ~2ms measuring
/// TSC ticks against Instant::now() as reference clock.
///
/// Returns (quotient, multiplier, epoch_tsc).
fn calibrate() -> (u64, u64, u64) {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() already returns nanoseconds, so Q=1, M=0.
        return (1, 0, 0);
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let wall_start = Instant::now();
        let tsc_start = read();

        let target = std::time::Duration::from_millis(2);
        while wall_start.elapsed() < target {}

        let tsc_end = read();
        let wall_ns = wall_start.elapsed().as_nanos() as u64;
        let tsc_ticks = tsc_end.wrapping_sub(tsc_start);

        if tsc_ticks == 0 {
            return (1, 0, tsc_start);
        }

        let q = wall_ns / tsc_ticks;
        let r = wall_ns % tsc_ticks;
        let m = compute_multiplier(r, tsc_ticks);

        (q, m, tsc_start)
    }
}

/// Calibrate the measurement bias (cost of a TSC read pair in ticks).
/// Uses trimmed mean (2% trim) for robustness against VM preemption.
///
/// Returns bias_ticks.
fn calibrate_bias() -> u64 {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        return 0;
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        const N: usize = 10_000;
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = read();
            compiler_fence(Ordering::SeqCst);
            let end = read();
            samples.push(end.wrapping_sub(start));
        }
        samples.sort_unstable();
        let trim = N / 50; // 2% trim
        let trimmed = &samples[trim..N - trim];
        let sum: u64 = trimmed.iter().sum();
        sum / trimmed.len() as u64
    }
}

/// Compute quotient and multiplier from a numer/denom ratio (test convenience).
///
/// Useful for constructing CalibrationData::new_test with a human-readable
/// ratio like (3, 2) instead of computing fixed-point values manually.
#[cfg(feature = "_test_internals")]
pub fn ratio_to_qm(numer: u64, denom: u64) -> (u64, u64) {
    if denom == 0 {
        return (0, 0);
    }
    let q = numer / denom;
    let r = numer % denom;
    let m = compute_multiplier(r, denom);
    (q, m)
}
