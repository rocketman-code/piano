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
    cpu_bias_ns: u64,
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
            cpu_bias_ns: 0,
        };

        // SAFETY: ONCE.call_once guarantees single initialization. After init,
        // CACHED is only read (never written). The static mut is required
        // because OnceLock is unavailable on MSRV 1.59.
        unsafe {
            ONCE.call_once(|| {
                let (quotient, multiplier, epoch_tsc) = calibrate();
                let bias_ticks = calibrate_bias();
                let cpu_bias_f64_bits = crate::cpu_clock::calibrate_bias();
                let cpu_bias_ns = f64::from_bits(cpu_bias_f64_bits).round() as u64;

                CACHED = CalibrationData {
                    quotient,
                    multiplier,
                    bias_ticks,
                    epoch_tsc,
                    cpu_bias_ns,
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
    ) -> Self {
        CalibrationData {
            quotient,
            multiplier,
            bias_ticks,
            epoch_tsc,
            cpu_bias_ns: 0,
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

    /// Return the calibrated CPU-time measurement bias in nanoseconds.
    #[inline(always)]
    pub fn cpu_bias_ns(&self) -> u64 {
        self.cpu_bias_ns
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
    // No memory safety implications: returns a u64 counter value.
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

/// Calibrate the tick-to-nanosecond ratio. Spins for CALIBRATION_SPIN_MS
/// measuring TSC ticks against Instant::now() as reference clock.
///
/// Returns (quotient, multiplier, epoch_tsc).
fn calibrate() -> (u64, u64, u64) {
    // Spin duration for tick-to-ns ratio measurement. Longer = more accurate
    // but slower startup. 2ms gives ~0.1% accuracy on modern hardware.
    const CALIBRATION_SPIN_MS: u64 = 2;

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() already returns nanoseconds, so Q=1, M=0.
        let _ = CALIBRATION_SPIN_MS;
        return (1, 0, 0);
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let wall_start = Instant::now();
        let tsc_start = read();

        let target = std::time::Duration::from_millis(CALIBRATION_SPIN_MS);
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
        const SAMPLES: usize = 10_000;
        const TRIM_PCT: usize = 2; // discard top/bottom 2% as outliers
        const TRIM_COUNT: usize = SAMPLES * TRIM_PCT / 100;

        let mut deltas = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let start = read();
            compiler_fence(Ordering::SeqCst);
            let end = read();
            deltas.push(end.wrapping_sub(start));
        }
        deltas.sort_unstable();
        let trimmed = &deltas[TRIM_COUNT..SAMPLES - TRIM_COUNT];
        let sum: u64 = trimmed.iter().sum();
        sum / trimmed.len() as u64
    }
}
