#![allow(unsafe_code)]

//! Profiling clock and timing subsystem.
//!
//! On x86_64 and aarch64, uses hardware cycle counters (RDTSC / CNTVCT)
//! for ~2-5ns reads (vs ~15-30ns for Instant::now()). On other platforms,
//! falls back to Instant::now().
//!
//! Calibration runs once at startup:
//! 1. calibrate() — measures tick-to-nanosecond ratio via 2ms spin
//! 2. calibrate_bias() — 10K back-to-back read pairs, trimmed mean (2% trim)
//!
//! Invariants:
//! - init() must be called before read(). Ctx::new() calls init().
//! - ticks_to_ns() converts raw ticks to nanoseconds using fixed-point
//!   arithmetic: ns = ticks * Q + (ticks * M >> 64).
//! - bias_ns() returns the calibrated measurement overhead in nanoseconds.
//! - All calibration state is published with Release ordering and read
//!   with Relaxed (init via Once guarantees visibility).

use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};
use std::sync::Once;
use std::time::Instant;

static QUOTIENT: AtomicU64 = AtomicU64::new(0);
static MULTIPLIER: AtomicU64 = AtomicU64::new(0);
static BIAS_TICKS: AtomicU64 = AtomicU64::new(0);
static EPOCH_TSC: AtomicU64 = AtomicU64::new(0);

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
                // Run existing calibration routines (writes to globals)
                calibrate();
                calibrate_bias();
                crate::cpu_clock::calibrate_bias();

                // Snapshot the globals into the struct
                CACHED = CalibrationData {
                    quotient: QUOTIENT.load(Ordering::Relaxed),
                    multiplier: MULTIPLIER.load(Ordering::Relaxed),
                    bias_ticks: BIAS_TICKS.load(Ordering::Relaxed),
                    epoch_tsc: EPOCH_TSC.load(Ordering::Relaxed),
                    cpu_bias_f64_bits: crate::cpu_clock::bias_f64_bits(),
                    guard_overhead_f64_bits: crate::cpu_clock::guard_overhead_f64_bits(),
                };
            });
            CACHED
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

/// Initialize the timing subsystem. Must be called before read().
/// Safe to call multiple times (Once guard).
pub(crate) fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        calibrate();
        calibrate_bias();
    });
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
        // with init() which also reads the clock.
        static FALLBACK_ONCE: Once = Once::new();
        static mut FALLBACK_EPOCH: Option<Instant> = None;
        // SAFETY: FALLBACK_ONCE.call_once guarantees single initialization.
        // After init, FALLBACK_EPOCH is only read (never written). Same
        // pattern as init() above.
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

/// Convert raw ticks to nanoseconds using calibrated fixed-point ratio.
/// ns = ticks * Q + (ticks * M >> 64)
///
/// On aarch64, the (ticks * M >> 64) compiles to a single umulh instruction.
/// On fallback platforms (Q=1, M=0), this is a no-op multiplication.
#[inline(always)]
pub fn ticks_to_ns(ticks: u64) -> u64 {
    let q = QUOTIENT.load(Ordering::Relaxed);
    let m = MULTIPLIER.load(Ordering::Relaxed);
    if q == 0 && m == 0 {
        return 0;
    }
    let hi = ((ticks as u128 * m as u128) >> 64) as u64;
    ticks.wrapping_mul(q).wrapping_add(hi)
}

/// Convert a raw tick value to epoch-relative nanoseconds.
/// Used in Guard::drop and PianoFuture::drop (cold path, outside
/// measurement window). Calls init() internally so conversion works
/// even if Ctx::new() hasn't been called (e.g. direct Guard/PianoFuture
/// construction in tests). The init() call is a no-op after first
/// invocation (Once atomic load on fast path).
#[inline(always)]
pub fn now_ns(raw_ticks: u64) -> u64 {
    init();
    ticks_to_ns(raw_ticks.wrapping_sub(EPOCH_TSC.load(Ordering::Relaxed)))
}

/// Return the calibrated measurement bias in nanoseconds.
/// This is the cost of two clock reads (the irreducible minimum).
/// Written to NDJSON header/trailer for report reader correction.
pub fn bias_ns() -> u64 {
    ticks_to_ns(BIAS_TICKS.load(Ordering::Relaxed))
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
pub fn calibrate() {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() already returns nanoseconds, so Q=1, M=0.
        QUOTIENT.store(1, Ordering::Release);
        MULTIPLIER.store(0, Ordering::Release);
        EPOCH_TSC.store(0, Ordering::Release);
        return;
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
            QUOTIENT.store(1, Ordering::Release);
            MULTIPLIER.store(0, Ordering::Release);
            EPOCH_TSC.store(tsc_start, Ordering::Release);
            return;
        }

        let q = wall_ns / tsc_ticks;
        let r = wall_ns % tsc_ticks;
        let m = compute_multiplier(r, tsc_ticks);

        QUOTIENT.store(q, Ordering::Release);
        MULTIPLIER.store(m, Ordering::Release);
        EPOCH_TSC.store(tsc_start, Ordering::Release);
    }
}

/// Calibrate the measurement bias (cost of a TSC read pair in ticks).
/// Uses trimmed mean (2% trim) for robustness against VM preemption.
pub fn calibrate_bias() {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        return;
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
        let mean_ticks = sum / trimmed.len() as u64;
        BIAS_TICKS.store(mean_ticks, Ordering::Release);
    }
}

/// Return the calibrated bias in raw ticks.
#[inline(always)]
pub fn bias_ticks() -> u64 {
    BIAS_TICKS.load(Ordering::Relaxed)
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

/// Store the TSC value captured at epoch.
pub fn set_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}

/// Return the TSC value captured at epoch.
#[cfg(any(test, feature = "_test_internals"))]
pub fn epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
}

// ---- Test accessor functions ----
//
// Load/store accessors for calibration state. Used by integration tests
// (which run in separate binaries to avoid corrupting globals visible to
// other in-process tests).

#[cfg(feature = "_test_internals")]
pub fn load_quotient() -> u64 {
    QUOTIENT.load(Ordering::Relaxed)
}
#[cfg(feature = "_test_internals")]
pub fn load_multiplier() -> u64 {
    MULTIPLIER.load(Ordering::Relaxed)
}
#[cfg(feature = "_test_internals")]
pub fn store_quotient(val: u64) {
    QUOTIENT.store(val, Ordering::Release);
}
#[cfg(feature = "_test_internals")]
pub fn store_multiplier(val: u64) {
    MULTIPLIER.store(val, Ordering::Release);
}

/// Set conversion from a numer/denom ratio (test convenience).
///
/// Computes Q and M from a fractional ratio so tests can express intent
/// as "set ratio to 3/2" without computing fixed-point values manually.
#[cfg(feature = "_test_internals")]
pub fn store_ratio(numer: u64, denom: u64) {
    if denom == 0 {
        QUOTIENT.store(0, Ordering::Release);
        MULTIPLIER.store(0, Ordering::Release);
        return;
    }
    let q = numer / denom;
    let r = numer % denom;
    let m = compute_multiplier(r, denom);
    QUOTIENT.store(q, Ordering::Release);
    MULTIPLIER.store(m, Ordering::Release);
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
