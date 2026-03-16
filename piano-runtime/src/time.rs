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
pub(crate) fn read() -> u64 {
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
pub(crate) fn ticks_to_ns(ticks: u64) -> u64 {
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
pub(crate) fn now_ns(raw_ticks: u64) -> u64 {
    init();
    ticks_to_ns(raw_ticks.wrapping_sub(EPOCH_TSC.load(Ordering::Relaxed)))
}

/// Return the calibrated measurement bias in nanoseconds.
/// This is the cost of two clock reads (the irreducible minimum).
/// Written to NDJSON header/trailer for report reader correction.
pub(crate) fn bias_ns() -> u64 {
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
fn calibrate() {
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
fn calibrate_bias() {
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
