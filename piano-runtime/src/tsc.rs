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

/// Fixed-point conversion from ticks to nanoseconds.
///
/// Decomposition: `ns = ticks * Q + (ticks * M >> 64)`
/// where `Q = wall_ns / tsc_ticks` (integer quotient) and
/// `M = ceil(R * 2^64 / tsc_ticks)` with `R = wall_ns % tsc_ticks`.
///
/// On aarch64, `(ticks as u128 * M as u128) >> 64` compiles to a single
/// `umulh` instruction. This replaces the previous `u128 / u128` path
/// which compiled to a `bl ___udivti3` software division call.
///
/// Stored as atomics so that `calibrate()` (called once from `epoch()`)
/// publishes them with Release and `ticks_to_ns()` reads with Relaxed
/// (after the Once-guarded init, every thread sees the calibrated values).
static QUOTIENT: AtomicU64 = AtomicU64::new(0);
static MULTIPLIER: AtomicU64 = AtomicU64::new(0);

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
///
/// Uses fixed-point multiplication: `ticks * Q + (ticks * M >> 64)`.
/// The `u128 * u128 >> 64` compiles to `umulh` on aarch64 (1 instruction)
/// and `mulq` on x86_64, replacing the previous `___udivti3` software
/// division (~2 ns -> ~0.4 ns on M4 Pro).
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
/// `Instant` as the reference. The calibration spin is brief (~2ms).
///
/// Decomposes the ratio `wall_ns / tsc_ticks` into a fixed-point form:
/// `Q = wall_ns / tsc_ticks`, `R = wall_ns % tsc_ticks`,
/// `M = ceil(R * 2^64 / tsc_ticks)`.
pub fn calibrate() {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        // Fallback: read() already returns nanoseconds, so Q=1, M=0.
        QUOTIENT.store(1, Ordering::Release);
        MULTIPLIER.store(0, Ordering::Release);
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
            QUOTIENT.store(1, Ordering::Release);
            MULTIPLIER.store(0, Ordering::Release);
            return;
        }

        // Decompose: ns = ticks * Q + (ticks * M >> 64)
        let q = wall_ns / tsc_ticks;
        let r = wall_ns % tsc_ticks;
        let m = if r == 0 {
            0u64
        } else {
            // ceil(r * 2^64 / tsc_ticks)
            let product = (r as u128) << 64;
            let div = product / tsc_ticks as u128;
            let rem = product % tsc_ticks as u128;
            if rem > 0 {
                (div + 1) as u64
            } else {
                div as u64
            }
        };

        QUOTIENT.store(q, Ordering::Release);
        MULTIPLIER.store(m, Ordering::Release);
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
    let m = if r == 0 {
        0u64
    } else {
        let product = (r as u128) << 64;
        let div = product / denom as u128;
        let rem = product % denom as u128;
        if rem > 0 {
            (div + 1) as u64
        } else {
            div as u64
        }
    };
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

/// The TSC value captured at epoch. Stored alongside the Instant epoch.
static EPOCH_TSC: AtomicU64 = AtomicU64::new(0);

pub fn set_epoch_tsc(val: u64) {
    EPOCH_TSC.store(val, Ordering::Release);
}

#[cfg(any(test, feature = "_test_internals"))]
pub fn epoch_tsc() -> u64 {
    EPOCH_TSC.load(Ordering::Relaxed)
}

// Tests that mutate global TSC state (QUOTIENT, MULTIPLIER, BIAS_TICKS,
// EPOCH_TSC) live in tests/tsc_internals.rs -- a separate binary that
// cannot corrupt globals visible to other in-process unit tests.
