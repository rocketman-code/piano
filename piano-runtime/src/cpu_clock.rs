//! Per-thread CPU time measurement via `clock_gettime(CLOCK_THREAD_CPUTIME_ID)`.
//!
//! Gated behind `cfg(unix)`. Uses inline FFI to avoid adding a `libc`
//! dependency -- the runtime stays zero-dep.
//!
//! On non-Unix platforms, `cpu_now_ns` is a compilation stub containing
//! `unreachable!()`, since the `cpu_time_enabled` bool prevents it from
//! ever being called at runtime.

#[cfg(unix)]
#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

#[cfg(unix)]
const _: () = assert!(
    std::mem::size_of::<usize>() == 8,
    "cpu-time requires a 64-bit target"
);

#[cfg(all(unix, target_os = "linux"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

#[cfg(all(unix, target_os = "macos"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 16;

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
compile_error!("cpu-time is only supported on Linux and macOS");

#[cfg(unix)]
extern "C" {
    fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
}

#[cfg(unix)]
use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};

/// CPU-time bias stored as f64 bits in AtomicU64. Amortized measurement
/// gives sub-nanosecond precision, eliminating the 42ns quantum systematic
/// bias that per-call saturating_sub creates on Apple Silicon.
#[cfg(unix)]
static CPU_BIAS_F64: AtomicU64 = AtomicU64::new(0);

/// Guard instrumentation overhead (guard_cost - bias) stored as f64 bits.
/// Added to parent.cpu_children per child call to correct parent inflation.
#[cfg(unix)]
static GUARD_OVERHEAD_F64: AtomicU64 = AtomicU64::new(0);

/// Calibrate the measurement bias: amortized cost of tsc::read() per call,
/// matching the exit sequence overhead between body-end and cpu_end capture.
/// Called once from epoch() after TSC calibration.
#[cfg(unix)]
pub fn calibrate_bias() {
    const N: usize = 100_000;
    let start = cpu_now_ns();
    for _ in 0..N {
        compiler_fence(Ordering::SeqCst);
        crate::tsc::read();
    }
    let end = cpu_now_ns();
    let bias = (end - start) as f64 / N as f64;
    CPU_BIAS_F64.store(bias.to_bits(), Ordering::Release);
}

/// Return the calibrated CPU-time bias as f64 nanoseconds.
/// Used at aggregation for amortized correction: corrected = raw - calls * bias.
#[cfg(unix)]
#[inline(always)]
pub fn bias_f64() -> f64 {
    f64::from_bits(CPU_BIAS_F64.load(Ordering::Relaxed))
}

/// Return the calibrated CPU-time bias as integer nanoseconds.
#[cfg(all(any(test, feature = "_test_internals"), unix))]
#[inline(always)]
pub fn bias_ns() -> u64 {
    bias_f64() as u64
}

/// Return the guard instrumentation overhead as f64 nanoseconds.
/// This is the per-child-call cost that falls inside the parent's CPU bracket
/// but outside the child's raw elapsed.
#[cfg(unix)]
#[inline(always)]
pub fn guard_overhead_f64() -> f64 {
    f64::from_bits(GUARD_OVERHEAD_F64.load(Ordering::Relaxed))
}

/// Return the guard instrumentation overhead as integer nanoseconds.
/// Truncates the f64 value (~0.5ns/call error, <0.1% of typical ~50ns value).
#[cfg(unix)]
#[inline(always)]
pub fn guard_overhead_ns() -> u64 {
    guard_overhead_f64() as u64
}

/// Store the calibrated guard overhead (called from collector after calibration).
#[cfg(unix)]
pub fn store_guard_overhead(val: f64) {
    GUARD_OVERHEAD_F64.store(val.to_bits(), Ordering::Release);
}

#[cfg(all(feature = "_test_internals", unix))]
pub fn store_cpu_bias_ns(val: u64) {
    CPU_BIAS_F64.store((val as f64).to_bits(), Ordering::Release);
}
#[cfg(all(feature = "_test_internals", unix))]
pub fn load_cpu_bias_ns() -> u64 {
    bias_ns()
}
#[cfg(all(feature = "_test_internals", unix))]
pub fn store_guard_overhead_ns(val: u64) {
    store_guard_overhead(val as f64);
}
#[cfg(all(feature = "_test_internals", unix))]
pub fn load_guard_overhead_ns() -> u64 {
    guard_overhead_ns()
}

/// Return the current thread's CPU time in nanoseconds.
///
/// Uses `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` which measures only time
/// the current thread spent executing on a CPU core. Sleeps, I/O waits,
/// and scheduling delays read as zero.
#[cfg(unix)]
pub fn cpu_now_ns() -> u64 {
    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime is a standard POSIX function. We pass a valid
    // pointer to a stack-allocated Timespec and a valid clock ID.
    let ret = unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if ret != 0 {
        return 0;
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Compilation stub for non-Unix platforms.
///
/// The `cpu_time_enabled` bool prevents this from ever being called at
/// runtime, but the function must exist so call sites compile without
/// cfg guards.
#[cfg(not(unix))]
pub(crate) fn cpu_now_ns() -> u64 {
    unreachable!("cpu_now_ns called on non-Unix; gated by cpu_time_enabled bool")
}
