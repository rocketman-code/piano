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
use std::sync::atomic::{compiler_fence, Ordering};

/// Calibrate the measurement bias: amortized cost of time::read() per call,
/// matching the exit sequence overhead between body-end and cpu_end capture.
/// Called once from CalibrationData::calibrate() after TSC calibration.
///
/// Returns the bias as f64 bits (f64::to_bits).
#[cfg(unix)]
pub fn calibrate_bias() -> u64 {
    // 100K iterations: enough to amortize syscall jitter while keeping
    // calibration under 1ms. Higher than time.rs's 10K because CPU-time
    // reads are noisier (syscall vs TSC instruction).
    const SAMPLES: usize = 100_000;
    let start = cpu_now_ns();
    for _ in 0..SAMPLES {
        compiler_fence(Ordering::SeqCst);
        crate::time::read();
    }
    let end = cpu_now_ns();
    let bias = (end - start) as f64 / SAMPLES as f64;
    bias.to_bits()
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
    const NS_PER_SEC: u64 = 1_000_000_000;
    ts.tv_sec as u64 * NS_PER_SEC + ts.tv_nsec as u64
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

/// No CPU-time calibration on non-Unix platforms. Returns 0 (no bias).
#[cfg(not(unix))]
pub fn calibrate_bias() -> u64 {
    0
}
