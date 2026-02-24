//! Per-thread CPU time measurement via `clock_gettime(CLOCK_THREAD_CPUTIME_ID)`.
//!
//! Gated behind the `cpu-time` Cargo feature. Uses inline FFI to avoid
//! adding a `libc` dependency -- the runtime stays zero-dep.

#[cfg(feature = "cpu-time")]
#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

#[cfg(feature = "cpu-time")]
const _: () = assert!(
    std::mem::size_of::<usize>() == 8,
    "cpu-time feature requires a 64-bit target"
);

#[cfg(all(feature = "cpu-time", target_os = "linux"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

#[cfg(all(feature = "cpu-time", target_os = "macos"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 16;

#[cfg(all(
    feature = "cpu-time",
    not(any(target_os = "linux", target_os = "macos"))
))]
compile_error!("cpu-time feature is only supported on Linux and macOS");

#[cfg(feature = "cpu-time")]
extern "C" {
    fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
}

/// Return the current thread's CPU time in nanoseconds.
///
/// Uses `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` which measures only time
/// the current thread spent executing on a CPU core. Sleeps, I/O waits,
/// and scheduling delays read as zero.
#[cfg(feature = "cpu-time")]
pub(crate) fn cpu_now_ns() -> u64 {
    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let ret = unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    debug_assert!(ret == 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(test)]
#[cfg(feature = "cpu-time")]
mod tests {
    use super::*;

    #[test]
    fn cpu_time_advances_during_compute() {
        let before = cpu_now_ns();
        let mut buf = [0u8; 4096];
        for i in 0u64..50_000 {
            for b in &mut buf {
                *b = b.wrapping_add(i as u8).wrapping_mul(31);
            }
        }
        std::hint::black_box(&buf);
        let after = cpu_now_ns();
        assert!(
            after > before,
            "CPU clock should advance during compute: before={before}, after={after}"
        );
    }

    #[test]
    fn cpu_time_does_not_advance_during_sleep() {
        let before = cpu_now_ns();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let after = cpu_now_ns();
        let delta_ms = (after - before) as f64 / 1_000_000.0;
        assert!(
            delta_ms < 5.0,
            "CPU clock should not advance during sleep, but delta was {delta_ms:.2}ms"
        );
    }
}
