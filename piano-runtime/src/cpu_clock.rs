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

    #[test]
    fn cpu_now_ns_returns_nonzero() {
        // If cpu_now_ns is replaced with constant 0 or 1, this catches it.
        // After process startup, CPU time is always > 0 (we've already
        // executed code to get here).
        let now = cpu_now_ns();
        assert!(now > 1, "cpu_now_ns() returned {now}, expected > 1");
    }

    #[test]
    fn cpu_now_ns_is_monotonic_over_compute() {
        // Exercises all arithmetic operators in the conversion:
        // tv_sec * 1_000_000_000 + tv_nsec
        // If * becomes + or /, or + becomes - or *, the result would be
        // wildly wrong (too small, negative wrap, or too large).
        let t1 = cpu_now_ns();
        // Do enough compute to advance by at least a few microseconds.
        let mut acc = 0u64;
        for i in 0..100_000u64 {
            acc = acc.wrapping_add(i.wrapping_mul(7));
        }
        std::hint::black_box(acc);
        let t2 = cpu_now_ns();

        let delta = t2 - t1;
        // Should be at least 1us of CPU work, well under 10s.
        assert!(
            delta >= 1_000,
            "cpu_now_ns delta {delta}ns is too small -- arithmetic bug?"
        );
        assert!(
            delta < 10_000_000_000,
            "cpu_now_ns delta {delta}ns is too large -- arithmetic bug?"
        );
    }

    #[test]
    fn cpu_now_ns_scale_is_nanoseconds() {
        // Verify the result is in nanosecond scale, not seconds or microseconds.
        // A process that has been running for any time will have CPU time > 1us.
        // If * is replaced with + (tv_sec + 1B + tv_nsec), the result would
        // be ~1B ns = 1 second regardless of actual CPU time.
        // If + is replaced with * (tv_sec * 1B * tv_nsec), overflow or
        // garbage results.
        let now = cpu_now_ns();

        // Should be at least 1 microsecond (1000 ns) -- we've executed
        // test framework code to get here.
        assert!(
            now > 1_000,
            "cpu_now_ns() = {now}, expected at least 1us of CPU time"
        );

        // Should be less than 1 hour (sanity upper bound).
        let one_hour_ns = 3_600_000_000_000u64;
        assert!(
            now < one_hour_ns,
            "cpu_now_ns() = {now}, exceeds 1 hour -- likely arithmetic bug"
        );
    }
}
