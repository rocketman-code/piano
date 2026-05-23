//! Domain types for the CLI reader pipeline.
//!
//! Source of truth: piano.carve (spec) -> carve -> carve-build ->
//! generated Rust. Types are checked in, not build-time generated.
//! Regenerate when the spec changes; CI checks freshness.

use crate::report::load::{ParsedCpu, ParsedWall};

// ── Corrected wall clock ────────────────────────────────────────

/// Wall-clock nanoseconds after bias subtraction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CorrectedWall(u64);

impl CorrectedWall {
    pub(crate) fn as_ms(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

// ── Corrected CPU time ──────────────────────────────────────────

/// CPU-time nanoseconds after bias subtraction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CorrectedCpu(u64);

impl CorrectedCpu {
    pub(crate) fn as_ms(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

// ── Bias correction ─────────────────────────────────────────────

/// Subtract per-call measurement bias from parsed wall time.
pub(crate) fn apply_wall_bias(
    parsed: ParsedWall,
    bias_per_call: ParsedWall,
    calls: u64,
) -> CorrectedWall {
    CorrectedWall(
        parsed
            .raw()
            .saturating_sub(bias_per_call.raw().saturating_mul(calls)),
    )
}

/// Subtract per-call measurement bias from parsed CPU time.
pub(crate) fn apply_cpu_bias(
    parsed: ParsedCpu,
    bias_per_call: ParsedCpu,
    calls: u64,
) -> CorrectedCpu {
    CorrectedCpu(
        parsed
            .raw()
            .saturating_sub(bias_per_call.raw().saturating_mul(calls)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bias_correction_produces_corrected_wall() {
        let parsed = ParsedWall::new_test(100_000);
        let bias = ParsedWall::new_test(10_000);
        let corrected = apply_wall_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.07);
    }

    #[test]
    fn bias_correction_saturates_at_zero() {
        let parsed = ParsedWall::new_test(1_000);
        let bias = ParsedWall::new_test(10_000);
        let corrected = apply_wall_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.0);
    }

    #[test]
    fn bias_correction_produces_corrected_cpu() {
        let parsed = ParsedCpu::new_test(80_000);
        let bias = ParsedCpu::new_test(5_000);
        let corrected = apply_cpu_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.065);
    }
}
