//! Domain types for the CLI reader pipeline.
//!
//! ParsedWall and CorrectedWall are distinct types -- the bias
//! correction boundary is type-enforced.

use std::ops::AddAssign;

// ── Parsed wall clock ───────────────────────────────────────────

/// Wall-clock nanoseconds from NDJSON deserialization, before bias correction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct ParsedWall(pub(crate) u64);

impl ParsedWall {
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl AddAssign for ParsedWall {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

// ── Corrected wall clock ────────────────────────────────────────

/// Wall-clock nanoseconds after bias subtraction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CorrectedWall(u64);

impl CorrectedWall {
    pub(crate) fn as_ms(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

// ── Parsed CPU time ─────────────────────────────────────────────

/// CPU-time nanoseconds from NDJSON deserialization, before bias correction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct ParsedCpu(pub(crate) u64);

impl ParsedCpu {
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl AddAssign for ParsedCpu {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
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

// ── Allocation deltas ───────────────────────────────────────────

/// Allocation delta counters (alloc minus free).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ParsedAlloc {
    pub(crate) alloc_count: u64,
    pub(crate) alloc_bytes: u64,
    pub(crate) free_count: u64,
    pub(crate) free_bytes: u64,
}

impl AddAssign for ParsedAlloc {
    fn add_assign(&mut self, rhs: Self) {
        self.alloc_count += rhs.alloc_count;
        self.alloc_bytes += rhs.alloc_bytes;
        self.free_count += rhs.free_count;
        self.free_bytes += rhs.free_bytes;
    }
}

// ── Function identity ───────────────────────────────────────────

/// Stable function identity for cross-run matching.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StableIdentity(pub String);

// ── Bias correction ─────────────────────────────────────────────

/// Subtract per-call measurement bias from parsed wall time.
pub(crate) fn apply_wall_bias(
    parsed: ParsedWall,
    bias_per_call: ParsedWall,
    calls: u64,
) -> CorrectedWall {
    CorrectedWall(
        parsed
            .0
            .saturating_sub(bias_per_call.0.saturating_mul(calls)),
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
            .0
            .saturating_sub(bias_per_call.0.saturating_mul(calls)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bias_correction_produces_corrected_wall() {
        let parsed = ParsedWall(100_000);
        let bias = ParsedWall(10_000);
        let corrected = apply_wall_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.07);
    }

    #[test]
    fn bias_correction_saturates_at_zero() {
        let parsed = ParsedWall(1_000);
        let bias = ParsedWall(10_000);
        let corrected = apply_wall_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.0);
    }

    #[test]
    fn bias_correction_produces_corrected_cpu() {
        let parsed = ParsedCpu(80_000);
        let bias = ParsedCpu(5_000);
        let corrected = apply_cpu_bias(parsed, bias, 3);
        assert_eq!(corrected.as_ms(), 0.065);
    }
}
