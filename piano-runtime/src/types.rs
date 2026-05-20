//! Domain types for the measurement pipeline.
//!
//! Each type is derived from the carve spec. Fields are private --
//! only the operation that establishes the property can construct
//! the value. Consumers read through public accessors.

use crate::alloc::{AllocDelta, AllocSnapshot};

// ── MeasurementValue entity: Ticks ──────────────────────────────

/// Raw hardware counter value (TSC on x86_64, CNTVCT on aarch64,
/// epoch-relative nanoseconds on fallback platforms).
/// Spec: MeasurementValue::Ticks.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct Ticks(u64);

impl Ticks {
    pub(crate) const ZERO: Self = Ticks(0);
    pub(crate) fn from_raw(v: u64) -> Self {
        Self(v)
    }
    pub fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn wrapping_sub(self, other: Ticks) -> Ticks {
        Ticks(self.0.wrapping_sub(other.0))
    }
}

// ── MeasurementValue entity: WallNs ─────────────────────────────

/// Calibrated wall-clock nanoseconds, epoch-relative.
/// Spec: MeasurementValue::WallNs.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct WallNs(u64);

impl WallNs {
    pub(crate) const ZERO: Self = WallNs(0);
    pub(crate) fn from_raw(v: u64) -> Self {
        Self(v)
    }
    pub fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn saturating_sub(self, other: WallNs) -> WallNs {
        WallNs(self.0.saturating_sub(other.0))
    }
}

#[cfg(feature = "_test_internals")]
impl WallNs {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}

impl core::ops::AddAssign for WallNs {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

// ── MeasurementValue entity: CpuNs ─────────────────────────────

/// CPU-time nanoseconds (per-thread, from clock_gettime).
/// Spec: MeasurementValue::CpuNs.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct CpuNs(u64);

impl CpuNs {
    pub(crate) const ZERO: Self = CpuNs(0);
    pub(crate) fn from_raw(v: u64) -> Self {
        Self(v)
    }
    pub fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn saturating_sub(self, other: CpuNs) -> CpuNs {
        CpuNs(self.0.saturating_sub(other.0))
    }
}

#[cfg(feature = "_test_internals")]
impl CpuNs {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
}

impl core::ops::AddAssign for CpuNs {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

// ── FunctionId entity: NameId ───────────────────────────────────

/// Function name ID (assigned by rewriter, consumed by runtime).
/// Spec: FunctionId::Assigned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct NameId(u32);

impl NameId {
    pub(crate) fn from_raw(v: u32) -> Self {
        Self(v)
    }
    pub fn raw(self) -> u32 {
        self.0
    }
}

#[cfg(feature = "_test_internals")]
impl NameId {
    pub fn new(v: u32) -> Self {
        Self(v)
    }
}

// ── AsyncExecution entity: PollActive ───────────────────────────

/// Pre-poll measurement snapshots. Consumed by `end_poll` to compute deltas.
/// Spec: AsyncExecution Polling state (capture_poll_start output).
pub(crate) struct PollActive {
    pub(crate) wall_start: Ticks,
    pub(crate) cpu_start: CpuNs,
    pub(crate) alloc_start: AllocSnapshot,
}

/// Per-poll measurement deltas. Must be destructured exhaustively.
/// Spec: AsyncExecution PollComplete -> Accumulated transition.
/// Adding a field forces every consumer to handle it at compile time.
pub(crate) struct PollDeltas {
    pub(crate) wall: WallNs,
    pub(crate) cpu: CpuNs,
    pub(crate) alloc: AllocDelta,
    pub(crate) children: WallNs,
}
