//! Domain types for the measurement pipeline.
//!
//! Source of truth: piano.carve (spec) -> carve -> carve-build ->
//! generated Rust. Types are checked in, not build-time generated.
//! Regenerate when the spec changes; CI checks freshness.

#[allow(unused_imports)]
pub use crate::alloc::{AllocDelta, AllocSnapshot};
#[allow(unused_imports)]
pub use crate::cpu_clock::CpuNs;
#[allow(unused_imports)]
pub(crate) use crate::inflight::InterruptedEntry;
#[allow(unused_imports)]
pub use crate::time::{Ticks, WallNs};

// ── NameId ──────────────────────────────────────────────────────

/// Function name ID (assigned by rewriter, consumed by runtime).
/// Boundary type: produced at the crate's public API entry points
/// (enter, enter_async) from u32 values assigned by the rewriter.
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
