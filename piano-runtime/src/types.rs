//! Domain types for the measurement pipeline.
//!
//! Source of truth: piano-runtime.carve. The measurement newtypes and
//! NameId are the spec-derived types from the generated zone, extended
//! with the crate's established surface (Copy, raw accessors, arithmetic)
//! in their producing modules and re-exported here.

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
/// The spec-derived newtype is the type; these extensions preserve the
/// crate's established surface on it.
pub use crate::generated::piano_runtime::name_id::NameId;

impl Copy for NameId {}

impl NameId {
    pub(crate) fn from_raw(v: u32) -> Self {
        Self::new(v)
    }
    pub fn raw(self) -> u32 {
        self.value()
    }
}
