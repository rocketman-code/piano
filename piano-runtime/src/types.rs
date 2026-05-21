//! Domain types for the measurement pipeline.
//!
//! Each type is defined in the module that contains its producing
//! operation. This module re-exports them for crate-wide access.
//! Structure visible everywhere, construction restricted to the
//! producing module.

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
