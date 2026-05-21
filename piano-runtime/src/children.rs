//! Per-thread children-time accumulator.
//!
//! A single `Cell<u64>` in TLS holds the accumulated inclusive time of
//! all children that have completed within the current Guard's scope.
//! Guard saves the previous value on creation (resetting to 0) and
//! restores it on drop (adding its own inclusive time to the parent's
//! accumulator).
//!
//! This enables self-time computation at measurement time:
//!   self_ns = inclusive_ns - children_inclusive_ns
//!
//! Without this, self-time requires post-hoc span tree reconstruction
//! from per-call records.
//!
//! Invariants:
//! - Managed exclusively by Guard and PianoFuture.
//! - Save/restore is RAII: every save has a corresponding restore on drop.
//! - Returns 0 when TLS is destroyed (thread teardown).
//! - `Cell<u64>` has no destructor (no TLS destruction ordering issues).

use crate::time::WallNs;
use std::cell::Cell;

thread_local! {
    static CHILDREN_NS: Cell<WallNs> = const { Cell::new(WallNs::ZERO) };
}

/// Read the current children-time accumulator for this thread.
/// Returns WallNs::ZERO if TLS is destroyed.
#[inline(always)]
pub fn current_children_ns() -> WallNs {
    CHILDREN_NS.try_with(|c| c.get()).unwrap_or(WallNs::ZERO)
}

/// Save the current children-time accumulator and reset to 0.
/// Returns the saved value (for Guard to restore on drop).
/// Returns WallNs::ZERO if TLS is destroyed.
#[inline(always)]
pub fn save_and_zero() -> WallNs {
    CHILDREN_NS
        .try_with(|c| {
            let prev = c.get();
            c.set(WallNs::ZERO);
            prev
        })
        .unwrap_or(WallNs::ZERO)
}

/// Restore a previously saved children-time value, adding the
/// caller's inclusive time so the parent sees it as a child.
/// Silent no-op if TLS is destroyed.
#[inline(always)]
pub fn restore_and_report(saved: WallNs, own_inclusive_ns: WallNs) {
    let _ = CHILDREN_NS.try_with(|c| c.set(WallNs::from_raw(saved.raw() + own_inclusive_ns.raw())));
}
