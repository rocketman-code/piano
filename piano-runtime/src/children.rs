//! Per-thread children-time accumulator.
//!
//! Each profiled function's self-time is:
//!   self_ns = inclusive_ns - children_inclusive_ns
//!
//! Children report their inclusive time to the enclosing parent scope.
//! Every child reports -- completed, cancelled, or interrupted.
//! The parent reads the accumulated sum on exit.
//!
//! Implementation: a TLS `Cell<WallNs>` holds the running sum. Guard
//! and PianoFuture scope it via save-and-zero on entry, report on
//! exit. Rust's reverse drop order (drop.rs:93) guarantees nested
//! children report before the parent reads.
//!
//! Returns 0 when TLS is destroyed (thread teardown).
//! `Cell<u64>` has no destructor (no TLS destruction ordering issues).

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

/// Begin a new children scope. Returns the parent's accumulated
/// children time and resets the accumulator to zero.
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

/// End a children scope: report own inclusive time to the parent
/// and restore the parent's accumulated children value.
/// Silent no-op if TLS is destroyed.
#[inline(always)]
pub fn restore_and_report(saved: WallNs, own_inclusive_ns: WallNs) {
    let _ = CHILDREN_NS.try_with(|c| c.set(saved + own_inclusive_ns));
}

/// Report inclusive time to the enclosing parent scope without
/// restoring a saved value. Used by PianoFuture which shares the
/// parent's children scope with potential siblings.
/// Silent no-op if TLS is destroyed.
#[inline(always)]
pub fn report_inclusive(own_inclusive_ns: WallNs) {
    let _ = CHILDREN_NS.try_with(|c| c.set(c.get() + own_inclusive_ns));
}
