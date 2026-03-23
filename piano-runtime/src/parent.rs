//! Per-thread parent span tracking.
//!
//! A single Cell<u64> in TLS holds the current parent span_id for
//! this thread. Guard saves the previous value on creation and
//! restores it on drop (RAII stack discipline).
//!
//! This replaces explicit Ctx parameter passing. The parent_span_id
//! flows implicitly through TLS instead of explicitly through
//! function parameters. No function signature changes needed.
//!
//! Invariants:
//! - Managed exclusively by Guard (single subsystem, no cross-system sharing).
//! - Save/restore is RAII: every set has a corresponding restore on drop.
//! - Returns 0 (root sentinel) when TLS is destroyed (thread teardown).
//! - Cell<u64> has no destructor (no TLS destruction ordering issues).

use std::cell::Cell;

thread_local! {
    static CURRENT_PARENT: Cell<u64> = const { Cell::new(0) };
}

/// Read the current parent span_id for this thread.
/// Returns 0 (root) if TLS is destroyed.
#[inline(always)]
pub fn current_parent() -> u64 {
    CURRENT_PARENT
        .try_with(|c| c.get())
        .unwrap_or(0)
}

/// Set the current parent span_id for this thread.
/// Returns the previous value (for Guard to save and restore on drop).
/// Returns 0 if TLS is destroyed (thread teardown no-op).
#[inline(always)]
pub fn set_parent(span_id: u64) -> u64 {
    CURRENT_PARENT
        .try_with(|c| {
            let prev = c.get();
            c.set(span_id);
            prev
        })
        .unwrap_or(0)
}

/// Restore a previously saved parent span_id.
/// Silent no-op if TLS is destroyed.
#[inline(always)]
pub fn restore_parent(prev: u64) {
    let _ = CURRENT_PARENT.try_with(|c| c.set(prev));
}
