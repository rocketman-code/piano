//! Span ID allocator -- assigns globally unique, non-zero IDs.
//!
//! Invariants:
//! - IDs are unique across all threads for the lifetime of the process.
//! - IDs are non-zero (0 is reserved as "no parent" / root sentinel).
//!
//! Enforcement: AtomicU64 starting at 1 with fetch_add(1, Relaxed).
//! Uniqueness is guaranteed by the atomic RMW regardless of ordering.
//! Overflow after 2^64 calls (~58 years at 10B/sec) wraps to 0,
//! which would violate the non-zero invariant. Not a practical concern
//! but documented as a known limitation.

use core::sync::atomic::{AtomicU64, Ordering};

pub fn next_span_id(alloc: &AtomicU64) -> u64 {
    alloc.fetch_add(1, Ordering::Relaxed)
}
