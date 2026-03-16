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

static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_span_id() -> u64 {
    NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed)
}
