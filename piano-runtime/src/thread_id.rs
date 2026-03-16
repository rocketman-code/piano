//! Unique per-thread identity allocation.
//!
//! Invariants:
//! - Each thread gets a unique non-zero ID, never recycled.
//! - The same thread always returns the same ID.
//!
//! Enforcement: AtomicU64 starting at 1 (0 = uninitialized sentinel
//! in TLS Cell, never returned to callers). TLS Cell caches the
//! assigned ID -- subsequent calls on the same thread return the
//! cached value without touching the atomic.
//!
//! Wrap-around: fetch_add on u64 wraps after 2^64 threads. At 1M
//! threads/sec, that takes ~584,942 years. Not a practical concern.
//! Returns 0 if TLS is destroyed (thread teardown).

use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::Cell;

static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_ID: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn current_thread_id() -> u64 {
    THREAD_ID
        .try_with(|id| {
            let val = id.get();
            if val == 0 {
                let new_id = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
                id.set(new_id);
                new_id
            } else {
                val
            }
        })
        .unwrap_or(0)
}
