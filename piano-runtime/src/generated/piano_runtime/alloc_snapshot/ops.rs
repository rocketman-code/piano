#[allow(unused_imports)]
use crate::*;

pub(crate) fn from_counts(
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
) -> crate::AllocSnapshot {
    super::AllocSnapshot::new(alloc_count, alloc_bytes, free_count, free_bytes)
}

pub fn snapshot_counter() -> crate::AllocSnapshot {
    crate::alloc::snapshot_alloc_counters()
}
