#[allow(unused_imports)]
use crate::*;

pub(crate) fn from_counts(
    alloc_count: u64,
    alloc_bytes: u64,
    free_count: u64,
    free_bytes: u64,
) -> crate::AllocDelta {
    super::AllocDelta::new(alloc_count, alloc_bytes, free_count, free_bytes)
}

pub fn compute_delta(
    end: &crate::AllocSnapshot,
    start: &crate::AllocSnapshot,
) -> crate::AllocDelta {
    end.delta_since(start)
}

pub fn accumulate_delta(acc: &crate::AllocDelta, delta: &crate::AllocDelta) -> crate::AllocDelta {
    let mut total = *acc;
    total += *delta;
    total
}
