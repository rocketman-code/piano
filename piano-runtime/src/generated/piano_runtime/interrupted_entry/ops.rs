#[allow(unused_imports)]
use crate::*;

/// Mint the interrupted entry from a live inflight slot: the abandoned
/// call's identity, its start tick, and its recursion depth.
pub fn interrupt_execution(
    id: &crate::NameId,
    start: &crate::Ticks,
    depth: u32,
) -> crate::InterruptedEntry {
    super::InterruptedEntry::new(*id, *start, depth)
}
