use crate::alloc::{AllocDelta, AllocSnapshot};
use crate::cpu_clock::CpuNs;
use crate::time::{Ticks, WallNs};

/// Pre-poll measurement snapshots. Consumed by `end_poll` to compute deltas.
///
/// Spec: AsyncExecution Polling state (capture_poll_start output).
pub(crate) struct PollActive {
    pub(crate) wall_start: Ticks,
    pub(crate) cpu_start: CpuNs,
    pub(crate) alloc_start: AllocSnapshot,
}

/// Per-poll measurement deltas. Must be destructured exhaustively.
///
/// Spec: AsyncExecution PollComplete -> Accumulated transition.
/// Adding a field forces every consumer to handle it at compile time.
pub(crate) struct PollDeltas {
    pub(crate) wall: WallNs,
    pub(crate) cpu: CpuNs,
    pub(crate) alloc: AllocDelta,
    pub(crate) children: WallNs,
}
