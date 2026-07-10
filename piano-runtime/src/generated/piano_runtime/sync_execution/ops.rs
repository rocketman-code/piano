#[allow(unused_imports)]
use crate::*;

/// The out-of-line bookkeeping half of guard entry: children scope,
/// allocation snapshot, and optional CPU read, all under the profiler
/// bookkeeping token so none of it contaminates user counts. The start
/// timestamp is a placeholder here; `stamped` writes the real one so the
/// TSC read stays inline in `enter` and lands last (the asm suite pins
/// this split: one shared out-of-line call, then the fence and the read).
#[inline(never)]
pub fn enter_sync(
    id: &crate::NameId,
) -> crate::generated::piano_runtime::sync_execution::SyncExecution {
    let bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
    let saved_children_ns = crate::children::save_and_zero();
    let snap = crate::alloc::snapshot_alloc_counters();
    let cpu_start = match crate::session::ProfileSession::get() {
        Some(s) if s.cpu_time_enabled => crate::cpu_clock::cpu_now_ns(),
        _ => crate::cpu_clock::CpuNs::from_raw(0),
    };
    drop(bookkeeping);

    super::SyncExecution::new(
        *id,
        crate::time::Ticks::from_raw(0),
        cpu_start,
        snap,
        saved_children_ns,
    )
}

/// Reconstruct the entered state with the real start timestamp. Inline so
/// the caller's TSC read is the last thing before the measured span opens;
/// the field moves are constant cost absorbed by the bias calibration.
#[inline(always)]
pub(crate) fn stamped(
    exec: crate::generated::piano_runtime::sync_execution::SyncExecution,
    start_ticks: crate::Ticks,
) -> crate::generated::piano_runtime::sync_execution::SyncExecution {
    super::SyncExecution::new(
        *exec.name_id(),
        start_ticks,
        *exec.cpu_start(),
        *exec.alloc_start(),
        *exec.saved_children_ns(),
    )
}

/// The whole exit path: end timestamp first (fence-bracketed), inflight
/// clear, deltas, self time = inclusive minus children, aggregate, and the
/// child's inclusive report to the parent scope. Consumes the entered
/// state; the returned value is the exited refinement riding the same
/// carrier.
pub fn exit_sync(
    exec: crate::generated::piano_runtime::sync_execution::SyncExecution,
) -> crate::generated::piano_runtime::sync_execution::SyncExecution {
    let end_ticks = crate::time::read();
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);

    let session = match crate::session::ProfileSession::get() {
        Some(s) => s,
        None => return exec,
    };
    crate::inflight::exit(*exec.name_id());
    let bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
    let cpu_end = if session.cpu_time_enabled {
        crate::cpu_clock::cpu_now_ns()
    } else {
        crate::cpu_clock::CpuNs::from_raw(0)
    };
    let snap_end = crate::alloc::snapshot_alloc_counters();
    let alloc_delta = snap_end.delta_since(exec.alloc_start());

    let start_ns = session.calibration.now_ns(*exec.start_ticks());
    let end_ns = session.calibration.now_ns(end_ticks);
    let inclusive_ns = end_ns.saturating_sub(start_ns);

    let my_children_ns = crate::children::current_children_ns();
    let self_ns = inclusive_ns.saturating_sub(my_children_ns);
    let cpu_self_ns = cpu_end.saturating_sub(*exec.cpu_start());

    crate::aggregator::aggregate(
        &bookkeeping,
        *exec.name_id(),
        self_ns,
        inclusive_ns,
        cpu_self_ns,
        alloc_delta,
        &session.agg_registry,
    );

    crate::children::restore_and_report(*exec.saved_children_ns(), inclusive_ns);
    exec
}
