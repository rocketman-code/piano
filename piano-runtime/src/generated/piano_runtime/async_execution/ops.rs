#[allow(unused_imports)]
use crate::*;

/// The initial idle execution: nothing accumulated, never polled
/// (wall_acc None is the ever-polled discriminant), nothing emitted.
pub fn create_async(
    id: &crate::NameId,
) -> crate::generated::piano_runtime::async_execution::AsyncExecution {
    super::AsyncExecution::new(
        *id,
        None,
        crate::cpu_clock::CpuNs::from_raw(0),
        crate::alloc::AllocDelta::zero(),
        crate::time::WallNs::from_raw(0),
        false,
    )
}

/// Close one poll: post-poll snapshots against the mid-poll's saved
/// pre-poll snapshots (fence-bracketed, bookkeeping-token guarded),
/// accumulate the deltas, close the per-poll children scope (the future
/// reports its total on completion, so the poll reports zero upward),
/// and return to idle with is_ever_polled established.
pub fn end_poll(
    exec: crate::MidPoll,
) -> crate::generated::piano_runtime::async_execution::AsyncExecution {
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
    let cpu_end = match crate::session::ProfileSession::get() {
        Some(s) if s.cpu_time_enabled => crate::cpu_clock::cpu_now_ns(),
        _ => crate::cpu_clock::CpuNs::from_raw(0),
    };
    let wall_end = crate::time::read();
    let alloc_delta = {
        let _bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
        crate::alloc::snapshot_alloc_counters().delta_since(exec.poll_alloc_start())
    };
    let children = crate::children::current_children_ns();

    let (wall_delta, cpu_delta) = match crate::session::ProfileSession::get() {
        Some(s) => {
            let start_ns = s.calibration.now_ns(*exec.poll_start_ticks());
            let end_ns = s.calibration.now_ns(wall_end);
            (
                end_ns.saturating_sub(start_ns),
                cpu_end.saturating_sub(*exec.poll_cpu_start()),
            )
        }
        None => (
            crate::time::WallNs::from_raw(0),
            crate::cpu_clock::CpuNs::from_raw(0),
        ),
    };

    crate::children::restore_and_report(
        *exec.poll_children_saved(),
        crate::time::WallNs::from_raw(0),
    );

    let mut wall_acc = (*exec.wall_acc()).unwrap_or(crate::time::WallNs::from_raw(0));
    wall_acc += wall_delta;
    let mut cpu_acc = *exec.cpu_acc();
    cpu_acc += cpu_delta;
    let mut alloc_acc = *exec.alloc_acc();
    alloc_acc += alloc_delta;
    let mut children_ns_acc = *exec.children_ns_acc();
    children_ns_acc += children;

    super::AsyncExecution::new(
        *exec.name_id(),
        Some(wall_acc),
        cpu_acc,
        alloc_acc,
        children_ns_acc,
        exec.emitted(),
    )
}

/// The drop-path fallback while a poll is in flight: an idle execution
/// carrying the mid-poll's accumulators with wall_acc primed, so a panic
/// unwinding through the inner poll still leaves an ever-polled execution
/// for the vehicle's best-effort emit.
pub(crate) fn primed_for_drop(
    mid: &crate::MidPoll,
) -> crate::generated::piano_runtime::async_execution::AsyncExecution {
    super::AsyncExecution::new(
        *mid.name_id(),
        Some((*mid.wall_acc()).unwrap_or(crate::time::WallNs::from_raw(0))),
        *mid.cpu_acc(),
        *mid.alloc_acc(),
        *mid.children_ns_acc(),
        mid.emitted(),
    )
}
