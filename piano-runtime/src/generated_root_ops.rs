#![allow(clippy::ptr_arg)]

pub fn read_tsc() -> crate::Ticks {
    crate::time::read()
}

pub fn read_cpu_clock() -> crate::CpuNs {
    crate::cpu_clock::cpu_now_ns()
}

pub fn calibrate_to_ns(v: &crate::Ticks) -> crate::WallNs {
    crate::time::CalibrationData::calibrate().now_ns(*v)
}

pub fn ticks_delta(end: &crate::Ticks, start: &crate::Ticks) -> crate::Ticks {
    end.wrapping_sub(*start)
}

pub fn wall_duration(end: &crate::WallNs, start: &crate::WallNs) -> crate::WallNs {
    end.saturating_sub(*start)
}

pub fn cpu_duration(end: &crate::CpuNs, start: &crate::CpuNs) -> crate::CpuNs {
    end.saturating_sub(*start)
}

pub fn wall_accumulate(acc: &crate::WallNs, delta: &crate::WallNs) -> crate::WallNs {
    let mut total = *acc;
    total += *delta;
    total
}

pub fn cpu_accumulate(acc: &crate::CpuNs, delta: &crate::CpuNs) -> crate::CpuNs {
    let mut total = *acc;
    total += *delta;
    total
}

pub fn report_child_inclusive(child_inclusive: &crate::WallNs) {
    crate::children::report_inclusive(*child_inclusive);
}

pub fn compute_self_time(inclusive: &crate::WallNs, children_sum: &crate::WallNs) -> crate::WallNs {
    inclusive.saturating_sub(*children_sum)
}

/// Open one poll: the per-poll children scope, the pre-poll snapshots
/// (alloc under the bookkeeping token, then wall and cpu, fence-bracketed),
/// carried into the mid-poll state alongside the accumulators. wall_acc is
/// primed to Some so a panic unwinding through the inner poll still leaves
/// an ever-polled execution for the drop-path emit.
pub fn begin_poll(
    exec: crate::generated::piano_runtime::async_execution::AsyncExecution,
) -> crate::MidPoll {
    let poll_children_saved = crate::children::save_and_zero();
    let poll_alloc_start = {
        let _bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
        crate::alloc::snapshot_alloc_counters()
    };
    let poll_start_ticks = crate::time::read();
    let poll_cpu_start = match crate::session::ProfileSession::get() {
        Some(s) if s.cpu_time_enabled => crate::cpu_clock::cpu_now_ns(),
        _ => crate::cpu_clock::CpuNs::from_raw(0),
    };
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);

    let wall_acc = Some((*exec.wall_acc()).unwrap_or(crate::time::WallNs::from_raw(0)));
    crate::generated::piano_runtime::async_execution::mid_poll::MidPoll::new(
        *exec.name_id(),
        wall_acc,
        *exec.cpu_acc(),
        *exec.alloc_acc(),
        *exec.children_ns_acc(),
        exec.emitted(),
        poll_start_ticks,
        poll_cpu_start,
        poll_alloc_start,
        poll_children_saved,
    )
}

/// The terminal completion transition: consumes the idle, ever-polled
/// execution. Ready is once-only, so nothing constructs an idle execution
/// from the completed state.
pub fn mark_completed(
    exec: crate::generated::piano_runtime::async_execution::AsyncExecution,
) -> crate::PollCompleted {
    crate::generated::piano_runtime::async_execution::poll_completed::PollCompleted::new(
        *exec.name_id(),
        *exec.wall_acc(),
        *exec.cpu_acc(),
        *exec.alloc_acc(),
        *exec.children_ns_acc(),
        exec.emitted(),
    )
}

/// Write the execution's aggregate once. A never-polled execution
/// (wall_acc None) emits nothing; the once-only discipline is the
/// caller's take-the-value-out vehicle, so re-invocation cannot occur
/// with the same execution.
pub fn emit(exec: &crate::generated::piano_runtime::async_execution::AsyncExecution) {
    let inclusive_ns = match exec.wall_acc() {
        Some(w) => *w,
        None => return,
    };
    let session = match crate::session::ProfileSession::get() {
        Some(s) => s,
        None => return,
    };
    let bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
    let self_ns = inclusive_ns.saturating_sub(*exec.children_ns_acc());
    crate::aggregator::aggregate(
        &bookkeeping,
        *exec.name_id(),
        self_ns,
        inclusive_ns,
        *exec.cpu_acc(),
        *exec.alloc_acc(),
        &session.agg_registry,
    );
    crate::children::report_inclusive(inclusive_ns);
}

pub fn aggregate(
    id: &crate::NameId,
    self_wall: &crate::WallNs,
    inclusive_wall: &crate::WallNs,
    cpu: &crate::CpuNs,
    alloc: &crate::AllocDelta,
) {
    let session = match crate::session::ProfileSession::get() {
        Some(session) => session,
        None => return,
    };
    let bookkeeping = crate::alloc::ProfilerBookkeeping::enter();
    crate::aggregator::aggregate(
        &bookkeeping,
        *id,
        *self_wall,
        *inclusive_wall,
        *cpu,
        *alloc,
        &session.agg_registry,
    );
}

pub fn serialize_header(
    names: &crate::NameTable,
    bias: &crate::WallNs,
    cpu_bias: &crate::CpuNs,
    run_id: &String,
    timestamp_ms: u128,
) -> crate::SerializedLine {
    crate::SerializedLine::new(crate::output::serialize_header_line(
        names,
        *bias,
        *cpu_bias,
        run_id.as_str(),
        timestamp_ms,
    ))
}

pub fn serialize_aggregate(
    id: &crate::NameId,
    self_wall: &crate::WallNs,
    inclusive_wall: &crate::WallNs,
    cpu_self: &crate::CpuNs,
    alloc: &crate::AllocDelta,
    calls: u64,
    thread: u64,
) -> crate::SerializedLine {
    crate::SerializedLine::new(crate::output::serialize_aggregate_line(
        *id,
        *self_wall,
        *inclusive_wall,
        *cpu_self,
        *alloc,
        calls,
        thread,
    ))
}

pub fn serialize_interrupted(
    id: &crate::NameId,
    elapsed: &crate::WallNs,
    depth: u32,
) -> crate::SerializedLine {
    crate::SerializedLine::new(crate::output::serialize_interrupted_line(
        *id, *elapsed, depth,
    ))
}

pub fn serialize_trailer(
    names: &crate::NameTable,
    bias: &crate::WallNs,
    cpu_bias: &crate::CpuNs,
) -> crate::SerializedLine {
    crate::SerializedLine::new(crate::output::serialize_trailer_line(
        names, *bias, *cpu_bias,
    ))
}

pub fn write_line(
    line: crate::SerializedLine,
    dest: &std::path::Path,
) -> Result<(), std::io::Error> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dest)?;
    file.write_all(line.value().as_bytes())
}
