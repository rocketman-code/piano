use piano_runtime::alloc::{AllocDelta, AllocSnapshot};
use piano_runtime::cpu_clock::CpuNs;
use piano_runtime::time::{Ticks, WallNs};

#[test]
fn root_tick_delta_wraps_counter_values() {
    let end = Ticks::from_raw(3);
    let start = Ticks::from_raw(u64::MAX - 1);

    let delta = piano_runtime::ticks_delta(&end, &start);

    assert_eq!(delta.raw(), 5);
}

#[test]
fn root_wall_and_cpu_durations_saturate_at_zero() {
    let wall = piano_runtime::wall_duration(&WallNs::from_raw(7), &WallNs::from_raw(10));
    let cpu = piano_runtime::cpu_duration(&CpuNs::from_raw(11), &CpuNs::from_raw(20));

    assert_eq!(wall.raw(), 0);
    assert_eq!(cpu.raw(), 0);
}

#[test]
fn root_alloc_delta_saturates_and_accumulates_fields() {
    let start = AllocSnapshot::from_counts(10, 100, 6, 60);
    let end = AllocSnapshot::from_counts(8, 150, 9, 55);

    let delta = piano_runtime::compute_delta(&end, &start);

    assert_eq!(delta.alloc_count(), 0);
    assert_eq!(delta.alloc_bytes(), 50);
    assert_eq!(delta.free_count(), 3);
    assert_eq!(delta.free_bytes(), 0);

    let acc = AllocDelta::from_counts(1, 2, 3, 4);
    let total = piano_runtime::accumulate_delta(&acc, &delta);

    assert_eq!(total.alloc_count(), 1);
    assert_eq!(total.alloc_bytes(), 52);
    assert_eq!(total.free_count(), 6);
    assert_eq!(total.free_bytes(), 4);
}
