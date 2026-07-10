//! Test fixtures for carve-generated invariants.

pub fn calibrate_to_ns_subject() -> crate::Ticks {
    crate::Ticks::from_raw(1_000)
}

pub fn compute_delta_subject() -> (crate::AllocSnapshot, crate::AllocSnapshot) {
    (
        crate::AllocSnapshot::from_counts(10, 100, 5, 50),
        crate::AllocSnapshot::from_counts(3, 40, 2, 20),
    )
}

pub fn compute_self_time_subject() -> (crate::WallNs, crate::WallNs) {
    (crate::WallNs::from_raw(100), crate::WallNs::from_raw(40))
}

pub fn cpu_duration_subject() -> (crate::CpuNs, crate::CpuNs) {
    (crate::CpuNs::from_raw(100), crate::CpuNs::from_raw(40))
}

pub fn serialize_aggregate_subject() -> (
    crate::NameId,
    crate::WallNs,
    crate::WallNs,
    crate::CpuNs,
    crate::AllocDelta,
    u64,
    u64,
) {
    (
        crate::NameId::from_raw(0),
        crate::WallNs::from_raw(20),
        crate::WallNs::from_raw(30),
        crate::CpuNs::from_raw(10),
        crate::AllocDelta::from_counts(1, 64, 0, 0),
        1,
        0,
    )
}

pub fn serialize_header_subject() -> (crate::NameTable, crate::WallNs, crate::CpuNs, String, u128) {
    (
        crate::NameTable::empty(),
        crate::WallNs::from_raw(1),
        crate::CpuNs::from_raw(2),
        String::from("test-run"),
        0,
    )
}

pub fn serialize_interrupted_subject() -> (crate::NameId, crate::WallNs, u32) {
    (crate::NameId::from_raw(0), crate::WallNs::from_raw(50), 1)
}

pub fn serialize_trailer_subject() -> (crate::NameTable, crate::WallNs, crate::CpuNs) {
    (
        crate::NameTable::empty(),
        crate::WallNs::from_raw(1),
        crate::CpuNs::from_raw(2),
    )
}

pub fn wall_duration_subject() -> (crate::WallNs, crate::WallNs) {
    (crate::WallNs::from_raw(100), crate::WallNs::from_raw(40))
}
