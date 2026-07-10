use piano_runtime::alloc::AllocDelta;
use piano_runtime::cpu_clock::CpuNs;
use piano_runtime::output::{write_header, write_trailer};
use piano_runtime::time::WallNs;
use piano_runtime::{NameId, NameTable};

fn golden_names() -> NameTable {
    NameTable::from_owned(vec![
        (0, String::from("work"), String::from("crate::work")),
        (1, String::from("helper"), String::from("crate::helper")),
    ])
}

#[test]
fn generated_header_matches_writer() {
    let names = golden_names();
    let line = piano_runtime::serialize_header(
        &names,
        &WallNs::from_raw(11),
        &CpuNs::from_raw(7),
        &String::from("golden-run"),
        1_700_000_000_000,
    );

    let mut expected = Vec::new();
    write_header(
        &mut expected,
        &[(0, "work", "crate::work"), (1, "helper", "crate::helper")],
        WallNs::from_raw(11),
        CpuNs::from_raw(7),
        "golden-run",
        1_700_000_000_000,
    )
    .unwrap();

    assert_eq!(line.value().as_bytes(), expected.as_slice());
}

#[test]
fn generated_trailer_matches_writer() {
    let names = golden_names();
    let line = piano_runtime::serialize_trailer(&names, &WallNs::from_raw(11), &CpuNs::from_raw(7));

    let mut expected = Vec::new();
    write_trailer(
        &mut expected,
        &[(0, "work", "crate::work"), (1, "helper", "crate::helper")],
        WallNs::from_raw(11),
        CpuNs::from_raw(7),
    )
    .unwrap();

    assert_eq!(line.value().as_bytes(), expected.as_slice());
}

#[test]
fn generated_aggregate_matches_existing_format() {
    let alloc = AllocDelta::from_counts(3, 96, 1, 32);
    let line = piano_runtime::serialize_aggregate(
        &NameId::new(0),
        &WallNs::from_raw(30),
        &WallNs::from_raw(50),
        &CpuNs::from_raw(12),
        &alloc,
        2,
        0,
    );

    let expected = concat!(
        "{\"thread\":0,\"name_id\":0,\"calls\":2,\"self_ns\":30,\"inclusive_ns\":50,",
        "\"cpu_self_ns\":12,\"alloc_count\":3,\"alloc_bytes\":96,",
        "\"free_count\":1,\"free_bytes\":32}\n"
    );

    assert_eq!(line.value(), expected);
}

#[test]
fn generated_interrupted_matches_existing_format() {
    let line = piano_runtime::serialize_interrupted(&NameId::new(1), &WallNs::from_raw(80), 2);

    let expected = concat!(
        "{\"name_id\":1,\"calls\":2,\"self_ns\":80,\"inclusive_ns\":80,",
        "\"cpu_self_ns\":0,\"alloc_count\":0,\"alloc_bytes\":0,",
        "\"free_count\":0,\"free_bytes\":0,\"interrupted\":true}\n"
    );

    assert_eq!(line.value(), expected);
}

#[test]
fn golden_run_from_generated_ops_matches_fixture() {
    let names = golden_names();
    let alloc0 = AllocDelta::from_counts(3, 96, 1, 32);
    let alloc1 = AllocDelta::zero();

    let actual = [
        piano_runtime::serialize_header(
            &names,
            &WallNs::from_raw(11),
            &CpuNs::from_raw(7),
            &String::from("golden-run"),
            1_700_000_000_000,
        )
        .value()
        .to_owned(),
        piano_runtime::serialize_aggregate(
            &NameId::new(0),
            &WallNs::from_raw(30),
            &WallNs::from_raw(50),
            &CpuNs::from_raw(12),
            &alloc0,
            2,
            0,
        )
        .value()
        .to_owned(),
        piano_runtime::serialize_aggregate(
            &NameId::new(1),
            &WallNs::from_raw(20),
            &WallNs::from_raw(25),
            &CpuNs::from_raw(9),
            &alloc1,
            1,
            1,
        )
        .value()
        .to_owned(),
        piano_runtime::serialize_interrupted(&NameId::new(1), &WallNs::from_raw(80), 2)
            .value()
            .to_owned(),
        piano_runtime::serialize_trailer(&names, &WallNs::from_raw(11), &CpuNs::from_raw(7))
            .value()
            .to_owned(),
    ]
    .concat();

    assert_eq!(
        actual,
        include_str!("data/golden_run.ndjson"),
        "golden fixture must be updated only by a deliberate regeneration"
    );
}
