use piano_runtime::aggregator::drain_thread_agg;
use piano_runtime::alloc::AllocDelta;
use piano_runtime::cpu_clock::CpuNs;
use piano_runtime::session::ProfileSession;
use piano_runtime::time::WallNs;
use piano_runtime::NameId;

#[test]
fn generated_aggregate_delegates_to_thread_aggregation() {
    std::thread::spawn(|| {
        ProfileSession::init(None, false, &[], "test", 0);
        let alloc = AllocDelta::from_counts(1, 64, 0, 0);

        piano_runtime::aggregate(
            &NameId::new(7),
            &WallNs::from_raw(10),
            &WallNs::from_raw(15),
            &CpuNs::from_raw(3),
            &alloc,
        );
        piano_runtime::aggregate(
            &NameId::new(7),
            &WallNs::from_raw(5),
            &WallNs::from_raw(8),
            &CpuNs::from_raw(2),
            &alloc,
        );

        let agg = drain_thread_agg();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].name_id.raw(), 7);
        assert_eq!(agg[0].calls, 2);
        assert_eq!(agg[0].self_ns.raw(), 15);
        assert_eq!(agg[0].inclusive_ns.raw(), 23);
        assert_eq!(agg[0].cpu_self_ns.raw(), 5);
        assert_eq!(agg[0].alloc.alloc_count(), 2);
        assert_eq!(agg[0].alloc.alloc_bytes(), 128);
    })
    .join()
    .unwrap();
}

#[test]
fn write_line_appends_serialized_lines_to_destination() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("profile.ndjson");
    let alloc = AllocDelta::zero();
    let first = piano_runtime::serialize_aggregate(
        &NameId::new(0),
        &WallNs::from_raw(1),
        &WallNs::from_raw(2),
        &CpuNs::from_raw(3),
        &alloc,
        4,
        5,
    );
    let second = piano_runtime::serialize_interrupted(&NameId::new(1), &WallNs::from_raw(9), 2);

    piano_runtime::write_line(first, &path).unwrap();
    piano_runtime::write_line(second, &path).unwrap();

    let written = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        written,
        concat!(
            "{\"thread\":5,\"name_id\":0,\"calls\":4,\"self_ns\":1,\"inclusive_ns\":2,",
            "\"cpu_self_ns\":3,\"alloc_count\":0,\"alloc_bytes\":0,",
            "\"free_count\":0,\"free_bytes\":0}\n",
            "{\"name_id\":1,\"calls\":2,\"self_ns\":9,\"inclusive_ns\":9,",
            "\"cpu_self_ns\":0,\"alloc_count\":0,\"alloc_bytes\":0,",
            "\"free_count\":0,\"free_bytes\":0,\"interrupted\":true}\n"
        )
    );
}
