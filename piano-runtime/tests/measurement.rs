use piano_runtime::measurement::Measurement;

#[test]
fn default_is_zeroed() {
    let m = Measurement::default();
    assert_eq!(m.span_id, 0);
    assert_eq!(m.parent_span_id, 0);
    assert_eq!(m.name_id, 0);
    assert_eq!(m.start_ns, 0);
    assert_eq!(m.end_ns, 0);
    assert_eq!(m.thread_id, 0);
    assert_eq!(m.alloc_count, 0);
    assert_eq!(m.alloc_bytes, 0);
}
