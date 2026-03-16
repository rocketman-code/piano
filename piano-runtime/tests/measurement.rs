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

#[test]
fn wall_time_ns_normal() {
    let m = Measurement {
        start_ns: 100,
        end_ns: 350,
        ..Measurement::default()
    };
    assert_eq!(m.wall_time_ns(), 250);
}

// INVARIANT TEST: wall_time_ns never panics, even on underflow.
#[test]
fn wall_time_ns_saturates_on_underflow() {
    let m = Measurement {
        start_ns: 500,
        end_ns: 100,
        ..Measurement::default()
    };
    assert_eq!(m.wall_time_ns(), 0, "saturating_sub should return 0, not panic");
}

#[test]
fn cpu_time_ns_normal() {
    let m = Measurement {
        cpu_start_ns: 10,
        cpu_end_ns: 60,
        ..Measurement::default()
    };
    assert_eq!(m.cpu_time_ns(), 50);
}

// INVARIANT TEST: cpu_time_ns never panics, even on underflow.
#[test]
fn cpu_time_ns_saturates_on_underflow() {
    let m = Measurement {
        cpu_start_ns: 999,
        cpu_end_ns: 1,
        ..Measurement::default()
    };
    assert_eq!(m.cpu_time_ns(), 0, "saturating_sub should return 0, not panic");
}

#[test]
fn is_root_when_parent_zero() {
    let m = Measurement {
        parent_span_id: 0,
        ..Measurement::default()
    };
    assert!(m.is_root());
}

#[test]
fn is_not_root_when_parent_nonzero() {
    let m = Measurement {
        parent_span_id: 42,
        ..Measurement::default()
    };
    assert!(!m.is_root());
}

#[test]
fn struct_size_is_96_bytes() {
    assert_eq!(
        std::mem::size_of::<Measurement>(),
        96,
        "Measurement should be 96 bytes (#[repr(C)])"
    );
}
