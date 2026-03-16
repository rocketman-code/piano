use piano_runtime::measurement::Measurement;
use piano_runtime::output::{write_header, write_measurement, write_measurements, write_trailer};

// Output module is a pure serializer -- no TLS, no shared state.
// Tests use in-memory Vec<u8> buffers. No spawned threads needed.

fn output_to_string(f: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>) -> String {
    let mut buf = Vec::new();
    f(&mut buf).expect("write should not fail to Vec<u8>");
    String::from_utf8(buf).expect("output should be valid UTF-8")
}

// ---------------------------------------------------------------------------
// write_measurement tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: each write_measurement produces exactly one line.
#[test]
fn measurement_produces_one_line() {
    let s = output_to_string(|w| write_measurement(w, &Measurement::default()));
    assert_eq!(s.matches('\n').count(), 1, "must produce exactly one newline");
    assert!(s.ends_with('\n'), "must end with newline");
}

// INVARIANT TEST: measurement line is valid JSON with all fields present.
#[test]
fn measurement_contains_all_fields() {
    let m = Measurement {
        span_id: 42,
        parent_span_id: 10,
        name_id: 7,
        start_ns: 1000,
        end_ns: 2000,
        thread_id: 3,
        cpu_start_ns: 100,
        cpu_end_ns: 200,
        alloc_count: 5,
        alloc_bytes: 512,
        free_count: 0,
        free_bytes: 0,
    };
    let s = output_to_string(|w| write_measurement(w, &m));

    assert!(s.contains("\"span_id\":42"), "missing span_id");
    assert!(s.contains("\"parent_span_id\":10"), "missing parent_span_id");
    assert!(s.contains("\"name_id\":7"), "missing name_id");
    assert!(s.contains("\"start_ns\":1000"), "missing start_ns");
    assert!(s.contains("\"end_ns\":2000"), "missing end_ns");
    assert!(s.contains("\"thread_id\":3"), "missing thread_id");
    assert!(s.contains("\"cpu_start_ns\":100"), "missing cpu_start_ns");
    assert!(s.contains("\"cpu_end_ns\":200"), "missing cpu_end_ns");
    assert!(s.contains("\"alloc_count\":5"), "missing alloc_count");
    assert!(s.contains("\"alloc_bytes\":512"), "missing alloc_bytes");
}

// INVARIANT TEST: measurement line starts with { and ends with }\n.
#[test]
fn measurement_is_valid_json_object() {
    let s = output_to_string(|w| write_measurement(w, &Measurement::default()));
    let trimmed = s.trim_end();
    assert!(trimmed.starts_with('{'), "must start with {{");
    assert!(trimmed.ends_with('}'), "must end with }}");
}

// INVARIANT TEST: large u64 values serialize correctly (no overflow).
#[test]
fn measurement_large_values() {
    let m = Measurement {
        span_id: u64::MAX,
        parent_span_id: u64::MAX,
        name_id: u32::MAX,
        start_ns: u64::MAX,
        end_ns: u64::MAX,
        thread_id: u64::MAX,
        cpu_start_ns: u64::MAX,
        cpu_end_ns: u64::MAX,
        alloc_count: u64::MAX,
        alloc_bytes: u64::MAX,
        free_count: u64::MAX,
        free_bytes: u64::MAX,
    };
    let s = output_to_string(|w| write_measurement(w, &m));
    let max_u64 = u64::MAX.to_string();
    let max_u32 = u32::MAX.to_string();

    assert!(
        s.contains(&format!("\"span_id\":{max_u64}")),
        "u64::MAX must serialize correctly"
    );
    assert!(
        s.contains(&format!("\"name_id\":{max_u32}")),
        "u32::MAX must serialize correctly"
    );
}

// INVARIANT TEST: zeroed measurement produces valid output.
#[test]
fn measurement_zeroed() {
    let s = output_to_string(|w| write_measurement(w, &Measurement::default()));
    assert!(s.contains("\"span_id\":0"), "zeroed span_id");
    assert!(s.contains("\"alloc_bytes\":0"), "zeroed alloc_bytes");
}

// INVARIANT TEST: nonzero free_count and free_bytes appear in output.
#[test]
fn output_includes_free_fields() {
    let m = Measurement {
        free_count: 3,
        free_bytes: 512,
        ..Measurement::default()
    };
    let s = output_to_string(|w| write_measurement(w, &m));
    assert!(s.contains("\"free_count\":3"), "missing free_count");
    assert!(s.contains("\"free_bytes\":512"), "missing free_bytes");
}

// ---------------------------------------------------------------------------
// write_measurements (batch) tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: batch write produces one line per measurement.
#[test]
fn batch_write_line_count() {
    let measurements: Vec<Measurement> = (0..5)
        .map(|i| Measurement {
            span_id: i,
            ..Measurement::default()
        })
        .collect();
    let s = output_to_string(|w| write_measurements(w, &measurements));
    assert_eq!(
        s.matches('\n').count(),
        5,
        "batch of 5 must produce 5 lines"
    );
}

// INVARIANT TEST: empty batch produces no output.
#[test]
fn batch_write_empty() {
    let s = output_to_string(|w| write_measurements(w, &[]));
    assert!(s.is_empty(), "empty batch must produce no output");
}

// ---------------------------------------------------------------------------
// write_header / write_trailer tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: header produces one line with type "header".
#[test]
fn header_produces_one_line() {
    let names = [(1, "foo::bar"), (2, "baz::qux")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert_eq!(s.matches('\n').count(), 1, "header must be one line");
    assert!(s.contains("\"type\":\"header\""), "must have type header");
}

// INVARIANT TEST: trailer produces one line with type "trailer".
#[test]
fn trailer_produces_one_line() {
    let names = [(1, "foo::bar"), (2, "baz::qux")];
    let s = output_to_string(|w| write_trailer(w, &names, 0));
    assert_eq!(s.matches('\n').count(), 1, "trailer must be one line");
    assert!(s.contains("\"type\":\"trailer\""), "must have type trailer");
}

// INVARIANT TEST: header and trailer have identical name content.
#[test]
fn header_and_trailer_same_names() {
    let names = [(1, "foo::bar"), (2, "baz::qux")];
    let header = output_to_string(|w| write_header(w, &names, 0));
    let trailer = output_to_string(|w| write_trailer(w, &names, 0));

    // Replace "header" with "trailer" in header output -- should match trailer
    let normalized = header.replace("\"type\":\"header\"", "\"type\":\"trailer\"");
    assert_eq!(normalized, trailer, "header and trailer must have identical name content");
}

// INVARIANT TEST: name table contains all provided names.
#[test]
fn header_contains_all_names() {
    let names = [(42, "my_mod::my_fn"), (100, "other::func")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(s.contains("\"42\":\"my_mod::my_fn\""), "missing name 42");
    assert!(s.contains("\"100\":\"other::func\""), "missing name 100");
}

// INVARIANT TEST: empty name table produces valid JSON.
#[test]
fn header_empty_names() {
    let s = output_to_string(|w| write_header(w, &[], 0));
    assert!(s.contains("\"names\":{}"), "empty names must produce empty object");
    assert!(s.starts_with('{'), "must be valid JSON object");
}

// INVARIANT TEST: single name table entry (no trailing comma issues).
#[test]
fn header_single_name() {
    let names = [(1, "foo")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(s.contains("\"1\":\"foo\""), "must contain the name");
    assert!(!s.contains(",,"), "must not have double commas");
}

// ---------------------------------------------------------------------------
// JSON escaping tests
// ---------------------------------------------------------------------------

// INVARIANT TEST: double quotes in function names are escaped.
#[test]
fn escapes_double_quotes() {
    let names = [(1, "fn_with_\"quotes\"")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(
        s.contains("fn_with_\\\"quotes\\\""),
        "double quotes must be escaped: {s}"
    );
}

// INVARIANT TEST: backslashes in function names are escaped.
#[test]
fn escapes_backslashes() {
    let names = [(1, "path\\to\\fn")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(
        s.contains("path\\\\to\\\\fn"),
        "backslashes must be escaped: {s}"
    );
}

// INVARIANT TEST: control characters are escaped as \\uXXXX.
#[test]
fn escapes_control_characters() {
    let names = [(1, "has\x01ctrl")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(
        s.contains("has\\u0001ctrl"),
        "control chars must be \\uXXXX escaped: {s}"
    );
}

// INVARIANT TEST: newlines and tabs are escaped with short forms.
#[test]
fn escapes_newlines_and_tabs() {
    let names = [(1, "has\nnewline\tand\ttabs")];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(s.contains("\\n"), "newline must be escaped");
    assert!(s.contains("\\t"), "tab must be escaped");
    assert!(!s.contains('\n') || s.ends_with('\n') && s.matches('\n').count() == 1,
        "only the trailing newline should be literal");
}

// INVARIANT TEST: typical Rust function paths need no escaping.
#[test]
fn typical_rust_paths_unchanged() {
    let names = [
        (1, "my_crate::module::function"),
        (2, "std::collections::HashMap"),
        (3, "<T as Trait>::method"),
    ];
    let s = output_to_string(|w| write_header(w, &names, 0));
    assert!(s.contains("my_crate::module::function"), "ASCII path unchanged");
    assert!(s.contains("std::collections::HashMap"), "std path unchanged");
    assert!(s.contains("<T as Trait>::method"), "generic path unchanged");
}

#[test]
fn header_includes_bias_ns() {
    let names: &[(u32, &str)] = &[(0, "main")];
    let s = output_to_string(|w| write_header(w, names, 42));
    assert!(
        s.contains("\"bias_ns\":42"),
        "header should contain bias_ns field: {s}"
    );
}

#[test]
fn trailer_includes_bias_ns() {
    let names: &[(u32, &str)] = &[(0, "main")];
    let s = output_to_string(|w| write_trailer(w, names, 99));
    assert!(
        s.contains("\"bias_ns\":99"),
        "trailer should contain bias_ns field: {s}"
    );
}
