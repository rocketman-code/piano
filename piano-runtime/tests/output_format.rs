//! Output format tests: verify NDJSON lines are well-formed and
//! contain expected fields.

use piano_runtime::aggregator::FnAgg;
use piano_runtime::output::{
    serialize_aggregate_to_stack, write_aggregates, write_header, write_trailer,
};

#[test]
fn header_contains_type_and_names() {
    let mut buf = Vec::new();
    write_header(&mut buf, &[(0, "work"), (1, "helper")], 8, 0, "test", 0).unwrap();
    let line = String::from_utf8(buf).unwrap();

    assert!(line.starts_with('{'), "header must start with {{");
    assert!(line.trim().ends_with('}'), "header must end with }}");
    assert!(line.contains("\"type\":\"header\""));
    assert!(line.contains("\"bias_ns\":8"));
    assert!(line.contains("\"0\":\"work\""));
    assert!(line.contains("\"1\":\"helper\""));
}

#[test]
fn trailer_shares_core_fields_with_header() {
    let mut hdr = Vec::new();
    write_header(&mut hdr, &[(0, "a"), (1, "b")], 5, 0, "test", 0).unwrap();
    let mut trl = Vec::new();
    write_trailer(&mut trl, &[(0, "a"), (1, "b")], 5, 0).unwrap();

    let h = String::from_utf8(hdr).unwrap();
    let t = String::from_utf8(trl).unwrap();

    // Both contain type, bias_ns, cpu_bias_ns, and names
    assert!(t.contains("\"type\":\"trailer\""));
    assert!(t.contains("\"bias_ns\":5"));
    assert!(t.contains("\"cpu_bias_ns\":0"));
    assert!(t.contains("\"0\":\"a\""));
    assert!(t.contains("\"1\":\"b\""));

    // Header has run metadata that trailer does not
    assert!(h.contains("\"run_id\":\"test\""));
    assert!(h.contains("\"timestamp_ms\":0"));
}

#[test]
fn aggregate_lines_contain_all_fields() {
    let aggs = vec![vec![FnAgg {
        name_id: 0,
        calls: 100,
        self_ns: 5000,
        inclusive_ns: 8000,
        cpu_self_ns: 3000,
        alloc_count: 10,
        alloc_bytes: 1024,
        free_count: 5,
        free_bytes: 512,
    }]];

    let mut buf = Vec::new();
    write_aggregates(&mut buf, &aggs).unwrap();
    let line = String::from_utf8(buf).unwrap();
    let line = line.trim();

    assert!(line.starts_with('{'));
    assert!(line.ends_with('}'));
    assert!(line.contains("\"thread\":0"));
    assert!(line.contains("\"name_id\":0"));
    assert!(line.contains("\"calls\":100"));
    assert!(line.contains("\"self_ns\":5000"));
    assert!(line.contains("\"inclusive_ns\":8000"));
    assert!(line.contains("\"cpu_self_ns\":3000"));
    assert!(line.contains("\"alloc_count\":10"));
    assert!(line.contains("\"alloc_bytes\":1024"));
    assert!(line.contains("\"free_count\":5"));
    assert!(line.contains("\"free_bytes\":512"));
}

#[test]
fn multi_thread_aggregates_have_distinct_thread_indices() {
    let aggs = vec![
        vec![FnAgg {
            name_id: 0,
            calls: 1,
            self_ns: 100,
            inclusive_ns: 100,
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }],
        vec![FnAgg {
            name_id: 0,
            calls: 1,
            self_ns: 200,
            inclusive_ns: 200,
            cpu_self_ns: 0,
            alloc_count: 0,
            alloc_bytes: 0,
            free_count: 0,
            free_bytes: 0,
        }],
    ];

    let mut buf = Vec::new();
    write_aggregates(&mut buf, &aggs).unwrap();
    let output = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"thread\":0"));
    assert!(lines[1].contains("\"thread\":1"));
}

#[test]
fn stack_serializer_produces_valid_ndjson() {
    let agg = FnAgg {
        name_id: 3,
        calls: 42,
        self_ns: 999,
        inclusive_ns: 1500,
        cpu_self_ns: 200,
        alloc_count: 7,
        alloc_bytes: 256,
        free_count: 2,
        free_bytes: 64,
    };

    let mut stack_buf = [0u8; 512];
    let len = serialize_aggregate_to_stack(&mut stack_buf, &agg, 0);
    let line = std::str::from_utf8(&stack_buf[..len]).unwrap().trim();

    assert!(line.starts_with('{'));
    assert!(line.ends_with('}'));
    assert!(line.contains("\"thread\":0"));
    assert!(line.contains("\"name_id\":3"));
    assert!(line.contains("\"calls\":42"));
    assert!(line.contains("\"self_ns\":999"));
}

#[test]
fn stack_serializer_matches_write_aggregates() {
    let agg = FnAgg {
        name_id: 3,
        calls: 42,
        self_ns: 999,
        inclusive_ns: 1500,
        cpu_self_ns: 200,
        alloc_count: 7,
        alloc_bytes: 256,
        free_count: 2,
        free_bytes: 64,
    };

    let mut formatted_buf = Vec::new();
    write_aggregates(&mut formatted_buf, &[vec![agg.clone()]]).unwrap();
    let formatted = String::from_utf8(formatted_buf).unwrap();

    let mut stack_buf = [0u8; 512];
    let len = serialize_aggregate_to_stack(&mut stack_buf, &agg, 0);
    let stack = std::str::from_utf8(&stack_buf[..len]).unwrap();

    assert_eq!(
        formatted.trim(),
        stack.trim(),
        "stack serializer and write_aggregates must produce identical output"
    );
}

/// NDJSON format contract: every field the CLI reads must be present in
/// the runtime's output. If the runtime drops a field, this test fails.
///
/// This is the structural guarantee that prevents the run_id bug from
/// recurring. The writer (piano-runtime) and reader (piano CLI) can't
/// share a type (different crate, zero-dep constraint). This test pins
/// the contract mechanically instead.
///
/// Pinned to: src/report/mod.rs (NdjsonNameTable, NdjsonAggregate)
///            src/report/load.rs (header_value.get("run_id"), etc.)
#[test]
fn ndjson_format_contract_header() {
    let mut buf = Vec::new();
    write_header(
        &mut buf,
        &[(0, "work"), (1, "helper")],
        8,
        3,
        "abc_1000",
        1700000000000,
    )
    .unwrap();
    let line = String::from_utf8(buf).unwrap();

    // Every field the CLI reads from the header via serde_json::Value::get
    let required_fields = [
        ("type", "\"type\":\"header\""),
        ("run_id", "\"run_id\":\"abc_1000\""),
        ("timestamp_ms", "\"timestamp_ms\":1700000000000"),
        ("bias_ns", "\"bias_ns\":8"),
        ("cpu_bias_ns", "\"cpu_bias_ns\":3"),
        ("names", "\"names\":{"),
    ];

    for (field, pattern) in &required_fields {
        assert!(
            line.contains(pattern),
            "header missing required field '{field}'. \
             CLI reads this field in load.rs. \
             Header line: {line}"
        );
    }
}

#[test]
fn ndjson_format_contract_trailer() {
    let mut buf = Vec::new();
    write_trailer(&mut buf, &[(0, "work")], 8, 3).unwrap();
    let line = String::from_utf8(buf).unwrap();

    let required_fields = [
        ("type", "\"type\":\"trailer\""),
        ("bias_ns", "\"bias_ns\":8"),
        ("cpu_bias_ns", "\"cpu_bias_ns\":3"),
        ("names", "\"names\":{"),
    ];

    for (field, pattern) in &required_fields {
        assert!(
            line.contains(pattern),
            "trailer missing required field '{field}'. \
             CLI reads this field via NdjsonNameTable deserialization. \
             Trailer line: {line}"
        );
    }
}

#[test]
fn ndjson_format_contract_aggregate() {
    let agg = FnAgg {
        name_id: 5,
        calls: 100,
        self_ns: 5000,
        inclusive_ns: 8000,
        cpu_self_ns: 3000,
        alloc_count: 10,
        alloc_bytes: 1024,
        free_count: 5,
        free_bytes: 512,
    };
    let mut buf = Vec::new();
    write_aggregates(&mut buf, &[vec![agg]]).unwrap();
    let line = String::from_utf8(buf).unwrap();

    // Every field the CLI reads via NdjsonAggregate deserialization
    let required_fields = [
        ("thread", "\"thread\":"),
        ("name_id", "\"name_id\":5"),
        ("calls", "\"calls\":100"),
        ("self_ns", "\"self_ns\":5000"),
        ("inclusive_ns", "\"inclusive_ns\":8000"),
        ("cpu_self_ns", "\"cpu_self_ns\":3000"),
        ("alloc_count", "\"alloc_count\":10"),
        ("alloc_bytes", "\"alloc_bytes\":1024"),
        ("free_count", "\"free_count\":5"),
        ("free_bytes", "\"free_bytes\":512"),
    ];

    for (field, pattern) in &required_fields {
        assert!(
            line.contains(pattern),
            "aggregate missing required field '{field}'. \
             CLI reads this field via NdjsonAggregate deserialization. \
             Aggregate line: {line}"
        );
    }
}

#[test]
fn header_with_multiple_names_has_correct_separators() {
    let mut buf = Vec::new();
    write_header(
        &mut buf,
        &[(0, "alpha"), (1, "beta"), (2, "gamma")],
        8,
        3,
        "test_run",
        1000,
    )
    .unwrap();
    let line = String::from_utf8(buf).unwrap();

    // Names must be comma-separated with no leading/trailing commas.
    // Extract the names object: everything between "names":{ and }}
    let names_start = line.find("\"names\":{").unwrap() + "\"names\":{".len();
    let names_end = line[names_start..].find("}}").unwrap() + names_start;
    let names_block = &line[names_start..names_end];

    // Should be: "0":"alpha","1":"beta","2":"gamma"
    assert!(
        names_block.contains("\"0\":\"alpha\",\"1\":\"beta\",\"2\":\"gamma\""),
        "names must be comma-separated without leading comma. Got: {names_block}"
    );
    assert!(
        !names_block.starts_with(','),
        "names must not start with comma"
    );
}

#[test]
fn json_escaping_handles_special_characters() {
    let mut buf = Vec::new();
    write_header(
        &mut buf,
        &[(0, "has\"quote"), (1, "has\\slash"), (2, "has\nnewline")],
        0,
        0,
        "test",
        0,
    )
    .unwrap();
    let line = String::from_utf8(buf).unwrap();

    // Quotes must be escaped as \"
    assert!(
        line.contains("has\\\"quote"),
        "quotes must be escaped. Got: {line}"
    );
    // Backslashes must be escaped as \\
    assert!(
        line.contains("has\\\\slash"),
        "backslashes must be escaped. Got: {line}"
    );
    // Newlines must be escaped as \n
    assert!(
        line.contains("has\\nnewline"),
        "newlines must be escaped. Got: {line}"
    );
}
