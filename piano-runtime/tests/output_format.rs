//! Output format tests: verify NDJSON lines are well-formed and
//! contain expected fields.

use piano_runtime::aggregator::FnAgg;
use piano_runtime::output::{write_header, write_trailer, write_aggregates, serialize_aggregate_to_stack};

#[test]
fn header_contains_type_and_names() {
    let mut buf = Vec::new();
    write_header(&mut buf, &[(0, "work"), (1, "helper")], 8, 0).unwrap();
    let line = String::from_utf8(buf).unwrap();

    assert!(line.starts_with('{'), "header must start with {{");
    assert!(line.trim().ends_with('}'), "header must end with }}");
    assert!(line.contains("\"type\":\"header\""));
    assert!(line.contains("\"bias_ns\":8"));
    assert!(line.contains("\"0\":\"work\""));
    assert!(line.contains("\"1\":\"helper\""));
}

#[test]
fn trailer_matches_header_structure() {
    let mut hdr = Vec::new();
    write_header(&mut hdr, &[(0, "a"), (1, "b")], 5, 0).unwrap();
    let mut trl = Vec::new();
    write_trailer(&mut trl, &[(0, "a"), (1, "b")], 5, 0).unwrap();

    let h = String::from_utf8(hdr).unwrap();
    let t = String::from_utf8(trl).unwrap();

    // Identical except for type field
    let normalized = h.replace("\"type\":\"header\"", "\"type\":\"trailer\"");
    assert_eq!(normalized.trim(), t.trim());
}

#[test]
fn aggregate_lines_contain_all_fields() {
    let aggs = vec![vec![
        FnAgg {
            name_id: 0, calls: 100, self_ns: 5000,
            inclusive_ns: 8000, cpu_self_ns: 3000,
            alloc_count: 10, alloc_bytes: 1024,
            free_count: 5, free_bytes: 512,
        },
    ]];

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
        vec![FnAgg { name_id: 0, calls: 1, self_ns: 100, inclusive_ns: 100,
            cpu_self_ns: 0, alloc_count: 0, alloc_bytes: 0, free_count: 0, free_bytes: 0 }],
        vec![FnAgg { name_id: 0, calls: 1, self_ns: 200, inclusive_ns: 200,
            cpu_self_ns: 0, alloc_count: 0, alloc_bytes: 0, free_count: 0, free_bytes: 0 }],
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
        name_id: 3, calls: 42, self_ns: 999,
        inclusive_ns: 1500, cpu_self_ns: 200,
        alloc_count: 7, alloc_bytes: 256,
        free_count: 2, free_bytes: 64,
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
        name_id: 3, calls: 42, self_ns: 999,
        inclusive_ns: 1500, cpu_self_ns: 200,
        alloc_count: 7, alloc_bytes: 256,
        free_count: 2, free_bytes: 64,
    };

    let mut formatted_buf = Vec::new();
    write_aggregates(&mut formatted_buf, &[vec![agg.clone()]]).unwrap();
    let formatted = String::from_utf8(formatted_buf).unwrap();

    let mut stack_buf = [0u8; 512];
    let len = serialize_aggregate_to_stack(&mut stack_buf, &agg, 0);
    let stack = std::str::from_utf8(&stack_buf[..len]).unwrap();

    assert_eq!(
        formatted.trim(), stack.trim(),
        "stack serializer and write_aggregates must produce identical output"
    );
}
