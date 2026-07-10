use piano_runtime::SerializedLine;

fn line(value: &str) -> SerializedLine {
    SerializedLine::from_value(String::from(value))
}

#[test]
fn self_contained_accepts_one_newline_terminated_object() {
    let value = line("{\"type\":\"header\",\"names\":{},\"qualified\":{}}\n");

    assert!(piano_runtime::predicates::is_self_contained(&value));
}

#[test]
fn self_contained_rejects_missing_terminal_newline() {
    let value = line("{\"type\":\"header\"}");

    assert!(!piano_runtime::predicates::is_self_contained(&value));
}

#[test]
fn self_contained_rejects_multiple_records() {
    let value = line("{\"type\":\"header\"}\n{\"type\":\"trailer\"}\n");

    assert!(!piano_runtime::predicates::is_self_contained(&value));
}

#[test]
fn self_contained_rejects_malformed_json() {
    let value = line("{\"type\":\"header\",}\n");

    assert!(!piano_runtime::predicates::is_self_contained(&value));
}

#[test]
fn dual_of_header_accepts_trailer_name_table_line() {
    let value = line(
        "{\"type\":\"trailer\",\"bias_ns\":11,\"cpu_bias_ns\":7,\
         \"names\":{\"0\":\"work\"},\"qualified\":{\"0\":\"crate::work\"}}\n",
    );

    assert!(piano_runtime::predicates::is_dual_of_header(&value));
}

#[test]
fn dual_of_header_rejects_aggregate_line() {
    let value = line("{\"thread\":0,\"name_id\":0,\"calls\":1}\n");

    assert!(!piano_runtime::predicates::is_dual_of_header(&value));
}
