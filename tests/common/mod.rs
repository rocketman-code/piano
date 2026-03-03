//! Shared NDJSON parsing utilities for integration tests.
//!
//! Parses the NDJSON format written by piano-runtime: a header line with
//! function names, followed by frame lines with per-function summaries.

use std::collections::HashMap;

/// Per-function aggregated data from NDJSON frames.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct FnStats {
    pub calls: u64,
    pub self_ns: u64,
}

/// Parse NDJSON content and aggregate per-function stats (calls + self_ns)
/// across all frames.
///
/// Returns a map from function name to aggregated `FnStats`.
pub fn aggregate_ndjson(content: &str) -> HashMap<String, FnStats> {
    let mut lines = content.lines();
    let header = lines.next().expect("NDJSON should have header line");

    let fn_names = parse_header_functions(header);

    let mut calls = vec![0u64; fn_names.len()];
    let mut self_ns = vec![0u64; fn_names.len()];

    for line in lines {
        if !line.contains("\"fns\"") {
            continue;
        }
        let fns_start = line.find("\"fns\":[").unwrap() + "\"fns\":[".len();
        let fns_end = line[fns_start..].rfind(']').unwrap();
        let fns_str = &line[fns_start..fns_start + fns_end];
        for entry in fns_str.split("},{") {
            let entry = entry.trim_start_matches('{').trim_end_matches('}');
            let id = extract_json_u64(entry, "\"id\":");
            let c = extract_json_u64(entry, "\"calls\":");
            let s = extract_json_u64(entry, "\"self_ns\":");
            if let Some(id) = id {
                let idx = id as usize;
                if idx < calls.len() {
                    calls[idx] += c.unwrap_or(0);
                    self_ns[idx] += s.unwrap_or(0);
                }
            }
        }
    }

    fn_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            (
                name,
                FnStats {
                    calls: calls[i],
                    self_ns: self_ns[i],
                },
            )
        })
        .collect()
}

/// Parse the "functions" array from an NDJSON header line.
fn parse_header_functions(header: &str) -> Vec<String> {
    let fns_start = header
        .find("\"functions\":[")
        .expect("header should have functions array")
        + "\"functions\":[".len();
    let fns_end = header[fns_start..]
        .find(']')
        .expect("functions array should close");
    let fns_str = &header[fns_start..fns_start + fns_end];
    fns_str
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect()
}

/// Extract an integer value for a given key from a JSON-like string fragment.
///
/// Looks for `prefix` followed by digits, returns the parsed u64.
pub fn extract_json_u64(s: &str, prefix: &str) -> Option<u64> {
    let start = s.find(prefix)? + prefix.len();
    let end = s[start..]
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s.len() - start);
    s[start..start + end].parse().ok()
}
