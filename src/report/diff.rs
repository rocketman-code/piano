use std::collections::HashMap;

use super::{DIM, FnEntry, HEADER, Run};

/// Truncate a label to `max_len` characters, appending '...' if truncated.
fn truncate_label(label: &str, max_len: usize) -> String {
    if label.chars().count() <= max_len {
        label.to_string()
    } else {
        let truncated: String = label.chars().take(max_len).collect();
        format!("{truncated}\u{2026}")
    }
}

/// Structured JSON entry for a diff comparison.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonDiffEntry {
    pub name: String,
    pub self_ms_a: f64,
    pub self_ms_b: f64,
    pub delta_ms: f64,
    #[serde(default)]
    pub delta_pct: Option<f64>,
    pub calls_a: u64,
    pub calls_b: u64,
    pub alloc_count_a: u64,
    pub alloc_count_b: u64,
    pub alloc_bytes_a: u64,
    pub alloc_bytes_b: u64,
    #[serde(default)]
    pub cpu_self_ms_a: Option<f64>,
    #[serde(default)]
    pub cpu_self_ms_b: Option<f64>,
}

/// Serialize a diff between two runs as a JSON array.
///
/// Each entry contains the function name, self time from each run,
/// the absolute delta, and the percentage change (null when the base is zero).
pub fn diff_runs_json(a: &Run, b: &Run) -> String {
    let a_map: HashMap<&str, &FnEntry> = a.functions.iter().map(|f| (f.name.as_str(), f)).collect();
    let b_map: HashMap<&str, &FnEntry> = b.functions.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut names: Vec<&str> = a_map.keys().chain(b_map.keys()).copied().collect();
    names.sort_unstable();
    names.dedup();
    names.sort_by(|na, nb| {
        let delta_a = (b_map.get(na).map_or(0.0, |e| e.self_ms)
            - a_map.get(na).map_or(0.0, |e| e.self_ms))
        .abs();
        let delta_b = (b_map.get(nb).map_or(0.0, |e| e.self_ms)
            - a_map.get(nb).map_or(0.0, |e| e.self_ms))
        .abs();
        delta_b
            .partial_cmp(&delta_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let json_entries: Vec<JsonDiffEntry> = names
        .iter()
        .map(|name| {
            let self_a = a_map.get(name).map_or(0.0, |e| e.self_ms);
            let self_b = b_map.get(name).map_or(0.0, |e| e.self_ms);
            let delta = self_b - self_a;
            let delta_pct = if self_a > 0.0 {
                Some(delta / self_a * 100.0)
            } else if delta == 0.0 {
                Some(0.0)
            } else {
                None
            };
            JsonDiffEntry {
                name: name.to_string(),
                self_ms_a: self_a,
                self_ms_b: self_b,
                delta_ms: delta,
                delta_pct,
                calls_a: a_map.get(name).map_or(0, |e| e.calls),
                calls_b: b_map.get(name).map_or(0, |e| e.calls),
                alloc_count_a: a_map.get(name).map_or(0, |e| e.alloc_count),
                alloc_count_b: b_map.get(name).map_or(0, |e| e.alloc_count),
                alloc_bytes_a: a_map.get(name).map_or(0, |e| e.alloc_bytes),
                alloc_bytes_b: b_map.get(name).map_or(0, |e| e.alloc_bytes),
                cpu_self_ms_a: a_map.get(name).and_then(|e| e.cpu_self_ms),
                cpu_self_ms_b: b_map.get(name).and_then(|e| e.cpu_self_ms),
            }
        })
        .collect();

    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

/// Show the delta between two runs, comparing functions by name.
///
/// `label_a` and `label_b` are used as column headers (e.g. tag names or file stems).
/// Labels longer than 20 characters are truncated with '...'.
pub fn diff_runs(a: &Run, b: &Run, label_a: &str, label_b: &str) -> String {
    // Warn if comparing runs from different formats.
    if a.source_format != b.source_format {
        eprintln!("warning: comparing runs with different source formats (JSON vs NDJSON)");
    }

    let a_map: HashMap<&str, &FnEntry> = a.functions.iter().map(|f| (f.name.as_str(), f)).collect();
    let b_map: HashMap<&str, &FnEntry> = b.functions.iter().map(|f| (f.name.as_str(), f)).collect();

    // Collect unique function names, sorted by absolute self-time delta descending
    // so the biggest changes appear first.
    let mut names: Vec<&str> = a_map.keys().chain(b_map.keys()).copied().collect();
    names.sort_unstable();
    names.dedup();
    names.sort_by(|na, nb| {
        let delta_a = (b_map.get(na).map_or(0.0, |e| e.self_ms)
            - a_map.get(na).map_or(0.0, |e| e.self_ms))
        .abs();
        let delta_b = (b_map.get(nb).map_or(0.0, |e| e.self_ms)
            - a_map.get(nb).map_or(0.0, |e| e.self_ms))
        .abs();
        delta_b
            .partial_cmp(&delta_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Check if either run has alloc data or CPU data.
    let has_allocs = a.functions.iter().any(|f| f.alloc_count > 0)
        || b.functions.iter().any(|f| f.alloc_count > 0);
    let has_cpu = a.functions.iter().any(|f| f.cpu_self_ms.is_some())
        || b.functions.iter().any(|f| f.cpu_self_ms.is_some());

    let label_a = truncate_label(label_a, 20);
    let label_b = truncate_label(label_b, 20);
    // Column width: at least 10 (for data values like "12345.67ms"), or wider to fit label.
    let col_a = label_a.chars().count().max(10);
    let col_b = label_b.chars().count().max(10);

    let cpu_label_a = format!("CPU.{label_a}");
    let cpu_label_b = format!("CPU.{label_b}");
    let cpu_col_a = cpu_label_a.chars().count().max(10);
    let cpu_col_b = cpu_label_b.chars().count().max(10);

    let mut out = String::new();
    // Build header based on available columns.
    {
        let mut header = format!(
            "{:<40} {:>col_a$} {:>col_b$} {:>10}",
            "Function", label_a, label_b, "Delta"
        );
        if has_cpu {
            header.push_str(&format!(
                " {cpu_label_a:>cpu_col_a$} {cpu_label_b:>cpu_col_b$}"
            ));
        }
        if has_allocs {
            header.push_str(&format!(" {:>10} {:>10}", "Allocs", "A.Delta"));
        }
        let width = header.len();
        out.push_str(&format!("{HEADER}{header}{HEADER:#}\n"));
        out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(width)));
    }

    for name in &names {
        let before = a_map.get(name).map_or(0.0, |e| e.self_ms);
        let after = b_map.get(name).map_or(0.0, |e| e.self_ms);
        let delta = after - before;

        out.push_str(&format!(
            "{name:<40} {before:>w_a$.2}ms {after:>w_b$.2}ms {delta:>+9.2}ms",
            w_a = col_a - 2,
            w_b = col_b - 2,
        ));

        if has_cpu {
            let cpu_before = a_map.get(name).and_then(|e| e.cpu_self_ms);
            let cpu_after = b_map.get(name).and_then(|e| e.cpu_self_ms);
            let fmt_cpu = |v: Option<f64>, col_w: usize| match v {
                Some(ms) => format!("{ms:>w$.2}ms", w = col_w - 2),
                None => format!("{:>col_w$}", "-"),
            };
            out.push_str(&format!(
                " {} {}",
                fmt_cpu(cpu_before, cpu_col_a),
                fmt_cpu(cpu_after, cpu_col_b)
            ));
        }

        if has_allocs {
            let allocs_after = b_map.get(name).map_or(0u64, |e| e.alloc_count);
            let allocs_before = a_map.get(name).map_or(0u64, |e| e.alloc_count);
            let allocs_delta = allocs_after as i128 - allocs_before as i128;
            out.push_str(&format!(" {allocs_after:>10} {allocs_delta:>+10}"));
        }

        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::load::load_ndjson;
    use super::super::tag::load_tagged_run;
    use super::super::{FnEntry, Run, RunFormat};
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn diff_shows_delta() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: Some(12.0),
                self_ms: 10.0,
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: Some(9.0),
                self_ms: 8.0,
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b, "Before", "After");
        assert!(diff.contains("walk"), "should mention walk");
        assert!(diff.contains("-2.00"), "should show negative delta: {diff}");
    }

    #[test]
    fn diff_shows_alloc_deltas() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: Some(12.0),
                self_ms: 10.0,
                cpu_self_ms: None,
                alloc_count: 100,
                alloc_bytes: 8192,
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: Some(9.0),
                self_ms: 8.0,
                cpu_self_ms: None,
                alloc_count: 50,
                alloc_bytes: 4096,
            }],
        };
        let diff = diff_runs(&a, &b, "Before", "After");
        assert!(diff.contains("Allocs"), "should have Allocs column header");
        assert!(
            diff.contains("-50"),
            "should show alloc count delta: {diff}"
        );
    }

    #[test]
    fn diff_alloc_count_does_not_wrap_above_i64_max() {
        let large_count: u64 = i64::MAX as u64 + 1_000;
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "alloc_heavy".into(),
                calls: 1,
                total_ms: Some(1.0),
                self_ms: 1.0,
                cpu_self_ms: None,
                alloc_count: large_count,
                alloc_bytes: 0,
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "alloc_heavy".into(),
                calls: 1,
                total_ms: Some(1.0),
                self_ms: 1.0,
                cpu_self_ms: None,
                alloc_count: 0,
                alloc_bytes: 0,
            }],
        };
        let diff = diff_runs(&a, &b, "Before", "After");
        // Extract the A.Delta column value from the alloc_heavy row.
        // With the old `as i64` cast, large_count wraps to negative i64 and
        // the delta becomes a large positive number (wrong direction).
        let line = diff.lines().find(|l| l.contains("alloc_heavy")).unwrap();
        let fields: Vec<&str> = line.split_whitespace().collect();
        // Last field is A.Delta (alloc delta).
        let delta_str = fields.last().unwrap();
        assert!(
            delta_str.starts_with('-'),
            "alloc delta should be negative (decrease from {large_count} to 0), got: {delta_str}"
        );
    }

    #[test]
    fn diff_shows_cpu_columns_when_present() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(10.0),
                self_ms: 10.0,
                cpu_self_ms: Some(8.0),
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(12.0),
                self_ms: 12.0,
                cpu_self_ms: Some(10.0),
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b, "Before", "After");
        assert!(
            diff.contains("CPU.Before"),
            "should have CPU.Before column. Got:\n{diff}"
        );
        assert!(
            diff.contains("CPU.After"),
            "should have CPU.After column. Got:\n{diff}"
        );
        assert!(
            diff.contains("8.00"),
            "should show before CPU. Got:\n{diff}"
        );
        assert!(
            diff.contains("10.00"),
            "should show after CPU. Got:\n{diff}"
        );
    }

    #[test]
    fn diff_mixed_cpu_one_with_one_without() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(10.0),
                self_ms: 10.0,
                cpu_self_ms: Some(8.0),
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(12.0),
                self_ms: 12.0,
                // No CPU data.
                ..Default::default()
            }],
        };
        // Should still render CPU columns (because A has CPU data).
        let diff = diff_runs(&a, &b, "Before", "After");
        assert!(
            diff.contains("CPU.Before"),
            "should show CPU columns when either run has CPU data. Got:\n{diff}"
        );
        assert!(
            diff.contains("8.00"),
            "should show A's CPU value. Got:\n{diff}"
        );
        // B's missing CPU renders as "-", not a misleading 0.00ms.
        // Extract the CPU.After column value from the data row.
        let data_line = diff.lines().find(|l| l.contains("work")).unwrap();
        assert!(
            data_line.ends_with('-'),
            "missing CPU should render as dash, not 0.00ms. Got:\n{data_line}"
        );
    }

    #[test]
    fn diff_neither_has_cpu_hides_cpu_columns() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(10.0),
                self_ms: 10.0,
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(12.0),
                self_ms: 12.0,
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b, "Before", "After");
        assert!(
            !diff.contains("CPU"),
            "should not show CPU columns when neither run has CPU data. Got:\n{diff}"
        );
    }

    #[test]
    fn diff_uses_custom_labels_in_headers() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(10.0),
                self_ms: 10.0,
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(12.0),
                self_ms: 12.0,
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b, "baseline", "optimized");
        assert!(
            diff.contains("baseline"),
            "should use label_a as column header. Got:\n{diff}"
        );
        assert!(
            diff.contains("optimized"),
            "should use label_b as column header. Got:\n{diff}"
        );
        assert!(
            !diff.contains("Before"),
            "should not contain hardcoded 'Before'. Got:\n{diff}"
        );
        assert!(
            !diff.contains("After"),
            "should not contain hardcoded 'After'. Got:\n{diff}"
        );
    }

    #[test]
    fn diff_custom_labels_in_cpu_headers() {
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(10.0),
                self_ms: 10.0,
                cpu_self_ms: Some(8.0),
                ..Default::default()
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                total_ms: Some(12.0),
                self_ms: 12.0,
                cpu_self_ms: Some(10.0),
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b, "v1", "v2");
        assert!(
            diff.contains("CPU.v1"),
            "should use CPU.label_a as CPU column header. Got:\n{diff}"
        );
        assert!(
            diff.contains("CPU.v2"),
            "should use CPU.label_b as CPU column header. Got:\n{diff}"
        );
    }

    #[test]
    fn diff_truncates_long_labels() {
        let entry = || FnEntry {
            name: "work".into(),
            calls: 1,
            total_ms: Some(10.0),
            self_ms: 10.0,
            ..Default::default()
        };
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![entry()],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![entry()],
        };
        let long_label = "my-really-long-tag-name-that-goes-on";
        let diff = diff_runs(&a, &b, long_label, "short");
        // Should be truncated to 20 chars with ellipsis.
        assert!(
            diff.contains("my-really-long-tag-n\u{2026}"),
            "should truncate label > 20 chars with ellipsis. Got:\n{diff}"
        );
        assert!(
            !diff.contains(long_label),
            "should not contain the full long label. Got:\n{diff}"
        );
    }

    #[test]
    fn diff_label_column_width_expands_for_label() {
        let entry = || FnEntry {
            name: "work".into(),
            calls: 1,
            total_ms: Some(10.0),
            self_ms: 10.0,
            ..Default::default()
        };
        let a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![entry()],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![entry()],
        };
        // "after-refactor" is 14 chars, wider than default 10.
        let diff = diff_runs(&a, &b, "before", "after-refactor");
        // The "after-refactor" header should appear untruncated.
        assert!(
            diff.contains("after-refactor"),
            "should expand column to fit label. Got:\n{diff}"
        );
    }

    #[test]
    fn ndjson_diff_does_not_produce_misleading_total_ms() {
        // When diffing two NDJSON runs, total_ms is 0.0 on both sides,
        // so it should not contaminate the diff output.
        let dir = TempDir::new().unwrap();
        let ndjson_a = r#"{"format_version":2,"run_id":"diff_a","timestamp_ms":1000,"functions":["work"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":5000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        let ndjson_b = r#"{"format_version":2,"run_id":"diff_b","timestamp_ms":2000,"functions":["work"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":8000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(dir.path().join("1000.ndjson"), ndjson_a).unwrap();
        fs::write(dir.path().join("2000.ndjson"), ndjson_b).unwrap();

        let (run_a, _) = load_ndjson(&dir.path().join("1000.ndjson")).unwrap();
        let (run_b, _) = load_ndjson(&dir.path().join("2000.ndjson")).unwrap();

        // Both runs should have total_ms == None
        assert!(run_a.functions[0].total_ms.is_none());
        assert!(run_b.functions[0].total_ms.is_none());

        // Diff should show self_ms delta (8ms - 5ms = +3ms), not total_ms.
        let diff = diff_runs(&run_a, &run_b, "before", "after");
        assert!(diff.contains("work"), "diff should contain function name");
        assert!(
            diff.contains("+3.00ms"),
            "diff should show +3.00ms self_ms delta"
        );
    }

    #[test]
    fn diff_runs_json_computes_delta() {
        let run_a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 10,
                self_ms: 20.0,
                ..Default::default()
            }],
        };
        let run_b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 12,
                self_ms: 25.0,
                ..Default::default()
            }],
        };
        let json = diff_runs_json(&run_a, &run_b);
        let entries: Vec<JsonDiffEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "work");
        assert!((entries[0].self_ms_a - 20.0).abs() < f64::EPSILON);
        assert!((entries[0].self_ms_b - 25.0).abs() < f64::EPSILON);
        assert!((entries[0].delta_ms - 5.0).abs() < f64::EPSILON);
        assert!((entries[0].delta_pct.unwrap() - 25.0).abs() < f64::EPSILON);
        assert_eq!(entries[0].calls_a, 10);
        assert_eq!(entries[0].calls_b, 12);
    }

    #[test]
    fn diff_runs_json_new_function_has_null_pct() {
        let run_a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![],
        };
        let run_b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "new_fn".into(),
                calls: 1,
                self_ms: 5.0,
                ..Default::default()
            }],
        };
        let json = diff_runs_json(&run_a, &run_b);
        // Verify delta_pct is present as null (not omitted) via Value parse.
        let raw: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert!(
            raw[0].get("delta_pct").unwrap().is_null(),
            "delta_pct should serialize as null, not be omitted. Got:\n{json}"
        );
        let entries: Vec<JsonDiffEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "new_fn");
        assert!((entries[0].self_ms_a).abs() < f64::EPSILON);
        assert!(entries[0].delta_pct.is_none());
    }

    #[test]
    fn diff_runs_json_zero_zero_has_zero_pct() {
        let run_a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "idle".into(),
                calls: 5,
                self_ms: 0.0,
                ..Default::default()
            }],
        };
        let run_b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "idle".into(),
                calls: 5,
                self_ms: 0.0,
                ..Default::default()
            }],
        };
        let json = diff_runs_json(&run_a, &run_b);
        let entries: Vec<JsonDiffEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "idle");
        assert_eq!(
            entries[0].delta_pct,
            Some(0.0),
            "0/0 case should produce Some(0.0), not None"
        );
    }

    #[test]
    fn diff_tagged_ndjson_runs_uses_frame_data() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&runs_dir).unwrap();
        fs::create_dir_all(&tags_dir).unwrap();

        // Run A: NDJSON only (no .json). "compute" has 2 frames, self_ns sums to 5ms.
        let ndjson_a = r#"{"format_version":2,"run_id":"aaa_1000","timestamp_ms":1000,"functions":["compute"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"fns":[{"id":0,"calls":1,"self_ns":3000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        // Run B: NDJSON only. "compute" has 2 frames, self_ns sums to 8ms.
        let ndjson_b = r#"{"format_version":2,"run_id":"bbb_2000","timestamp_ms":2000,"functions":["compute"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":4000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"fns":[{"id":0,"calls":1,"self_ns":4000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(runs_dir.join("1000.ndjson"), ndjson_a).unwrap();
        fs::write(runs_dir.join("2000.ndjson"), ndjson_b).unwrap();

        // Tag both runs.
        fs::write(tags_dir.join("before"), "aaa_1000").unwrap();
        fs::write(tags_dir.join("after"), "bbb_2000").unwrap();

        // Load via tag path -- same path as cmd_diff uses.
        let run_a = load_tagged_run(&tags_dir, &runs_dir, "before").unwrap();
        let run_b = load_tagged_run(&tags_dir, &runs_dir, "after").unwrap();

        // Verify NDJSON data was loaded (not empty/zero).
        let compute_a = run_a
            .functions
            .iter()
            .find(|f| f.name == "compute")
            .unwrap();
        assert_eq!(
            compute_a.calls, 2,
            "run A should have 2 calls from 2 frames"
        );
        assert!(
            (compute_a.self_ms - 5.0).abs() < 0.01,
            "run A self_ms should be ~5.0ms (from NDJSON), got {}",
            compute_a.self_ms
        );

        let compute_b = run_b
            .functions
            .iter()
            .find(|f| f.name == "compute")
            .unwrap();
        assert_eq!(
            compute_b.calls, 2,
            "run B should have 2 calls from 2 frames"
        );
        assert!(
            (compute_b.self_ms - 8.0).abs() < 0.01,
            "run B self_ms should be ~8.0ms (from NDJSON), got {}",
            compute_b.self_ms
        );

        // Diff output should show the NDJSON-derived values.
        let diff = diff_runs(&run_a, &run_b, "before", "after");
        assert!(
            diff.contains("compute"),
            "diff should contain function name: {diff}"
        );
        assert!(
            diff.contains("+3.00"),
            "diff should show +3.00ms delta (8.0 - 5.0): {diff}"
        );
    }

    #[test]
    fn diff_runs_json_includes_alloc_and_cpu_fields() {
        let run_a = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 5,
                total_ms: Some(20.0),
                self_ms: 15.0,
                cpu_self_ms: Some(12.0),
                alloc_count: 100,
                alloc_bytes: 8192,
            }],
        };
        let run_b = Run {
            run_id: None,
            timestamp_ms: 2000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 7,
                total_ms: Some(25.0),
                self_ms: 18.0,
                cpu_self_ms: Some(14.0),
                alloc_count: 200,
                alloc_bytes: 16384,
            }],
        };

        let json = diff_runs_json(&run_a, &run_b);
        let entries: Vec<JsonDiffEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);

        let e = &entries[0];
        assert_eq!(e.alloc_count_a, 100);
        assert_eq!(e.alloc_count_b, 200);
        assert_eq!(e.alloc_bytes_a, 8192);
        assert_eq!(e.alloc_bytes_b, 16384);
        assert_eq!(e.cpu_self_ms_a, Some(12.0));
        assert_eq!(e.cpu_self_ms_b, Some(14.0));

        // Verify JSON keys are present via raw Value parse.
        let raw: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let obj = &raw[0];
        assert!(obj.get("alloc_count_a").is_some(), "missing alloc_count_a");
        assert!(obj.get("alloc_count_b").is_some(), "missing alloc_count_b");
        assert!(obj.get("alloc_bytes_a").is_some(), "missing alloc_bytes_a");
        assert!(obj.get("alloc_bytes_b").is_some(), "missing alloc_bytes_b");
        assert!(obj.get("cpu_self_ms_a").is_some(), "missing cpu_self_ms_a");
        assert!(obj.get("cpu_self_ms_b").is_some(), "missing cpu_self_ms_b");
    }
}
