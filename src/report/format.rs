use super::{DIM, FnEntry, HEADER, Run};

// Column widths. Each constant is the total rendered width of the column
// including any suffix (like "ms"). Header, separator, and data rows all
// use these same constants, so misalignment is impossible by construction.
const FN_W: usize = 40;
const TIME_W: usize = 11; // "12345.67ms" fits in 11; covers Self and CPU
const CALLS_W: usize = 8;
const COUNT_W: usize = 8; // alloc_count, free_count
const BYTES_W: usize = 12; // alloc_bytes, free_bytes (human-readable)

// Display precision (decimal places).
const TIME_DECIMALS: usize = 2; // ms values: "15.00ms"
const BYTES_DECIMALS: usize = 1; // human-readable: "1.5MB"

/// Format a run as a text table sorted by self_ms descending.
///
/// When `show_all` is false, entries with zero calls are hidden.
/// When `limit` is `Some(n)`, only the top `n` entries are shown.
/// A footer indicates how many were omitted (zero-call and/or truncated).
pub fn format_table(run: &Run, show_all: bool, limit: Option<usize>) -> String {
    let mut entries = run.functions.clone();
    let total_count = entries.len();
    if !show_all {
        entries.retain(|e| e.calls > 0);
    }
    entries.sort_by(|a, b| {
        b.self_ms
            .partial_cmp(&a.self_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let after_filter_count = entries.len();
    if let Some(n) = limit {
        entries.truncate(n);
    }

    let has_cpu = entries.iter().any(|e| e.cpu_self_ms.is_some());
    let has_alloc = entries.iter().any(|e| e.alloc_count > 0);
    let has_free = entries.iter().any(|e| e.free_count > 0);

    let mut out = String::new();

    let mut header = format!("{:<FN_W$} {:>TIME_W$}", "Function", "Self");
    if has_cpu {
        header.push_str(&format!(" {:>TIME_W$}", "CPU"));
    }
    header.push_str(&format!(" {:>CALLS_W$}", "Calls"));
    if has_alloc {
        header.push_str(&format!(
            " {:>COUNT_W$} {:>BYTES_W$}",
            "Allocs", "Alloc Bytes"
        ));
    }
    if has_free {
        header.push_str(&format!(
            " {:>COUNT_W$} {:>BYTES_W$}",
            "Frees", "Free Bytes"
        ));
    }
    let width = header.len();
    out.push_str(&format!("{HEADER}{header}{HEADER:#}\n"));
    out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(width)));

    for entry in &entries {
        let self_val = format!("{:.TIME_DECIMALS$}ms", entry.self_ms);
        let mut line = format!("{:<FN_W$} {:>TIME_W$}", entry.name, self_val);
        if has_cpu {
            let cpu_val = match entry.cpu_self_ms {
                Some(v) => format!("{v:.TIME_DECIMALS$}ms"),
                None => "-".to_string(),
            };
            line.push_str(&format!(" {cpu_val:>TIME_W$}"));
        }
        line.push_str(&format!(" {:>CALLS_W$}", entry.calls));
        if has_alloc {
            line.push_str(&format!(
                " {:>COUNT_W$} {:>BYTES_W$}",
                entry.alloc_count,
                format_bytes(entry.alloc_bytes)
            ));
        }
        if has_free {
            line.push_str(&format!(
                " {:>COUNT_W$} {:>BYTES_W$}",
                entry.free_count,
                format_bytes(entry.free_bytes)
            ));
        }
        out.push_str(&line);
        out.push('\n');
    }
    append_hidden_footer(&mut out, total_count, after_filter_count, entries.len());
    out
}

/// Format a byte count as a human-readable string (B, KB, MB, GB).
fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;

    let b = bytes as f64;
    if b < KB {
        format!("{bytes}B")
    } else if b < MB {
        format!("{:.BYTES_DECIMALS$}KB", b / KB)
    } else if b < GB {
        format!("{:.BYTES_DECIMALS$}MB", b / MB)
    } else {
        format!("{:.BYTES_DECIMALS$}GB", b / GB)
    }
}

/// Append a footer line describing hidden entries.
///
/// Accounts for both zero-call filtering and top-N truncation.
fn append_hidden_footer(
    out: &mut String,
    total_count: usize,
    after_filter_count: usize,
    shown_count: usize,
) {
    let zero_call_hidden = total_count - after_filter_count;
    let truncated = after_filter_count - shown_count;
    let total_hidden = zero_call_hidden + truncated;

    if total_hidden == 0 {
        return;
    }

    let label = if total_hidden == 1 {
        "function"
    } else {
        "functions"
    };

    // Build a hint about which flags would help.
    let hint = if truncated > 0 {
        // Truncation is active (may also have zero-call filtering).
        "use --top N or --all to show"
    } else {
        // Only zero-call filtering.
        "use --all to show"
    };

    out.push_str(&format!(
        "{DIM}\n{total_hidden} {label} hidden; {hint}\n{DIM:#}"
    ));
}

/// Format multiple thread runs as separate tables, one per thread.
///
/// Each section is prefixed with a thread header showing the 1-based index.
/// For a single thread, this produces output identical to `format_table` but
/// with a "Thread 1" header.
pub fn format_per_thread_tables(runs: &[Run], show_all: bool, limit: Option<usize>) -> String {
    let mut out = String::new();
    for (i, run) in runs.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{HEADER}--- Thread {} ---{HEADER:#}\n", i + 1));
        out.push_str(&format_table(run, show_all, limit));
    }
    out
}

/// Structured JSON entry for a single function in a report.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonFnEntry {
    pub name: String,
    pub self_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_self_ms: Option<f64>,
    pub calls: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}

/// Serialize a `Run` as a JSON array of function entries.
///
/// Mirrors the table columns: function name, self time, CPU time, calls,
/// alloc count, alloc bytes, free count, free bytes. Sorted by self time descending.
/// When `limit` is `Some(n)`, only the top `n` entries are included.
pub fn format_json(run: &Run, show_all: bool, limit: Option<usize>) -> String {
    let mut entries: Vec<&FnEntry> = run.functions.iter().collect();
    if !show_all {
        entries.retain(|e| e.calls > 0);
    }
    entries.sort_by(|a, b| {
        b.self_ms
            .partial_cmp(&a.self_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if let Some(n) = limit {
        entries.truncate(n);
    }

    let json_entries: Vec<JsonFnEntry> = entries
        .iter()
        .map(|e| JsonFnEntry {
            name: e.name.clone(),
            self_ms: e.self_ms,
            cpu_self_ms: e.cpu_self_ms,
            calls: e.calls,
            alloc_count: e.alloc_count,
            alloc_bytes: e.alloc_bytes,
            free_count: e.free_count,
            free_bytes: e.free_bytes,
        })
        .collect();

    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

#[cfg(test)]
mod tests {
    use super::super::{FnEntry, Run, RunFormat};
    use super::*;

    use crate::report::test_util::assert_aligned;

    #[test]
    fn format_table_sorts_by_self_time() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "fast".into(),
                    calls: 1,
                    total_ms: Some(2.0),
                    self_ms: 1.0,
                    ..Default::default()
                },
                FnEntry {
                    name: "slow".into(),
                    calls: 1,
                    total_ms: Some(20.0),
                    self_ms: 15.0,
                    ..Default::default()
                },
            ],
        };
        let table = format_table(&run, true, None);
        let slow_pos = table.find("slow").expect("slow not in table");
        let fast_pos = table.find("fast").expect("fast not in table");
        assert!(
            slow_pos < fast_pos,
            "slow (self_ms=15) should appear before fast (self_ms=1)"
        );
    }

    #[test]
    fn format_table_hides_zero_call_entries_by_default() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "called".into(),
                    calls: 5,
                    total_ms: Some(10.0),
                    self_ms: 8.0,
                    ..Default::default()
                },
                FnEntry {
                    name: "uncalled".into(),
                    ..Default::default()
                },
            ],
        };
        let table = format_table(&run, false, None);
        assert!(table.contains("called"), "should show called function");
        assert!(
            !table.contains("uncalled"),
            "should hide zero-call function"
        );

        // Footer should indicate hidden functions.
        assert!(
            table.contains("1 function hidden; use --all to show"),
            "should show hidden footer. Got:\n{table}"
        );

        let table_all = format_table(&run, true, None);
        assert!(
            table_all.contains("uncalled"),
            "should show zero-call function with show_all"
        );
        // No footer when show_all is true.
        assert!(
            !table_all.contains("hidden"),
            "should not show footer with show_all. Got:\n{table_all}"
        );
    }

    #[test]
    fn format_table_no_footer_when_all_called() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "active".into(),
                calls: 3,
                total_ms: Some(5.0),
                self_ms: 4.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("hidden"),
            "no footer when nothing hidden. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_shows_cpu_column_when_present() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "compute".into(),
                calls: 10,
                total_ms: Some(50.0),
                self_ms: 40.0,
                cpu_self_ms: Some(35.0),
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            table.contains("CPU"),
            "should have CPU column header. Got:\n{table}"
        );
        assert!(
            table.contains("35.00"),
            "should show CPU ms value. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_hides_cpu_column_when_absent() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "compute".into(),
                calls: 10,
                total_ms: Some(50.0),
                self_ms: 40.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("CPU"),
            "should not have CPU column. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_cpu_with_all_includes_zero_call() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "active".into(),
                    calls: 5,
                    total_ms: Some(20.0),
                    self_ms: 15.0,
                    cpu_self_ms: Some(12.0),
                    ..Default::default()
                },
                FnEntry {
                    name: "unused".into(),
                    cpu_self_ms: Some(0.0),
                    ..Default::default()
                },
            ],
        };
        // Without --all: hides unused.
        let table = format_table(&run, false, None);
        assert!(table.contains("CPU"), "should have CPU column");
        assert!(!table.contains("unused"), "should hide zero-call fn");
        assert!(
            table.contains("1 function hidden; use --all to show"),
            "should show hidden footer. Got:\n{table}"
        );

        // With --all: shows unused with CPU column present.
        let table_all = format_table(&run, true, None);
        assert!(table_all.contains("CPU"), "should have CPU column");
        assert!(
            table_all.contains("unused"),
            "should show zero-call fn with --all. Got:\n{table_all}"
        );
        assert!(
            !table_all.contains("hidden"),
            "should not show footer with show_all. Got:\n{table_all}"
        );
    }

    #[test]
    fn format_table_no_total_column() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 5,
                total_ms: Some(20.0),
                self_ms: 15.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("Total"),
            "Total column should not appear. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_self_before_calls() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 5,
                total_ms: Some(20.0),
                self_ms: 15.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        let self_pos = table.find("Self").expect("Self header missing");
        let calls_pos = table.find("Calls").expect("Calls header missing");
        assert!(
            self_pos < calls_pos,
            "Self column should appear before Calls. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_cpu_column_order() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 5,
                total_ms: Some(20.0),
                self_ms: 15.0,
                cpu_self_ms: Some(12.0),
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("Total"),
            "Total column should not appear with CPU. Got:\n{table}"
        );
        let self_pos = table.find("Self").expect("Self header missing");
        let cpu_pos = table.find("CPU").expect("CPU header missing");
        let calls_pos = table.find("Calls").expect("Calls header missing");
        assert!(
            self_pos < cpu_pos && cpu_pos < calls_pos,
            "Column order should be Self | CPU | Calls. Got:\n{table}"
        );
    }

    #[test]
    fn format_json_sorts_by_self_time() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "fast".into(),
                    calls: 1,
                    self_ms: 1.0,
                    ..Default::default()
                },
                FnEntry {
                    name: "slow".into(),
                    calls: 2,
                    self_ms: 15.0,
                    ..Default::default()
                },
            ],
        };
        let json = format_json(&run, false, None);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "slow");
        assert!((entries[0].self_ms - 15.0).abs() < f64::EPSILON);
        assert_eq!(entries[0].calls, 2);
        assert_eq!(entries[1].name, "fast");
    }

    #[test]
    fn format_json_filters_zero_calls() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "called".into(),
                    calls: 5,
                    self_ms: 3.0,
                    ..Default::default()
                },
                FnEntry {
                    name: "unused".into(),
                    calls: 0,
                    self_ms: 0.0,
                    ..Default::default()
                },
            ],
        };
        let json = format_json(&run, false, None);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "called");

        let json_all = format_json(&run, true, None);
        let entries_all: Vec<JsonFnEntry> = serde_json::from_str(&json_all).unwrap();
        assert_eq!(entries_all.len(), 2);
    }

    #[test]
    fn format_json_includes_cpu_time() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                self_ms: 10.0,
                cpu_self_ms: Some(8.5),
                alloc_count: 42,
                alloc_bytes: 1024,
                ..Default::default()
            }],
        };
        let json = format_json(&run, false, None);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries[0].cpu_self_ms, Some(8.5));
        assert_eq!(entries[0].alloc_count, 42);
        assert_eq!(entries[0].alloc_bytes, 1024);
    }

    /// Helper: build a Run with N functions named fn_1..fn_N,
    /// each with calls=1 and self_ms = index (so fn_1 is slowest when sorted desc).
    fn make_run_with_n_fns(n: usize) -> Run {
        Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: (0..n)
                .map(|i| FnEntry {
                    name: format!("fn_{}", i + 1),
                    calls: 1,
                    total_ms: Some((n - i) as f64 * 2.0),
                    self_ms: (n - i) as f64,
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[test]
    fn format_table_limit_truncates_output() {
        let run = make_run_with_n_fns(5);
        let table = format_table(&run, true, Some(3));
        // Only the top 3 by self_ms should appear (fn_1, fn_2, fn_3).
        assert!(table.contains("fn_1"), "should show fn_1 (highest self_ms)");
        assert!(table.contains("fn_2"), "should show fn_2");
        assert!(table.contains("fn_3"), "should show fn_3");
        assert!(!table.contains("fn_4"), "should hide fn_4 (truncated)");
        assert!(!table.contains("fn_5"), "should hide fn_5 (truncated)");
    }

    #[test]
    fn format_table_limit_shows_truncation_footer() {
        let run = make_run_with_n_fns(5);
        let table = format_table(&run, false, Some(2));
        // 5 entries, all called, top 2 shown => 3 truncated.
        assert!(
            table.contains("3 functions hidden; use --top N or --all to show"),
            "should show truncation footer. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_limit_and_zero_call_combined_footer() {
        let mut run = make_run_with_n_fns(4);
        // Add 2 zero-call entries.
        run.functions.push(FnEntry {
            name: "unused_a".into(),
            ..Default::default()
        });
        run.functions.push(FnEntry {
            name: "unused_b".into(),
            ..Default::default()
        });
        // show_all=false hides the 2 zero-call entries; limit=Some(2) truncates to 2.
        // Total functions: 6, after zero-call filter: 4, shown: 2, hidden: 4.
        let table = format_table(&run, false, Some(2));
        assert!(
            table.contains("4 functions hidden"),
            "should combine zero-call and truncation count. Got:\n{table}"
        );
        assert!(
            table.contains("use --top N or --all to show"),
            "should hint both flags. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_limit_none_shows_all_called() {
        let run = make_run_with_n_fns(5);
        // No limit, show_all=true => all 5 shown, no footer.
        let table = format_table(&run, true, None);
        for i in 1..=5 {
            assert!(
                table.contains(&format!("fn_{i}")),
                "should show fn_{i} with no limit"
            );
        }
        assert!(
            !table.contains("hidden"),
            "no footer when nothing hidden. Got:\n{table}"
        );
    }

    #[test]
    fn format_json_limit_truncates_output() {
        let run = make_run_with_n_fns(5);
        let json_str = format_json(&run, true, Some(3));
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(entries.len(), 3, "limit=3 should produce 3 entries");
        // Sorted by self_ms desc: fn_1 (5.0), fn_2 (4.0), fn_3 (3.0).
        assert_eq!(entries[0].name, "fn_1");
        assert_eq!(entries[1].name, "fn_2");
        assert_eq!(entries[2].name, "fn_3");
    }

    #[test]
    fn format_json_limit_with_zero_call_filter() {
        let mut run = make_run_with_n_fns(3);
        run.functions.push(FnEntry {
            name: "unused".into(),
            ..Default::default()
        });
        // show_all=false hides zero-call, limit=2 truncates.
        let json_str = format_json(&run, false, Some(2));
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(entries.len(), 2, "should have 2 entries after filter+limit");
        assert!(
            entries.iter().all(|e| e.name != "unused"),
            "zero-call entry should be hidden"
        );
    }

    #[test]
    fn format_table_shows_alloc_columns_when_present() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "fetch_all".into(),
                calls: 1,
                total_ms: Some(350.0),
                self_ms: 341.21,
                alloc_count: 840,
                alloc_bytes: 64000,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            table.contains("Allocs"),
            "should have Allocs column header. Got:\n{table}"
        );
        assert!(
            table.contains("Alloc Bytes"),
            "should have Alloc Bytes column header. Got:\n{table}"
        );
        assert!(
            table.contains("840"),
            "should show alloc count. Got:\n{table}"
        );
        assert!(
            table.contains("62.5KB"),
            "should show alloc bytes as human-readable. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_hides_alloc_columns_when_zero() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "compute".into(),
                calls: 10,
                total_ms: Some(50.0),
                self_ms: 40.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("Allocs"),
            "should not have Allocs column. Got:\n{table}"
        );
        assert!(
            !table.contains("Alloc Bytes"),
            "should not have Alloc Bytes column. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_shows_both_cpu_and_alloc_when_present() {
        let run = Run {
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
                alloc_bytes: 2048,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            table.contains("CPU"),
            "should have CPU column header. Got:\n{table}"
        );
        assert!(
            table.contains("Allocs"),
            "should have Allocs column header. Got:\n{table}"
        );
        assert!(
            table.contains("Alloc Bytes"),
            "should have Alloc Bytes column header. Got:\n{table}"
        );
        // Verify column order: Self < CPU < Calls < Allocs < Alloc Bytes
        let self_pos = table.find("Self").expect("Self header missing");
        let cpu_pos = table.find("CPU").expect("CPU header missing");
        let calls_pos = table.find("Calls").expect("Calls header missing");
        let allocs_pos = table.find("Allocs").expect("Allocs header missing");
        assert!(
            self_pos < cpu_pos && cpu_pos < calls_pos && calls_pos < allocs_pos,
            "Column order should be Self | CPU | Calls | Allocs | Alloc Bytes. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_alloc_bytes_human_readable() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![
                FnEntry {
                    name: "tiny".into(),
                    calls: 1,
                    self_ms: 1.0,
                    alloc_count: 1,
                    alloc_bytes: 512,
                    ..Default::default()
                },
                FnEntry {
                    name: "medium".into(),
                    calls: 1,
                    self_ms: 2.0,
                    alloc_count: 10,
                    alloc_bytes: 1_048_576, // 1 MB
                    ..Default::default()
                },
                FnEntry {
                    name: "large".into(),
                    calls: 1,
                    self_ms: 3.0,
                    alloc_count: 100,
                    alloc_bytes: 1_073_741_824, // 1 GB
                    ..Default::default()
                },
            ],
        };
        let table = format_table(&run, false, None);
        assert!(
            table.contains("512B"),
            "should show bytes for small values. Got:\n{table}"
        );
        assert!(
            table.contains("1.0MB"),
            "should show MB for megabyte values. Got:\n{table}"
        );
        assert!(
            table.contains("1.0GB"),
            "should show GB for gigabyte values. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_shows_free_columns_when_present() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "dealloc_heavy".into(),
                calls: 1,
                total_ms: Some(50.0),
                self_ms: 40.0,
                free_count: 200,
                free_bytes: 32768,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            table.contains("Frees"),
            "should have Frees column header. Got:\n{table}"
        );
        assert!(
            table.contains("Free Bytes"),
            "should have Free Bytes column header. Got:\n{table}"
        );
        assert!(
            table.contains("200"),
            "should show free count. Got:\n{table}"
        );
        assert!(
            table.contains("32.0KB"),
            "should show free bytes as human-readable. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_hides_free_columns_when_zero() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "compute".into(),
                calls: 10,
                total_ms: Some(50.0),
                self_ms: 40.0,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(
            !table.contains("Frees"),
            "should not have Frees column. Got:\n{table}"
        );
        assert!(
            !table.contains("Free Bytes"),
            "should not have Free Bytes column. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_shows_alloc_and_free_together() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "churn".into(),
                calls: 1,
                self_ms: 10.0,
                alloc_count: 500,
                alloc_bytes: 1_048_576,
                free_count: 480,
                free_bytes: 1_000_000,
                ..Default::default()
            }],
        };
        let table = format_table(&run, false, None);
        assert!(table.contains("Allocs"), "should have Allocs column");
        assert!(table.contains("Frees"), "should have Frees column");
        // Verify column order: Allocs before Frees
        let allocs_pos = table.find("Allocs").expect("Allocs header missing");
        let frees_pos = table.find("Frees").expect("Frees header missing");
        assert!(
            allocs_pos < frees_pos,
            "Allocs should appear before Frees. Got:\n{table}"
        );
    }

    /// Structural invariant: all content lines (header, separator, data)
    /// must have equal visible width.
    #[test]
    fn format_table_columns_aligned() {
        fn run_with(entry: FnEntry) -> Run {
            Run {
                run_id: None,
                timestamp_ms: 1000,
                source_format: RunFormat::default(),
                functions: vec![entry],
            }
        }

        // Minimal: Self + Calls only (no optional columns)
        let base = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            ..Default::default()
        });
        assert_aligned(&format_table(&base, false, None), "base");

        // Self + CPU + Calls
        let with_cpu = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            cpu_self_ms: Some(12.0),
            ..Default::default()
        });
        assert_aligned(&format_table(&with_cpu, false, None), "cpu");

        // Self + Calls + Allocs
        let with_alloc = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            alloc_count: 100,
            alloc_bytes: 2048,
            ..Default::default()
        });
        assert_aligned(&format_table(&with_alloc, false, None), "alloc");

        // Self + Calls + Frees
        let with_free = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            free_count: 80,
            free_bytes: 1500,
            ..Default::default()
        });
        assert_aligned(&format_table(&with_free, false, None), "free");

        // Self + CPU + Calls + Allocs
        let cpu_alloc = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            cpu_self_ms: Some(12.0),
            alloc_count: 100,
            alloc_bytes: 2048,
            ..Default::default()
        });
        assert_aligned(&format_table(&cpu_alloc, false, None), "cpu+alloc");

        // Self + CPU + Calls + Frees
        let cpu_free = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            cpu_self_ms: Some(12.0),
            free_count: 80,
            free_bytes: 1500,
            ..Default::default()
        });
        assert_aligned(&format_table(&cpu_free, false, None), "cpu+free");

        // Self + Calls + Allocs + Frees
        let alloc_free = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            alloc_count: 100,
            alloc_bytes: 2048,
            free_count: 80,
            free_bytes: 1500,
            ..Default::default()
        });
        assert_aligned(&format_table(&alloc_free, false, None), "alloc+free");

        // All columns: Self + CPU + Calls + Allocs + Frees
        let all_cols = run_with(FnEntry {
            name: "work".into(),
            calls: 5,
            self_ms: 15.0,
            cpu_self_ms: Some(12.0),
            alloc_count: 100,
            alloc_bytes: 2048,
            free_count: 80,
            free_bytes: 1500,
            ..Default::default()
        });
        assert_aligned(&format_table(&all_cols, false, None), "all");
    }

    #[test]
    fn per_thread_tables_produces_thread_headers_and_tables() {
        let runs = vec![
            Run {
                run_id: None,
                timestamp_ms: 1000,
                source_format: RunFormat::default(),
                functions: vec![FnEntry {
                    name: "work".into(),
                    calls: 10,
                    self_ms: 50.0,
                    ..Default::default()
                }],
            },
            Run {
                run_id: None,
                timestamp_ms: 1000,
                source_format: RunFormat::default(),
                functions: vec![FnEntry {
                    name: "work".into(),
                    calls: 5,
                    self_ms: 25.0,
                    ..Default::default()
                }],
            },
        ];
        let output = format_per_thread_tables(&runs, false, None);
        assert!(
            output.contains("Thread 1"),
            "should have Thread 1 header: {output}"
        );
        assert!(
            output.contains("Thread 2"),
            "should have Thread 2 header: {output}"
        );
        assert_eq!(
            output.matches("Function").count(),
            2,
            "should have two table headers (one per thread): {output}"
        );
        assert_eq!(
            output.matches("work").count(),
            2,
            "should show work in both threads: {output}"
        );
    }

    #[test]
    fn per_thread_tables_columns_aligned() {
        // Alignment originates in format_table, not the composition layer.
        // Test format_table directly on each thread's Run, matching the
        // pattern in format_table_columns_aligned.
        let thread_runs = [
            Run {
                run_id: None,
                timestamp_ms: 1000,
                source_format: RunFormat::default(),
                functions: vec![FnEntry {
                    name: "compute".into(),
                    calls: 5,
                    self_ms: 42.0,
                    ..Default::default()
                }],
            },
            Run {
                run_id: None,
                timestamp_ms: 1000,
                source_format: RunFormat::default(),
                functions: vec![FnEntry {
                    name: "render".into(),
                    calls: 3,
                    self_ms: 18.0,
                    ..Default::default()
                }],
            },
        ];
        for (i, run) in thread_runs.iter().enumerate() {
            assert_aligned(
                &format_table(run, false, None),
                &format!("thread-{}", i + 1),
            );
        }
    }

    #[test]
    fn format_json_includes_free_fields() {
        let run = Run {
            run_id: None,
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 1,
                self_ms: 10.0,
                alloc_count: 42,
                alloc_bytes: 1024,
                free_count: 38,
                free_bytes: 900,
                ..Default::default()
            }],
        };
        let json = format_json(&run, false, None);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries[0].free_count, 38);
        assert_eq!(entries[0].free_bytes, 900);
    }
}
