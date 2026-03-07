use std::collections::HashMap;

use super::{DIM, FnEntry, FrameData, FrameFnEntry, HEADER, Run, format_bytes, format_ns};

/// Format a run as a text table sorted by self_ms descending.
///
/// When `show_all` is false, entries with zero calls are hidden and a
/// footer indicates how many were omitted.
pub fn format_table(run: &Run, show_all: bool) -> String {
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

    let has_cpu = entries.iter().any(|e| e.cpu_self_ms.is_some());

    let mut out = String::new();
    if has_cpu {
        out.push_str(&format!(
            "{HEADER}{:<40} {:>10} {:>10} {:>8}{HEADER:#}\n",
            "Function", "Self", "CPU", "Calls"
        ));
        out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(72)));
    } else {
        out.push_str(&format!(
            "{HEADER}{:<40} {:>10} {:>8}{HEADER:#}\n",
            "Function", "Self", "Calls"
        ));
        out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(60)));
    }

    for entry in &entries {
        if has_cpu {
            let cpu_str = match entry.cpu_self_ms {
                Some(v) => format!("{v:>9.2}ms"),
                None => format!("{:>11}", "-"),
            };
            out.push_str(&format!(
                "{:<40} {:>9.2}ms {} {:>8}\n",
                entry.name, entry.self_ms, cpu_str, entry.calls
            ));
        } else {
            out.push_str(&format!(
                "{:<40} {:>9.2}ms {:>8}\n",
                entry.name, entry.self_ms, entry.calls
            ));
        }
    }
    if !show_all {
        let hidden = total_count - entries.len();
        if hidden > 0 {
            let label = if hidden == 1 { "function" } else { "functions" };
            out.push_str(&format!(
                "{DIM}\n{hidden} {label} hidden; use --all to show\n{DIM:#}"
            ));
        }
    }
    out
}

/// Format multiple thread runs as separate tables, one per thread.
///
/// Each section is prefixed with a thread header showing the 1-based index.
/// For a single thread, this produces output identical to `format_table` but
/// with a "Thread 1" header.
pub fn format_per_thread_tables(runs: &[Run], show_all: bool) -> String {
    let mut out = String::new();
    for (i, run) in runs.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{HEADER}--- Thread {} ---{HEADER:#}\n", i + 1));
        out.push_str(&format_table(run, show_all));
    }
    out
}

/// Group frame data by thread ID, returning a sorted Vec of (tid, FrameData).
fn group_frames_by_tid(frame_data: &FrameData) -> Vec<(usize, FrameData)> {
    let mut by_thread: HashMap<usize, Vec<Vec<FrameFnEntry>>> = HashMap::new();
    for frame in &frame_data.frames {
        let tid = frame.first().and_then(|e| e.tid).unwrap_or(0);
        by_thread.entry(tid).or_default().push(frame.to_vec());
    }
    let mut threads: Vec<(usize, FrameData)> = by_thread
        .into_iter()
        .map(|(tid, frames)| {
            (
                tid,
                FrameData {
                    fn_names: frame_data.fn_names.clone(),
                    frames,
                },
            )
        })
        .collect();
    threads.sort_by_key(|(tid, _)| *tid);
    threads
}

/// Per-thread JSON entry.
#[derive(serde::Serialize)]
struct JsonThreadEntry {
    thread: usize,
    functions: Vec<JsonFnEntry>,
}

/// Serialize per-thread aggregated data as a JSON array.
pub fn format_per_thread_json(frame_data: &FrameData, show_all: bool) -> String {
    let threads = group_frames_by_tid(frame_data);
    let entries: Vec<JsonThreadEntry> = threads
        .into_iter()
        .map(|(tid, thread_frames)| {
            let functions = aggregate_frames_to_json_entries(&thread_frames, show_all);
            JsonThreadEntry {
                thread: tid,
                functions,
            }
        })
        .collect();

    serde_json::to_string_pretty(&entries).expect("JSON serialization should not fail")
}

/// Format per-thread breakdown tables from frame data.
pub fn format_per_thread_tables_from_frames(frame_data: &FrameData, show_all: bool) -> String {
    let threads = group_frames_by_tid(frame_data);
    let mut out = String::new();
    for (i, (tid, thread_frames)) in threads.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{HEADER}--- Thread {tid} ---{HEADER:#}\n"));
        out.push_str(&format_table_with_frames(thread_frames, show_all));
    }
    out
}

/// Format frame-level data as a summary table with allocation columns.
///
/// Columns: Function | Self | Calls | Allocs | Alloc Bytes
/// Footer: hidden-function count when applicable.
pub fn format_table_with_frames(frame_data: &FrameData, show_all: bool) -> String {
    struct FnStats {
        name: String,
        total_calls: u64,
        total_self_ns: u64,
        total_cpu_self_ns: Option<u64>,
        total_allocs: u64,
        total_alloc_bytes: u64,
    }

    let has_cpu = frame_data
        .frames
        .iter()
        .any(|f| f.iter().any(|e| e.cpu_self_ns.is_some()));

    let mut stats_map: HashMap<usize, FnStats> = HashMap::new();
    for frame in &frame_data.frames {
        for entry in frame {
            let fn_id = entry.fn_id;
            let stats = stats_map.entry(fn_id).or_insert_with(|| FnStats {
                name: frame_data
                    .fn_names
                    .get(fn_id)
                    .cloned()
                    .unwrap_or_else(|| format!("<fn_{fn_id}>")),
                total_calls: 0,
                total_self_ns: 0,
                total_cpu_self_ns: if has_cpu { Some(0) } else { None },
                total_allocs: 0,
                total_alloc_bytes: 0,
            });
            stats.total_calls += entry.calls;
            stats.total_self_ns += entry.self_ns;
            if let (Some(total), Some(cpu)) = (&mut stats.total_cpu_self_ns, entry.cpu_self_ns) {
                *total += cpu;
            }
            stats.total_allocs += entry.alloc_count;
            stats.total_alloc_bytes += entry.alloc_bytes;
        }
    }

    // Include zero-call functions from fn_names when show_all is set.
    if show_all {
        for (fn_id, name) in frame_data.fn_names.iter().enumerate() {
            stats_map.entry(fn_id).or_insert_with(|| FnStats {
                name: name.clone(),
                total_calls: 0,
                total_self_ns: 0,
                total_cpu_self_ns: if has_cpu { Some(0) } else { None },
                total_allocs: 0,
                total_alloc_bytes: 0,
            });
        }
    }

    let mut entries: Vec<FnStats> = stats_map.into_values().collect();
    let total_count = frame_data.fn_names.len();
    if !show_all {
        entries.retain(|e| e.total_calls > 0);
    }
    entries.sort_by(|a, b| b.total_self_ns.cmp(&a.total_self_ns));

    let mut out = String::new();
    if has_cpu {
        out.push_str(&format!(
            "{HEADER}{:<40} {:>10} {:>10} {:>8} {:>8} {:>12}{HEADER:#}\n",
            "Function", "Self", "CPU", "Calls", "Allocs", "Alloc Bytes"
        ));
        out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(93)));
    } else {
        out.push_str(&format!(
            "{HEADER}{:<40} {:>10} {:>8} {:>8} {:>12}{HEADER:#}\n",
            "Function", "Self", "Calls", "Allocs", "Alloc Bytes"
        ));
        out.push_str(&format!("{DIM}{}{DIM:#}\n", "-".repeat(82)));
    }

    for e in &entries {
        let self_str = format_ns(e.total_self_ns);
        let bytes_str = format_bytes(e.total_alloc_bytes);
        if has_cpu {
            let cpu_str = match e.total_cpu_self_ns {
                Some(ns) => format_ns(ns),
                None => format!("{:>10}", "-"),
            };
            out.push_str(&format!(
                "{:<40} {:>10} {:>10} {:>8} {:>8} {:>12}\n",
                e.name, self_str, cpu_str, e.total_calls, e.total_allocs, bytes_str
            ));
        } else {
            out.push_str(&format!(
                "{:<40} {:>10} {:>8} {:>8} {:>12}\n",
                e.name, self_str, e.total_calls, e.total_allocs, bytes_str
            ));
        }
    }

    let hidden = total_count - entries.len();
    if hidden > 0 {
        let label = if hidden == 1 { "function" } else { "functions" };
        out.push_str(&format!(
            "{DIM}\n{hidden} {label} hidden; use --all to show{DIM:#}"
        ));
    }

    out
}

/// Format per-frame breakdown table.
///
/// Each row is one frame. Columns: Frame | Total | [one column per function] | Allocs | Alloc Bytes
pub fn format_frames_table(frame_data: &FrameData) -> String {
    let fn_names = &frame_data.fn_names;
    let n_fns = fn_names.len();

    // Compute per-function column width: at least 12, or the longest name.
    let fn_col_width = fn_names.iter().map(|n| n.len()).max().unwrap_or(12).max(12);

    // Header.
    let mut out = String::new();
    out.push_str(&format!("{HEADER}{:>6} {:>10}", "Frame", "Total"));
    for name in fn_names {
        out.push_str(&format!(" {name:>fn_col_width$}"));
    }
    out.push_str(&format!(
        " {:>8} {:>12}{HEADER:#}\n",
        "Allocs", "Alloc Bytes"
    ));
    out.push_str(&format!(
        "{DIM}{}{DIM:#}\n",
        "-".repeat(34 + n_fns * (fn_col_width + 1))
    ));

    // Rows.
    for (i, frame) in frame_data.frames.iter().enumerate() {
        let total: u64 = frame.iter().map(|e| e.self_ns).sum();

        out.push_str(&format!("{:>6} {:>10}", i + 1, format_ns(total)));

        // One column per function.
        for fn_id in 0..n_fns {
            let entry = frame.iter().find(|e| e.fn_id == fn_id);
            let ns = entry.map_or(0, |e| e.self_ns);
            out.push_str(&format!(" {:>width$}", format_ns(ns), width = fn_col_width));
        }

        let allocs: u64 = frame.iter().map(|e| e.alloc_count).sum();
        let bytes: u64 = frame.iter().map(|e| e.alloc_bytes).sum();
        out.push_str(&format!(" {:>8} {:>12}", allocs, format_bytes(bytes)));
        out.push('\n');
    }

    out.push_str(&format!(
        "{DIM}\n{} frames\n{DIM:#}",
        frame_data.frames.len(),
    ));

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
}

/// Serialize a `Run` as a JSON array of function entries.
///
/// Mirrors the table columns: function name, self time, CPU time, calls,
/// alloc count, alloc bytes. Sorted by self time descending.
pub fn format_json(run: &Run, show_all: bool) -> String {
    let mut entries: Vec<&FnEntry> = run.functions.iter().collect();
    if !show_all {
        entries.retain(|e| e.calls > 0);
    }
    entries.sort_by(|a, b| {
        b.self_ms
            .partial_cmp(&a.self_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let json_entries: Vec<JsonFnEntry> = entries
        .iter()
        .map(|e| JsonFnEntry {
            name: e.name.clone(),
            self_ms: e.self_ms,
            cpu_self_ms: e.cpu_self_ms,
            calls: e.calls,
            alloc_count: e.alloc_count,
            alloc_bytes: e.alloc_bytes,
        })
        .collect();

    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

/// Accumulated per-function counters for JSON frame aggregation.
#[derive(Default)]
struct JsonFnAgg {
    name: String,
    calls: u64,
    self_ns: u64,
    cpu_self_ns: Option<u64>,
    alloc_count: u64,
    alloc_bytes: u64,
}

/// Aggregate per-frame data into per-function `JsonFnEntry` totals.
///
/// This is the shared logic behind `format_json_with_frames` (which serializes
/// the result) and `format_per_thread_json` (which nests the entries by thread).
fn aggregate_frames_to_json_entries(frame_data: &FrameData, show_all: bool) -> Vec<JsonFnEntry> {
    let has_cpu = frame_data
        .frames
        .iter()
        .any(|f| f.iter().any(|e| e.cpu_self_ns.is_some()));

    let mut stats_map: HashMap<usize, JsonFnAgg> = HashMap::new();
    for frame in &frame_data.frames {
        for entry in frame {
            let fn_id = entry.fn_id;
            let stats = stats_map.entry(fn_id).or_insert_with(|| {
                let name = frame_data
                    .fn_names
                    .get(fn_id)
                    .cloned()
                    .unwrap_or_else(|| format!("<fn_{fn_id}>"));
                JsonFnAgg {
                    name,
                    cpu_self_ns: if has_cpu { Some(0) } else { None },
                    ..Default::default()
                }
            });
            stats.calls += entry.calls;
            stats.self_ns += entry.self_ns;
            if let (Some(total), Some(cpu)) = (&mut stats.cpu_self_ns, entry.cpu_self_ns) {
                *total += cpu;
            }
            stats.alloc_count += entry.alloc_count;
            stats.alloc_bytes += entry.alloc_bytes;
        }
    }

    if show_all {
        for (fn_id, name) in frame_data.fn_names.iter().enumerate() {
            stats_map.entry(fn_id).or_insert_with(|| JsonFnAgg {
                name: name.clone(),
                cpu_self_ns: if has_cpu { Some(0) } else { None },
                ..Default::default()
            });
        }
    }

    let mut entries: Vec<JsonFnAgg> = stats_map.into_values().collect();
    if !show_all {
        entries.retain(|e| e.calls > 0);
    }
    entries.sort_by(|a, b| b.self_ns.cmp(&a.self_ns));

    entries
        .iter()
        .map(|e| JsonFnEntry {
            name: e.name.clone(),
            self_ms: e.self_ns as f64 / 1_000_000.0,
            cpu_self_ms: e.cpu_self_ns.map(|ns| ns as f64 / 1_000_000.0),
            calls: e.calls,
            alloc_count: e.alloc_count,
            alloc_bytes: e.alloc_bytes,
        })
        .collect()
}

/// Serialize frame-aggregated data as a JSON array of function entries.
///
/// Aggregates per-frame data into per-function totals, matching the summary
/// table structure. Self time is converted from nanoseconds to milliseconds.
pub fn format_json_with_frames(frame_data: &FrameData, show_all: bool) -> String {
    let json_entries = aggregate_frames_to_json_entries(frame_data, show_all);
    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

/// Per-frame JSON entry for --frames --json output.
#[derive(serde::Serialize)]
struct JsonFrameEntry {
    frame: usize,
    functions: Vec<JsonFnEntry>,
}

/// Serialize frame-level data as a JSON array of per-frame objects.
///
/// Each frame contains its 1-based index and a list of function entries
/// with timing and allocation data. Functions are sorted by self time
/// descending within each frame.
pub fn format_frames_json(frame_data: &FrameData, show_all: bool) -> String {
    let has_cpu = frame_data
        .frames
        .iter()
        .any(|f| f.iter().any(|e| e.cpu_self_ns.is_some()));

    let json_frames: Vec<JsonFrameEntry> = frame_data
        .frames
        .iter()
        .enumerate()
        .map(|(i, frame)| {
            let mut fns: Vec<JsonFnEntry> = frame
                .iter()
                .filter(|e| show_all || e.calls > 0)
                .map(|e| {
                    let name = frame_data
                        .fn_names
                        .get(e.fn_id)
                        .cloned()
                        .unwrap_or_else(|| format!("<fn_{}>", e.fn_id));
                    JsonFnEntry {
                        name,
                        self_ms: e.self_ns as f64 / 1_000_000.0,
                        cpu_self_ms: if has_cpu {
                            Some(e.cpu_self_ns.unwrap_or(0) as f64 / 1_000_000.0)
                        } else {
                            None
                        },
                        calls: e.calls,
                        alloc_count: e.alloc_count,
                        alloc_bytes: e.alloc_bytes,
                    }
                })
                .collect();
            fns.sort_by(|a, b| {
                b.self_ms
                    .partial_cmp(&a.self_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            JsonFrameEntry {
                frame: i + 1,
                functions: fns,
            }
        })
        .collect();

    serde_json::to_string_pretty(&json_frames).expect("JSON serialization should not fail")
}

#[cfg(test)]
mod tests {
    use super::super::{FnEntry, FrameData, FrameFnEntry, Run, RunFormat};
    use super::*;

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
        let table = format_table(&run, true);
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
        let table = format_table(&run, false);
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

        let table_all = format_table(&run, true);
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
        let table = format_table(&run, false);
        assert!(
            !table.contains("hidden"),
            "no footer when nothing hidden. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_with_frames_shows_allocs() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "physics".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 2_000_000,
                        cpu_self_ns: None,
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 1_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 8_000_000,
                        cpu_self_ns: None,
                        alloc_count: 50,
                        alloc_bytes: 16384,
                        free_count: 40,
                        free_bytes: 12288,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 1_100_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
            ],
        };
        let table = format_table_with_frames(&frame_data, true);
        assert!(!table.contains("p50"), "should not have p50 column");
        assert!(!table.contains("p99"), "should not have p99 column");
        assert!(
            !table.contains("frames"),
            "should not have frame count footer"
        );
        assert!(table.contains("Allocs"), "should have allocs column");
        assert!(
            table.contains("Alloc Bytes"),
            "should have alloc bytes column"
        );
        assert!(table.contains("update"), "should list update");
        assert!(table.contains("physics"), "should list physics");
    }

    #[test]
    fn format_frames_table_shows_per_frame_breakdown() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "physics".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 2_000_000,
                        cpu_self_ns: None,
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 1_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 2_100_000,
                        cpu_self_ns: None,
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 1_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 12_000_000,
                        cpu_self_ns: None,
                        alloc_count: 50,
                        alloc_bytes: 16384,
                        free_count: 40,
                        free_bytes: 12288,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 1_100_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
            ],
        };
        let table = format_frames_table(&frame_data);
        assert!(table.contains("Frame"), "should have Frame column header");
        assert!(table.contains("update"), "should have function column");
        assert!(table.contains("physics"), "should have function column");
        assert!(
            !table.contains("<<"),
            "should not contain spike markers. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_with_frames_hides_zero_call_by_default() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "unused".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 1_000_000,
                cpu_self_ns: None,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        // Default (show_all=false) should hide "unused"
        let table = format_table_with_frames(&frame_data, false);
        assert!(table.contains("update"), "should show called function");
        assert!(
            !table.contains("unused"),
            "should hide zero-call function by default. Got:\n{table}"
        );
        assert!(
            table.contains("1 function hidden; use --all to show"),
            "should show hidden footer. Got:\n{table}"
        );

        // show_all=true should include "unused"
        let table_all = format_table_with_frames(&frame_data, true);
        assert!(
            table_all.contains("unused"),
            "should show zero-call function with show_all. Got:\n{table_all}"
        );
        assert!(
            !table_all.contains("hidden"),
            "should not show footer with show_all. Got:\n{table_all}"
        );
    }

    #[test]
    fn format_table_with_frames_does_not_truncate_long_names() {
        let long_name = "print_session_status";
        let frame_data = FrameData {
            fn_names: vec![long_name.into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 1_000_000,
                cpu_self_ns: None,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let table = format_table_with_frames(&frame_data, false);
        assert!(
            table.contains(long_name),
            "should show full function name '{long_name}', not truncated. Got:\n{table}"
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
        let table = format_table(&run, false);
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
        let table = format_table(&run, false);
        assert!(
            !table.contains("CPU"),
            "should not have CPU column. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_with_frames_shows_cpu_column() {
        let frame_data = FrameData {
            fn_names: vec!["compute".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 5_000_000,
                cpu_self_ns: Some(4_000_000),
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let table = format_table_with_frames(&frame_data, false);
        assert!(
            table.contains("CPU"),
            "should have CPU column header. Got:\n{table}"
        );
        assert!(
            table.contains("4.00ms"),
            "should show CPU value. Got:\n{table}"
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
        let table = format_table(&run, false);
        assert!(table.contains("CPU"), "should have CPU column");
        assert!(!table.contains("unused"), "should hide zero-call fn");
        assert!(
            table.contains("1 function hidden; use --all to show"),
            "should show hidden footer. Got:\n{table}"
        );

        // With --all: shows unused with CPU column present.
        let table_all = format_table(&run, true);
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
    fn format_table_with_frames_cpu_and_all() {
        let frame_data = FrameData {
            fn_names: vec!["active".into(), "unused".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 5_000_000,
                cpu_self_ns: Some(4_000_000),
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        // Without --all: hides unused, shows CPU.
        let table = format_table_with_frames(&frame_data, false);
        assert!(table.contains("CPU"), "should show CPU column");
        assert!(table.contains("active"), "should show active fn");
        assert!(!table.contains("unused"), "should hide unused fn");
        assert!(
            table.contains("1 function hidden; use --all to show"),
            "should show hidden footer. Got:\n{table}"
        );

        // With --all: shows both, CPU column still present.
        let table_all = format_table_with_frames(&frame_data, true);
        assert!(table_all.contains("CPU"), "should show CPU column");
        assert!(
            table_all.contains("unused"),
            "should show unused fn with --all. Got:\n{table_all}"
        );
        assert!(
            !table_all.contains("hidden"),
            "should not show footer with show_all. Got:\n{table_all}"
        );
    }

    #[test]
    fn format_table_with_frames_no_cpu_in_data() {
        let frame_data = FrameData {
            fn_names: vec!["update".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 2_000_000,
                cpu_self_ns: None,
                alloc_count: 5,
                alloc_bytes: 1024,
                free_count: 3,
                free_bytes: 512,
            }]],
        };
        let table = format_table_with_frames(&frame_data, false);
        assert!(
            !table.contains("CPU"),
            "should not show CPU column when no CPU data. Got:\n{table}"
        );
        assert!(table.contains("update"), "should still show function");
    }

    #[test]
    fn format_table_with_frames_self_before_calls() {
        let frame_data = FrameData {
            fn_names: vec!["work".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 5_000_000,
                cpu_self_ns: None,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let table = format_table_with_frames(&frame_data, false);
        let self_pos = table.find("Self").expect("Self header missing");
        let calls_pos = table.find("Calls").expect("Calls header missing");
        assert!(
            self_pos < calls_pos,
            "Self column should appear before Calls. Got:\n{table}"
        );
    }

    #[test]
    fn format_table_with_frames_cpu_column_order() {
        let frame_data = FrameData {
            fn_names: vec!["work".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 5_000_000,
                cpu_self_ns: Some(4_000_000),
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let table = format_table_with_frames(&frame_data, false);
        let self_pos = table.find("Self").expect("Self header missing");
        let cpu_pos = table.find("CPU").expect("CPU header missing");
        let calls_pos = table.find("Calls").expect("Calls header missing");
        assert!(
            self_pos < cpu_pos && cpu_pos < calls_pos,
            "Column order should be Self | CPU | Calls. Got:\n{table}"
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
        let table = format_table(&run, false);
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
        let table = format_table(&run, false);
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
        let table = format_table(&run, false);
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
    fn partial_frame_functions_aggregate_correctly() {
        // A function appearing in only one frame should still aggregate
        // correctly in the summary table.
        let frame_data = FrameData {
            fn_names: vec!["main_fn".into(), "update".into(), "worker_fn".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 5_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 2_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    // worker_fn only appears in frame 0
                    FrameFnEntry {
                        fn_id: 2,
                        tid: None,
                        calls: 50,
                        self_ns: 3_000_000,
                        cpu_self_ns: None,
                        alloc_count: 100,
                        alloc_bytes: 5000,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 1,
                        self_ns: 4_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 2_500_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    // worker_fn absent from frame 1
                ],
            ],
        };

        let table = format_table_with_frames(&frame_data, false);

        // worker_fn should appear with its aggregated totals (50 calls, 100 allocs)
        let worker_line = table
            .lines()
            .find(|l| l.contains("worker_fn"))
            .expect("worker_fn should appear in table");
        assert!(
            worker_line.contains("50"),
            "worker_fn should show 50 calls, got: {worker_line}"
        );
        assert!(
            worker_line.contains("100"),
            "worker_fn should show 100 allocs, got: {worker_line}"
        );

        // All three functions should appear
        assert!(table.contains("main_fn"), "main_fn should appear in table");
        assert!(table.contains("update"), "update should appear in table");
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
        let json = format_json(&run, false);
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
        let json = format_json(&run, false);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "called");

        let json_all = format_json(&run, true);
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
        let json = format_json(&run, false);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries[0].cpu_self_ms, Some(8.5));
        assert_eq!(entries[0].alloc_count, 42);
        assert_eq!(entries[0].alloc_bytes, 1024);
    }

    #[test]
    fn format_json_with_frames_aggregates() {
        let frame_data = FrameData {
            fn_names: vec!["alpha".into(), "beta".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 2,
                        self_ns: 5_000_000,
                        cpu_self_ns: None,
                        alloc_count: 10,
                        alloc_bytes: 200,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 3_000_000,
                        cpu_self_ns: None,
                        alloc_count: 5,
                        alloc_bytes: 100,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![FrameFnEntry {
                    fn_id: 0,
                    tid: None,
                    calls: 3,
                    self_ns: 7_000_000,
                    cpu_self_ns: None,
                    alloc_count: 15,
                    alloc_bytes: 300,
                    free_count: 0,
                    free_bytes: 0,
                }],
            ],
        };
        let json = format_json_with_frames(&frame_data, false);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 2);
        // alpha: 5ms + 7ms = 12ms, sorted first
        assert_eq!(entries[0].name, "alpha");
        assert!((entries[0].self_ms - 12.0).abs() < f64::EPSILON);
        assert_eq!(entries[0].calls, 5);
        assert_eq!(entries[0].alloc_count, 25);
        assert_eq!(entries[0].alloc_bytes, 500);
        // beta: 3ms
        assert_eq!(entries[1].name, "beta");
        assert!((entries[1].self_ms - 3.0).abs() < f64::EPSILON);
        assert_eq!(entries[1].cpu_self_ms, None);
    }

    #[test]
    fn format_json_with_frames_cpu_time() {
        let frame_data = FrameData {
            fn_names: vec!["work".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 10_000_000,
                cpu_self_ns: Some(8_000_000),
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let json = format_json_with_frames(&frame_data, false);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries[0].cpu_self_ms, Some(8.0));
    }

    #[test]
    fn format_json_with_frames_show_all_includes_zero_call_fns() {
        let frame_data = FrameData {
            fn_names: vec!["called".into(), "uncalled".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
                tid: None,
                calls: 1,
                self_ns: 1_000_000,
                cpu_self_ns: None,
                alloc_count: 0,
                alloc_bytes: 0,
                free_count: 0,
                free_bytes: 0,
            }]],
        };
        let json = format_json_with_frames(&frame_data, false);
        let entries: Vec<JsonFnEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);

        let json_all = format_json_with_frames(&frame_data, true);
        let entries_all: Vec<JsonFnEntry> = serde_json::from_str(&json_all).unwrap();
        assert_eq!(entries_all.len(), 2);
    }

    #[test]
    fn format_frames_json_output() {
        let frame_data = FrameData {
            fn_names: vec!["alpha".into(), "beta".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
                        tid: None,
                        calls: 3,
                        self_ns: 5_000_000,
                        cpu_self_ns: None,
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 0,
                        free_bytes: 0,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        tid: None,
                        calls: 1,
                        self_ns: 2_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                vec![FrameFnEntry {
                    fn_id: 0,
                    tid: None,
                    calls: 2,
                    self_ns: 3_000_000,
                    cpu_self_ns: None,
                    alloc_count: 5,
                    alloc_bytes: 2048,
                    free_count: 0,
                    free_bytes: 0,
                }],
            ],
        };

        let json_str = format_frames_json(&frame_data, false);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 2, "should have 2 frames");

        // Frame 1 has both functions
        assert_eq!(parsed[0]["frame"], 1);
        let fns = parsed[0]["functions"].as_array().unwrap();
        assert_eq!(fns.len(), 2);
        // Sorted by self_ms descending: alpha (5ms) before beta (2ms)
        assert_eq!(fns[0]["name"], "alpha");
        assert_eq!(fns[0]["self_ms"], 5.0);
        assert_eq!(fns[0]["calls"], 3);
        assert_eq!(fns[0]["alloc_count"], 10);
        assert_eq!(fns[0]["alloc_bytes"], 4096);
        assert_eq!(fns[1]["name"], "beta");

        // Frame 2 has only alpha
        assert_eq!(parsed[1]["frame"], 2);
        let fns = parsed[1]["functions"].as_array().unwrap();
        assert_eq!(fns.len(), 1);
        assert_eq!(fns[0]["name"], "alpha");
        assert_eq!(fns[0]["self_ms"], 3.0);
    }

    #[test]
    fn format_per_thread_json_output() {
        let frame_data = FrameData {
            fn_names: vec!["alpha".into(), "beta".into()],
            frames: vec![
                vec![FrameFnEntry {
                    fn_id: 0,
                    tid: Some(0),
                    calls: 3,
                    self_ns: 5_000_000,
                    cpu_self_ns: None,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                }],
                vec![FrameFnEntry {
                    fn_id: 1,
                    tid: Some(1),
                    calls: 1,
                    self_ns: 2_000_000,
                    cpu_self_ns: None,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                }],
                vec![FrameFnEntry {
                    fn_id: 0,
                    tid: Some(0),
                    calls: 2,
                    self_ns: 3_000_000,
                    cpu_self_ns: None,
                    alloc_count: 0,
                    alloc_bytes: 0,
                    free_count: 0,
                    free_bytes: 0,
                }],
            ],
        };

        let json_str = format_per_thread_json(&frame_data, false);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 2, "should have 2 threads");
        assert_eq!(parsed[0]["thread"], 0);
        assert_eq!(parsed[1]["thread"], 1);

        // Thread 0: alpha has 5 calls, 8ms total self
        let fns = parsed[0]["functions"].as_array().unwrap();
        assert_eq!(fns[0]["name"], "alpha");
        assert_eq!(fns[0]["calls"], 5);
        assert!((fns[0]["self_ms"].as_f64().unwrap() - 8.0).abs() < 0.01);

        // Thread 1: beta has 1 call, 2ms
        let fns = parsed[1]["functions"].as_array().unwrap();
        assert_eq!(fns[0]["name"], "beta");
        assert_eq!(fns[0]["calls"], 1);
    }
}
