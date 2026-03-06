use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anstyle::{Effects, Style};

use crate::error::{Error, io_context};

const HEADER: Style = Style::new().bold();
const DIM: Style = Style::new().effects(Effects::DIMMED);

/// Describes the file format a Run was loaded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunFormat {
    #[default]
    Json,
    Ndjson,
}

/// A single profiling run loaded from a JSON file written by piano-runtime.
#[derive(Debug, serde::Deserialize)]
pub struct Run {
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(alias = "timestamp")]
    pub timestamp_ms: u128,
    pub functions: Vec<FnEntry>,
    /// The file format this run was loaded from (not serialized).
    #[serde(skip)]
    pub source_format: RunFormat,
}

/// Timing data for one function within a profiling run.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct FnEntry {
    pub name: String,
    pub calls: u64,
    #[serde(default)]
    pub total_ms: Option<f64>,
    pub self_ms: f64,
    #[serde(default)]
    pub cpu_self_ms: Option<f64>,
    #[serde(default)]
    pub alloc_count: u64,
    #[serde(default)]
    pub alloc_bytes: u64,
}

/// Per-frame data loaded from an NDJSON file.
pub struct FrameData {
    pub fn_names: Vec<String>,
    pub frames: Vec<Vec<FrameFnEntry>>,
}

/// Accumulated per-function counters across all frames (used during NDJSON aggregation).
#[derive(Default, Clone, Copy)]
struct FnAgg {
    calls: u64,
    self_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
    cpu_self_ns: u64,
}

/// Per-function entry within a single frame.
pub struct FrameFnEntry {
    pub fn_id: usize,
    pub calls: u64,
    pub self_ns: u64,
    pub cpu_self_ns: Option<u64>,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}

/// NDJSON header line.
#[derive(serde::Deserialize)]
struct NdjsonHeader {
    #[serde(rename = "format_version")]
    _format_version: u32,
    run_id: Option<String>,
    timestamp_ms: u128,
    #[serde(default)]
    functions: Vec<String>,
    #[serde(default)]
    has_cpu_time: bool,
}

/// NDJSON frame line.
#[derive(serde::Deserialize)]
struct NdjsonFrame {
    #[serde(rename = "frame")]
    _frame: usize,
    fns: Vec<NdjsonFnEntry>,
}

/// Per-function entry within an NDJSON frame line.
#[derive(serde::Deserialize)]
struct NdjsonFnEntry {
    id: usize,
    calls: u64,
    self_ns: u64,
    #[serde(default)]
    csn: Option<u64>,
    #[serde(default)]
    ac: u64,
    #[serde(default)]
    ab: u64,
    #[serde(default)]
    fc: u64,
    #[serde(default)]
    fb: u64,
}

/// NDJSON v4 trailer line (written at shutdown with the function name table).
#[derive(serde::Deserialize)]
struct NdjsonTrailer {
    functions: Vec<String>,
}

/// Read a profiling run from a JSON or NDJSON file on disk.
pub fn load_run(path: &Path) -> Result<Run, Error> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext == "ndjson" {
        let (run, _frame_data) = load_ndjson(path)?;
        return Ok(run);
    }
    let contents = std::fs::read_to_string(path).map_err(|source| Error::RunReadError {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&contents).map_err(|e| Error::InvalidRunData {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })
}

/// Load an NDJSON file, returning both the aggregated Run and frame-level data.
pub fn load_ndjson(path: &Path) -> Result<(Run, FrameData), Error> {
    let contents = std::fs::read_to_string(path).map_err(|source| Error::RunReadError {
        path: path.to_path_buf(),
        source,
    })?;
    let all_lines: Vec<&str> = contents.lines().collect();

    let header_line = all_lines.first().ok_or_else(|| Error::InvalidRunData {
        path: path.to_path_buf(),
        reason: "empty NDJSON file".into(),
    })?;
    let header: NdjsonHeader =
        serde_json::from_str(header_line).map_err(|e| Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("invalid NDJSON header: {e}"),
        })?;

    // Determine v3 vs v4 format and identify frame lines.
    // v3: function names in header, all remaining lines are frames.
    // v4: header.functions is empty; last non-empty line may be a trailer with names.
    let (fn_names, frame_lines): (Vec<String>, &[&str]) = if !header.functions.is_empty() {
        // v3: names in header, everything after header is frames.
        (header.functions, &all_lines[1..])
    } else {
        // v4: check the last non-empty line for a trailer.
        let body = &all_lines[1..];
        let last_non_empty = body.iter().rposition(|l| !l.trim().is_empty());
        match last_non_empty {
            Some(idx) => {
                let candidate = body[idx].trim();
                match serde_json::from_str::<NdjsonTrailer>(candidate) {
                    Ok(trailer) => (trailer.functions, &body[..idx]),
                    Err(_) => {
                        // No valid trailer -- generate placeholder names after parsing frames.
                        (Vec::new(), body)
                    }
                }
            }
            None => (Vec::new(), body),
        }
    };

    let mut frames: Vec<Vec<FrameFnEntry>> = Vec::new();
    for line in frame_lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let frame: NdjsonFrame = serde_json::from_str(line).map_err(|e| Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("invalid NDJSON frame: {e}"),
        })?;
        let entries: Vec<FrameFnEntry> = frame
            .fns
            .into_iter()
            .map(|f| FrameFnEntry {
                fn_id: f.id,
                calls: f.calls,
                self_ns: f.self_ns,
                cpu_self_ns: f.csn,
                alloc_count: f.ac,
                alloc_bytes: f.ab,
                free_count: f.fc,
                free_bytes: f.fb,
            })
            .collect();
        frames.push(entries);
    }

    // If no names were resolved (v4 crash recovery), generate placeholders from frame data.
    let fn_names = if fn_names.is_empty() {
        let max_id = frames
            .iter()
            .flat_map(|f| f.iter())
            .map(|e| e.fn_id)
            .max()
            .unwrap_or(0);
        (0..=max_id).map(|i| format!("fn_{i}")).collect()
    } else {
        fn_names
    };

    // Aggregate into Run.
    let has_cpu = header.has_cpu_time;
    let mut fn_agg: HashMap<usize, FnAgg> = HashMap::new();
    for frame in &frames {
        for entry in frame {
            let agg = fn_agg.entry(entry.fn_id).or_default();
            agg.calls += entry.calls;
            agg.self_ns += entry.self_ns;
            agg.alloc_count += entry.alloc_count;
            agg.alloc_bytes += entry.alloc_bytes;
            agg.cpu_self_ns += entry.cpu_self_ns.unwrap_or(0);
        }
    }

    // Build FnEntry for every registered function, including zero-call ones.
    let functions: Vec<FnEntry> = fn_names
        .iter()
        .enumerate()
        .map(|(fn_id, name)| {
            let agg = fn_agg.get(&fn_id).copied().unwrap_or_default();
            let self_ms = agg.self_ns as f64 / 1_000_000.0;
            FnEntry {
                name: name.clone(),
                calls: agg.calls,
                total_ms: None, // NDJSON format has no total_ms (only per-frame self_ns)
                self_ms,
                cpu_self_ms: if has_cpu {
                    Some(agg.cpu_self_ns as f64 / 1_000_000.0)
                } else {
                    None
                },
                alloc_count: agg.alloc_count,
                alloc_bytes: agg.alloc_bytes,
            }
        })
        .collect();

    let run = Run {
        run_id: header.run_id,
        timestamp_ms: header.timestamp_ms,
        functions,
        source_format: RunFormat::Ndjson,
    };

    let frame_data = FrameData { fn_names, frames };

    Ok((run, frame_data))
}

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
/// Frames where total exceeds 2x median are flagged as spikes.
pub fn format_frames_table(frame_data: &FrameData) -> String {
    let fn_names = &frame_data.fn_names;
    let n_fns = fn_names.len();

    // Compute per-frame totals for spike detection.
    let frame_totals: Vec<u64> = frame_data
        .frames
        .iter()
        .map(|f| f.iter().map(|e| e.self_ns).sum())
        .collect();

    let mut sorted_totals = frame_totals.clone();
    sorted_totals.sort_unstable();
    let median = percentile(&sorted_totals, 50.0);
    let spike_threshold = median.saturating_mul(2);

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
        let total = frame_totals[i];
        let is_spike = total > spike_threshold && median > 0;

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

        if is_spike {
            out.push_str(" <<");
        }
        out.push('\n');
    }

    let n_spikes = frame_totals
        .iter()
        .filter(|&&t| t > spike_threshold && median > 0)
        .count();
    out.push_str(&format!(
        "{DIM}\n{} frames | {} spikes (>2x median)\n{DIM:#}",
        frame_data.frames.len(),
        n_spikes
    ));

    out
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn format_ns(ns: u64) -> String {
    let us = ns as f64 / 1_000.0;
    if us < 1000.0 {
        format!("{us:.1}us")
    } else {
        format!("{:.2}ms", us / 1_000.0)
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Truncate a label to `max_len` characters, appending '…' if truncated.
fn truncate_label(label: &str, max_len: usize) -> String {
    if label.chars().count() <= max_len {
        label.to_string()
    } else {
        let truncated: String = label.chars().take(max_len).collect();
        format!("{truncated}\u{2026}")
    }
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

/// Serialize frame-aggregated data as a JSON array of function entries.
///
/// Aggregates per-frame data into per-function totals, matching the summary
/// table structure. Self time is converted from nanoseconds to milliseconds.
pub fn format_json_with_frames(frame_data: &FrameData, show_all: bool) -> String {
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

    let json_entries: Vec<JsonFnEntry> = entries
        .iter()
        .map(|e| JsonFnEntry {
            name: e.name.clone(),
            self_ms: e.self_ns as f64 / 1_000_000.0,
            cpu_self_ms: e.cpu_self_ns.map(|ns| ns as f64 / 1_000_000.0),
            calls: e.calls,
            alloc_count: e.alloc_count,
            alloc_bytes: e.alloc_bytes,
        })
        .collect();

    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

/// Structured JSON entry for a diff comparison.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonDiffEntry {
    pub name: String,
    pub self_ms_a: f64,
    pub self_ms_b: f64,
    pub delta_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_pct: Option<f64>,
    pub calls_a: u64,
    pub calls_b: u64,
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
            }
        })
        .collect();

    serde_json::to_string_pretty(&json_entries).expect("JSON serialization should not fail")
}

/// Show the delta between two runs, comparing functions by name.
///
/// `label_a` and `label_b` are used as column headers (e.g. tag names or file stems).
/// Labels longer than 20 characters are truncated with '…'.
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

/// Collect all run files (.json and .ndjson) in the given directory, sorted by filename.
fn collect_run_files(runs_dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let entries = std::fs::read_dir(runs_dir).map_err(|source| Error::RunReadError {
        path: runs_dir.to_path_buf(),
        source,
    })?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str())?;
            if ext != "json" && ext != "ndjson" {
                return None;
            }
            let _ts: u128 = path.file_stem()?.to_str()?.parse().ok()?;
            Some(path)
        })
        .collect();
    // Sort by filename with explicit .ndjson preference: when two files share
    // the same timestamp stem, the .ndjson file sorts after the .json file so
    // that callers picking the last element get the richer format.
    files.sort_by(|a, b| {
        a.file_stem().cmp(&b.file_stem()).then_with(|| {
            let a_ndjson = a.extension().is_some_and(|e| e == "ndjson");
            let b_ndjson = b.extension().is_some_and(|e| e == "ndjson");
            a_ndjson.cmp(&b_ndjson)
        })
    });

    // Deduplicate by stem: when both <stem>.json and <stem>.ndjson exist,
    // keep only the .ndjson file (the richer format). Loading both would
    // double-count every function.
    let ndjson_stems: HashSet<String> = files
        .iter()
        .filter(|p| p.extension().is_some_and(|e| e == "ndjson"))
        .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
        .collect();
    if !ndjson_stems.is_empty() {
        files.retain(|p| {
            let is_json = p.extension().is_some_and(|e| e == "json");
            if !is_json {
                return true;
            }
            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            !ndjson_stems.contains(stem)
        });
    }

    Ok(files)
}

/// Merge multiple runs into one, summing calls/total_ms/self_ms per function name.
fn merge_runs(runs: &[&Run]) -> Run {
    let mut merged: HashMap<String, FnEntry> = HashMap::new();
    let mut max_ts: u128 = 0;
    let mut run_id = None;
    let mut format = RunFormat::Json;

    for run in runs {
        max_ts = max_ts.max(run.timestamp_ms);
        if run_id.is_none() {
            run_id.clone_from(&run.run_id);
        }
        if run.source_format == RunFormat::Ndjson {
            format = RunFormat::Ndjson;
        }
        for f in &run.functions {
            let entry = merged.entry(f.name.clone()).or_insert(FnEntry {
                name: f.name.clone(),
                calls: 0,
                total_ms: None,
                self_ms: 0.0,
                cpu_self_ms: None,
                alloc_count: 0,
                alloc_bytes: 0,
            });
            entry.calls += f.calls;
            if let Some(t) = f.total_ms {
                *entry.total_ms.get_or_insert(0.0) += t;
            }
            entry.self_ms += f.self_ms;
            if let Some(cpu) = f.cpu_self_ms {
                *entry.cpu_self_ms.get_or_insert(0.0) += cpu;
            }
            entry.alloc_count += f.alloc_count;
            entry.alloc_bytes += f.alloc_bytes;
        }
    }

    Run {
        run_id,
        timestamp_ms: max_ts,
        functions: merged.into_values().collect(),
        source_format: format,
    }
}

/// Validate that a tag name is safe to use as a filename.
///
/// Rejects empty strings, path separators, `.`/`..` components, and null bytes
/// to prevent path traversal and confusing filesystem behavior.
fn validate_tag_name(tag: &str) -> Result<(), Error> {
    if tag.is_empty() {
        return Err(Error::InvalidTagName(
            "provide a tag name (e.g., `baseline`, `v1`)".into(),
        ));
    }
    let safe: String = tag.chars().flat_map(char::escape_default).collect();
    if tag == "." || tag == ".." {
        return Err(Error::InvalidTagName(format!(
            "valid tags are plain names (e.g., `baseline`), got '{safe}'"
        )));
    }
    if tag.contains('/') || tag.contains('\\') {
        return Err(Error::InvalidTagName(format!(
            "valid tags cannot include slashes, got '{safe}'"
        )));
    }
    if tag.contains('\0') {
        return Err(Error::InvalidTagName(format!(
            "valid tags are printable text (e.g., `baseline`, `v1`), got '{safe}'"
        )));
    }
    Ok(())
}

/// Save a tag pointing to a run_id.
pub fn save_tag(tags_dir: &Path, tag: &str, run_id: &str) -> Result<(), Error> {
    validate_tag_name(tag)?;
    std::fs::create_dir_all(tags_dir).map_err(io_context("create directory", tags_dir))?;
    let path = tags_dir.join(tag);
    std::fs::write(&path, run_id).map_err(io_context("write", &path))?;
    Ok(())
}

/// Resolve a tag name to a run_id string.
pub fn resolve_tag(tags_dir: &Path, tag: &str) -> Result<String, Error> {
    validate_tag_name(tag)?;
    let tag_path = tags_dir.join(tag);
    let run_id = std::fs::read_to_string(&tag_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            Error::RunNotFound {
                tag: tag.to_owned(),
            }
        } else {
            Error::RunReadError {
                path: tag_path,
                source,
            }
        }
    })?;
    Ok(run_id.trim().to_owned())
}

/// Load a run by resolving a tag name to a run_id, then consolidating.
pub fn load_tagged_run(tags_dir: &Path, runs_dir: &Path, tag: &str) -> Result<Run, Error> {
    let run_id = resolve_tag(tags_dir, tag)?;
    load_run_by_id(runs_dir, &run_id).map_err(|e| match e {
        Error::NoRuns => Error::RunNotFound {
            tag: tag.to_owned(),
        },
        other => other,
    })
}

/// Find a tag name that points to the given run_id, if any.
///
/// Scans all tag files in tags_dir. Returns the first match.
pub fn reverse_resolve_tag(tags_dir: &Path, run_id: &str) -> Option<String> {
    let entries = std::fs::read_dir(tags_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Ok(contents) = std::fs::read_to_string(&path) {
            if contents.trim() == run_id {
                return path.file_name()?.to_str().map(String::from);
            }
        }
    }
    None
}

/// Find the NDJSON file for a given run_id, if one exists.
pub fn find_ndjson_by_run_id(runs_dir: &Path, run_id: &str) -> Result<Option<PathBuf>, Error> {
    use std::io::BufRead;

    let all_files = collect_run_files(runs_dir)?;
    for path in &all_files {
        if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
            continue;
        }
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        let mut reader = std::io::BufReader::new(file);
        let mut first_line = String::new();
        if reader.read_line(&mut first_line).unwrap_or(0) == 0 {
            continue;
        }
        // Parse just the header to check the run_id.
        if let Ok(header) = serde_json::from_str::<serde_json::Value>(&first_line) {
            if header.get("run_id").and_then(|v| v.as_str()) == Some(run_id) {
                return Ok(Some(path.clone()));
            }
        }
    }
    Ok(None)
}

/// Load and merge all run files matching a specific run_id.
pub fn load_run_by_id(runs_dir: &Path, run_id: &str) -> Result<Run, Error> {
    let all_files = collect_run_files(runs_dir)?;
    let mut matching: Vec<Run> = Vec::new();
    for path in &all_files {
        let Ok(run) = load_run(path) else { continue };
        if run.run_id.as_deref() == Some(run_id) {
            matching.push(run);
        }
    }
    if matching.is_empty() {
        return Err(Error::NoRuns);
    }
    let refs: Vec<&Run> = matching.iter().collect();
    Ok(merge_runs(&refs))
}

/// Load all run files and return them grouped by run_id, sorted by max
/// timestamp ascending. Each group contains all files sharing a run_id
/// (multi-threaded runs). Legacy files without a run_id get a synthetic
/// key so they form their own single-file group.
fn load_grouped_runs(runs_dir: &Path) -> Result<Vec<Vec<Run>>, Error> {
    let all_files = collect_run_files(runs_dir)?;
    if all_files.is_empty() {
        return Ok(Vec::new());
    }

    let runs: Vec<Run> = all_files
        .iter()
        .filter_map(|path| load_run(path).ok())
        .collect();

    if runs.is_empty() {
        return Ok(Vec::new());
    }

    // Group by run_id, using a synthetic key for legacy files.
    let mut groups: HashMap<String, Vec<Run>> = HashMap::new();
    for run in runs {
        let key = run
            .run_id
            .clone()
            .unwrap_or_else(|| format!("_legacy_{}", run.timestamp_ms));
        groups.entry(key).or_default().push(run);
    }

    // Sort groups by their max timestamp (ascending).
    let mut group_list: Vec<Vec<Run>> = groups.into_values().collect();
    group_list.sort_by_key(|runs| runs.iter().map(|r| r.timestamp_ms).max().unwrap_or(0));

    Ok(group_list)
}

/// Load the latest run, consolidating all files sharing the same run_id.
///
/// Files written by different threads within one process share a run_id. This
/// function finds the latest run_id (by max timestamp) and merges all files
/// that belong to it. Legacy files without a run_id fall back to single-file
/// loading (the highest-timestamp file).
pub fn load_latest_run(runs_dir: &Path) -> Result<Run, Error> {
    let groups = load_grouped_runs(runs_dir)?;
    let last_group = groups.last().ok_or(Error::NoRuns)?;
    let refs: Vec<&Run> = last_group.iter().collect();
    Ok(merge_runs(&refs))
}

/// Load the latest run's individual thread files without merging.
///
/// Returns one `Run` per file sharing the latest run_id, sorted by timestamp.
/// Each `Run` represents one thread's data. For single-threaded programs,
/// this returns a single-element vector identical to `load_latest_run`.
pub fn load_latest_runs_per_thread(runs_dir: &Path) -> Result<Vec<Run>, Error> {
    let groups = load_grouped_runs(runs_dir)?;
    let mut last_group = groups.into_iter().last().ok_or(Error::NoRuns)?;
    last_group.sort_by_key(|r| r.timestamp_ms);
    Ok(last_group)
}

/// Load the two most recent runs, grouped by run_id.
///
/// Returns `(previous, latest)` where latest has the highest timestamp.
/// Files sharing a run_id are merged (multi-threaded runs).
pub fn load_two_latest_runs(runs_dir: &Path) -> Result<(Run, Run), Error> {
    let groups = load_grouped_runs(runs_dir)?;
    if groups.len() < 2 {
        return Err(Error::NotEnoughRuns);
    }
    let len = groups.len();
    let prev_refs: Vec<&Run> = groups[len - 2].iter().collect();
    let latest_refs: Vec<&Run> = groups[len - 1].iter().collect();
    Ok((merge_runs(&prev_refs), merge_runs(&latest_refs)))
}

/// Find the path to the latest run file without loading it.
///
/// Returns `Some(path)` to the latest file (preferring .ndjson over .json
/// at the same timestamp), or `None` if the directory is empty.
pub fn find_latest_run_file(runs_dir: &Path) -> Result<Option<PathBuf>, Error> {
    let all_files = collect_run_files(runs_dir)?;
    // Files are sorted by name (timestamp ascending); last is latest.
    Ok(all_files.into_iter().next_back())
}

/// Like `find_latest_run_file`, but only returns files whose timestamp stem
/// is >= `since_ms`. Prevents reading stale data from a previous run when
/// the runtime crashed without writing new data.
pub fn find_latest_run_file_since(
    runs_dir: &Path,
    since_ms: u128,
) -> Result<Option<PathBuf>, Error> {
    let all_files = collect_run_files(runs_dir)?;
    Ok(all_files
        .into_iter()
        .filter(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u128>().ok())
                .is_some_and(|ts| ts >= since_ms)
        })
        .next_back())
}

/// Format a SystemTime as a relative duration string ("N sec/min/hours/days ago").
pub fn relative_time(t: std::time::SystemTime) -> String {
    let elapsed = t.elapsed().unwrap_or_default();
    let secs = elapsed.as_secs();
    if secs < 60 {
        format!("{secs} sec ago")
    } else if secs < 3600 {
        format!("{} min ago", secs / 60)
    } else if secs < 86400 {
        let h = secs / 3600;
        let unit = if h == 1 { "hour" } else { "hours" };
        format!("{h} {unit} ago")
    } else {
        let d = secs / 86400;
        let unit = if d == 1 { "day" } else { "days" };
        format!("{d} {unit} ago")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn sample_json() -> &'static str {
        r#"{
            "timestamp_ms": 1700000000000,
            "functions": [
                {"name": "walk", "calls": 3, "total_ms": 10.5, "self_ms": 7.2},
                {"name": "parse", "calls": 100, "total_ms": 45.0, "self_ms": 30.1}
            ]
        }"#
    }

    #[test]
    fn load_run_from_json() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("1700000000000.json");
        fs::write(&path, sample_json()).unwrap();

        let run = load_run(&path).unwrap();
        assert_eq!(run.timestamp_ms, 1700000000000);
        assert_eq!(run.functions.len(), 2);
        assert_eq!(run.functions[0].name, "walk");
        assert_eq!(run.functions[0].calls, 3);
        assert!((run.functions[0].total_ms.unwrap() - 10.5).abs() < f64::EPSILON);
        assert!((run.functions[0].self_ms - 7.2).abs() < f64::EPSILON);
    }

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
    fn latest_run_consolidates_by_run_id() {
        let dir = TempDir::new().unwrap();
        let run_a = r#"{"run_id":"123_1000","timestamp_ms":1000,"functions":[
            {"name":"parse","calls":50,"total_ms":100.0,"self_ms":100.0}
        ]}"#;
        let run_b = r#"{"run_id":"123_1000","timestamp_ms":1001,"functions":[
            {"name":"parse","calls":30,"total_ms":60.0,"self_ms":60.0},
            {"name":"resolve","calls":30,"total_ms":45.0,"self_ms":45.0}
        ]}"#;
        let old = r#"{"run_id":"99_500","timestamp_ms":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        fs::write(dir.path().join("1000.json"), run_a).unwrap();
        fs::write(dir.path().join("1001.json"), run_b).unwrap();
        fs::write(dir.path().join("500.json"), old).unwrap();

        let run = load_latest_run(dir.path()).unwrap();
        let parse = run
            .functions
            .iter()
            .find(|f| f.name == "parse")
            .expect("parse");
        assert_eq!(parse.calls, 80);
        assert!((parse.total_ms.unwrap() - 160.0).abs() < 0.01);
        assert!((parse.self_ms - 160.0).abs() < 0.01);

        let resolve = run
            .functions
            .iter()
            .find(|f| f.name == "resolve")
            .expect("resolve");
        assert_eq!(resolve.calls, 30);

        assert!(run.functions.iter().all(|f| f.name != "old_fn"));
    }

    #[test]
    fn save_and_load_tag() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        std::fs::create_dir_all(&tags_dir).unwrap();
        std::fs::create_dir_all(&runs_dir).unwrap();

        let run_json = r#"{"run_id":"42_9000","timestamp_ms":9000,"functions":[
            {"name":"work","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        fs::write(runs_dir.join("9000.json"), run_json).unwrap();

        save_tag(&tags_dir, "baseline", "42_9000").unwrap();
        let run = load_tagged_run(&tags_dir, &runs_dir, "baseline").unwrap();
        assert_eq!(run.functions.len(), 1);
        assert_eq!(run.functions[0].name, "work");
    }

    #[test]
    fn load_run_by_id_merges_matching_files() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        std::fs::create_dir_all(&runs_dir).unwrap();

        let run_a = r#"{"run_id":"abc_1000","timestamp_ms":1000,"functions":[
            {"name":"parse","calls":50,"total_ms":100.0,"self_ms":100.0}
        ]}"#;
        let run_b = r#"{"run_id":"abc_1000","timestamp_ms":1001,"functions":[
            {"name":"parse","calls":30,"total_ms":60.0,"self_ms":60.0}
        ]}"#;
        let other = r#"{"run_id":"other_500","timestamp_ms":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        fs::write(runs_dir.join("1000.json"), run_a).unwrap();
        fs::write(runs_dir.join("1001.json"), run_b).unwrap();
        fs::write(runs_dir.join("500.json"), other).unwrap();

        let run = load_run_by_id(&runs_dir, "abc_1000").unwrap();
        let parse = run.functions.iter().find(|f| f.name == "parse").unwrap();
        assert_eq!(parse.calls, 80);
        assert!(run.functions.iter().all(|f| f.name != "old_fn"));
    }

    #[test]
    fn load_run_by_id_errors_on_missing_id() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        std::fs::create_dir_all(&runs_dir).unwrap();

        let run_json = r#"{"run_id":"abc_1000","timestamp_ms":1000,"functions":[]}"#;
        fs::write(runs_dir.join("1000.json"), run_json).unwrap();

        let result = load_run_by_id(&runs_dir, "nonexistent");
        assert!(matches!(result.unwrap_err(), Error::NoRuns));
    }

    #[test]
    fn load_tagged_run_errors_on_missing_tag() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        std::fs::create_dir_all(&tags_dir).unwrap();
        std::fs::create_dir_all(&runs_dir).unwrap();

        let result = load_tagged_run(&tags_dir, &runs_dir, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn load_tagged_run_returns_run_not_found_for_stale_tag() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        std::fs::create_dir_all(&tags_dir).unwrap();
        std::fs::create_dir_all(&runs_dir).unwrap();

        // Tag points to a run_id that doesn't exist in runs_dir.
        save_tag(&tags_dir, "baseline", "deleted_1000").unwrap();

        let err = load_tagged_run(&tags_dir, &runs_dir, "baseline").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("baseline"),
            "error should mention tag name: {msg}"
        );
        assert!(
            msg.contains("piano tag"),
            "error should suggest listing tags: {msg}"
        );
    }

    #[test]
    fn save_tag_creates_tags_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("nested").join("tags");

        save_tag(&tags_dir, "v1", "some_id").unwrap();

        let contents = fs::read_to_string(tags_dir.join("v1")).unwrap();
        assert_eq!(contents, "some_id");
    }

    #[test]
    fn save_tag_rejects_invalid_names() {
        let dir = TempDir::new().unwrap();
        assert!(save_tag(dir.path(), "", "id").is_err());
        assert!(save_tag(dir.path(), ".", "id").is_err());
        assert!(save_tag(dir.path(), "..", "id").is_err());
        assert!(save_tag(dir.path(), "../etc/passwd", "id").is_err());
        assert!(save_tag(dir.path(), "foo/bar", "id").is_err());
        assert!(save_tag(dir.path(), "foo\\bar", "id").is_err());
        assert!(save_tag(dir.path(), "foo\0bar", "id").is_err());
    }

    #[test]
    fn load_latest_run_errors_on_empty_dir() {
        let dir = TempDir::new().unwrap();
        let result = load_latest_run(dir.path());
        assert!(result.is_err(), "expected Err for empty dir");
    }

    #[test]
    fn load_latest_run_legacy_files_without_run_id() {
        let dir = TempDir::new().unwrap();
        let old = r#"{"timestamp_ms":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let newer = r#"{"timestamp_ms":1000,"functions":[
            {"name":"new_fn","calls":2,"total_ms":10.0,"self_ms":8.0}
        ]}"#;
        fs::write(dir.path().join("500.json"), old).unwrap();
        fs::write(dir.path().join("1000.json"), newer).unwrap();

        let run = load_latest_run(dir.path()).unwrap();
        assert_eq!(run.functions.len(), 1);
        assert_eq!(run.functions[0].name, "new_fn");
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
    fn load_run_accepts_legacy_timestamp_field() {
        let dir = TempDir::new().unwrap();
        let json = r#"{"timestamp":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let path = dir.path().join("500.json");
        fs::write(&path, json).unwrap();
        let run = load_run(&path).unwrap();
        assert_eq!(run.timestamp_ms, 500);
    }

    #[test]
    fn load_latest_run_skips_corrupt_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("100.json"), "not valid json").unwrap();
        let valid = r#"{"run_id":"ok_200","timestamp_ms":200,"functions":[
            {"name":"good","calls":1,"total_ms":1.0,"self_ms":1.0}
        ]}"#;
        fs::write(dir.path().join("200.json"), valid).unwrap();
        let run = load_latest_run(dir.path()).unwrap();
        assert_eq!(run.functions[0].name, "good");
    }

    #[test]
    fn load_run_by_id_skips_corrupt_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("100.json"), "garbage").unwrap();
        let valid = r#"{"run_id":"target_200","timestamp_ms":200,"functions":[
            {"name":"found","calls":1,"total_ms":2.0,"self_ms":2.0}
        ]}"#;
        fs::write(dir.path().join("200.json"), valid).unwrap();
        let run = load_run_by_id(dir.path(), "target_200").unwrap();
        assert_eq!(run.functions[0].name, "found");
    }

    #[test]
    fn load_ndjson_run() {
        let dir = TempDir::new().unwrap();
        let content = r#"{"format_version":2,"run_id":"test_1","timestamp_ms":1000,"functions":["update","physics"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":10,"ab":4096,"fc":8,"fb":3072},{"id":1,"calls":1,"self_ns":1000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"fns":[{"id":0,"calls":1,"self_ns":2100000,"ac":12,"ab":5000,"fc":10,"fb":4000},{"id":1,"calls":1,"self_ns":950000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(dir.path().join("1000.ndjson"), content).unwrap();

        let run = load_latest_run(dir.path()).unwrap();
        assert_eq!(run.functions.len(), 2);

        let update = run.functions.iter().find(|f| f.name == "update").unwrap();
        assert_eq!(update.calls, 2);
        // total self_ms should be (2000000 + 2100000) / 1_000_000 = 4.1ms
        assert!(
            (update.self_ms - 4.1).abs() < 0.01,
            "expected ~4.1ms, got {}",
            update.self_ms
        );
    }

    #[test]
    fn load_ndjson_v4_with_trailer() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.ndjson");
        // v4 format: header with no functions, frames, trailer with functions
        fs::write(
            &path,
            r#"{"format_version":4,"run_id":"test","timestamp_ms":1000}
{"frame":0,"fns":[{"id":0,"calls":3,"self_ns":5000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"fns":[{"id":1,"calls":1,"self_ns":2000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"functions":["alpha","beta"]}
"#,
        )
        .unwrap();
        let (run, frame_data) = load_ndjson(&path).unwrap();
        assert_eq!(run.run_id.as_deref(), Some("test"));
        assert_eq!(run.functions.len(), 2);
        assert_eq!(run.functions[0].name, "alpha");
        assert_eq!(run.functions[0].calls, 3);
        assert_eq!(run.functions[1].name, "beta");
        assert_eq!(run.functions[1].calls, 1);
        assert_eq!(frame_data.fn_names, vec!["alpha", "beta"]);
        assert_eq!(frame_data.frames.len(), 2);
    }

    #[test]
    fn load_ndjson_v4_without_trailer() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("crash.ndjson");
        // v4 format: header with no functions, frames, NO trailer (simulates crash)
        fs::write(
            &path,
            r#"{"format_version":4,"run_id":"crash","timestamp_ms":2000}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":100,"ac":0,"ab":0,"fc":0,"fb":0},{"id":2,"calls":1,"self_ns":200,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#,
        )
        .unwrap();
        let (run, frame_data) = load_ndjson(&path).unwrap();
        // With no trailer, placeholder names should be generated
        assert_eq!(run.functions.len(), 3); // fn_0, fn_1, fn_2 (max id is 2)
        assert_eq!(run.functions[0].name, "fn_0");
        assert_eq!(run.functions[2].name, "fn_2");
        assert_eq!(frame_data.frames.len(), 1);
    }

    #[test]
    fn load_ndjson_v3_still_works() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("v3.ndjson");
        fs::write(
            &path,
            r#"{"format_version":3,"timestamp_ms":1000,"functions":["foo","bar"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":100,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#,
        )
        .unwrap();
        let (run, _) = load_ndjson(&path).unwrap();
        assert_eq!(run.functions[0].name, "foo");
    }

    #[test]
    fn ndjson_total_ms_is_none() {
        let dir = TempDir::new().unwrap();
        let content = r#"{"format_version":2,"run_id":"total_ms_test","timestamp_ms":2000,"functions":["compute","helper"]}
{"frame":0,"fns":[{"id":0,"calls":5,"self_ns":10000000,"ac":0,"ab":0,"fc":0,"fb":0},{"id":1,"calls":10,"self_ns":3000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(dir.path().join("2000.ndjson"), content).unwrap();

        let (run, _frame_data) = load_ndjson(&dir.path().join("2000.ndjson")).unwrap();
        assert_eq!(run.source_format, RunFormat::Ndjson);

        // NDJSON has no total (elapsed) time, so total_ms must be None.
        for f in &run.functions {
            assert!(
                f.total_ms.is_none(),
                "{}: total_ms should be None for NDJSON, got {:?}",
                f.name,
                f.total_ms
            );
        }

        // self_ms should still be computed correctly.
        let compute = run.functions.iter().find(|f| f.name == "compute").unwrap();
        assert!((compute.self_ms - 10.0).abs() < 0.01);
        let helper = run.functions.iter().find(|f| f.name == "helper").unwrap();
        assert!((helper.self_ms - 3.0).abs() < 0.01);
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
    fn load_latest_run_errors_when_all_files_corrupt() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("100.json"), "garbage").unwrap();
        fs::write(dir.path().join("200.json"), "also garbage").unwrap();
        let result = load_latest_run(dir.path());
        assert!(result.is_err(), "expected Err when all files are corrupt");
    }

    #[test]
    fn format_table_with_frames_shows_allocs() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "physics".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
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
    fn format_frames_table_shows_per_frame_breakdown() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "physics".into()],
            frames: vec![
                vec![
                    FrameFnEntry {
                        fn_id: 0,
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
                        calls: 1,
                        self_ns: 1_000_000,
                        cpu_self_ns: None,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
                // Spike frame: 4x the typical total
                vec![
                    FrameFnEntry {
                        fn_id: 0,
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
        // Spike detection: frame 2 has 8ms update vs 2ms, should be flagged
        assert!(
            table.contains("<<"),
            "should flag the spike frame. Got:\n{table}"
        );
    }

    #[test]
    fn load_ndjson_includes_zero_call_functions() {
        let dir = TempDir::new().unwrap();
        // "render" is registered but never appears in any frame
        let content = r#"{"format_version":2,"run_id":"test_1","timestamp_ms":1000,"functions":["update","physics","render"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":10,"ab":4096,"fc":8,"fb":3072},{"id":1,"calls":1,"self_ns":1000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        let path = dir.path().join("1000.ndjson");
        fs::write(&path, content).unwrap();

        let (run, _frame_data) = load_ndjson(&path).unwrap();

        // All 3 functions should be present, including zero-call "render"
        assert_eq!(
            run.functions.len(),
            3,
            "expected 3 functions (including zero-call), got {}: {:?}",
            run.functions.len(),
            run.functions.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
        let render = run.functions.iter().find(|f| f.name == "render").unwrap();
        assert_eq!(render.calls, 0);
        assert_eq!(render.alloc_count, 0);
    }

    #[test]
    fn format_table_with_frames_hides_zero_call_by_default() {
        let frame_data = FrameData {
            fn_names: vec!["update".into(), "unused".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
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
    fn load_ndjson_with_cpu_time() {
        let dir = TempDir::new().unwrap();
        let content = r#"{"format_version":2,"run_id":"cpu_1","timestamp_ms":1000,"functions":["compute"],"has_cpu_time":true}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":5000000,"csn":4000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        let path = dir.path().join("1000.ndjson");
        fs::write(&path, content).unwrap();

        let (run, frame_data) = load_ndjson(&path).unwrap();
        let compute = run.functions.iter().find(|f| f.name == "compute").unwrap();
        assert!(compute.cpu_self_ms.is_some(), "should have cpu_self_ms");
        assert!(
            (compute.cpu_self_ms.unwrap() - 4.0).abs() < 0.01,
            "expected ~4.0ms, got {}",
            compute.cpu_self_ms.unwrap()
        );

        let frame_entry = &frame_data.frames[0][0];
        assert_eq!(frame_entry.cpu_self_ns, Some(4_000_000));
    }

    #[test]
    fn load_ndjson_without_cpu_time_has_no_cpu_data() {
        let dir = TempDir::new().unwrap();
        let content = r#"{"format_version":2,"run_id":"no_cpu","timestamp_ms":1000,"functions":["update"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        let path = dir.path().join("1000.ndjson");
        fs::write(&path, content).unwrap();

        let (run, frame_data) = load_ndjson(&path).unwrap();
        let update = run.functions.iter().find(|f| f.name == "update").unwrap();
        assert!(
            update.cpu_self_ms.is_none(),
            "should not have cpu_self_ms when has_cpu_time is absent"
        );
        assert_eq!(frame_data.frames[0][0].cpu_self_ns, None);

        // Report should not show CPU column.
        let table = format_table(&run, false);
        assert!(
            !table.contains("CPU"),
            "should not show CPU column for non-CPU NDJSON. Got:\n{table}"
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
    fn collect_run_files_deduplicates_json_when_ndjson_exists() {
        let dir = TempDir::new().unwrap();

        // Create both .json and .ndjson with the same stem.
        let json = r#"{"run_id":"dup_5000","timestamp_ms":5000,"functions":[
            {"name":"work","calls":10,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let ndjson = r#"{"format_version":2,"run_id":"dup_5000","timestamp_ms":5000,"functions":["work"]}
{"frame":0,"fns":[{"id":0,"calls":10,"self_ns":5000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(dir.path().join("5000.json"), json).unwrap();
        fs::write(dir.path().join("5000.ndjson"), ndjson).unwrap();
        // A standalone .json with a different stem should survive.
        let other = r#"{"run_id":"other_6000","timestamp_ms":6000,"functions":[]}"#;
        fs::write(dir.path().join("6000.json"), other).unwrap();

        let files = collect_run_files(dir.path()).unwrap();
        let stems_and_exts: Vec<_> = files
            .iter()
            .map(|p| {
                (
                    p.file_stem().unwrap().to_str().unwrap().to_string(),
                    p.extension().unwrap().to_str().unwrap().to_string(),
                )
            })
            .collect();

        // 5000.json should be removed; 5000.ndjson and 6000.json remain.
        assert_eq!(
            files.len(),
            2,
            "expected 2 files after dedup, got {files:?}"
        );
        assert!(
            stems_and_exts.contains(&("5000".into(), "ndjson".into())),
            "should keep .ndjson: {stems_and_exts:?}"
        );
        assert!(
            !stems_and_exts.contains(&("5000".into(), "json".into())),
            "should remove duplicate .json: {stems_and_exts:?}"
        );
        assert!(
            stems_and_exts.contains(&("6000".into(), "json".into())),
            "should keep standalone .json: {stems_and_exts:?}"
        );
    }

    #[test]
    fn load_run_by_id_no_double_count_with_json_and_ndjson() {
        // When both 5000.json and 5000.ndjson exist with the same run_id,
        // collect_run_files deduplicates — only the .ndjson is loaded.
        let dir = TempDir::new().unwrap();

        let ndjson = r#"{"format_version":2,"run_id":"dup_5000","timestamp_ms":5000,"functions":["main_fn"]}
{"frame":0,"fns":[{"id":0,"calls":10,"self_ns":5000000,"ac":100,"ab":4096,"fc":0,"fb":0}]}
"#;
        let json = r#"{
            "run_id": "dup_5000",
            "timestamp_ms": 5000,
            "functions": [
                {"name": "main_fn", "calls": 10, "total_ms": 5.0, "self_ms": 5.0},
                {"name": "worker_fn", "calls": 20, "total_ms": 3.0, "self_ms": 3.0}
            ]
        }"#;
        fs::write(dir.path().join("5000.ndjson"), ndjson).unwrap();
        fs::write(dir.path().join("5000.json"), json).unwrap();

        let run = load_run_by_id(dir.path(), "dup_5000").unwrap();

        // main_fn should have calls=10 (not 20 from double-counting).
        let main_fn = run
            .functions
            .iter()
            .find(|f| f.name == "main_fn")
            .expect("main_fn should be present");
        assert_eq!(
            main_fn.calls, 10,
            "main_fn calls should be 10, not doubled. Got {}",
            main_fn.calls
        );

        // The .json is deduplicated away — only .ndjson data is used.
        assert_eq!(
            run.functions.len(),
            1,
            "only NDJSON data should be loaded, not companion JSON"
        );
    }

    #[test]
    fn merge_runs_mixed_cpu_data() {
        let run_a = Run {
            run_id: Some("test_1".into()),
            timestamp_ms: 1000,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 5,
                total_ms: Some(20.0),
                self_ms: 15.0,
                cpu_self_ms: Some(10.0),
                ..Default::default()
            }],
        };
        let run_b = Run {
            run_id: Some("test_1".into()),
            timestamp_ms: 1001,
            source_format: RunFormat::default(),
            functions: vec![FnEntry {
                name: "work".into(),
                calls: 3,
                total_ms: Some(12.0),
                self_ms: 9.0,
                // No CPU data in this run.
                ..Default::default()
            }],
        };
        let merged = merge_runs(&[&run_a, &run_b]);
        let work = merged.functions.iter().find(|f| f.name == "work").unwrap();
        assert_eq!(work.calls, 8);
        // CPU should be Some(10.0) — only accumulated from runs that have it.
        assert_eq!(work.cpu_self_ms, Some(10.0));
    }

    #[test]
    fn resolve_tag_returns_run_id() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&tags_dir).unwrap();

        save_tag(&tags_dir, "baseline", "abc_1000").unwrap();
        let run_id = resolve_tag(&tags_dir, "baseline").unwrap();
        assert_eq!(run_id, "abc_1000");
    }

    #[test]
    fn resolve_tag_errors_on_missing_tag() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&tags_dir).unwrap();

        let err = resolve_tag(&tags_dir, "nonexistent").unwrap_err();
        assert!(
            matches!(err, Error::RunNotFound { ref tag } if tag == "nonexistent"),
            "expected RunNotFound, got: {err:?}"
        );
    }

    #[test]
    fn find_ndjson_by_run_id_finds_matching_file() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();

        let ndjson = r#"{"format_version":2,"run_id":"test_42","timestamp_ms":5000,"functions":["work"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        let ndjson_path = runs_dir.join("5000.ndjson");
        fs::write(&ndjson_path, ndjson).unwrap();

        let result = find_ndjson_by_run_id(&runs_dir, "test_42").unwrap();
        assert_eq!(result, Some(ndjson_path));
    }

    #[test]
    fn find_ndjson_by_run_id_returns_none_for_json_only() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();

        let json = r#"{"run_id":"test_42","timestamp_ms":5000,"functions":[
            {"name":"work","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        fs::write(runs_dir.join("5000.json"), json).unwrap();

        let result = find_ndjson_by_run_id(&runs_dir, "test_42").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn find_ndjson_by_run_id_returns_none_for_mismatched_id() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();

        let ndjson = r#"{"format_version":2,"run_id":"other_id","timestamp_ms":5000,"functions":["work"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
        fs::write(runs_dir.join("5000.ndjson"), ndjson).unwrap();

        let result = find_ndjson_by_run_id(&runs_dir, "nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn ndjson_deserializes_large_alloc_counts() {
        // NDJSON ac/fc fields should support values above u32::MAX.
        let dir = TempDir::new().unwrap();

        let large_ac: u64 = 5_000_000_000;
        let large_fc: u64 = 4_500_000_000;
        let ndjson = format!(
            r#"{{"format_version":3,"run_id":"large_ac","timestamp_ms":8000,"functions":["allocator"]}}
{{"frame":0,"fns":[{{"id":0,"calls":1,"self_ns":1000000,"ac":{large_ac},"ab":50000,"fc":{large_fc},"fb":40000}}]}}
"#
        );
        fs::write(dir.path().join("8000.ndjson"), &ndjson).unwrap();

        let ndjson_path = dir.path().join("8000.ndjson");
        let (_run, frame_data) = load_ndjson(&ndjson_path).unwrap();

        let entry = &frame_data.frames[0][0];
        assert_eq!(
            entry.alloc_count, large_ac,
            "ac field should deserialize values above u32::MAX"
        );
        assert_eq!(
            entry.free_count, large_fc,
            "fc field should deserialize values above u32::MAX"
        );
    }

    #[test]
    fn format_table_with_frames_self_before_calls() {
        let frame_data = FrameData {
            fn_names: vec!["work".into()],
            frames: vec![vec![FrameFnEntry {
                fn_id: 0,
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
    fn relative_time_seconds() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(30);
        assert_eq!(relative_time(t), "30 sec ago");
    }

    #[test]
    fn relative_time_minutes() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(150);
        assert_eq!(relative_time(t), "2 min ago");
    }

    #[test]
    fn relative_time_hours() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(7200);
        assert_eq!(relative_time(t), "2 hours ago");
    }

    #[test]
    fn relative_time_days() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(172800);
        assert_eq!(relative_time(t), "2 days ago");
    }

    #[test]
    fn relative_time_singular_hour() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(3600);
        assert_eq!(relative_time(t), "1 hour ago");
    }

    #[test]
    fn relative_time_singular_day() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::now() - Duration::from_secs(86400);
        assert_eq!(relative_time(t), "1 day ago");
    }

    #[test]
    fn load_two_latest_runs_returns_previous_and_latest() {
        let dir = TempDir::new().unwrap();
        let old = r#"{"run_id":"1_500","timestamp_ms":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let newer = r#"{"run_id":"2_1000","timestamp_ms":1000,"functions":[
            {"name":"new_fn","calls":2,"total_ms":10.0,"self_ms":8.0}
        ]}"#;
        fs::write(dir.path().join("500.json"), old).unwrap();
        fs::write(dir.path().join("1000.json"), newer).unwrap();

        let (previous, latest) = load_two_latest_runs(dir.path()).unwrap();
        assert_eq!(previous.run_id.as_deref(), Some("1_500"));
        assert_eq!(latest.run_id.as_deref(), Some("2_1000"));
    }

    #[test]
    fn load_two_latest_runs_errors_with_one_run() {
        let dir = TempDir::new().unwrap();
        let only = r#"{"run_id":"1_500","timestamp_ms":500,"functions":[]}"#;
        fs::write(dir.path().join("500.json"), only).unwrap();

        let result = load_two_latest_runs(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn load_two_latest_runs_errors_on_empty_dir() {
        let dir = TempDir::new().unwrap();
        let result = load_two_latest_runs(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn load_two_latest_runs_merges_multi_thread_files() {
        let dir = TempDir::new().unwrap();
        // Two files from the same run (multi-threaded)
        let run1_a = r#"{"run_id":"1_500","timestamp_ms":500,"functions":[
            {"name":"parse","calls":50,"total_ms":100.0,"self_ms":100.0}
        ]}"#;
        let run1_b = r#"{"run_id":"1_500","timestamp_ms":501,"functions":[
            {"name":"resolve","calls":30,"total_ms":60.0,"self_ms":60.0}
        ]}"#;
        // One file from a different run
        let run2 = r#"{"run_id":"2_1000","timestamp_ms":1000,"functions":[
            {"name":"new_fn","calls":2,"total_ms":10.0,"self_ms":8.0}
        ]}"#;
        fs::write(dir.path().join("500.json"), run1_a).unwrap();
        fs::write(dir.path().join("501.json"), run1_b).unwrap();
        fs::write(dir.path().join("1000.json"), run2).unwrap();

        let (previous, latest) = load_two_latest_runs(dir.path()).unwrap();
        // Previous should have both parse and resolve merged
        assert_eq!(previous.functions.len(), 2);
        assert_eq!(latest.run_id.as_deref(), Some("2_1000"));
    }

    #[test]
    fn reverse_resolve_tag_finds_matching_tag() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&tags_dir).unwrap();
        fs::write(tags_dir.join("baseline"), "42_9000").unwrap();
        fs::write(tags_dir.join("other"), "99_1000").unwrap();

        let result = reverse_resolve_tag(&tags_dir, "42_9000");
        assert_eq!(result, Some("baseline".to_string()));
    }

    #[test]
    fn reverse_resolve_tag_returns_none_when_no_match() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&tags_dir).unwrap();
        fs::write(tags_dir.join("baseline"), "42_9000").unwrap();

        let result = reverse_resolve_tag(&tags_dir, "nonexistent");
        assert_eq!(result, None);
    }

    #[test]
    fn reverse_resolve_tag_returns_none_for_missing_dir() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("no_such_dir");

        let result = reverse_resolve_tag(&tags_dir, "42_9000");
        assert_eq!(result, None);
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

        // Load via tag path — same path as cmd_diff uses.
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
    fn two_latest_runs_with_tags_and_relative_time() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        let tags_dir = dir.path().join("tags");
        fs::create_dir_all(&runs_dir).unwrap();
        fs::create_dir_all(&tags_dir).unwrap();

        let run_old = r#"{"run_id":"1_500","timestamp_ms":500,"functions":[
            {"name":"old_fn","calls":1,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let run_new = r#"{"run_id":"2_1000","timestamp_ms":1000,"functions":[
            {"name":"new_fn","calls":2,"total_ms":10.0,"self_ms":8.0}
        ]}"#;
        fs::write(runs_dir.join("500.json"), run_old).unwrap();
        fs::write(runs_dir.join("1000.json"), run_new).unwrap();

        // Tag the old run
        fs::write(tags_dir.join("baseline"), "1_500").unwrap();

        let (prev, latest) = load_two_latest_runs(&runs_dir).unwrap();
        assert_eq!(prev.run_id.as_deref(), Some("1_500"));
        assert_eq!(latest.run_id.as_deref(), Some("2_1000"));

        // Reverse resolve: tagged run returns tag name
        let label_prev = reverse_resolve_tag(&tags_dir, "1_500");
        assert_eq!(label_prev, Some("baseline".to_string()));

        // Reverse resolve: untagged run returns None
        let label_latest = reverse_resolve_tag(&tags_dir, "2_1000");
        assert_eq!(label_latest, None);

        // relative_time works on file modified time
        let meta = fs::metadata(runs_dir.join("1000.json")).unwrap();
        let label = relative_time(meta.modified().unwrap());
        assert!(
            label.contains("ago"),
            "expected relative time, got: {label}"
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
        let entries: Vec<JsonDiffEntry> = serde_json::from_str(&json).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "new_fn");
        assert!((entries[0].self_ms_a).abs() < f64::EPSILON);
        assert!(entries[0].delta_pct.is_none());
    }
}
