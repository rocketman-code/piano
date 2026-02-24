use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::Error;

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
    pub total_ms: f64,
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

/// Per-function entry within a single frame.
pub struct FrameFnEntry {
    pub fn_id: usize,
    pub calls: u64,
    pub self_ns: u64,
    pub cpu_self_ns: Option<u64>,
    pub alloc_count: u32,
    pub alloc_bytes: u64,
    pub free_count: u32,
    pub free_bytes: u64,
}

/// NDJSON header line.
#[derive(serde::Deserialize)]
struct NdjsonHeader {
    #[serde(rename = "format_version")]
    _format_version: u32,
    run_id: Option<String>,
    timestamp_ms: u128,
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
    ac: u32,
    #[serde(default)]
    ab: u64,
    #[serde(default)]
    fc: u32,
    #[serde(default)]
    fb: u64,
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
    let mut lines = contents.lines();

    let header_line = lines.next().ok_or_else(|| Error::InvalidRunData {
        path: path.to_path_buf(),
        reason: "empty NDJSON file".into(),
    })?;
    let header: NdjsonHeader =
        serde_json::from_str(header_line).map_err(|e| Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("invalid NDJSON header: {e}"),
        })?;

    let mut frames: Vec<Vec<FrameFnEntry>> = Vec::new();
    for line in lines {
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

    // Merge cross-thread functions from companion JSON if present.
    // NDJSON only has main-thread frame data (from collect_frames TLS).
    // The companion JSON has all threads (from collect_all Arc registry).
    let mut fn_names = header.functions;
    let json_companion = path.with_extension("json");
    if json_companion.exists() {
        match std::fs::read_to_string(&json_companion)
            .map_err(|e| e.to_string())
            .and_then(|c| serde_json::from_str::<Run>(&c).map_err(|e| e.to_string()))
        {
            Ok(json_run) => {
                let existing: HashSet<String> = fn_names.iter().cloned().collect();
                for fn_entry in &json_run.functions {
                    if !existing.contains(&fn_entry.name) {
                        let new_id = fn_names.len();
                        fn_names.push(fn_entry.name.clone());
                        // Add aggregate data to first frame. Worker threads lack
                        // frame boundaries, so we can't assign to specific frames.
                        if let Some(first_frame) = frames.first_mut() {
                            first_frame.push(FrameFnEntry {
                                fn_id: new_id,
                                calls: fn_entry.calls,
                                self_ns: (fn_entry.self_ms * 1_000_000.0) as u64,
                                cpu_self_ns: fn_entry
                                    .cpu_self_ms
                                    .map(|ms| (ms * 1_000_000.0) as u64),
                                alloc_count: fn_entry.alloc_count as u32,
                                alloc_bytes: fn_entry.alloc_bytes,
                                free_count: 0,
                                free_bytes: 0,
                            });
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "warning: companion {} unreadable: {e}",
                    json_companion.display()
                );
            }
        }
    }

    // Aggregate into Run for backward compatibility.
    let has_cpu = header.has_cpu_time;
    let mut fn_agg: HashMap<usize, (u64, u64, u64, u64, u64)> = HashMap::new();
    for frame in &frames {
        for entry in frame {
            let agg = fn_agg.entry(entry.fn_id).or_insert((0, 0, 0, 0, 0));
            agg.0 += entry.calls;
            agg.1 += entry.self_ns;
            agg.2 += entry.alloc_count as u64;
            agg.3 += entry.alloc_bytes;
            agg.4 += entry.cpu_self_ns.unwrap_or(0);
        }
    }

    // Build FnEntry for every registered function, including zero-call ones.
    let functions: Vec<FnEntry> = fn_names
        .iter()
        .enumerate()
        .map(|(fn_id, name)| {
            let (calls, self_ns, alloc_count, alloc_bytes, cpu_self_ns) =
                fn_agg.get(&fn_id).copied().unwrap_or((0, 0, 0, 0, 0));
            let self_ms = self_ns as f64 / 1_000_000.0;
            FnEntry {
                name: name.clone(),
                calls,
                total_ms: self_ms, // NDJSON format has no total_ms; approximate with self_ms
                self_ms,
                cpu_self_ms: if has_cpu {
                    Some(cpu_self_ns as f64 / 1_000_000.0)
                } else {
                    None
                },
                alloc_count,
                alloc_bytes,
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
/// When `show_all` is false, entries with zero calls are hidden.
pub fn format_table(run: &Run, show_all: bool) -> String {
    let mut entries = run.functions.clone();
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
            "{:<40} {:>8} {:>10} {:>10} {:>10}\n",
            "Function", "Calls", "Total", "Self", "CPU"
        ));
        out.push_str(&format!("{}\n", "-".repeat(84)));
    } else {
        out.push_str(&format!(
            "{:<40} {:>8} {:>10} {:>10}\n",
            "Function", "Calls", "Total", "Self"
        ));
        out.push_str(&format!("{}\n", "-".repeat(72)));
    }

    for entry in &entries {
        if has_cpu {
            let cpu_str = match entry.cpu_self_ms {
                Some(v) => format!("{v:>9.2}ms"),
                None => format!("{:>11}", "-"),
            };
            out.push_str(&format!(
                "{:<40} {:>8} {:>9.2}ms {:>9.2}ms {}\n",
                entry.name, entry.calls, entry.total_ms, entry.self_ms, cpu_str
            ));
        } else {
            out.push_str(&format!(
                "{:<40} {:>8} {:>9.2}ms {:>9.2}ms\n",
                entry.name, entry.calls, entry.total_ms, entry.self_ms
            ));
        }
    }
    out
}

/// Format frame-level data as a table with percentile and allocation columns.
///
/// Columns: Function | Calls | Self | p50 | p99 | Allocs | Bytes
/// Footer: frame count summary.
pub fn format_table_with_frames(frame_data: &FrameData, show_all: bool) -> String {
    struct FnStats {
        name: String,
        total_calls: u64,
        total_self_ns: u64,
        total_cpu_self_ns: Option<u64>,
        total_allocs: u64,
        total_alloc_bytes: u64,
        per_frame_self_ns: Vec<u64>,
    }

    let has_cpu = frame_data
        .frames
        .iter()
        .any(|f| f.iter().any(|e| e.cpu_self_ns.is_some()));

    let mut stats_map: HashMap<usize, FnStats> = HashMap::new();
    for frame in &frame_data.frames {
        // Track which fn_ids appear in this frame (to fill zeros for missing ones).
        let mut seen: HashMap<usize, bool> = stats_map.keys().map(|&k| (k, false)).collect();

        for entry in frame {
            seen.insert(entry.fn_id, true);
            let stats = stats_map.entry(entry.fn_id).or_insert_with(|| FnStats {
                name: frame_data
                    .fn_names
                    .get(entry.fn_id)
                    .cloned()
                    .unwrap_or_else(|| format!("<fn_{}>", entry.fn_id)),
                total_calls: 0,
                total_self_ns: 0,
                total_cpu_self_ns: if has_cpu { Some(0) } else { None },
                total_allocs: 0,
                total_alloc_bytes: 0,
                per_frame_self_ns: Vec::new(),
            });
            stats.total_calls += entry.calls;
            stats.total_self_ns += entry.self_ns;
            if let (Some(total), Some(cpu)) = (&mut stats.total_cpu_self_ns, entry.cpu_self_ns) {
                *total += cpu;
            }
            stats.total_allocs += entry.alloc_count as u64;
            stats.total_alloc_bytes += entry.alloc_bytes;
            stats.per_frame_self_ns.push(entry.self_ns);
        }

        // Functions not present in this frame get a zero entry.
        for (fn_id, present) in &seen {
            if !present && let Some(stats) = stats_map.get_mut(fn_id) {
                stats.per_frame_self_ns.push(0);
            }
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
                per_frame_self_ns: Vec::new(),
            });
        }
    }

    let mut entries: Vec<FnStats> = stats_map.into_values().collect();
    if !show_all {
        entries.retain(|e| e.total_calls > 0);
    }
    entries.sort_by(|a, b| b.total_self_ns.cmp(&a.total_self_ns));

    // Pad per-frame vectors so late-appearing functions have zeros for earlier frames.
    let total_frames = frame_data.frames.len();
    for e in &mut entries {
        while e.per_frame_self_ns.len() < total_frames {
            e.per_frame_self_ns.push(0);
        }
    }

    // Sort per-frame vectors for percentile calculation.
    for e in &mut entries {
        e.per_frame_self_ns.sort_unstable();
    }

    let mut out = String::new();
    if has_cpu {
        out.push_str(&format!(
            "{:<40} {:>8} {:>10} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
            "Function", "Calls", "Self", "CPU", "p50", "p99", "Allocs", "Bytes"
        ));
        out.push_str(&format!("{}\n", "-".repeat(112)));
    } else {
        out.push_str(&format!(
            "{:<40} {:>8} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
            "Function", "Calls", "Self", "p50", "p99", "Allocs", "Bytes"
        ));
        out.push_str(&format!("{}\n", "-".repeat(100)));
    }

    for e in &entries {
        let self_str = format_ns(e.total_self_ns);
        let p50 = percentile(&e.per_frame_self_ns, 50.0);
        let p99 = percentile(&e.per_frame_self_ns, 99.0);
        let p50_str = format_ns(p50);
        let p99_str = format_ns(p99);
        let bytes_str = format_bytes(e.total_alloc_bytes);
        if has_cpu {
            let cpu_str = match e.total_cpu_self_ns {
                Some(ns) => format_ns(ns),
                None => format!("{:>10}", "-"),
            };
            out.push_str(&format!(
                "{:<40} {:>8} {:>10} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
                e.name,
                e.total_calls,
                self_str,
                cpu_str,
                p50_str,
                p99_str,
                e.total_allocs,
                bytes_str
            ));
        } else {
            out.push_str(&format!(
                "{:<40} {:>8} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
                e.name, e.total_calls, self_str, p50_str, p99_str, e.total_allocs, bytes_str
            ));
        }
    }

    let n_frames = frame_data.frames.len();
    out.push_str(&format!("\n{n_frames} frames"));

    out
}

/// Format per-frame breakdown table.
///
/// Each row is one frame. Columns: Frame | Total | [one column per function] | Allocs | Bytes
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
    out.push_str(&format!("{:>6} {:>10}", "Frame", "Total"));
    for name in fn_names {
        out.push_str(&format!(" {name:>fn_col_width$}"));
    }
    out.push_str(&format!(" {:>8} {:>10} {}\n", "Allocs", "Bytes", ""));
    out.push_str(&format!(
        "{}\n",
        "-".repeat(32 + n_fns * (fn_col_width + 1))
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

        let allocs: u64 = frame.iter().map(|e| e.alloc_count as u64).sum();
        let bytes: u64 = frame.iter().map(|e| e.alloc_bytes).sum();
        out.push_str(&format!(" {:>8} {:>10}", allocs, format_bytes(bytes)));

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
        "\n{} frames | {} spikes (>2x median)\n",
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

/// Show the delta between two runs, comparing functions by name.
///
/// Includes timing deltas and allocation deltas when alloc data is present.
pub fn diff_runs(a: &Run, b: &Run) -> String {
    // Warn if comparing runs from different formats (total_ms may not be comparable).
    if a.source_format != b.source_format {
        eprintln!(
            "warning: comparing runs with different formats; total_ms values may not be directly comparable"
        );
    }

    let a_map: HashMap<&str, &FnEntry> = a.functions.iter().map(|f| (f.name.as_str(), f)).collect();
    let b_map: HashMap<&str, &FnEntry> = b.functions.iter().map(|f| (f.name.as_str(), f)).collect();

    // Collect all function names, sorted for deterministic output.
    let mut names: Vec<&str> = a_map.keys().chain(b_map.keys()).copied().collect();
    names.sort_unstable();
    names.dedup();

    // Check if either run has alloc data or CPU data.
    let has_allocs = a.functions.iter().any(|f| f.alloc_count > 0)
        || b.functions.iter().any(|f| f.alloc_count > 0);
    let has_cpu = a.functions.iter().any(|f| f.cpu_self_ms.is_some())
        || b.functions.iter().any(|f| f.cpu_self_ms.is_some());

    let mut out = String::new();
    // Build header based on available columns.
    {
        let mut header = format!(
            "{:<40} {:>10} {:>10} {:>10}",
            "Function", "Before", "After", "Delta"
        );
        if has_cpu {
            header.push_str(&format!(" {:>10} {:>10}", "CPU.Bef", "CPU.Aft"));
        }
        if has_allocs {
            header.push_str(&format!(" {:>10} {:>10}", "Allocs", "A.Delta"));
        }
        header.push('\n');
        let width = header.trim_end().len();
        out.push_str(&header);
        out.push_str(&format!("{}\n", "-".repeat(width)));
    }

    for name in &names {
        let before = a_map.get(name).map_or(0.0, |e| e.self_ms);
        let after = b_map.get(name).map_or(0.0, |e| e.self_ms);
        let delta = after - before;

        out.push_str(&format!(
            "{name:<40} {before:>9.2}ms {after:>9.2}ms {delta:>+9.2}ms",
        ));

        if has_cpu {
            let cpu_before = a_map.get(name).and_then(|e| e.cpu_self_ms);
            let cpu_after = b_map.get(name).and_then(|e| e.cpu_self_ms);
            let fmt_cpu = |v: Option<f64>| match v {
                Some(ms) => format!("{ms:>9.2}ms"),
                None => format!("{:>11}", "-"),
            };
            out.push_str(&format!(" {} {}", fmt_cpu(cpu_before), fmt_cpu(cpu_after)));
        }

        if has_allocs {
            let allocs_after = b_map.get(name).map_or(0i64, |e| e.alloc_count as i64);
            let allocs_before = a_map.get(name).map_or(0i64, |e| e.alloc_count as i64);
            let allocs_delta = allocs_after - allocs_before;
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
    // keep only the .ndjson file. load_ndjson already merges companion JSON
    // data, so loading both would double-count every function.
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
                total_ms: 0.0,
                self_ms: 0.0,
                cpu_self_ms: None,
                alloc_count: 0,
                alloc_bytes: 0,
            });
            entry.calls += f.calls;
            entry.total_ms += f.total_ms;
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
        return Err(Error::InvalidTagName("tag name must not be empty".into()));
    }
    if tag == "." || tag == ".." {
        return Err(Error::InvalidTagName(format!(
            "'{tag}' is not a valid tag name"
        )));
    }
    if tag.contains('/') || tag.contains('\\') {
        return Err(Error::InvalidTagName(
            "tag name must not contain path separators".into(),
        ));
    }
    if tag.contains('\0') {
        return Err(Error::InvalidTagName(
            "tag name must not contain null bytes".into(),
        ));
    }
    Ok(())
}

/// Save a tag pointing to a run_id.
pub fn save_tag(tags_dir: &Path, tag: &str, run_id: &str) -> Result<(), Error> {
    validate_tag_name(tag)?;
    std::fs::create_dir_all(tags_dir)?;
    let path = tags_dir.join(tag);
    std::fs::write(&path, run_id)?;
    Ok(())
}

/// Load a run by resolving a tag name to a run_id, then consolidating.
pub fn load_tagged_run(tags_dir: &Path, runs_dir: &Path, tag: &str) -> Result<Run, Error> {
    validate_tag_name(tag)?;
    let tag_path = tags_dir.join(tag);
    let run_id = std::fs::read_to_string(&tag_path).map_err(|source| Error::RunReadError {
        path: tag_path,
        source,
    })?;
    let run_id = run_id.trim();
    load_run_by_id(runs_dir, run_id)
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
        return Err(Error::NoRuns(runs_dir.to_path_buf()));
    }
    let refs: Vec<&Run> = matching.iter().collect();
    Ok(merge_runs(&refs))
}

/// Load the latest run, consolidating all files sharing the same run_id.
///
/// Files written by different threads within one process share a run_id. This
/// function finds the latest run_id (by max timestamp) and merges all files
/// that belong to it. Legacy files without a run_id fall back to single-file
/// loading (the highest-timestamp file).
pub fn load_latest_run(runs_dir: &Path) -> Result<Run, Error> {
    let all_files = collect_run_files(runs_dir)?;
    if all_files.is_empty() {
        return Err(Error::NoRuns(runs_dir.to_path_buf()));
    }

    let mut runs: Vec<Run> = Vec::new();
    for path in &all_files {
        match load_run(path) {
            Ok(run) => runs.push(run),
            Err(_) => continue,
        }
    }

    if runs.is_empty() {
        return Err(Error::NoRuns(runs_dir.to_path_buf()));
    }

    // Find the latest run_id by max timestamp.
    let latest_run_id = runs
        .iter()
        .filter_map(|r| r.run_id.as_ref().map(|id| (id.as_str(), r.timestamp_ms)))
        .max_by_key(|(_, ts)| *ts)
        .map(|(id, _)| id.to_owned());

    let to_merge: Vec<&Run> = match &latest_run_id {
        Some(id) => runs
            .iter()
            .filter(|r| r.run_id.as_deref() == Some(id))
            .collect(),
        None => {
            // Legacy files without run_id: just use the latest single file.
            let latest = runs.iter().max_by_key(|r| r.timestamp_ms);
            latest.into_iter().collect()
        }
    };

    Ok(merge_runs(&to_merge))
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
        assert!((run.functions[0].total_ms - 10.5).abs() < f64::EPSILON);
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
                    total_ms: 2.0,
                    self_ms: 1.0,
                    ..Default::default()
                },
                FnEntry {
                    name: "slow".into(),
                    calls: 1,
                    total_ms: 20.0,
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
                total_ms: 12.0,
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
                total_ms: 9.0,
                self_ms: 8.0,
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b);
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
        assert!((parse.total_ms - 160.0).abs() < 0.01);
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
        assert!(result.is_err());
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
                    total_ms: 10.0,
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

        let table_all = format_table(&run, true);
        assert!(
            table_all.contains("uncalled"),
            "should show zero-call function with show_all"
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
    fn load_latest_run_errors_when_all_files_corrupt() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("100.json"), "garbage").unwrap();
        fs::write(dir.path().join("200.json"), "also garbage").unwrap();
        let result = load_latest_run(dir.path());
        assert!(result.is_err(), "expected Err when all files are corrupt");
    }

    #[test]
    fn format_table_with_frames_shows_percentiles_and_allocs() {
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
        assert!(table.contains("p50"), "should have p50 column");
        assert!(table.contains("p99"), "should have p99 column");
        assert!(table.contains("Allocs"), "should have allocs column");
        assert!(table.contains("update"), "should list update");
        assert!(table.contains("physics"), "should list physics");
        assert!(
            table.contains("frames"),
            "should have frame count in footer"
        );
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
                total_ms: 12.0,
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
                total_ms: 9.0,
                self_ms: 8.0,
                cpu_self_ms: None,
                alloc_count: 50,
                alloc_bytes: 4096,
            }],
        };
        let diff = diff_runs(&a, &b);
        assert!(diff.contains("Allocs"), "should have Allocs column header");
        assert!(
            diff.contains("-50"),
            "should show alloc count delta: {diff}"
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

        // show_all=true should include "unused"
        let table_all = format_table_with_frames(&frame_data, true);
        assert!(
            table_all.contains("unused"),
            "should show zero-call function with show_all. Got:\n{table_all}"
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
                total_ms: 50.0,
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
                total_ms: 50.0,
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
                total_ms: 10.0,
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
                total_ms: 12.0,
                self_ms: 12.0,
                cpu_self_ms: Some(10.0),
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b);
        assert!(
            diff.contains("CPU.Bef"),
            "should have CPU.Bef column. Got:\n{diff}"
        );
        assert!(
            diff.contains("CPU.Aft"),
            "should have CPU.Aft column. Got:\n{diff}"
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
                total_ms: 10.0,
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
                total_ms: 12.0,
                self_ms: 12.0,
                // No CPU data.
                ..Default::default()
            }],
        };
        // Should still render CPU columns (because A has CPU data).
        let diff = diff_runs(&a, &b);
        assert!(
            diff.contains("CPU.Bef"),
            "should show CPU columns when either run has CPU data. Got:\n{diff}"
        );
        assert!(
            diff.contains("8.00"),
            "should show A's CPU value. Got:\n{diff}"
        );
        // B's missing CPU renders as "-", not a misleading 0.00ms.
        // Extract the CPU.Aft column value from the data row.
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
                total_ms: 10.0,
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
                total_ms: 12.0,
                self_ms: 12.0,
                ..Default::default()
            }],
        };
        let diff = diff_runs(&a, &b);
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
                    total_ms: 20.0,
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

        // With --all: shows unused with CPU column present.
        let table_all = format_table(&run, true);
        assert!(table_all.contains("CPU"), "should have CPU column");
        assert!(
            table_all.contains("unused"),
            "should show zero-call fn with --all. Got:\n{table_all}"
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

        // With --all: shows both, CPU column still present.
        let table_all = format_table_with_frames(&frame_data, true);
        assert!(table_all.contains("CPU"), "should show CPU column");
        assert!(
            table_all.contains("unused"),
            "should show unused fn with --all. Got:\n{table_all}"
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
    fn load_ndjson_merges_cross_thread_functions_from_companion_json() {
        // NDJSON has main-thread functions only (from collect_frames TLS).
        // Companion JSON has ALL functions (from collect_all Arc registry).
        // load_ndjson should merge worker-thread-only functions into FrameData.
        let dir = TempDir::new().unwrap();

        let ndjson = r#"{"format_version":2,"run_id":"test_1","timestamp_ms":5000,"functions":["main","orchestrate"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":10000000,"ac":100,"ab":50000,"fc":10,"fb":5000},{"id":1,"calls":1,"self_ns":5000000,"ac":50,"ab":25000,"fc":5,"fb":2500}]}
"#;
        fs::write(dir.path().join("5000.ndjson"), ndjson).unwrap();

        let json = r#"{
            "run_id": "test_1",
            "timestamp_ms": 5000,
            "functions": [
                {"name": "main", "calls": 1, "total_ms": 15.0, "self_ms": 10.0, "cpu_self_ms": 8.0},
                {"name": "orchestrate", "calls": 1, "total_ms": 5.0, "self_ms": 5.0, "cpu_self_ms": 0.0},
                {"name": "busy_wait_ms", "calls": 200, "total_ms": 3.0, "self_ms": 3.0, "cpu_self_ms": 2.5},
                {"name": "expensive_computation", "calls": 50, "total_ms": 1.5, "self_ms": 1.5, "cpu_self_ms": 1.2}
            ]
        }"#;
        fs::write(dir.path().join("5000.json"), json).unwrap();

        let ndjson_path = dir.path().join("5000.ndjson");
        let (_run, frame_data) = load_ndjson(&ndjson_path).unwrap();

        // Should have all 4 functions, not just the 2 from NDJSON.
        assert_eq!(
            frame_data.fn_names.len(),
            4,
            "should merge worker-thread functions from companion JSON. Got: {:?}",
            frame_data.fn_names
        );
        assert!(frame_data.fn_names.contains(&"busy_wait_ms".to_string()));
        assert!(
            frame_data
                .fn_names
                .contains(&"expensive_computation".to_string())
        );

        // The merged functions should appear in the default table output.
        let table = format_table_with_frames(&frame_data, false);
        assert!(
            table.contains("busy_wait_ms"),
            "worker-thread function should appear in report. Got:\n{table}"
        );
        assert!(
            table.contains("expensive_computation"),
            "worker-thread function should appear in report. Got:\n{table}"
        );
        assert!(
            table.contains("200"),
            "busy_wait_ms should show 200 calls. Got:\n{table}"
        );
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
        // load_run_by_id must NOT double-count metrics.
        let dir = TempDir::new().unwrap();

        let ndjson = r#"{"format_version":2,"run_id":"dup_5000","timestamp_ms":5000,"functions":["main_fn"]}
{"frame":0,"fns":[{"id":0,"calls":10,"self_ns":5000000,"ac":100,"ab":4096,"fc":0,"fb":0}]}
"#;
        // The companion JSON has the same function plus a worker-thread one.
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

        // worker_fn should be merged from companion JSON via load_ndjson.
        let worker_fn = run
            .functions
            .iter()
            .find(|f| f.name == "worker_fn")
            .expect("worker_fn should be merged from companion JSON");
        assert_eq!(worker_fn.calls, 20);
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
                total_ms: 20.0,
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
                total_ms: 12.0,
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
}
