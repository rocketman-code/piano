use std::collections::HashMap;
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
                alloc_count: f.ac,
                alloc_bytes: f.ab,
                free_count: f.fc,
                free_bytes: f.fb,
            })
            .collect();
        frames.push(entries);
    }

    // Aggregate into Run for backward compatibility.
    let mut fn_agg: HashMap<usize, (u64, u64, u64, u64)> = HashMap::new();
    for frame in &frames {
        for entry in frame {
            let agg = fn_agg.entry(entry.fn_id).or_insert((0, 0, 0, 0));
            agg.0 += entry.calls;
            agg.1 += entry.self_ns;
            agg.2 += entry.alloc_count as u64;
            agg.3 += entry.alloc_bytes;
        }
    }

    let functions: Vec<FnEntry> = fn_agg
        .into_iter()
        .map(|(fn_id, (calls, self_ns, alloc_count, alloc_bytes))| {
            let name = header
                .functions
                .get(fn_id)
                .cloned()
                .unwrap_or_else(|| format!("<unknown_{fn_id}>"));
            let self_ms = self_ns as f64 / 1_000_000.0;
            FnEntry {
                name,
                calls,
                total_ms: self_ms, // NDJSON format has no total_ms; approximate with self_ms
                self_ms,
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

    let frame_data = FrameData {
        fn_names: header.functions,
        frames,
    };

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

    let mut out = String::new();
    out.push_str(&format!(
        "{:<40} {:>8} {:>10} {:>10}\n",
        "Function", "Calls", "Total", "Self"
    ));
    out.push_str(&format!("{}\n", "-".repeat(72)));

    for entry in &entries {
        out.push_str(&format!(
            "{:<40} {:>8} {:>9.2}ms {:>9.2}ms\n",
            entry.name, entry.calls, entry.total_ms, entry.self_ms
        ));
    }
    out
}

/// Format frame-level data as a table with percentile and allocation columns.
///
/// Columns: Function | Calls | Self | p50 | p99 | Allocs | Bytes
/// Footer: frame count summary.
pub fn format_table_with_frames(frame_data: &FrameData) -> String {
    struct FnStats {
        name: String,
        total_calls: u64,
        total_self_ns: u64,
        total_allocs: u64,
        total_alloc_bytes: u64,
        per_frame_self_ns: Vec<u64>,
    }

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
                total_allocs: 0,
                total_alloc_bytes: 0,
                per_frame_self_ns: Vec::new(),
            });
            stats.total_calls += entry.calls;
            stats.total_self_ns += entry.self_ns;
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

    let mut entries: Vec<FnStats> = stats_map.into_values().collect();
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
    out.push_str(&format!(
        "{:<40} {:>8} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
        "Function", "Calls", "Self", "p50", "p99", "Allocs", "Bytes"
    ));
    out.push_str(&format!("{}\n", "-".repeat(100)));

    for e in &entries {
        let self_str = format_ns(e.total_self_ns);
        let p50 = percentile(&e.per_frame_self_ns, 50.0);
        let p99 = percentile(&e.per_frame_self_ns, 99.0);
        let p50_str = format_ns(p50);
        let p99_str = format_ns(p99);
        let bytes_str = format_bytes(e.total_alloc_bytes);
        out.push_str(&format!(
            "{:<40} {:>8} {:>10} {:>10} {:>10} {:>8} {:>10}\n",
            e.name, e.total_calls, self_str, p50_str, p99_str, e.total_allocs, bytes_str
        ));
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

    // Header.
    let mut out = String::new();
    out.push_str(&format!("{:>6} {:>10}", "Frame", "Total"));
    for name in fn_names {
        let truncated: String = name.chars().take(12).collect();
        out.push_str(&format!(" {:>12}", truncated));
    }
    out.push_str(&format!(" {:>8} {:>10} {}\n", "Allocs", "Bytes", ""));
    out.push_str(&format!("{}\n", "-".repeat(32 + n_fns * 13)));

    // Rows.
    for (i, frame) in frame_data.frames.iter().enumerate() {
        let total = frame_totals[i];
        let is_spike = total > spike_threshold && median > 0;

        out.push_str(&format!("{:>6} {:>10}", i + 1, format_ns(total)));

        // One column per function.
        for fn_id in 0..n_fns {
            let entry = frame.iter().find(|e| e.fn_id == fn_id);
            let ns = entry.map_or(0, |e| e.self_ns);
            out.push_str(&format!(" {:>12}", format_ns(ns)));
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

    // Check if either run has alloc data.
    let has_allocs = a.functions.iter().any(|f| f.alloc_count > 0)
        || b.functions.iter().any(|f| f.alloc_count > 0);

    let mut out = String::new();
    if has_allocs {
        out.push_str(&format!(
            "{:<40} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
            "Function", "Before", "After", "Delta", "Allocs", "A.Delta"
        ));
        out.push_str(&format!("{}\n", "-".repeat(94)));
    } else {
        out.push_str(&format!(
            "{:<40} {:>10} {:>10} {:>10}\n",
            "Function", "Before", "After", "Delta"
        ));
        out.push_str(&format!("{}\n", "-".repeat(74)));
    }

    for name in &names {
        let before = a_map.get(name).map_or(0.0, |e| e.self_ms);
        let after = b_map.get(name).map_or(0.0, |e| e.self_ms);
        let delta = after - before;

        if has_allocs {
            let allocs_before = a_map.get(name).map_or(0i64, |e| e.alloc_count as i64);
            let allocs_after = b_map.get(name).map_or(0i64, |e| e.alloc_count as i64);
            let allocs_delta = allocs_after - allocs_before;
            out.push_str(&format!(
                "{:<40} {:>9.2}ms {:>9.2}ms {:>+9.2}ms {:>10} {:>+10}\n",
                name, before, after, delta, allocs_after, allocs_delta
            ));
        } else {
            out.push_str(&format!(
                "{:<40} {:>9.2}ms {:>9.2}ms {:>+9.2}ms\n",
                name, before, after, delta
            ));
        }
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
                alloc_count: 0,
                alloc_bytes: 0,
            });
            entry.calls += f.calls;
            entry.total_ms += f.total_ms;
            entry.self_ms += f.self_ms;
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
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        calls: 1,
                        self_ns: 1_000_000,
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
                        alloc_count: 50,
                        alloc_bytes: 16384,
                        free_count: 40,
                        free_bytes: 12288,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        calls: 1,
                        self_ns: 1_100_000,
                        alloc_count: 0,
                        alloc_bytes: 0,
                        free_count: 0,
                        free_bytes: 0,
                    },
                ],
            ],
        };
        let table = format_table_with_frames(&frame_data);
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
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        calls: 1,
                        self_ns: 1_000_000,
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
                        alloc_count: 10,
                        alloc_bytes: 4096,
                        free_count: 8,
                        free_bytes: 3072,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        calls: 1,
                        self_ns: 1_000_000,
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
                        alloc_count: 50,
                        alloc_bytes: 16384,
                        free_count: 40,
                        free_bytes: 12288,
                    },
                    FrameFnEntry {
                        fn_id: 1,
                        calls: 1,
                        self_ns: 1_100_000,
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
}
