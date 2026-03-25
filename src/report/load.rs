use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::Error;

use super::{
    FnAgg, FnEntry, NdjsonAggregate, NdjsonMeasurement, NdjsonNameTable, Run, RunCompleteness,
    RunFormat,
};

const NS_PER_MS: f64 = 1_000_000.0;

/// Read a profiling run from a JSON or NDJSON file on disk.
pub fn load_run(path: &Path) -> Result<Run, Error> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext == "ndjson" {
        let (run, _completeness) = load_ndjson(path, false)?;
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

/// Load an NDJSON file, returning the aggregated Run and whether the file
/// was complete or recovered from a crash.
///
/// NDJSON format:
/// - Header: `{"type":"header","names":{"0":"fn_name",...},"bias_ns":N}`
/// - Measurement: `{"span_id":N,"parent_span_id":N,"name_id":N,...}`
/// - Trailer: `{"type":"trailer","names":{"0":"fn_name",...},"bias_ns":N}`
///
/// Three file states are valid:
/// - Complete: header + measurements + trailer (name table from trailer)
/// - Recovered: header + measurements, no trailer (crashed/killed run, use header names)
/// - Header-only: header with zero measurements (valid empty run)
///
/// A truncated last measurement line (from SIGKILL mid-write) is silently
/// skipped rather than aborting the parse.
///
/// Self-attribution is computed from the span tree: for each span, self values
/// are the span's inclusive values minus the sum of its direct children's values.
/// Aggregation groups self-attributed values by name_id.
pub fn load_ndjson(path: &Path, uncorrected: bool) -> Result<(Run, RunCompleteness), Error> {
    let parsed = parse_ndjson(path)?;
    let bias_ns = if uncorrected { 0 } else { parsed.bias_ns };
    let cpu_bias_ns = if uncorrected { 0 } else { parsed.cpu_bias_ns };

    let (fn_agg, has_cpu) = if !parsed.aggregates.is_empty() {
        // Aggregated format: self-time pre-computed by runtime
        let mut agg: HashMap<u32, FnAgg> = HashMap::new();
        let mut has_cpu = false;
        for a in &parsed.aggregates {
            let entry = agg.entry(a.name_id).or_default();
            entry.calls += a.calls;
            entry.self_ns += a.self_ns;
            entry.inclusive_ns += a.inclusive_ns;
            entry.alloc_count += a.alloc_count;
            entry.alloc_bytes += a.alloc_bytes;
            entry.free_count += a.free_count;
            entry.free_bytes += a.free_bytes;
            entry.cpu_self_ns += a.cpu_self_ns;
            if a.cpu_self_ns > 0 {
                has_cpu = true;
            }
        }
        (agg, has_cpu)
    } else {
        // Raw spans format: compute self-time from span tree
        let self_values = compute_self_attribution(&parsed.measurements);
        let has_cpu = parsed.measurements.iter().any(|m| m.cpu_end_ns > 0);
        (aggregate_self_values(&self_values), has_cpu)
    };

    let functions = build_fn_entries(&parsed.fn_names, &fn_agg, has_cpu, bias_ns, cpu_bias_ns);

    let run = Run {
        run_id: parsed.run_id,
        timestamp_ms: parsed.timestamp_ms,
        functions,
        source_format: RunFormat::Ndjson,
    };
    Ok((run, parsed.completeness))
}

/// Load an NDJSON file and split results by thread, returning one `Run` per
/// thread (sorted by thread ID ascending). Returns `None` if the file has no
/// thread data (all thread_ids are zero or identical).
pub fn load_ndjson_per_thread(path: &Path, uncorrected: bool) -> Result<Option<Vec<Run>>, Error> {
    let parsed = parse_ndjson(path)?;
    let bias_ns = if uncorrected { 0 } else { parsed.bias_ns };
    let cpu_bias_ns = if uncorrected { 0 } else { parsed.cpu_bias_ns };

    // Aggregated format: group by thread field
    if !parsed.aggregates.is_empty() {
        let mut thread_ids: Vec<u64> = parsed.aggregates.iter().map(|a| a.thread).collect();
        thread_ids.sort_unstable();
        thread_ids.dedup();
        if thread_ids.len() <= 1 {
            return Ok(None);
        }

        let has_cpu = parsed.aggregates.iter().any(|a| a.cpu_self_ns > 0);
        let mut runs: Vec<(u64, Run)> = Vec::new();
        for &tid in &thread_ids {
            let mut fn_agg: HashMap<u32, FnAgg> = HashMap::new();
            for a in parsed.aggregates.iter().filter(|a| a.thread == tid) {
                let entry = fn_agg.entry(a.name_id).or_default();
                entry.calls += a.calls;
                entry.self_ns += a.self_ns;
                entry.inclusive_ns += a.inclusive_ns;
                entry.alloc_count += a.alloc_count;
                entry.alloc_bytes += a.alloc_bytes;
                entry.free_count += a.free_count;
                entry.free_bytes += a.free_bytes;
                entry.cpu_self_ns += a.cpu_self_ns;
            }
            let functions =
                build_fn_entries(&parsed.fn_names, &fn_agg, has_cpu, bias_ns, cpu_bias_ns);
            runs.push((
                tid,
                Run {
                    run_id: parsed.run_id.clone(),
                    timestamp_ms: parsed.timestamp_ms,
                    functions,
                    source_format: RunFormat::Ndjson,
                },
            ));
        }
        runs.sort_by_key(|(tid, _)| *tid);
        return Ok(Some(runs.into_iter().map(|(_, run)| run).collect()));
    }

    // Raw spans format: group by thread_id field
    let mut thread_ids: Vec<u64> = parsed.measurements.iter().map(|m| m.thread_id).collect();
    thread_ids.sort_unstable();
    thread_ids.dedup();
    if thread_ids.len() <= 1 {
        return Ok(None);
    }

    let self_values = compute_self_attribution(&parsed.measurements);
    let has_cpu = parsed.measurements.iter().any(|m| m.cpu_end_ns > 0);

    // Group self-attributed values by thread_id.
    let mut by_thread: HashMap<u64, Vec<&SpanSelfValues>> = HashMap::new();
    for sv in &self_values {
        by_thread.entry(sv.thread_id).or_default().push(sv);
    }

    let mut runs: Vec<(u64, Run)> = by_thread
        .into_iter()
        .map(|(tid, spans)| {
            let fn_agg = aggregate_self_values(spans);
            let functions =
                build_fn_entries(&parsed.fn_names, &fn_agg, has_cpu, bias_ns, cpu_bias_ns);
            (
                tid,
                Run {
                    run_id: parsed.run_id.clone(),
                    timestamp_ms: parsed.timestamp_ms,
                    functions,
                    source_format: RunFormat::Ndjson,
                },
            )
        })
        .collect();

    runs.sort_by_key(|(tid, _)| *tid);
    Ok(Some(runs.into_iter().map(|(_, run)| run).collect()))
}

/// Self-attributed values for a single span after subtracting children.
struct SpanSelfValues {
    name_id: u32,
    thread_id: u64,
    self_wall_ns: u64,
    self_cpu_ns: u64,
    self_alloc_count: u64,
    self_alloc_bytes: u64,
    self_free_count: u64,
    self_free_bytes: u64,
}

/// Compute self-attribution for every span in the measurement list.
///
/// For each span: self = inclusive - sum(direct children's inclusive).
/// Direct children are identified by parent_span_id == span's span_id.
fn compute_self_attribution(measurements: &[NdjsonMeasurement]) -> Vec<SpanSelfValues> {
    // Index: span_id -> index into measurements.
    let span_index: HashMap<u64, usize> = measurements
        .iter()
        .enumerate()
        .map(|(i, m)| (m.span_id, i))
        .collect();

    // For each span, accumulate the sum of its direct children's inclusive values.
    // Key: parent_span_id, Value: (sum_wall, sum_cpu, sum_alloc_count, sum_alloc_bytes, sum_free_count, sum_free_bytes).
    let mut children_sums: HashMap<u64, (u64, u64, u64, u64, u64, u64)> = HashMap::new();
    for m in measurements {
        if m.parent_span_id != 0 && span_index.contains_key(&m.parent_span_id) {
            let entry = children_sums.entry(m.parent_span_id).or_default();
            entry.0 += m.end_ns.saturating_sub(m.start_ns);
            entry.1 += m.cpu_end_ns.saturating_sub(m.cpu_start_ns);
            entry.2 += m.alloc_count;
            entry.3 += m.alloc_bytes;
            entry.4 += m.free_count;
            entry.5 += m.free_bytes;
        }
    }

    measurements
        .iter()
        .map(|m| {
            let wall = m.end_ns.saturating_sub(m.start_ns);
            let cpu = m.cpu_end_ns.saturating_sub(m.cpu_start_ns);
            let (child_wall, child_cpu, child_ac, child_ab, child_fc, child_fb) =
                children_sums.get(&m.span_id).copied().unwrap_or_default();

            SpanSelfValues {
                name_id: m.name_id,
                thread_id: m.thread_id,
                self_wall_ns: wall.saturating_sub(child_wall),
                self_cpu_ns: cpu.saturating_sub(child_cpu),
                self_alloc_count: m.alloc_count.saturating_sub(child_ac),
                self_alloc_bytes: m.alloc_bytes.saturating_sub(child_ab),
                self_free_count: m.free_count.saturating_sub(child_fc),
                self_free_bytes: m.free_bytes.saturating_sub(child_fb),
            }
        })
        .collect()
}

/// Build an ordered name table from the raw string-keyed map.
///
/// Keys are numeric name_ids as strings (e.g., "0", "1", "2").
/// Returns a Vec where index i holds the name for name_id i.
fn build_name_table(raw: &HashMap<String, String>) -> Vec<String> {
    if raw.is_empty() {
        return Vec::new();
    }
    let mut pairs: Vec<(u32, String)> = raw
        .iter()
        .filter_map(|(k, v)| k.parse::<u32>().ok().map(|id| (id, v.clone())))
        .collect();
    pairs.sort_by_key(|(id, _)| *id);

    let max_id = pairs.last().map(|(id, _)| *id).unwrap_or(0);
    let mut names = vec![String::new(); (max_id + 1) as usize];
    for (id, name) in pairs {
        names[id as usize] = name;
    }
    names
}

/// Parsed NDJSON file contents. Single parse site so adding a field to the
/// NDJSON format can't silently break one of the callers.
struct ParsedNdjson {
    run_id: Option<String>,
    timestamp_ms: u128,
    fn_names: Vec<String>,
    measurements: Vec<NdjsonMeasurement>,
    aggregates: Vec<NdjsonAggregate>,
    completeness: RunCompleteness,
    bias_ns: u64,
    cpu_bias_ns: u64,
}

/// Parse an NDJSON file into its component parts: header metadata, name table,
/// measurements, and completeness status.
fn parse_ndjson(path: &Path) -> Result<ParsedNdjson, Error> {
    let contents = std::fs::read_to_string(path).map_err(|source| Error::RunReadError {
        path: path.to_path_buf(),
        source,
    })?;
    let all_lines: Vec<&str> = contents.lines().collect();

    let header_line = all_lines.first().ok_or_else(|| Error::InvalidRunData {
        path: path.to_path_buf(),
        reason: "empty NDJSON file".into(),
    })?;
    let header: NdjsonNameTable =
        serde_json::from_str(header_line).map_err(|e| Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("invalid NDJSON header: {e}"),
        })?;
    if header.kind != "header" {
        return Err(Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("expected header line, got type={:?}", header.kind),
        });
    }

    // Extract run_id and timestamp_ms from header line via raw JSON value,
    // since NdjsonNameTable doesn't carry them (they're optional metadata fields).
    let header_value: serde_json::Value =
        serde_json::from_str(header_line).map_err(|e| Error::InvalidRunData {
            path: path.to_path_buf(),
            reason: format!("invalid NDJSON header: {e}"),
        })?;
    let run_id = header_value
        .get("run_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let timestamp_ms = header_value
        .get("timestamp_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u128;
    let bias_ns = header_value
        .get("bias_ns")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let cpu_bias_ns = header_value
        .get("cpu_bias_ns")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Parse body lines: aggregates, raw measurements, and trailer.
    let body = &all_lines[1..];
    let mut measurements: Vec<NdjsonMeasurement> = Vec::new();
    let mut aggregates: Vec<NdjsonAggregate> = Vec::new();
    let mut trailer_names: Option<HashMap<String, String>> = None;
    let mut completeness = RunCompleteness::Recovered;

    for line in body {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(name_table) = serde_json::from_str::<NdjsonNameTable>(line) {
            if name_table.kind == "trailer" {
                trailer_names = Some(name_table.names);
                completeness = RunCompleteness::Complete;
                continue;
            }
        }
        // Try aggregated format first (has "calls" field, no "span_id")
        if let Ok(agg) = serde_json::from_str::<NdjsonAggregate>(line) {
            if agg.calls > 0 {
                aggregates.push(agg);
                continue;
            }
        }
        // Fall back to raw measurement format
        if let Ok(m) = serde_json::from_str::<NdjsonMeasurement>(line) {
            measurements.push(m);
        }
    }

    let raw_names = trailer_names.unwrap_or(header.names);
    let fn_names = build_name_table(&raw_names);

    Ok(ParsedNdjson {
        run_id,
        timestamp_ms,
        fn_names,
        measurements,
        aggregates,
        completeness,
        bias_ns,
        cpu_bias_ns,
    })
}

/// Aggregate self-attributed values into per-function stats.
fn aggregate_self_values<'a>(
    spans: impl IntoIterator<Item = &'a SpanSelfValues>,
) -> HashMap<u32, FnAgg> {
    let mut fn_agg: HashMap<u32, FnAgg> = HashMap::new();
    for sv in spans {
        let agg = fn_agg.entry(sv.name_id).or_default();
        agg.calls += 1;
        agg.self_ns += sv.self_wall_ns;
        agg.cpu_self_ns += sv.self_cpu_ns;
        agg.alloc_count += sv.self_alloc_count;
        agg.alloc_bytes += sv.self_alloc_bytes;
        agg.free_count += sv.self_free_count;
        agg.free_bytes += sv.self_free_bytes;
    }
    fn_agg
}

/// Build FnEntry list from name table and aggregated stats. Single construction
/// site so adding a field to FnEntry can't silently break one of the callers.
///
/// Applies aggregate bias correction: subtracts (bias_ns * calls) from each
/// function's self_ns and cpu_self_ns. This removes the systematic measurement
/// overhead (cost of TSC reads and clock_gettime calls per invocation).
/// Correction is aggregate, not per-call, to avoid clipping individual samples.
fn build_fn_entries(
    fn_names: &[String],
    fn_agg: &HashMap<u32, FnAgg>,
    has_cpu: bool,
    bias_ns: u64,
    cpu_bias_ns: u64,
) -> Vec<FnEntry> {
    fn_names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let name_id = idx as u32;
            let agg = fn_agg.get(&name_id).copied().unwrap_or_default();
            let corrected_self_ns = agg.self_ns.saturating_sub(bias_ns * agg.calls);
            let corrected_inclusive_ns = agg.inclusive_ns.saturating_sub(bias_ns * agg.calls);
            let corrected_cpu_self_ns = agg.cpu_self_ns.saturating_sub(cpu_bias_ns * agg.calls);
            FnEntry {
                name: name.clone(),
                calls: agg.calls,
                total_ms: if agg.inclusive_ns > 0 {
                    Some(corrected_inclusive_ns as f64 / NS_PER_MS)
                } else {
                    None
                },
                self_ms: corrected_self_ns as f64 / NS_PER_MS,
                cpu_self_ms: if has_cpu {
                    Some(corrected_cpu_self_ns as f64 / NS_PER_MS)
                } else {
                    None
                },
                alloc_count: agg.alloc_count,
                alloc_bytes: agg.alloc_bytes,
                free_count: agg.free_count,
                free_bytes: agg.free_bytes,
            }
        })
        .collect()
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
            // Filenames are {timestamp}-{pid}.ndjson or {timestamp}.json.
            // Extract timestamp from stem (everything before the first '-').
            let stem = path.file_stem()?.to_str()?;
            let ts_part = stem.split('-').next()?;
            let _ts: u128 = ts_part.parse().ok()?;
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
                free_count: 0,
                free_bytes: 0,
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
            entry.free_count += f.free_count;
            entry.free_bytes += f.free_bytes;
        }
    }

    Run {
        run_id,
        timestamp_ms: max_ts,
        functions: merged.into_values().collect(),
        source_format: format,
    }
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
                .and_then(|s| s.split('-').next())
                .and_then(|s| s.parse::<u128>().ok())
                .is_some_and(|ts| ts >= since_ms)
        })
        .next_back())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // --- Helper: generate NDJSON content from runtime output format ---

    /// Build an NDJSON header line.
    fn ndjson_header(run_id: &str, timestamp_ms: u64, names: &[(u32, &str)]) -> String {
        let names_json: String = names
            .iter()
            .map(|(id, name)| format!("\"{id}\":\"{name}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"type\":\"header\",\"run_id\":\"{run_id}\",\"timestamp_ms\":{timestamp_ms},\"bias_ns\":0,\"names\":{{{names_json}}}}}"
        )
    }

    /// Build an NDJSON trailer line.
    fn ndjson_trailer(names: &[(u32, &str)]) -> String {
        let names_json: String = names
            .iter()
            .map(|(id, name)| format!("\"{id}\":\"{name}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{\"type\":\"trailer\",\"bias_ns\":0,\"names\":{{{names_json}}}}}")
    }

    /// Build an NDJSON measurement line.
    fn ndjson_measurement(
        span_id: u64,
        parent_span_id: u64,
        name_id: u32,
        start_ns: u64,
        end_ns: u64,
        thread_id: u64,
        cpu_start_ns: u64,
        cpu_end_ns: u64,
        alloc_count: u64,
        alloc_bytes: u64,
    ) -> String {
        format!(
            concat!(
                "{{\"span_id\":{},\"parent_span_id\":{},\"name_id\":{},",
                "\"start_ns\":{},\"end_ns\":{},\"thread_id\":{},",
                "\"cpu_start_ns\":{},\"cpu_end_ns\":{},",
                "\"alloc_count\":{},\"alloc_bytes\":{},",
                "\"free_count\":0,\"free_bytes\":0}}"
            ),
            span_id,
            parent_span_id,
            name_id,
            start_ns,
            end_ns,
            thread_id,
            cpu_start_ns,
            cpu_end_ns,
            alloc_count,
            alloc_bytes,
        )
    }

    /// Build an NDJSON aggregate line.
    fn ndjson_aggregate(
        thread: u64,
        name_id: u32,
        calls: u64,
        self_ns: u64,
        inclusive_ns: u64,
        cpu_self_ns: u64,
        alloc_count: u64,
        alloc_bytes: u64,
        free_count: u64,
        free_bytes: u64,
    ) -> String {
        format!(
            concat!(
                "{{\"thread\":{},\"name_id\":{},\"calls\":{},\"self_ns\":{},",
                "\"inclusive_ns\":{},\"cpu_self_ns\":{},",
                "\"alloc_count\":{},\"alloc_bytes\":{},",
                "\"free_count\":{},\"free_bytes\":{}}}"
            ),
            thread,
            name_id,
            calls,
            self_ns,
            inclusive_ns,
            cpu_self_ns,
            alloc_count,
            alloc_bytes,
            free_count,
            free_bytes,
        )
    }

    fn sample_json() -> &'static str {
        r#"{
            "timestamp_ms": 1700000000000,
            "functions": [
                {"name": "walk", "calls": 3, "total_ms": 10.5, "self_ms": 7.2},
                {"name": "parse", "calls": 100, "total_ms": 45.0, "self_ms": 30.1}
            ]
        }"#
    }

    // --- JSON loading tests (unchanged, these test load_run for .json files) ---

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
    fn load_latest_run_errors_when_all_files_corrupt() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("100.json"), "garbage").unwrap();
        fs::write(dir.path().join("200.json"), "also garbage").unwrap();
        let result = load_latest_run(dir.path());
        assert!(result.is_err(), "expected Err when all files are corrupt");
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

    // --- Run infrastructure tests (load_two_latest, find_ndjson_by_run_id, etc.) ---

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
        let run1_a = r#"{"run_id":"1_500","timestamp_ms":500,"functions":[
            {"name":"parse","calls":50,"total_ms":100.0,"self_ms":100.0}
        ]}"#;
        let run1_b = r#"{"run_id":"1_500","timestamp_ms":501,"functions":[
            {"name":"resolve","calls":30,"total_ms":60.0,"self_ms":60.0}
        ]}"#;
        let run2 = r#"{"run_id":"2_1000","timestamp_ms":1000,"functions":[
            {"name":"new_fn","calls":2,"total_ms":10.0,"self_ms":8.0}
        ]}"#;
        fs::write(dir.path().join("500.json"), run1_a).unwrap();
        fs::write(dir.path().join("501.json"), run1_b).unwrap();
        fs::write(dir.path().join("1000.json"), run2).unwrap();

        let (previous, latest) = load_two_latest_runs(dir.path()).unwrap();
        assert_eq!(previous.functions.len(), 2);
        assert_eq!(latest.run_id.as_deref(), Some("2_1000"));
    }

    #[test]
    fn find_ndjson_by_run_id_finds_matching_file() {
        let dir = TempDir::new().unwrap();
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&runs_dir).unwrap();

        let names = &[(0, "work")];
        let content = format!(
            "{}\n{}\n",
            ndjson_header("test_42", 5000, names),
            ndjson_measurement(1, 0, 0, 100, 200, 1, 0, 0, 0, 0),
        );
        let ndjson_path = runs_dir.join("5000.ndjson");
        fs::write(&ndjson_path, content).unwrap();

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

        let names = &[(0, "work")];
        let content = format!(
            "{}\n{}\n",
            ndjson_header("other_id", 5000, names),
            ndjson_measurement(1, 0, 0, 100, 200, 1, 0, 0, 0, 0),
        );
        fs::write(runs_dir.join("5000.ndjson"), content).unwrap();

        let result = find_ndjson_by_run_id(&runs_dir, "nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn collect_run_files_deduplicates_json_when_ndjson_exists() {
        let dir = TempDir::new().unwrap();

        let json = r#"{"run_id":"dup_5000","timestamp_ms":5000,"functions":[
            {"name":"work","calls":10,"total_ms":5.0,"self_ms":5.0}
        ]}"#;
        let names = &[(0, "work")];
        let ndjson = format!(
            "{}\n{}\n",
            ndjson_header("dup_5000", 5000, names),
            ndjson_measurement(1, 0, 0, 100, 5000100, 1, 0, 0, 0, 0),
        );
        fs::write(dir.path().join("5000.json"), json).unwrap();
        fs::write(dir.path().join("5000.ndjson"), ndjson).unwrap();
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

        assert_eq!(
            files.len(),
            2,
            "expected 2 files after dedup, got {files:?}"
        );
        assert!(stems_and_exts.contains(&("5000".into(), "ndjson".into())));
        assert!(!stems_and_exts.contains(&("5000".into(), "json".into())));
        assert!(stems_and_exts.contains(&("6000".into(), "json".into())));
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
                ..Default::default()
            }],
        };
        let merged = merge_runs(&[&run_a, &run_b]);
        let work = merged.functions.iter().find(|f| f.name == "work").unwrap();
        assert_eq!(work.calls, 8);
        assert_eq!(work.cpu_self_ms, Some(10.0));
    }

    // --- NDJSON parsing -- header, measurement, trailer ---

    #[test]
    fn r1_parse_complete_ndjson_file() {
        // Complete file: header + measurements + trailer.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("complete.ndjson");
        let names = &[(0, "setup"), (1, "compute")];

        let content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("r1_complete", 3000, names),
            // setup: root span, 10us wall, 0 allocs
            ndjson_measurement(1, 0, 0, 1000, 11000, 1, 0, 0, 0, 0),
            // compute: root span, 7us wall, 0 allocs
            ndjson_measurement(2, 0, 1, 20000, 27000, 1, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, completeness) = load_ndjson(&path, false).unwrap();
        assert_eq!(completeness, RunCompleteness::Complete);
        assert_eq!(run.run_id.as_deref(), Some("r1_complete"));
        assert_eq!(run.timestamp_ms, 3000);
        assert_eq!(run.functions.len(), 2);
        assert_eq!(run.functions[0].name, "setup");
        assert_eq!(run.functions[0].calls, 1);
        assert_eq!(run.functions[1].name, "compute");
        assert_eq!(run.functions[1].calls, 1);
        assert_eq!(run.source_format, RunFormat::Ndjson);
    }

    // --- File state and crash recovery (incomplete files use header name table) ---

    #[test]
    fn r2_incomplete_file_uses_header_names() {
        // Incomplete file: header + measurements, NO trailer (crashed/killed run).
        // Uses header name table; completeness is Recovered.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("incomplete.ndjson");
        let names = &[(0, "alpha"), (1, "beta")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("r2_incomplete", 4000, names),
            ndjson_measurement(1, 0, 0, 100, 600, 1, 0, 0, 1, 64),
            ndjson_measurement(2, 0, 1, 700, 1000, 1, 0, 0, 0, 0),
        );
        fs::write(&path, content).unwrap();

        let (run, completeness) = load_ndjson(&path, false).unwrap();
        assert_eq!(completeness, RunCompleteness::Recovered);
        assert_eq!(run.functions.len(), 2);
        assert_eq!(run.functions[0].name, "alpha");
        assert_eq!(run.functions[1].name, "beta");
        assert_eq!(run.functions[0].calls, 1);
        assert_eq!(run.functions[1].calls, 1);
    }

    #[test]
    fn r2_header_only_produces_empty_run() {
        // Header-only file: valid header, zero measurements, no trailer.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("header_only.ndjson");

        let content = format!("{}\n", ndjson_header("r2_empty", 6000, &[]));
        fs::write(&path, content).unwrap();

        let (run, completeness) = load_ndjson(&path, false).unwrap();
        assert_eq!(completeness, RunCompleteness::Recovered);
        assert_eq!(run.run_id.as_deref(), Some("r2_empty"));
        assert_eq!(run.timestamp_ms, 6000);
        assert!(
            run.functions.is_empty(),
            "header-only should have no functions"
        );
    }

    #[test]
    fn r2_truncated_last_measurement_skipped() {
        // SIGKILL mid-write: last measurement line is truncated JSON.
        // Parser should skip the bad line and return Recovered.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("truncated.ndjson");
        let names = &[(0, "alpha"), (1, "beta")];

        let content = format!(
            "{}\n{}\n{}\n{{\"span_id\":3,\"parent_span_id\":0,\"name_id\":0,\"start_ns\":10",
            ndjson_header("r2_truncated", 5000, names),
            ndjson_measurement(1, 0, 0, 100, 600, 1, 0, 0, 0, 0),
            ndjson_measurement(2, 0, 1, 700, 1000, 1, 0, 0, 0, 0),
        );
        fs::write(&path, content).unwrap();

        let (run, completeness) = load_ndjson(&path, false).unwrap();
        assert_eq!(completeness, RunCompleteness::Recovered);
        // Only the two valid measurements should be parsed.
        assert_eq!(run.functions.len(), 2);
        assert_eq!(run.functions[0].name, "alpha");
        assert_eq!(run.functions[1].name, "beta");
        assert_eq!(run.functions[0].calls, 1);
        assert_eq!(run.functions[1].calls, 1);
    }

    #[test]
    fn r2_trailer_overrides_header_names() {
        // When trailer is present, its names override header names.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("override.ndjson");
        let header_names = &[(0, "header_name_0"), (1, "header_name_1")];
        let trailer_names = &[(0, "trailer_name_0"), (1, "trailer_name_1")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("override", 1000, header_names),
            ndjson_measurement(1, 0, 0, 100, 200, 1, 0, 0, 0, 0),
            ndjson_trailer(trailer_names),
        );
        fs::write(&path, content).unwrap();

        let (run, completeness) = load_ndjson(&path, false).unwrap();
        assert_eq!(completeness, RunCompleteness::Complete);
        assert_eq!(run.functions[0].name, "trailer_name_0");
        assert_eq!(run.functions[1].name, "trailer_name_1");
    }

    // --- Self-attribution from span tree (inclusive - children = self) ---

    #[test]
    fn r4_self_attribution_parent_minus_children() {
        // parent span (10us wall, 8us cpu, 100 allocs, 1024 bytes)
        //   child1 span (3us wall, 2us cpu, 30 allocs, 256 bytes)
        //   child2 span (4us wall, 3us cpu, 20 allocs, 128 bytes)
        //
        // parent self: 10-3-4=3us wall, 8-2-3=3us cpu, 100-30-20=50 allocs, 1024-256-128=640 bytes
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("self_attr.ndjson");
        let names = &[(0, "parent_fn"), (1, "child1_fn"), (2, "child2_fn")];

        let content = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            ndjson_header("self_attr", 1000, names),
            // parent: span_id=1, parent=0 (root), 10us wall, 8us cpu
            ndjson_measurement(1, 0, 0, 1000, 11000, 1, 500, 8500, 100, 1024),
            // child1: span_id=2, parent=1, 3us wall, 2us cpu
            ndjson_measurement(2, 1, 1, 2000, 5000, 1, 1000, 3000, 30, 256),
            // child2: span_id=3, parent=1, 4us wall, 3us cpu
            ndjson_measurement(3, 1, 2, 6000, 10000, 1, 3500, 6500, 20, 128),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();

        let parent = run
            .functions
            .iter()
            .find(|f| f.name == "parent_fn")
            .unwrap();
        let child1 = run
            .functions
            .iter()
            .find(|f| f.name == "child1_fn")
            .unwrap();
        let child2 = run
            .functions
            .iter()
            .find(|f| f.name == "child2_fn")
            .unwrap();

        // parent self_wall = 10000 - 3000 - 4000 = 3000 ns = 0.003 ms
        assert!(
            (parent.self_ms - 0.003).abs() < 0.0001,
            "parent self_ms should be ~0.003, got {}",
            parent.self_ms
        );
        // parent self_cpu = 8000 - 2000 - 3000 = 3000 ns = 0.003 ms
        assert!(
            (parent.cpu_self_ms.unwrap() - 0.003).abs() < 0.0001,
            "parent cpu_self_ms should be ~0.003, got {}",
            parent.cpu_self_ms.unwrap()
        );
        // parent self allocs = 100 - 30 - 20 = 50
        assert_eq!(parent.alloc_count, 50);
        // parent self alloc_bytes = 1024 - 256 - 128 = 640
        assert_eq!(parent.alloc_bytes, 640);

        // child1: leaf span, self = inclusive
        let child1_wall_ns: f64 = 3000.0; // 5000 - 2000
        assert!(
            (child1.self_ms - child1_wall_ns / NS_PER_MS).abs() < 0.0001,
            "child1 self_ms should be ~0.003, got {}",
            child1.self_ms
        );
        assert_eq!(child1.alloc_count, 30);

        // child2: leaf span, self = inclusive
        let child2_wall_ns: f64 = 4000.0; // 10000 - 6000
        assert!(
            (child2.self_ms - child2_wall_ns / NS_PER_MS).abs() < 0.0001,
            "child2 self_ms should be ~0.004, got {}",
            child2.self_ms
        );
        assert_eq!(child2.alloc_count, 20);
    }

    #[test]
    fn r4_leaf_span_self_equals_inclusive() {
        // A leaf span (no children) should have self = inclusive.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("leaf.ndjson");
        let names = &[(0, "leaf_fn")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("leaf", 1000, names),
            ndjson_measurement(1, 0, 0, 1000, 6000, 1, 500, 4500, 42, 2048),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        let leaf = run.functions.iter().find(|f| f.name == "leaf_fn").unwrap();

        // wall = 6000 - 1000 = 5000 ns = 0.005 ms
        assert!(
            (leaf.self_ms - 0.005).abs() < 0.0001,
            "leaf self_ms should be 0.005, got {}",
            leaf.self_ms
        );
        // cpu = 4500 - 500 = 4000 ns = 0.004 ms
        assert!(
            (leaf.cpu_self_ms.unwrap() - 0.004).abs() < 0.0001,
            "leaf cpu_self_ms should be 0.004, got {}",
            leaf.cpu_self_ms.unwrap()
        );
        assert_eq!(leaf.alloc_count, 42);
        assert_eq!(leaf.alloc_bytes, 2048);
    }

    // --- Aggregation by name_id (multiple spans for same function are summed) ---

    #[test]
    fn r5_aggregation_multiple_spans_same_function() {
        // Two invocations of the same function (name_id=0) should sum.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aggregate.ndjson");
        let names = &[(0, "repeated_fn")];

        let content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("agg", 1000, names),
            // First call: 5us wall, 10 allocs
            ndjson_measurement(1, 0, 0, 1000, 6000, 1, 0, 0, 10, 512),
            // Second call: 3us wall, 5 allocs
            ndjson_measurement(2, 0, 0, 10000, 13000, 1, 0, 0, 5, 256),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        let repeated = run
            .functions
            .iter()
            .find(|f| f.name == "repeated_fn")
            .unwrap();

        assert_eq!(repeated.calls, 2);
        // self_wall = 5000 + 3000 = 8000 ns = 0.008 ms
        assert!(
            (repeated.self_ms - 0.008).abs() < 0.0001,
            "aggregated self_ms should be 0.008, got {}",
            repeated.self_ms
        );
        assert_eq!(repeated.alloc_count, 15); // 10 + 5
        assert_eq!(repeated.alloc_bytes, 768); // 512 + 256
    }

    #[test]
    fn r5_zero_call_function_in_name_table() {
        // A function registered in the name table but never invoked.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("zero_call.ndjson");
        let names = &[(0, "active"), (1, "unused")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("zero_call", 1000, names),
            ndjson_measurement(1, 0, 0, 100, 200, 1, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        assert_eq!(run.functions.len(), 2);
        let unused = run.functions.iter().find(|f| f.name == "unused").unwrap();
        assert_eq!(unused.calls, 0);
        assert_eq!(unused.alloc_count, 0);
        assert!((unused.self_ms).abs() < f64::EPSILON);
    }

    #[test]
    fn r5_total_ms_is_none() {
        // NDJSON format has no total_ms concept -- all entries should have None.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("total_ms.ndjson");
        let names = &[(0, "compute")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("total_ms", 2000, names),
            ndjson_measurement(1, 0, 0, 1000, 11000000, 1, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        for f in &run.functions {
            assert!(
                f.total_ms.is_none(),
                "{}: total_ms should be None for NDJSON format, got {:?}",
                f.name,
                f.total_ms
            );
        }
    }

    // --- Thread grouping (measurements carry thread_id for per-thread breakdown) ---

    #[test]
    fn r6_aggregation_merges_across_threads() {
        // Without --threads, all threads are merged in the aggregated Run.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("merged_threads.ndjson");
        let names = &[(0, "work")];

        let content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("merged", 1000, names),
            ndjson_measurement(1, 0, 0, 100, 600, 1, 0, 0, 5, 100),
            ndjson_measurement(2, 0, 0, 200, 900, 2, 0, 0, 3, 50),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        let work = run.functions.iter().find(|f| f.name == "work").unwrap();
        assert_eq!(work.calls, 2); // 1 from each thread
        // self_wall = 500 + 700 = 1200 ns = 0.0012 ms
        assert!(
            (work.self_ms - 0.0012).abs() < 0.0001,
            "merged self_ms should be ~0.0012, got {}",
            work.self_ms
        );
        assert_eq!(work.alloc_count, 8); // 5 + 3
        assert_eq!(work.alloc_bytes, 150); // 100 + 50
    }

    // --- CPU time detection ---

    #[test]
    fn cpu_time_detected_from_measurements() {
        // CPU time is detected by non-zero cpu_end_ns in any measurement.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cpu.ndjson");
        let names = &[(0, "compute")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("cpu", 1000, names),
            ndjson_measurement(1, 0, 0, 1000, 6000, 1, 500, 4500, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        let compute = run.functions.iter().find(|f| f.name == "compute").unwrap();
        assert!(compute.cpu_self_ms.is_some(), "should have cpu_self_ms");
        // cpu = 4500 - 500 = 4000 ns = 0.004 ms
        assert!(
            (compute.cpu_self_ms.unwrap() - 0.004).abs() < 0.0001,
            "expected ~0.004ms, got {}",
            compute.cpu_self_ms.unwrap()
        );
    }

    #[test]
    fn no_cpu_time_when_all_zero() {
        // When all measurements have cpu_start_ns=0 and cpu_end_ns=0,
        // cpu_self_ms should be None.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("no_cpu.ndjson");
        let names = &[(0, "work")];

        let content = format!(
            "{}\n{}\n{}\n",
            ndjson_header("no_cpu", 1000, names),
            ndjson_measurement(1, 0, 0, 100, 600, 1, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run, _) = load_ndjson(&path, false).unwrap();
        let work = run.functions.iter().find(|f| f.name == "work").unwrap();
        assert!(
            work.cpu_self_ms.is_none(),
            "should not have cpu_self_ms when no CPU time"
        );
    }

    // --- Determinism ---

    #[test]
    fn r5_deterministic_same_input_same_output() {
        // Same NDJSON file always produces same aggregated result.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("determinism.ndjson");
        let names = &[(0, "alpha"), (1, "beta")];

        let content = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            ndjson_header("det", 1000, names),
            ndjson_measurement(1, 0, 0, 100, 600, 1, 0, 0, 10, 100),
            ndjson_measurement(2, 1, 1, 200, 400, 1, 0, 0, 3, 30),
            ndjson_measurement(3, 0, 0, 1000, 2000, 1, 0, 0, 5, 50),
            ndjson_trailer(names),
        );
        fs::write(&path, content).unwrap();

        let (run1, _) = load_ndjson(&path, false).unwrap();
        let (run2, _) = load_ndjson(&path, false).unwrap();

        for (f1, f2) in run1.functions.iter().zip(run2.functions.iter()) {
            assert_eq!(f1.name, f2.name);
            assert_eq!(f1.calls, f2.calls);
            assert!((f1.self_ms - f2.self_ms).abs() < f64::EPSILON);
            assert_eq!(f1.alloc_count, f2.alloc_count);
            assert_eq!(f1.alloc_bytes, f2.alloc_bytes);
        }
    }

    // --- Integration with load_latest_run ---

    #[test]
    fn load_latest_run_with_ndjson() {
        let dir = TempDir::new().unwrap();
        let names = &[(0, "update"), (1, "physics")];

        // parent "update" calls child "physics"
        let content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("test_1", 1000, names),
            // update: 10us wall total, contains physics
            ndjson_measurement(1, 0, 0, 1000, 11000, 1, 0, 0, 22, 9096),
            // physics: child of update, 4us wall
            ndjson_measurement(2, 1, 1, 2000, 6000, 1, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        fs::write(dir.path().join("1000.ndjson"), content).unwrap();

        let run = load_latest_run(dir.path()).unwrap();
        assert_eq!(run.functions.len(), 2);

        let update = run.functions.iter().find(|f| f.name == "update").unwrap();
        assert_eq!(update.calls, 1);
        // update self_wall = 10000 - 4000 = 6000 ns = 0.006 ms
        assert!(
            (update.self_ms - 0.006).abs() < 0.0001,
            "expected ~0.006ms, got {}",
            update.self_ms
        );
        assert_eq!(update.alloc_count, 22);

        let physics = run.functions.iter().find(|f| f.name == "physics").unwrap();
        assert_eq!(physics.calls, 1);
        // physics self_wall = 4000 ns (leaf) = 0.004 ms
        assert!(
            (physics.self_ms - 0.004).abs() < 0.0001,
            "expected ~0.004ms, got {}",
            physics.self_ms
        );
    }

    // --- C1: Aggregate-to-Run equivalence (load.rs:49-67 vs load.rs:68-74) ---
    //
    // The aggregate path (new format) and raw span path (old format) must produce
    // identical FnEntry fields for equivalent profiling data.

    #[test]
    fn c1_flat_spans_match_aggregates() {
        // Three root spans (no parent-child), two functions.
        // self_time == inclusive_time since there are no children.
        //
        // work (name_id 0): 2 calls, 5000ns each, 3000ns CPU each, 1 alloc of 64 bytes each
        // helper (name_id 1): 1 call, 2000ns, 1000ns CPU, 0 allocs
        let dir = TempDir::new().unwrap();
        let names = &[(0, "work"), (1, "helper")];

        // Raw spans version
        let span_content = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            ndjson_header("equiv_1", 5000, names),
            ndjson_measurement(1, 0, 0, 1000, 6000, 1, 100, 3100, 1, 64),
            ndjson_measurement(2, 0, 1, 7000, 9000, 1, 3200, 4200, 0, 0),
            ndjson_measurement(3, 0, 0, 10000, 15000, 1, 4300, 7300, 1, 64),
            ndjson_trailer(names),
        );
        let span_path = dir.path().join("spans.ndjson");
        fs::write(&span_path, span_content).unwrap();

        // Aggregate version (pre-computed self-time matching the span tree output)
        let agg_content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("equiv_1", 5000, names),
            ndjson_aggregate(0, 0, 2, 10000, 10000, 6000, 2, 128, 0, 0),
            ndjson_aggregate(0, 1, 1, 2000, 2000, 1000, 0, 0, 0, 0),
            ndjson_trailer(names),
        );
        let agg_path = dir.path().join("aggs.ndjson");
        fs::write(&agg_path, agg_content).unwrap();

        let (span_run, _) = load_ndjson(&span_path, false).unwrap();
        let (agg_run, _) = load_ndjson(&agg_path, false).unwrap();

        assert_eq!(span_run.functions.len(), agg_run.functions.len());
        for (s, a) in span_run.functions.iter().zip(agg_run.functions.iter()) {
            assert_eq!(s.name, a.name, "name mismatch");
            assert_eq!(s.calls, a.calls, "calls mismatch for {}", s.name);
            assert!(
                (s.self_ms - a.self_ms).abs() < 1e-9,
                "self_ms mismatch for {}: span={} agg={}",
                s.name,
                s.self_ms,
                a.self_ms,
            );
            assert_eq!(
                s.cpu_self_ms.is_some(),
                a.cpu_self_ms.is_some(),
                "cpu_self_ms presence mismatch for {}",
                s.name,
            );
            if let (Some(sc), Some(ac)) = (s.cpu_self_ms, a.cpu_self_ms) {
                assert!(
                    (sc - ac).abs() < 1e-9,
                    "cpu_self_ms mismatch for {}: span={} agg={}",
                    s.name,
                    sc,
                    ac,
                );
            }
            assert_eq!(
                s.alloc_count, a.alloc_count,
                "alloc_count mismatch for {}",
                s.name
            );
            assert_eq!(
                s.alloc_bytes, a.alloc_bytes,
                "alloc_bytes mismatch for {}",
                s.name
            );
            assert_eq!(
                s.free_count, a.free_count,
                "free_count mismatch for {}",
                s.name
            );
            assert_eq!(
                s.free_bytes, a.free_bytes,
                "free_bytes mismatch for {}",
                s.name
            );
        }
    }

    #[test]
    fn c1_parent_child_spans_match_aggregates() {
        // Parent-child relationship: self-time != inclusive-time.
        //
        // outer (name_id 0): span 0-10000ns, contains inner as child
        //   inner (name_id 1): span 2000-7000ns (child of outer)
        //
        // Self-attribution from span tree:
        //   outer: inclusive=10000, children_wall=5000, self=5000
        //   inner: inclusive=5000, no children, self=5000
        let dir = TempDir::new().unwrap();
        let names = &[(0, "outer"), (1, "inner")];

        // Raw spans version
        let span_content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("equiv_2", 6000, names),
            ndjson_measurement(1, 0, 0, 0, 10000, 1, 0, 8000, 3, 192),
            ndjson_measurement(2, 1, 1, 2000, 7000, 1, 0, 4000, 1, 64),
            ndjson_trailer(names),
        );
        let span_path = dir.path().join("parent_child_spans.ndjson");
        fs::write(&span_path, span_content).unwrap();

        // Aggregate version with pre-computed self-time
        // outer: self_ns = 10000 - 5000 = 5000, inclusive = 10000
        //        cpu_self = 8000 - 4000 = 4000
        //        alloc: 3 - 1 = 2 count, 192 - 64 = 128 bytes
        // inner: self_ns = 5000, inclusive = 5000, cpu_self = 4000
        //        alloc: 1 count, 64 bytes
        let agg_content = format!(
            "{}\n{}\n{}\n{}\n",
            ndjson_header("equiv_2", 6000, names),
            ndjson_aggregate(0, 0, 1, 5000, 10000, 4000, 2, 128, 0, 0),
            ndjson_aggregate(0, 1, 1, 5000, 5000, 4000, 1, 64, 0, 0),
            ndjson_trailer(names),
        );
        let agg_path = dir.path().join("parent_child_aggs.ndjson");
        fs::write(&agg_path, agg_content).unwrap();

        let (span_run, _) = load_ndjson(&span_path, false).unwrap();
        let (agg_run, _) = load_ndjson(&agg_path, false).unwrap();

        for (s, a) in span_run.functions.iter().zip(agg_run.functions.iter()) {
            assert_eq!(s.name, a.name, "name mismatch");
            assert_eq!(s.calls, a.calls, "calls mismatch for {}", s.name);
            assert!(
                (s.self_ms - a.self_ms).abs() < 1e-9,
                "self_ms mismatch for {}: span={} agg={}",
                s.name,
                s.self_ms,
                a.self_ms,
            );
            assert_eq!(
                s.alloc_count, a.alloc_count,
                "alloc_count mismatch for {}",
                s.name
            );
            assert_eq!(
                s.alloc_bytes, a.alloc_bytes,
                "alloc_bytes mismatch for {}",
                s.name
            );
        }
    }
}
