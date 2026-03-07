use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::Error;

use super::{
    FnAgg, FnEntry, FrameData, FrameFnEntry, NdjsonFrame, NdjsonHeader, NdjsonTrailer, Run,
    RunFormat,
};

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
        let tid = frame.tid;
        let entries: Vec<FrameFnEntry> = frame
            .fns
            .into_iter()
            .map(|f| FrameFnEntry {
                fn_id: f.id,
                tid,
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
        let table = super::super::format::format_table(&run, false);
        assert!(
            !table.contains("CPU"),
            "should not show CPU column for non-CPU NDJSON. Got:\n{table}"
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
    fn load_ndjson_with_tid() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tid.ndjson");
        fs::write(
            &path,
            r#"{"format_version":4,"run_id":"tid_test","timestamp_ms":1000}
{"frame":0,"tid":0,"fns":[{"id":0,"calls":3,"self_ns":5000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"tid":1,"fns":[{"id":0,"calls":2,"self_ns":3000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":2,"tid":0,"fns":[{"id":1,"calls":1,"self_ns":1000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"functions":["alpha","beta"]}
"#,
        )
        .unwrap();
        let (_run, frame_data) = load_ndjson(&path).unwrap();
        assert_eq!(frame_data.frames.len(), 3);
        assert_eq!(frame_data.frames[0][0].tid, Some(0));
        assert_eq!(frame_data.frames[2][0].tid, Some(0));
        assert_eq!(frame_data.frames[1][0].tid, Some(1));
    }

    #[test]
    fn load_ndjson_without_tid_defaults_to_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("no_tid.ndjson");
        fs::write(
            &path,
            r#"{"format_version":4,"run_id":"old","timestamp_ms":1000}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":100,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"functions":["foo"]}
"#,
        )
        .unwrap();
        let (_run, frame_data) = load_ndjson(&path).unwrap();
        assert_eq!(frame_data.frames[0][0].tid, None);
    }
}
