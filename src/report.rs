use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A single profiling run loaded from a JSON file written by piano-runtime.
#[derive(Debug, serde::Deserialize)]
pub struct Run {
    #[serde(default)]
    pub run_id: Option<String>,
    pub timestamp_ms: u128,
    pub functions: Vec<FnEntry>,
}

/// Timing data for one function within a profiling run.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FnEntry {
    pub name: String,
    pub calls: u64,
    pub total_ms: f64,
    pub self_ms: f64,
}

/// Read a profiling run from a JSON file on disk.
pub fn load_run(path: &Path) -> Result<Run, Error> {
    let contents = std::fs::read_to_string(path).map_err(|source| Error::RunReadError {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&contents).map_err(|e| Error::InvalidRunData {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })
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

/// Show the delta between two runs, comparing functions by name.
pub fn diff_runs(a: &Run, b: &Run) -> String {
    let a_map: HashMap<&str, &FnEntry> = a.functions.iter().map(|f| (f.name.as_str(), f)).collect();
    let b_map: HashMap<&str, &FnEntry> = b.functions.iter().map(|f| (f.name.as_str(), f)).collect();

    // Collect all function names, sorted for deterministic output.
    let mut names: Vec<&str> = a_map.keys().chain(b_map.keys()).copied().collect();
    names.sort_unstable();
    names.dedup();

    let mut out = String::new();
    out.push_str(&format!(
        "{:<40} {:>10} {:>10} {:>10}\n",
        "Function", "Before", "After", "Delta"
    ));
    out.push_str(&format!("{}\n", "-".repeat(74)));

    for name in &names {
        let before = a_map.get(name).map_or(0.0, |e| e.self_ms);
        let after = b_map.get(name).map_or(0.0, |e| e.self_ms);
        let delta = after - before;
        out.push_str(&format!(
            "{:<40} {:>9.2}ms {:>9.2}ms {:>+9.2}ms\n",
            name, before, after, delta
        ));
    }
    out
}

/// Collect all JSON run files in the given directory, sorted by filename.
fn collect_run_files(runs_dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let entries = std::fs::read_dir(runs_dir).map_err(|source| Error::RunReadError {
        path: runs_dir.to_path_buf(),
        source,
    })?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                return None;
            }
            let _ts: u128 = path.file_stem()?.to_str()?.parse().ok()?;
            Some(path)
        })
        .collect();
    files.sort();
    Ok(files)
}

/// Merge multiple runs into one, summing calls/total_ms/self_ms per function name.
fn merge_runs(runs: &[&Run]) -> Run {
    let mut merged: HashMap<String, FnEntry> = HashMap::new();
    let mut max_ts: u128 = 0;
    let mut run_id = None;

    for run in runs {
        max_ts = max_ts.max(run.timestamp_ms);
        if run_id.is_none() {
            run_id.clone_from(&run.run_id);
        }
        for f in &run.functions {
            let entry = merged.entry(f.name.clone()).or_insert(FnEntry {
                name: f.name.clone(),
                calls: 0,
                total_ms: 0.0,
                self_ms: 0.0,
            });
            entry.calls += f.calls;
            entry.total_ms += f.total_ms;
            entry.self_ms += f.self_ms;
        }
    }

    Run {
        run_id,
        timestamp_ms: max_ts,
        functions: merged.into_values().collect(),
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
        let run = load_run(path)?;
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
        runs.push(load_run(path)?);
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
            functions: vec![
                FnEntry {
                    name: "fast".into(),
                    calls: 1,
                    total_ms: 2.0,
                    self_ms: 1.0,
                },
                FnEntry {
                    name: "slow".into(),
                    calls: 1,
                    total_ms: 20.0,
                    self_ms: 15.0,
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
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: 12.0,
                self_ms: 10.0,
            }],
        };
        let b = Run {
            run_id: None,
            timestamp_ms: 2000,
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: 9.0,
                self_ms: 8.0,
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
            functions: vec![
                FnEntry {
                    name: "called".into(),
                    calls: 5,
                    total_ms: 10.0,
                    self_ms: 8.0,
                },
                FnEntry {
                    name: "uncalled".into(),
                    calls: 0,
                    total_ms: 0.0,
                    self_ms: 0.0,
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
}
