use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A single profiling run loaded from a JSON file written by piano-runtime.
#[derive(Debug, serde::Deserialize)]
pub struct Run {
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
pub fn format_table(run: &Run) -> String {
    let mut entries = run.functions.clone();
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

/// Find the most recent run file in a directory by parsing timestamps from filenames.
///
/// Run files are named `<timestamp>.json`. This function parses the stem as u64
/// and returns the path with the highest timestamp.
pub fn latest_run(runs_dir: &Path) -> Result<PathBuf, Error> {
    let entries: Vec<PathBuf> = std::fs::read_dir(runs_dir)
        .map_err(|source| Error::RunReadError {
            path: runs_dir.to_path_buf(),
            source,
        })?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                return None;
            }
            // Verify the stem parses as a millisecond timestamp.
            let _ts: u128 = path.file_stem()?.to_str()?.parse().ok()?;
            Some(path)
        })
        .collect();

    if entries.is_empty() {
        return Err(Error::NoRuns(runs_dir.to_path_buf()));
    }

    entries
        .into_iter()
        .max_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u128>().ok())
                .unwrap_or(0)
        })
        .ok_or_else(|| Error::NoRuns(runs_dir.to_path_buf()))
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
        let table = format_table(&run);
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
            timestamp_ms: 1000,
            functions: vec![FnEntry {
                name: "walk".into(),
                calls: 3,
                total_ms: 12.0,
                self_ms: 10.0,
            }],
        };
        let b = Run {
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
    fn latest_run_finds_most_recent_by_timestamp() {
        let dir = TempDir::new().unwrap();
        for name in [
            "1700000000000.json",
            "1700000002000.json",
            "1700000001500.json",
        ] {
            fs::write(dir.path().join(name), "{}").unwrap();
        }
        let latest = latest_run(dir.path()).unwrap();
        assert_eq!(latest.file_name().unwrap(), "1700000002000.json");
    }

    #[test]
    fn latest_run_errors_on_empty_dir() {
        let dir = TempDir::new().unwrap();
        let result = latest_run(dir.path());
        assert!(result.is_err(), "expected Err for empty dir");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no runs found"),
            "unexpected error: {err}"
        );
    }
}
