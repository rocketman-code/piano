use std::path::Path;

use crate::error::{Error, io_context};

use super::load::load_run_by_id;

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
pub fn load_tagged_run(tags_dir: &Path, runs_dir: &Path, tag: &str) -> Result<super::Run, Error> {
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

#[cfg(test)]
mod tests {
    use super::super::relative_time;
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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

        let (prev, latest) = super::super::load::load_two_latest_runs(&runs_dir).unwrap();
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

    // --- C2: Tag/diff round-trip through NDJSON aggregate format ---
    //
    // The chain save_tag -> resolve_tag -> load_run_by_id -> load_ndjson -> Run
    // must work when the run file is NDJSON with aggregate lines.
    // Existing tests only cover legacy JSON.

    #[test]
    fn tag_round_trip_ndjson_aggregate() {
        let dir = TempDir::new().unwrap();
        let tags_dir = dir.path().join("tags");
        let runs_dir = dir.path().join("runs");
        fs::create_dir_all(&tags_dir).unwrap();
        fs::create_dir_all(&runs_dir).unwrap();

        // Write an NDJSON file with aggregate lines (new format)
        let ndjson = concat!(
            "{\"type\":\"header\",\"run_id\":\"agg_7000\",\"timestamp_ms\":7000,",
            "\"bias_ns\":0,\"cpu_bias_ns\":0,\"names\":{\"0\":\"process\",\"1\":\"parse\"}}\n",
            "{\"thread\":0,\"name_id\":0,\"calls\":100,\"self_ns\":50000,",
            "\"inclusive_ns\":80000,\"cpu_self_ns\":30000,",
            "\"alloc_count\":10,\"alloc_bytes\":1024,\"free_count\":5,\"free_bytes\":512}\n",
            "{\"thread\":0,\"name_id\":1,\"calls\":200,\"self_ns\":30000,",
            "\"inclusive_ns\":30000,\"cpu_self_ns\":20000,",
            "\"alloc_count\":0,\"alloc_bytes\":0,\"free_count\":0,\"free_bytes\":0}\n",
            "{\"type\":\"trailer\",\"bias_ns\":0,\"cpu_bias_ns\":0,",
            "\"names\":{\"0\":\"process\",\"1\":\"parse\"}}\n",
        );
        fs::write(runs_dir.join("7000-1234.ndjson"), ndjson).unwrap();

        // Tag it
        save_tag(&tags_dir, "baseline", "agg_7000").unwrap();

        // Load via tag
        let run = load_tagged_run(&tags_dir, &runs_dir, "baseline").unwrap();

        assert_eq!(run.functions.len(), 2);

        let process = run.functions.iter().find(|f| f.name == "process").unwrap();
        assert_eq!(process.calls, 100);
        assert!(process.self_ms > 0.0, "self_ms should be positive");
        assert!(process.cpu_self_ms.is_some(), "should have CPU time");
        assert_eq!(process.alloc_count, 10);
        assert_eq!(process.alloc_bytes, 1024);
        assert_eq!(process.free_count, 5);
        assert_eq!(process.free_bytes, 512);

        let parse = run.functions.iter().find(|f| f.name == "parse").unwrap();
        assert_eq!(parse.calls, 200);
        assert_eq!(parse.alloc_count, 0);
    }
}
