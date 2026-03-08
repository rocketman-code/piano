use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use ignore::WalkBuilder;
use serde::Deserialize;
use toml_edit::DocumentMut;

use crate::error::{Error, io_context};
use crate::source_map::SourceMap;

// --- Cargo metadata types ---

#[derive(Debug, Deserialize)]
pub struct CargoMetadata {
    pub workspace_root: PathBuf,
    pub packages: Vec<MetadataPackage>,
}

#[derive(Debug, Deserialize)]
pub struct MetadataPackage {
    pub name: String,
    pub manifest_path: PathBuf,
    pub targets: Vec<MetadataTarget>,
}

#[derive(Debug, Deserialize)]
pub struct MetadataTarget {
    pub name: String,
    pub kind: Vec<String>,
    pub src_path: PathBuf,
}

/// Run `cargo metadata --format-version 1 --no-deps` in the given directory
/// and parse the result.
pub fn cargo_metadata(project_dir: &Path) -> Result<CargoMetadata, Error> {
    let output = Command::new("cargo")
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .arg("--no-deps")
        .current_dir(project_dir)
        .env_remove("RUSTUP_TOOLCHAIN")
        .output()
        .map_err(|e| Error::BuildFailed(format!("failed to run cargo metadata: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::BuildFailed(format!(
            "cargo metadata failed: {stderr}"
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout)
        .map_err(|e| Error::BuildFailed(format!("failed to parse cargo metadata: {e}")))
}

/// Find a binary target in the metadata for the given package.
///
/// When `bin_name` is `Some`, looks for that specific binary target.
/// When `None`, returns the first binary target found.
///
/// Returns `(package_name, src_path)` where src_path is the absolute path
/// to the binary's entry point.
pub fn find_bin_target(
    metadata: &CargoMetadata,
    package_name: Option<&str>,
    bin_name: Option<&str>,
) -> Result<(String, PathBuf), Error> {
    // Filter packages: if package_name given, match it; otherwise use all.
    let candidates: Vec<&MetadataPackage> = if let Some(pkg) = package_name {
        metadata.packages.iter().filter(|p| p.name == pkg).collect()
    } else {
        metadata.packages.iter().collect()
    };

    if candidates.is_empty() {
        let name = package_name.unwrap_or("<any>");
        return Err(Error::BuildFailed(format!(
            "no package '{name}' found in cargo metadata"
        )));
    }

    for pkg in &candidates {
        for target in &pkg.targets {
            if !target.kind.iter().any(|k| k == "bin") {
                continue;
            }
            if let Some(wanted) = bin_name {
                if target.name != wanted {
                    continue;
                }
            }
            return Ok((pkg.name.clone(), target.src_path.clone()));
        }
    }

    let bin_desc = bin_name.unwrap_or("default");
    let pkg_desc = package_name.unwrap_or("<any>");
    Err(Error::BuildFailed(format!(
        "no binary target '{bin_desc}' found in package '{pkg_desc}'"
    )))
}

/// Find the package in metadata whose manifest_path is closest to `project_dir`.
///
/// This is used when no explicit `--bin` is given to find the "current" package
/// the user is working in (e.g., when running from a workspace member directory).
pub fn find_current_package<'a>(
    metadata: &'a CargoMetadata,
    project_dir: &Path,
) -> Option<&'a MetadataPackage> {
    let project_dir = project_dir.canonicalize().ok()?;
    metadata.packages.iter().find(|pkg| {
        pkg.manifest_path
            .parent()
            .and_then(|p| p.canonicalize().ok())
            .is_some_and(|p| p == project_dir)
    })
}

/// Copy the user's project into a staging directory, respecting .gitignore
/// and skipping the `target/` directory.
pub fn prepare_staging(project_root: &Path, staging_dir: &Path) -> Result<(), Error> {
    // Wipe existing staging contents so stale files from previous runs
    // don't leak into the build. The directory itself is preserved.
    if staging_dir.exists() {
        for entry in
            std::fs::read_dir(staging_dir).map_err(io_context("read directory", staging_dir))?
        {
            let entry = entry.map_err(io_context("read directory entry", staging_dir))?;
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).map_err(io_context("remove directory", &path))?;
            } else {
                std::fs::remove_file(&path).map_err(io_context("remove file", &path))?;
            }
        }
    }

    let walker = WalkBuilder::new(project_root)
        .hidden(false)
        .follow_links(true)
        .filter_entry(|entry| {
            // Skip target/ only at the project root level (depth 1).
            entry.depth() != 1 || entry.file_name().to_string_lossy() != "target"
        })
        .build();

    for entry in walker {
        let entry = entry.map_err(|e| std::io::Error::other(e.to_string()))?;
        let source = entry.path();
        let relative = source
            .strip_prefix(project_root)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let dest = staging_dir.join(relative);

        if entry.file_type().is_some_and(|ft| ft.is_dir()) {
            std::fs::create_dir_all(&dest).map_err(io_context("create directory", &dest))?;
        } else if entry.file_type().is_some_and(|ft| ft.is_file()) {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(io_context("create directory", parent))?;
            }
            std::fs::copy(source, &dest).map_err(io_context("copy file to", &dest))?;
        }
    }

    Ok(())
}

/// How to reference piano-runtime in the staged Cargo.toml.
pub(crate) enum RuntimeSource<'a> {
    /// Published crate version (e.g. "0.1.0").
    Version(&'a str),
    /// Local path (for development before publishing).
    Path(&'a Path),
}

/// Add `piano-runtime` as a dependency in the staged project's Cargo.toml.
/// Uses `toml_edit` for structured manipulation (never string replacement).
pub fn inject_runtime_dependency(
    staging_dir: &Path,
    runtime_version: &str,
    features: &[&str],
) -> Result<(), Error> {
    inject_runtime(
        staging_dir,
        RuntimeSource::Version(runtime_version),
        features,
    )
}

/// Add `piano-runtime` as a path dependency in the staged project's Cargo.toml.
pub fn inject_runtime_path_dependency(
    staging_dir: &Path,
    runtime_path: &Path,
    features: &[&str],
) -> Result<(), Error> {
    inject_runtime(staging_dir, RuntimeSource::Path(runtime_path), features)
}

fn inject_runtime(
    staging_dir: &Path,
    source: RuntimeSource<'_>,
    features: &[&str],
) -> Result<(), Error> {
    let cargo_toml_path = staging_dir.join("Cargo.toml");
    let content =
        std::fs::read_to_string(&cargo_toml_path).map_err(io_context("read", &cargo_toml_path))?;

    let mut doc: DocumentMut = content.parse::<DocumentMut>().map_err(|e| {
        Error::BuildFailed(format!(
            "failed to parse {}: {e}",
            cargo_toml_path.display()
        ))
    })?;

    // Ensure [dependencies] table exists.
    if !doc.contains_table("dependencies") {
        doc["dependencies"] = toml_edit::Item::Table(toml_edit::Table::new());
    }

    if features.is_empty() {
        match source {
            RuntimeSource::Version(v) => {
                doc["dependencies"]["piano-runtime"] = toml_edit::value(v);
            }
            RuntimeSource::Path(p) => {
                let mut table = toml_edit::InlineTable::new();
                table.insert("path", p.to_string_lossy().as_ref().into());
                doc["dependencies"]["piano-runtime"] =
                    toml_edit::Item::Value(toml_edit::Value::InlineTable(table));
            }
        }
    } else {
        let mut table = toml_edit::InlineTable::new();
        match source {
            RuntimeSource::Version(v) => {
                table.insert("version", v.into());
            }
            RuntimeSource::Path(p) => {
                table.insert("path", p.to_string_lossy().as_ref().into());
            }
        }
        let mut feat_array = toml_edit::Array::new();
        for f in features {
            feat_array.push(*f);
        }
        table.insert("features", toml_edit::Value::Array(feat_array));
        doc["dependencies"]["piano-runtime"] =
            toml_edit::Item::Value(toml_edit::Value::InlineTable(table));
    }

    std::fs::write(&cargo_toml_path, doc.to_string())
        .map_err(io_context("write", &cargo_toml_path))?;

    Ok(())
}

/// Extract human-readable compiler errors from cargo's JSON output.
fn extract_rendered_errors(json_output: &str) -> Vec<String> {
    json_output
        .lines()
        .filter_map(|line| {
            let msg: serde_json::Value = serde_json::from_str(line).ok()?;
            if msg.get("reason")?.as_str()? != "compiler-message" {
                return None;
            }
            msg.get("message")?
                .get("rendered")?
                .as_str()
                .map(String::from)
        })
        .collect()
}

static SPAN_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r" --> ([^:]+):(\d+):(\d+)").unwrap());
static GUTTER_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^(\s*)(\d+)( \|)").unwrap());

/// Remap line numbers in a rendered rustc error using the provided source maps.
///
/// Processes line-by-line: `-->` lines set the current file context, and
/// subsequent gutter lines (`N |`) use that file's source map for remapping.
fn remap_rendered_error(rendered: &str, file_maps: &HashMap<PathBuf, SourceMap>) -> String {
    let mut lines = Vec::new();
    let mut current_map: Option<&SourceMap> = None;

    for line in rendered.lines() {
        if let Some(caps) = SPAN_RE.captures(line) {
            let file = &caps[1];
            let line_num: u32 = caps[2].parse().unwrap_or(0);
            let col = &caps[3];
            let map = file_maps.get(Path::new(file));
            current_map = map;
            if let Some(m) = map {
                let remapped = m.remap_line(line_num).unwrap_or(line_num);
                lines.push(
                    SPAN_RE
                        .replace(line, format!(" --> {file}:{remapped}:{col}"))
                        .into_owned(),
                );
            } else {
                lines.push(line.to_string());
            }
        } else if let Some(caps) = GUTTER_RE.captures(line) {
            if let Some(map) = current_map {
                let spaces = &caps[1];
                let line_num: u32 = caps[2].parse().unwrap_or(0);
                let suffix = &caps[3];
                let remapped = map.remap_line(line_num).unwrap_or(line_num);
                let width = caps[2].len();
                let rest = &line[caps[0].len()..];
                lines.push(format!("{spaces}{remapped:>width$}{suffix}{rest}"));
            } else {
                lines.push(line.to_string());
            }
        } else {
            lines.push(line.to_string());
        }
    }

    let mut result = lines.join("\n");
    // Preserve trailing newline if the original had one.
    if rendered.ends_with('\n') {
        result.push('\n');
    }
    result
}

/// Remove lines referencing piano internals from rendered error output.
fn filter_piano_internals(rendered: &str) -> String {
    let mut filtered = Vec::new();
    let mut skip_annotations = false;

    for line in rendered.lines() {
        if line.contains("piano_runtime::")
            || line.contains("__piano_guard")
            || line.contains("__piano_ctx")
            || line.contains("_PIANO_ALLOC")
        {
            skip_annotations = true;
            continue;
        }
        if skip_annotations {
            let trimmed = line.trim();
            if trimmed.starts_with('|') || trimmed.starts_with("...") {
                continue;
            }
            skip_annotations = false;
        }
        filtered.push(line);
    }
    filtered.join("\n")
}

/// Find the project root by walking up from `start_dir` looking for Cargo.toml.
///
/// Returns the canonicalized directory containing the nearest Cargo.toml.
/// Starts checking `start_dir` itself, then walks up through parents.
pub fn find_project_root(start_dir: &Path) -> Result<PathBuf, Error> {
    let start = start_dir
        .canonicalize()
        .map_err(|_| Error::NoProjectFound(start_dir.to_path_buf()))?;
    let mut dir = start.as_path();
    loop {
        if dir.join("Cargo.toml").exists() {
            return Ok(dir.to_path_buf());
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return Err(Error::NoProjectFound(start_dir.to_path_buf())),
        }
    }
}

/// Build the instrumented binary using `cargo build --release --message-format=json`.
/// Returns the path to the compiled executable.
///
/// When `package` is `Some`, passes `-p <name>` to cargo to build a specific
/// workspace member (used when staging an entire workspace).
/// When `bin` is `Some`, passes `--bin <name>` to cargo to build a specific
/// binary target.
pub fn build_instrumented(
    staging_dir: &Path,
    target_dir: &Path,
    package: Option<&str>,
    bin: Option<&str>,
    source_maps: &HashMap<PathBuf, SourceMap>,
) -> Result<PathBuf, Error> {
    // Remove RUSTUP_TOOLCHAIN so the target project's rust-toolchain.toml
    // is respected. Without this, nested cargo invocations inherit the
    // parent's toolchain, ignoring the project's pinned version.
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--message-format=json")
        .env("CARGO_TARGET_DIR", target_dir)
        .env_remove("RUSTUP_TOOLCHAIN")
        .current_dir(staging_dir);
    if let Some(pkg) = package {
        cmd.arg("-p").arg(pkg);
    }
    if let Some(bin_name) = bin {
        cmd.arg("--bin").arg(bin_name);
    }
    let output = cmd.output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let rendered = extract_rendered_errors(&stdout);
        if rendered.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::BuildFailed(stderr.into_owned()));
        }
        let mut error_text = rendered.join("");
        if !source_maps.is_empty() {
            error_text = remap_rendered_error(&error_text, source_maps);
            error_text = filter_piano_internals(&error_text);
        }
        return Err(Error::BuildFailed(error_text));
    }

    // Parse JSON lines to find the last compiler-artifact with an executable.
    // Cargo emits dependencies first; the project's own binary comes last.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut binary_path = None;
    for line in stdout.lines() {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if msg.get("reason").and_then(|r| r.as_str()) == Some("compiler-artifact")
            && let Some(exe) = msg.get("executable").and_then(|e| e.as_str())
        {
            binary_path = Some(PathBuf::from(exe));
        }
    }

    binary_path
        .ok_or_else(|| Error::BuildFailed("no executable found in cargo build output".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: create a file within a directory, creating parents as needed.
    fn create_file(base: &Path, relative: &str, content: &str) {
        let path = base.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn staging_copies_project_structure() {
        let project = TempDir::new().unwrap();
        let staging = TempDir::new().unwrap();

        create_file(project.path(), "Cargo.toml", "[package]\nname = \"demo\"");
        create_file(project.path(), "src/main.rs", "fn main() {}");
        create_file(project.path(), "src/lib.rs", "pub fn lib() {}");
        create_file(project.path(), "src/util/helper.rs", "pub fn help() {}");

        // Also create a target/ dir that should be skipped
        create_file(project.path(), "target/debug/demo", "binary-content");

        prepare_staging(project.path(), staging.path()).unwrap();

        assert!(staging.path().join("Cargo.toml").exists());
        assert!(staging.path().join("src/main.rs").exists());
        assert!(staging.path().join("src/lib.rs").exists());
        assert!(staging.path().join("src/util/helper.rs").exists());
        assert!(!staging.path().join("target").exists());

        // Verify content was copied correctly
        let content = std::fs::read_to_string(staging.path().join("Cargo.toml")).unwrap();
        assert_eq!(content, "[package]\nname = \"demo\"");
    }

    #[test]
    fn inject_dependency_adds_piano_runtime() {
        let staging = TempDir::new().unwrap();
        let toml_content = r#"[package]
name = "demo"
version = "0.1.0"

[dependencies]
serde = "1"
"#;
        create_file(staging.path(), "Cargo.toml", toml_content);

        inject_runtime_dependency(staging.path(), "0.1.0", &[]).unwrap();

        let result = std::fs::read_to_string(staging.path().join("Cargo.toml")).unwrap();
        let doc: DocumentMut = result.parse().unwrap();

        // piano-runtime was added
        assert_eq!(doc["dependencies"]["piano-runtime"].as_str(), Some("0.1.0"),);
        // serde is preserved
        assert_eq!(doc["dependencies"]["serde"].as_str(), Some("1"),);
    }

    #[test]
    fn extract_compiler_errors_from_json() {
        let json_lines = concat!(
            r#"{"reason":"compiler-message","message":{"rendered":"error[E0308]: mismatched types\n --> src/main.rs:2:5\n"}}"#,
            "\n",
            r#"{"reason":"compiler-message","message":{"rendered":"error: aborting due to previous error\n"}}"#,
            "\n",
            r#"{"reason":"build-finished","success":false}"#,
        );
        let errors = extract_rendered_errors(json_lines);
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("mismatched types"));
    }

    #[test]
    fn inject_dependency_creates_section_if_missing() {
        let staging = TempDir::new().unwrap();
        let toml_content = r#"[package]
name = "demo"
version = "0.1.0"
"#;
        create_file(staging.path(), "Cargo.toml", toml_content);

        inject_runtime_dependency(staging.path(), "0.2.0", &[]).unwrap();

        let result = std::fs::read_to_string(staging.path().join("Cargo.toml")).unwrap();
        let doc: DocumentMut = result.parse().unwrap();

        assert_eq!(doc["dependencies"]["piano-runtime"].as_str(), Some("0.2.0"),);
    }

    #[test]
    fn inject_dependency_with_features() {
        let staging = TempDir::new().unwrap();
        let toml_content = r#"[package]
name = "test"
version = "0.1.0"
edition = "2021"
"#;
        create_file(staging.path(), "Cargo.toml", toml_content);

        inject_runtime_dependency(staging.path(), "0.3.0", &["cpu-time"]).unwrap();

        let result = std::fs::read_to_string(staging.path().join("Cargo.toml")).unwrap();
        assert!(
            result.contains("cpu-time"),
            "should inject cpu-time feature: {result}"
        );
        assert!(
            result.contains("piano-runtime"),
            "should inject piano-runtime: {result}"
        );
        // Verify it parses as valid TOML with version and features
        let doc: DocumentMut = result.parse().unwrap();
        let dep = doc["dependencies"]["piano-runtime"]
            .as_inline_table()
            .unwrap();
        assert_eq!(dep.get("version").and_then(|v| v.as_str()), Some("0.3.0"));
    }

    #[test]
    fn find_bin_target_finds_default_binary() {
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/project"),
            packages: vec![MetadataPackage {
                name: "demo".to_string(),
                manifest_path: PathBuf::from("/project/Cargo.toml"),
                targets: vec![MetadataTarget {
                    name: "demo".to_string(),
                    kind: vec!["bin".to_string()],
                    src_path: PathBuf::from("/project/src/main.rs"),
                }],
            }],
        };

        let (name, path) = find_bin_target(&metadata, None, None).unwrap();
        assert_eq!(name, "demo");
        assert_eq!(path, PathBuf::from("/project/src/main.rs"));
    }

    #[test]
    fn find_bin_target_finds_named_binary() {
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/project"),
            packages: vec![MetadataPackage {
                name: "demo".to_string(),
                manifest_path: PathBuf::from("/project/Cargo.toml"),
                targets: vec![
                    MetadataTarget {
                        name: "server".to_string(),
                        kind: vec!["bin".to_string()],
                        src_path: PathBuf::from("/project/src/server.rs"),
                    },
                    MetadataTarget {
                        name: "worker".to_string(),
                        kind: vec!["bin".to_string()],
                        src_path: PathBuf::from("/project/src/worker.rs"),
                    },
                ],
            }],
        };

        let (name, path) = find_bin_target(&metadata, None, Some("worker")).unwrap();
        assert_eq!(name, "demo");
        assert_eq!(path, PathBuf::from("/project/src/worker.rs"));
    }

    #[test]
    fn find_bin_target_skips_lib_targets() {
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/project"),
            packages: vec![MetadataPackage {
                name: "demo".to_string(),
                manifest_path: PathBuf::from("/project/Cargo.toml"),
                targets: vec![
                    MetadataTarget {
                        name: "demo".to_string(),
                        kind: vec!["lib".to_string()],
                        src_path: PathBuf::from("/project/src/lib.rs"),
                    },
                    MetadataTarget {
                        name: "demo".to_string(),
                        kind: vec!["bin".to_string()],
                        src_path: PathBuf::from("/project/src/main.rs"),
                    },
                ],
            }],
        };

        let (_, path) = find_bin_target(&metadata, None, None).unwrap();
        assert_eq!(path, PathBuf::from("/project/src/main.rs"));
    }

    #[test]
    fn find_bin_target_errors_when_not_found() {
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/project"),
            packages: vec![MetadataPackage {
                name: "demo".to_string(),
                manifest_path: PathBuf::from("/project/Cargo.toml"),
                targets: vec![MetadataTarget {
                    name: "demo".to_string(),
                    kind: vec!["bin".to_string()],
                    src_path: PathBuf::from("/project/src/main.rs"),
                }],
            }],
        };

        let result = find_bin_target(&metadata, None, Some("nonexistent"));
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should mention target name: {msg}"
        );
    }

    #[test]
    fn find_bin_target_filters_by_package() {
        let metadata = CargoMetadata {
            workspace_root: PathBuf::from("/ws"),
            packages: vec![
                MetadataPackage {
                    name: "core".to_string(),
                    manifest_path: PathBuf::from("/ws/crates/core/Cargo.toml"),
                    targets: vec![MetadataTarget {
                        name: "myapp".to_string(),
                        kind: vec!["bin".to_string()],
                        src_path: PathBuf::from("/ws/crates/core/src/main.rs"),
                    }],
                },
                MetadataPackage {
                    name: "utils".to_string(),
                    manifest_path: PathBuf::from("/ws/crates/utils/Cargo.toml"),
                    targets: vec![MetadataTarget {
                        name: "utils".to_string(),
                        kind: vec!["lib".to_string()],
                        src_path: PathBuf::from("/ws/crates/utils/src/lib.rs"),
                    }],
                },
            ],
        };

        let (name, path) = find_bin_target(&metadata, Some("core"), None).unwrap();
        assert_eq!(name, "core");
        assert_eq!(path, PathBuf::from("/ws/crates/core/src/main.rs"));
    }

    #[test]
    fn find_current_package_matches_dir() {
        let tmp = TempDir::new().unwrap();
        let pkg_dir = tmp.path().join("crates/core");
        std::fs::create_dir_all(&pkg_dir).unwrap();
        create_file(&pkg_dir, "Cargo.toml", "[package]\nname = \"core\"");

        let metadata = CargoMetadata {
            workspace_root: tmp.path().to_path_buf(),
            packages: vec![MetadataPackage {
                name: "core".to_string(),
                manifest_path: pkg_dir.join("Cargo.toml"),
                targets: vec![],
            }],
        };

        let pkg = find_current_package(&metadata, &pkg_dir);
        assert!(pkg.is_some());
        assert_eq!(pkg.unwrap().name, "core");
    }

    #[test]
    #[cfg(unix)]
    fn staging_follows_symlinked_directories() {
        let project = TempDir::new().unwrap();
        let staging = TempDir::new().unwrap();

        // Create a real src directory outside the project.
        let real_src = TempDir::new().unwrap();
        create_file(real_src.path(), "main.rs", "fn main() {}");
        create_file(real_src.path(), "lib.rs", "pub fn lib() {}");

        create_file(project.path(), "Cargo.toml", "[package]\nname = \"demo\"");
        // Symlink project/src -> real_src
        std::os::unix::fs::symlink(real_src.path(), project.path().join("src")).unwrap();

        prepare_staging(project.path(), staging.path()).unwrap();

        assert!(staging.path().join("Cargo.toml").exists());
        assert!(
            staging.path().join("src/main.rs").exists(),
            "symlinked src/main.rs should be copied to staging"
        );
        assert!(
            staging.path().join("src/lib.rs").exists(),
            "symlinked src/lib.rs should be copied to staging"
        );
    }

    #[test]
    #[cfg(unix)]
    fn staging_follows_symlinked_files() {
        let project = TempDir::new().unwrap();
        let staging = TempDir::new().unwrap();

        // Create a real file outside the project.
        let real_file = TempDir::new().unwrap();
        create_file(real_file.path(), "shared.rs", "pub fn shared() {}");

        create_file(project.path(), "Cargo.toml", "[package]\nname = \"demo\"");
        create_file(project.path(), "src/main.rs", "fn main() {}");
        // Symlink project/src/shared.rs -> real_file/shared.rs
        std::os::unix::fs::symlink(
            real_file.path().join("shared.rs"),
            project.path().join("src/shared.rs"),
        )
        .unwrap();

        prepare_staging(project.path(), staging.path()).unwrap();

        assert!(
            staging.path().join("src/shared.rs").exists(),
            "symlinked file should be copied to staging"
        );
        let content = std::fs::read_to_string(staging.path().join("src/shared.rs")).unwrap();
        assert_eq!(content, "pub fn shared() {}");
    }

    #[test]
    fn staging_removes_stale_files() {
        let project = TempDir::new().unwrap();
        let staging = TempDir::new().unwrap();

        create_file(project.path(), "Cargo.toml", "[package]\nname = \"demo\"");
        create_file(project.path(), "src/main.rs", "fn main() {}");

        // Pre-populate staging with a stale file that doesn't exist in project
        create_file(staging.path(), "src/old_module.rs", "pub fn stale() {}");

        prepare_staging(project.path(), staging.path()).unwrap();

        assert!(staging.path().join("src/main.rs").exists());
        assert!(
            !staging.path().join("src/old_module.rs").exists(),
            "stale file should be removed from staging"
        );
    }

    #[test]
    fn remap_rendered_error_line_numbers() {
        let rendered = "error[E0308]: mismatched types\n --> src/main.rs:7:18\n  |\n7 |     let x: i32 = \"hello\";\n  |                  ^^^^^^^ expected `i32`, found `&str`\n";
        let mut file_maps = std::collections::HashMap::new();
        let mut map = crate::source_map::SourceMap::new();
        map.record(1, 2, 2);
        file_maps.insert(PathBuf::from("src/main.rs"), map);

        let result = remap_rendered_error(rendered, &file_maps);
        assert!(result.contains("--> src/main.rs:5:18"), "got: {result}");
        assert!(
            result.contains("5 |"),
            "gutter should be remapped: {result}"
        );
    }

    #[test]
    fn filter_piano_internals_from_errors() {
        let rendered = "error[E0308]: mismatched types\n --> src/main.rs:5:10\n  |\n3 |     let __piano_guard = piano_runtime::enter(\"foo\");\n  |         -------------- this is of type `Guard`\n4 |     let x: i32 = \"hello\";\n  |                  ^^^^^^^ expected `i32`, found `&str`\n";
        let result = filter_piano_internals(rendered);
        assert!(
            !result.contains("piano_runtime"),
            "should filter piano_runtime: {result}"
        );
        assert!(
            !result.contains("__piano_guard"),
            "should filter __piano_guard: {result}"
        );
        assert!(
            result.contains("expected `i32`"),
            "should keep user error: {result}"
        );
    }
}
