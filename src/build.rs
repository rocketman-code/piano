use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use ignore::WalkBuilder;
use serde::Deserialize;
use toml_edit::DocumentMut;

use crate::error::{Error, io_context};

/// Which cargo target to build.
pub enum CargoTarget<'a> {
    Bin(&'a str),
    Example(&'a str),
}

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

/// Find a target of the given kind in the metadata for the given package.
///
/// `kind` is the cargo target kind: "bin" or "example".
/// When `name` is `Some`, looks for that specific target.
/// When `None`, returns the first target of that kind found.
///
/// Returns `(package_name, src_path)` where src_path is the absolute path
/// to the target's entry point.
pub fn find_target(
    metadata: &CargoMetadata,
    package_name: Option<&str>,
    name: Option<&str>,
    kind: &str,
) -> Result<(String, PathBuf), Error> {
    let candidates: Vec<&MetadataPackage> = if let Some(pkg) = package_name {
        metadata.packages.iter().filter(|p| p.name == pkg).collect()
    } else {
        metadata.packages.iter().collect()
    };

    if candidates.is_empty() {
        let pkg = package_name.unwrap_or("<any>");
        return Err(Error::BuildFailed(format!(
            "no package '{pkg}' found in cargo metadata"
        )));
    }

    for pkg in &candidates {
        for target in &pkg.targets {
            if !target.kind.iter().any(|k| k == kind) {
                continue;
            }
            if let Some(wanted) = name {
                if target.name != wanted {
                    continue;
                }
            }
            return Ok((pkg.name.clone(), target.src_path.clone()));
        }
    }

    let target_desc = name.unwrap_or("default");
    let pkg_desc = package_name.unwrap_or("<any>");
    Err(Error::BuildFailed(format!(
        "no {kind} target '{target_desc}' found in package '{pkg_desc}'"
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

/// Extract rendered errors from cargo JSON, filtering piano-internal diagnostics
/// and cleaning instrumented source lines back to their original form.
fn extract_user_errors(
    json_output: &str,
    modified_files: &std::collections::HashSet<PathBuf>,
    project_dir: &Path,
) -> Vec<String> {
    json_output
        .lines()
        .filter_map(|line| {
            let msg: serde_json::Value = serde_json::from_str(line).ok()?;
            if msg.get("reason")?.as_str()? != "compiler-message" {
                return None;
            }
            let message_obj = msg.get("message")?;
            let message_text = message_obj.get("message")?.as_str()?;
            if is_piano_diagnostic(message_text) {
                return None;
            }
            let rendered = message_obj.get("rendered")?.as_str()?;

            // Collect all spans from main diagnostic and children
            let mut all_spans: Vec<serde_json::Value> = message_obj
                .get("spans")
                .and_then(|s| s.as_array())
                .cloned()
                .unwrap_or_default();

            if let Some(children) = message_obj.get("children").and_then(|c| c.as_array()) {
                for child in children {
                    if let Some(child_spans) = child.get("spans").and_then(|s| s.as_array()) {
                        all_spans.extend(child_spans.iter().cloned());
                    }
                }
            }

            let cleaned = clean_rendered(rendered, &all_spans, modified_files, |file, line_num| {
                let full_path = project_dir.join(file);
                let content = std::fs::read_to_string(&full_path).ok()?;
                content.lines().nth(line_num - 1).map(String::from)
            });

            Some(cleaned)
        })
        .collect()
}

/// Replace instrumented source lines in rendered diagnostic output with
/// original (uninstrumented) source lines.
///
/// For each span referencing a modified file, finds the corresponding
/// source display line in `rendered` (format: `NN | <text>`) and replaces
/// the text portion with the original source line read via `read_line`.
///
/// Zero-shift guarantees line numbers are correct. The original file
/// guarantees no injection artifacts. If a pattern doesn't match,
/// `rendered` is returned unchanged (fail open, not destructive).
fn clean_rendered(
    rendered: &str,
    spans: &[serde_json::Value],
    modified_files: &std::collections::HashSet<PathBuf>,
    read_line: impl Fn(&Path, usize) -> Option<String>,
) -> String {
    let mut replacements: Vec<(usize, String, String)> = Vec::new();

    for span in spans {
        let Some(file_name) = span.get("file_name").and_then(|f| f.as_str()) else {
            continue;
        };
        let file_path = PathBuf::from(file_name);
        if !modified_files.contains(&file_path) {
            continue;
        }

        let line_start = span.get("line_start").and_then(|l| l.as_u64()).unwrap_or(0) as usize;

        let Some(text_arr) = span.get("text").and_then(|t| t.as_array()) else {
            continue;
        };

        let suggested = span.get("suggested_replacement").and_then(|s| s.as_str());

        for (i, text_entry) in text_arr.iter().enumerate() {
            let Some(instrumented) = text_entry.get("text").and_then(|t| t.as_str()) else {
                continue;
            };
            let line_num = line_start + i;
            if let Some(original) = read_line(&file_path, line_num) {
                replacements.push((line_num, instrumented.to_string(), original.clone()));

                // Help suggestions: rustc renders the post-fix source line in
                // `rendered`. The span's text is the pre-fix line; we must also
                // replace the post-fix version. Construct it by applying the
                // suggested_replacement at the highlight range.
                if let Some(replacement_text) = suggested {
                    let hl_start = text_entry
                        .get("highlight_start")
                        .and_then(|h| h.as_u64())
                        .unwrap_or(0) as usize;
                    let hl_end = text_entry
                        .get("highlight_end")
                        .and_then(|h| h.as_u64())
                        .unwrap_or(0) as usize;
                    // highlight positions are 1-indexed columns
                    if hl_start > 0 && hl_end > 0 && hl_end <= instrumented.len() + 1 {
                        let post_fix = format!(
                            "{}{}{}",
                            &instrumented[..hl_start - 1],
                            replacement_text,
                            &instrumented[hl_end - 1..],
                        );
                        replacements.push((line_num, post_fix, original));
                    }
                }
            }
        }
    }

    if replacements.is_empty() {
        return rendered.to_string();
    }

    let mut result = rendered.to_string();
    for (_line_num, instrumented, original) in &replacements {
        // In rendered, source lines appear as "NN | <text>". We can't rely on
        // exact NN formatting (variable padding), so we match on the instrumented
        // text itself -- which is the exact text from the span.
        // This is safe because the text comes from structured JSON (not guessing).
        result = result.replace(instrumented.as_str(), original.as_str());
    }

    result
}

fn is_piano_diagnostic(message: &str) -> bool {
    message.contains("__piano_")
        || message.contains("__PIANO_")
        || message.contains("`PIANO_NAMES`")
        || message.contains("`piano_runtime::")
}

/// Detect Send-bound errors caused by piano's async wrapping and return user guidance.
///
/// Returns `Some(message)` when the error contains a Send-bound pattern AND mentions
/// piano internals (`PianoFuture` or `piano_runtime`), indicating the wrapping caused it.
/// Returns `None` for Send errors in user code or non-Send errors.
fn detect_send_bound_guidance(error_text: &str) -> Option<String> {
    let has_send_error = error_text.contains("is not Send")
        || error_text.contains("cannot be sent between threads safely")
        || error_text.contains("doesn't implement Send")
        || error_text.contains("which is required by") && error_text.contains("Send");

    if !has_send_error {
        return None;
    }

    let piano_involved = error_text.contains("PianoFuture") || error_text.contains("piano_runtime");
    if !piano_involved {
        return None;
    }

    // Try to extract the function name from the error context.
    // Rustc errors include both diagnostic text (e.g. `function `foo``) and
    // source code lines (e.g. `fn foo() ->`). We match both patterns.
    static FN_NAME_RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?:fn|function) `?(\w+)`?(?:\(|`)").unwrap());

    let name = FN_NAME_RE
        .captures(error_text)
        .map(|caps| caps[1].to_string());

    let fn_desc = match &name {
        Some(n) => format!("function '{n}'"),
        None => "a function".to_string(),
    };

    Some(format!(
        "piano: {fn_desc} cannot be wrapped for async profiling \
         (non-Send type captured). Use --skip <name> to exclude it."
    ))
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

/// Result of pre-building piano-runtime as an rlib.
pub struct PrebuiltRuntime {
    /// Path to the compiled libpiano_runtime.rlib
    pub rlib_path: PathBuf,
    /// Directory containing the rlib and any transitive deps (for -L dependency=)
    pub deps_dir: PathBuf,
}

/// Pre-build piano-runtime as an rlib using the user's toolchain.
///
/// Creates a minimal crate that depends on `piano-runtime` from crates.io,
/// builds it with `cargo build --release`, and locates the output rlib.
pub fn prebuild_runtime(
    project_dir: &Path,
    target_dir: &Path,
    features: &[&str],
) -> Result<PrebuiltRuntime, Error> {
    let src_dir = target_dir.join("runtime-src");
    std::fs::create_dir_all(src_dir.join("src"))
        .map_err(io_context("create directory", &src_dir))?;

    let version = env!("PIANO_RUNTIME_VERSION");

    let dep_spec = if features.is_empty() {
        format!("\"{version}\"")
    } else {
        let feat_list = features
            .iter()
            .map(|f| format!("\"{f}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{{ version = \"{version}\", features = [{feat_list}] }}")
    };

    let cargo_toml = format!(
        "[package]\n\
         name = \"piano-runtime-fetcher\"\n\
         version = \"0.0.0\"\n\
         edition = \"2021\"\n\
         \n\
         [dependencies]\n\
         piano-runtime = {dep_spec}\n\
         \n\
         [workspace]\n"
    );

    std::fs::write(src_dir.join("Cargo.toml"), cargo_toml)
        .map_err(io_context("write Cargo.toml", &src_dir))?;
    std::fs::write(src_dir.join("src/lib.rs"), "")
        .map_err(io_context("write src/lib.rs", &src_dir))?;

    // Features are specified in the Cargo.toml dep, so pass empty to the builder.
    build_runtime_rlib(&src_dir, project_dir, target_dir, &[])
}

/// Pre-build piano-runtime from a local source path (for --runtime-path).
///
/// Copies the runtime source to a standalone directory (with `[workspace]`
/// injected) so that Cargo doesn't resolve the parent workspace. This prevents
/// failures when the user's project pins an older toolchain that can't parse
/// the piano workspace's edition.
pub fn prebuild_runtime_from_path(
    runtime_path: &Path,
    project_dir: &Path,
    target_dir: &Path,
    features: &[&str],
) -> Result<PrebuiltRuntime, Error> {
    let dest = target_dir.join("runtime-src");
    copy_runtime_standalone(runtime_path, &dest)?;
    build_runtime_rlib(&dest, project_dir, target_dir, features)
}

/// Copy runtime source to a standalone directory with `[workspace]` injected.
fn copy_runtime_standalone(src: &Path, dest: &Path) -> Result<(), Error> {
    let src_dir = src.join("src");
    let dest_src = dest.join("src");
    std::fs::create_dir_all(&dest_src).map_err(io_context("create directory", &dest_src))?;

    // Copy and patch Cargo.toml
    let cargo_toml = std::fs::read_to_string(src.join("Cargo.toml"))
        .map_err(io_context("read Cargo.toml", &src.join("Cargo.toml")))?;
    let mut doc: DocumentMut = cargo_toml
        .parse()
        .map_err(|e| Error::BuildFailed(format!("failed to parse runtime Cargo.toml: {e}")))?;
    doc.remove("bench");
    doc.remove("dev-dependencies");
    doc.remove("test");
    let mut content = doc.to_string();
    if !content.contains("[workspace]") {
        content.push_str("\n[workspace]\n");
    }
    std::fs::write(dest.join("Cargo.toml"), content)
        .map_err(io_context("write Cargo.toml", &dest.join("Cargo.toml")))?;

    // Copy source files recursively
    copy_rs_files_recursive(&src_dir, &dest_src)?;

    Ok(())
}

fn copy_rs_files_recursive(src: &Path, dest: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(dest).map_err(io_context("create directory", dest))?;
    for entry in std::fs::read_dir(src).map_err(io_context("read directory", src))? {
        let entry = entry.map_err(io_context("read directory entry", src))?;
        let path = entry.path();
        let file_name = entry.file_name();
        if path.is_dir() {
            copy_rs_files_recursive(&path, &dest.join(file_name))?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            std::fs::copy(&path, dest.join(file_name)).map_err(io_context("copy file", &path))?;
        }
    }
    Ok(())
}

fn build_runtime_rlib(
    src_dir: &Path,
    project_dir: &Path,
    target_dir: &Path,
    features: &[&str],
) -> Result<PrebuiltRuntime, Error> {
    let build_dir = target_dir.join("runtime-build");

    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--message-format=json")
        .arg("--manifest-path")
        .arg(src_dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", &build_dir)
        .env_remove("RUSTUP_TOOLCHAIN")
        .current_dir(project_dir);

    if !features.is_empty() {
        cmd.arg("--features").arg(features.join(","));
    }

    let output = cmd
        .output()
        .map_err(|e| Error::BuildFailed(format!("failed to build piano-runtime: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::BuildFailed(format!(
            "piano-runtime build failed: {stderr}"
        )));
    }

    // Parse JSON to find the rlib artifact
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut rlib_path = None;
    for line in stdout.lines() {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if msg.get("reason").and_then(|r| r.as_str()) != Some("compiler-artifact") {
            continue;
        }
        let Some(target) = msg.get("target") else {
            continue;
        };
        if target.get("name").and_then(|n| n.as_str()) != Some("piano_runtime") {
            continue;
        }
        if let Some(filenames) = msg.get("filenames").and_then(|f| f.as_array()) {
            for f in filenames {
                if let Some(s) = f.as_str() {
                    if s.ends_with(".rlib") {
                        rlib_path = Some(PathBuf::from(s));
                    }
                }
            }
        }
    }

    // Fallback: if JSON parsing didn't find it (e.g., older Cargo versions
    // with different JSON schema or "Fresh" artifacts), search the build dir.
    let rlib_path = match rlib_path {
        Some(p) => p,
        None => {
            let release_dir = build_dir.join("release");
            find_rlib_in_dir(&release_dir)
                .or_else(|_| find_rlib_in_dir(&release_dir.join("deps")))?
        }
    };
    // The deps dir is release/deps/ which contains transitive dependency rlibs
    // (needed for -L dependency=). The rlib itself may be in release/ or release/deps/
    // depending on whether piano-runtime is the root crate or a dependency.
    let deps_dir = build_dir.join("release").join("deps");

    Ok(PrebuiltRuntime {
        rlib_path,
        deps_dir,
    })
}

/// Search a directory for a `libpiano_runtime*.rlib` file.
fn find_rlib_in_dir(dir: &Path) -> Result<PathBuf, Error> {
    if !dir.is_dir() {
        return Err(Error::BuildFailed(format!(
            "no rlib found: {} does not exist",
            dir.display()
        )));
    }
    for entry in std::fs::read_dir(dir).map_err(io_context("read directory", dir))? {
        let entry = entry.map_err(io_context("read directory entry", dir))?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("libpiano_runtime") && name.ends_with(".rlib") {
                return Ok(path);
            }
        }
    }
    Err(Error::BuildFailed(
        "no rlib found in piano-runtime build output".into(),
    ))
}

/// Clean up stale .piano.rs temp files left by crashed wrapper invocations.
pub fn clean_stale_piano_files(src_dir: &Path) -> Result<(), Error> {
    clean_piano_files_recursive(src_dir)
}

fn clean_piano_files_recursive(dir: &Path) -> Result<(), Error> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    for entry in entries {
        let entry = entry.map_err(io_context("read directory entry", dir))?;
        let path = entry.path();
        if path.is_dir() {
            // Skip target/ directories
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            clean_piano_files_recursive(&path)?;
        } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with('.') && name.ends_with(".piano.rs") {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

/// Build the instrumented binary using the wrapper pipeline.
///
/// Sets `RUSTC_WORKSPACE_WRAPPER` to the current piano binary and
/// `PIANO_WRAPPER_CONFIG` to the config file path, then runs
/// `cargo build --release --message-format=json`.
pub fn build_instrumented(
    project_dir: &Path,
    target_dir: &Path,
    package: Option<&str>,
    target: Option<CargoTarget<'_>>,
    config_path: &Path,
    modified_files: &std::collections::HashSet<PathBuf>,
) -> Result<PathBuf, Error> {
    let piano_exe = std::env::current_exe()
        .map_err(|e| Error::BuildFailed(format!("failed to locate piano binary: {e}")))?;

    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg("--message-format=json")
        .env("CARGO_TARGET_DIR", target_dir)
        .env("RUSTC_WORKSPACE_WRAPPER", &piano_exe)
        .env(crate::wrapper::CONFIG_ENV, config_path)
        // Disable LTO: the pre-built piano-runtime rlib is injected via --extern
        // and won't have LLVM bitcode matching the user's LTO settings. Since
        // piano already modifies the binary with instrumentation, LTO optimization
        // differences are irrelevant for profiling.
        .env("CARGO_PROFILE_RELEASE_LTO", "false")
        .env_remove("RUSTUP_TOOLCHAIN")
        .current_dir(project_dir);
    if let Some(pkg) = package {
        cmd.arg("-p").arg(pkg);
    }
    match target {
        Some(CargoTarget::Bin(name)) => {
            cmd.arg("--bin").arg(name);
        }
        Some(CargoTarget::Example(name)) => {
            cmd.arg("--example").arg(name);
        }
        None => {}
    }
    let output = cmd.output()?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let rendered = extract_rendered_errors(&stdout);
        if rendered.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::BuildFailed(stderr.into_owned()));
        }
        let all_text = rendered.join("");
        // Detect Send-bound errors on unfiltered text (needs piano references)
        if let Some(guidance) = detect_send_bound_guidance(&all_text) {
            eprintln!("{guidance}");
        }
        // Filter piano diagnostics and clean rendered output
        let user_errors = extract_user_errors(&stdout, modified_files, project_dir);
        let error_text = user_errors.join("");
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

        let (name, path) = find_target(&metadata, None, None, "bin").unwrap();
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

        let (name, path) = find_target(&metadata, None, Some("worker"), "bin").unwrap();
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

        let (_, path) = find_target(&metadata, None, None, "bin").unwrap();
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

        let result = find_target(&metadata, None, Some("nonexistent"), "bin");
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

        let (name, path) = find_target(&metadata, Some("core"), None, "bin").unwrap();
        assert_eq!(name, "core");
        assert_eq!(path, PathBuf::from("/ws/crates/core/src/main.rs"));
    }

    #[test]
    fn find_target_finds_example() {
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
                    MetadataTarget {
                        name: "bench".to_string(),
                        kind: vec!["example".to_string()],
                        src_path: PathBuf::from("/project/examples/bench.rs"),
                    },
                ],
            }],
        };

        let (name, path) = find_target(&metadata, None, Some("bench"), "example").unwrap();
        assert_eq!(name, "demo");
        assert_eq!(path, PathBuf::from("/project/examples/bench.rs"));

        let result = find_target(&metadata, None, None, "example");
        assert!(result.is_ok());

        let result = find_target(&metadata, None, None, "bin");
        assert_eq!(result.unwrap().1, PathBuf::from("/project/src/main.rs"));
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
    fn piano_diagnostic_filtering() {
        // Piano-internal diagnostics: should be filtered
        assert!(is_piano_diagnostic("unused variable: `__piano_guard`"));
        assert!(is_piano_diagnostic("unused variable: `__piano_sink`"));
        assert!(is_piano_diagnostic("unused import: `piano_runtime::enter`"));
        assert!(is_piano_diagnostic("constant `PIANO_NAMES` is never used"));
        assert!(is_piano_diagnostic("static `__PIANO_ALLOC` is never used"));

        // User diagnostics: should NOT be filtered
        assert!(!is_piano_diagnostic("mismatched types"));
        assert!(!is_piano_diagnostic("cannot find type `UnknownType`"));
        assert!(
            !is_piano_diagnostic("unused variable: `piano_runtime`"),
            "user variable named piano_runtime should not be filtered"
        );
        assert!(
            !is_piano_diagnostic("function `piano_runtime_func` is never used"),
            "user function containing piano_runtime should not be filtered"
        );
        assert!(
            !is_piano_diagnostic("unused variable: `PIANO_NAMES_CONFIG`"),
            "user variable starting with PIANO_NAMES should not be filtered"
        );
    }

    #[test]
    fn extract_user_errors_filters_piano_diagnostics() {
        use std::collections::HashSet;
        let json_lines = [
            r#"{"reason":"compiler-message","message":{"message":"mismatched types","rendered":"error: mismatched types\n"}}"#,
            r#"{"reason":"compiler-message","message":{"message":"unused variable: `__piano_guard`","rendered":"warning: unused variable\n"}}"#,
        ].join("\n");
        let modified: HashSet<PathBuf> = HashSet::new();
        let errors = extract_user_errors(&json_lines, &modified, Path::new("/tmp"));
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("mismatched types"));
    }

    #[test]
    fn send_bound_error_with_piano_future_returns_guidance() {
        let error_text = concat!(
            "error[E0277]: `Rc<Cell<i32>>` cannot be sent between threads safely\n",
            " --> src/handler.rs:12:5\n",
            "  |\n",
            "12 |     some_call().await;\n",
            "  |     ^^^^^^^^^^ `Rc<Cell<i32>>` cannot be sent between threads safely\n",
            "  |\n",
            "  = help: within `PianoFuture<impl Future<Output = ()>>`, ",
            "the trait `Send` is not implemented for `Rc<Cell<i32>>`\n",
            "  = note: required by a bound in fn `handle_request`\n",
        );

        let result = detect_send_bound_guidance(error_text);
        assert!(
            result.is_some(),
            "should detect Send error with PianoFuture"
        );
        let msg = result.unwrap();
        assert!(
            msg.contains("cannot be wrapped for async profiling"),
            "should contain guidance: {msg}"
        );
        assert!(msg.contains("--skip"), "should mention --skip flag: {msg}");
    }

    #[test]
    fn send_bound_error_without_piano_returns_none() {
        let error_text = concat!(
            "error[E0277]: `Rc<i32>` cannot be sent between threads safely\n",
            " --> src/main.rs:5:10\n",
            "  |\n",
            "5 |     tokio::spawn(async move { drop(rc); });\n",
            "  |     ^^^^^^^^^^^^ `Rc<i32>` cannot be sent between threads safely\n",
        );

        let result = detect_send_bound_guidance(error_text);
        assert!(
            result.is_none(),
            "should not trigger for user's own Send error"
        );
    }

    #[test]
    fn non_send_error_returns_none() {
        let error_text = concat!(
            "error[E0308]: mismatched types\n",
            " --> src/main.rs:3:18\n",
            "  |\n",
            "3 |     let x: i32 = \"hello\";\n",
            "  |                  ^^^^^^^ expected `i32`, found `&str`\n",
        );

        let result = detect_send_bound_guidance(error_text);
        assert!(result.is_none(), "should not trigger for non-Send errors");
    }

    #[test]
    fn send_bound_guidance_extracts_function_name() {
        let error_text = concat!(
            "error[E0277]: `Rc<Cell<i32>>` is not Send\n",
            " --> src/api.rs:20:1\n",
            "  |\n",
            "20 | fn process_request() -> impl Future<Output = ()> {\n",
            "  | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n",
            "  = note: within `PianoFuture<impl Future>`, ",
            "the trait `Send` is not implemented for `Rc<Cell<i32>>`\n",
        );

        let result = detect_send_bound_guidance(error_text);
        assert!(result.is_some());
        let msg = result.unwrap();
        assert!(
            msg.contains("function 'process_request'"),
            "should extract function name: {msg}"
        );
    }

    #[test]
    fn clean_rendered_replaces_instrumented_source_line() {
        use std::collections::HashSet;
        let rendered = concat!(
            "error[E0308]: mismatched types\n",
            " --> src/main.rs:2:18\n",
            "  |\n",
            "2 |     let x: i32 = \"hello\"; let __piano_guard = piano_runtime::enter(0);\n",
            "  |            ---   ^^^^^^^ expected `i32`, found `&str`\n",
            "  |            |\n",
            "  |            expected due to this\n",
            "\n",
        );
        let spans: Vec<serde_json::Value> = serde_json::from_str(
            r#"[{"file_name": "src/main.rs", "line_start": 2, "line_end": 2, "text": [{"text": "    let x: i32 = \"hello\"; let __piano_guard = piano_runtime::enter(0);"}]}]"#
        ).unwrap();
        let modified_files: HashSet<PathBuf> = [PathBuf::from("src/main.rs")].into();

        let cleaned = clean_rendered(rendered, &spans, &modified_files, |file, line| {
            if file == Path::new("src/main.rs") && line == 2 {
                Some("    let x: i32 = \"hello\";".to_string())
            } else {
                None
            }
        });

        assert!(
            !cleaned.contains("__piano_guard"),
            "guard should be removed: {cleaned}"
        );
        assert!(
            !cleaned.contains("piano_runtime"),
            "runtime should be removed: {cleaned}"
        );
        assert!(
            cleaned.contains("let x: i32 = \"hello\";"),
            "original source should appear: {cleaned}"
        );
        assert!(
            cleaned.contains(" --> src/main.rs:2:18"),
            "location preserved: {cleaned}"
        );
    }

    #[test]
    fn clean_rendered_leaves_unmodified_files_alone() {
        use std::collections::HashSet;
        let rendered = "error: something\n --> lib.rs:5:3\n  |\n5 | fn helper() {\n";
        let spans: Vec<serde_json::Value> = serde_json::from_str(
            r#"[{"file_name": "lib.rs", "line_start": 5, "line_end": 5, "text": [{"text": "fn helper() {"}]}]"#
        ).unwrap();
        let modified_files: HashSet<PathBuf> = [PathBuf::from("src/main.rs")].into();

        let cleaned = clean_rendered(rendered, &spans, &modified_files, |_, _| None);
        assert_eq!(cleaned, rendered, "unmodified files should be untouched");
    }

    #[test]
    fn clean_rendered_handles_multiple_spans() {
        use std::collections::HashSet;
        let rendered = concat!(
            "error: stuff\n",
            "1 | fn a() { let __piano_guard = piano_runtime::enter(0);\n",
            "5 | fn b() { let __piano_guard = piano_runtime::enter(1);\n",
        );
        let spans: Vec<serde_json::Value> = serde_json::from_str(
            r#"[
                {"file_name": "src/main.rs", "line_start": 1, "line_end": 1, "text": [{"text": "fn a() { let __piano_guard = piano_runtime::enter(0);"}]},
                {"file_name": "src/main.rs", "line_start": 5, "line_end": 5, "text": [{"text": "fn b() { let __piano_guard = piano_runtime::enter(1);"}]}
            ]"#
        ).unwrap();
        let modified_files: HashSet<PathBuf> = [PathBuf::from("src/main.rs")].into();

        let cleaned = clean_rendered(rendered, &spans, &modified_files, |_, line| match line {
            1 => Some("fn a() {".to_string()),
            5 => Some("fn b() {".to_string()),
            _ => None,
        });

        assert!(!cleaned.contains("__piano_guard"));
        assert!(cleaned.contains("1 | fn a() {"));
        assert!(cleaned.contains("5 | fn b() {"));
    }

    #[test]
    fn clean_rendered_no_spans_returns_unchanged() {
        use std::collections::HashSet;
        let rendered = "error: something bad\n";
        let spans: Vec<serde_json::Value> = vec![];
        let modified_files: HashSet<PathBuf> = [PathBuf::from("src/main.rs")].into();

        let cleaned = clean_rendered(rendered, &spans, &modified_files, |_, _| None);
        assert_eq!(cleaned, rendered);
    }
}
