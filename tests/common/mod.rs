//! Shared NDJSON parsing utilities for integration tests.
//!
//! Parses the NDJSON v4 format written by piano-runtime: a header line,
//! frame lines with per-function summaries, and a trailer line with
//! the function names array.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;

/// Per-function aggregated data from NDJSON frames.
#[derive(Debug, Default)]
pub struct FnStats {
    pub calls: u64,
    pub self_ns: u64,
}

/// Parse NDJSON content and aggregate per-function stats (calls + self_ns)
/// across all frames.
///
/// Returns a map from function name to aggregated `FnStats`.
pub fn aggregate_ndjson(content: &str) -> HashMap<String, FnStats> {
    let all_lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        all_lines.len() >= 2,
        "NDJSON should have at least header + trailer"
    );

    // v4: function names are in the trailer (last line), not the header.
    let trailer = *all_lines.last().unwrap();
    let fn_names = parse_trailer_functions(trailer);

    let mut calls = vec![0u64; fn_names.len()];
    let mut self_ns = vec![0u64; fn_names.len()];

    // Frame lines are between header and trailer.
    for &line in &all_lines[1..all_lines.len() - 1] {
        if !line.contains("\"fns\"") {
            continue;
        }
        let fns_start = line.find("\"fns\":[").unwrap() + "\"fns\":[".len();
        let fns_end = line[fns_start..].rfind(']').unwrap();
        let fns_str = &line[fns_start..fns_start + fns_end];
        for entry in fns_str.split("},{") {
            let entry = entry.trim_start_matches('{').trim_end_matches('}');
            let id = extract_json_u64(entry, "\"id\":");
            let c = extract_json_u64(entry, "\"calls\":");
            let s = extract_json_u64(entry, "\"self_ns\":");
            if let Some(id) = id {
                let idx = id as usize;
                if idx < calls.len() {
                    calls[idx] += c.unwrap_or(0);
                    self_ns[idx] += s.unwrap_or(0);
                }
            }
        }
    }

    fn_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            (
                name,
                FnStats {
                    calls: calls[i],
                    self_ns: self_ns[i],
                },
            )
        })
        .collect()
}

/// Parse the "functions" array from an NDJSON v4 trailer line.
fn parse_trailer_functions(trailer: &str) -> Vec<String> {
    let fns_start = trailer
        .find("\"functions\":[")
        .expect("trailer should have functions array")
        + "\"functions\":[".len();
    let fns_end = trailer[fns_start..]
        .find(']')
        .expect("functions array should close");
    let fns_str = &trailer[fns_start..fns_start + fns_end];
    fns_str
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect()
}

/// Find the largest NDJSON file in a directory.
///
/// When streaming mode produces multiple files (e.g. streaming + fallback),
/// the complete v4 file (header + frames + trailer) is the largest.
pub fn largest_ndjson_file(dir: &std::path::Path) -> PathBuf {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .expect("runs dir should exist")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
        .collect();
    assert!(
        !entries.is_empty(),
        "expected at least one .ndjson file in {dir:?}"
    );
    entries
        .iter()
        .max_by_key(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .unwrap()
        .path()
}

/// Extract an integer value for a given key from a JSON-like string fragment.
///
/// Looks for `prefix` followed by digits, returns the parsed u64.
pub fn extract_json_u64(s: &str, prefix: &str) -> Option<u64> {
    let start = s.find(prefix)? + prefix.len();
    let end = s[start..]
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s.len() - start);
    s[start..start + end].parse().ok()
}

// ---------------------------------------------------------------------------
// Seed infrastructure: pre-compile dependencies once, share across tests
// ---------------------------------------------------------------------------

use std::sync::OnceLock;

/// Cached seed build artifacts for dep pre-population.
pub struct SeedArtifacts {
    _dir: tempfile::TempDir,
    /// Path to {tempdir}/seed/target/piano
    pub target_piano: PathBuf,
}

/// Return a seed with pre-compiled tokio + piano-runtime artifacts.
pub fn tokio_seed() -> &'static SeedArtifacts {
    static SEED: OnceLock<SeedArtifacts> = OnceLock::new();
    SEED.get_or_init(|| {
        build_seed(
            "tokio-seed",
            r#"[package]
name = "tokio-seed"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "tokio-seed"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
            "async fn seed_fn() -> u64 { 1 }\n#[tokio::main]\nasync fn main() { seed_fn().await; }\n",
            &["--fn", "seed_fn", "--fn", "main"],
        )
    })
}

/// Return a seed with pre-compiled rayon + piano-runtime artifacts.
pub fn rayon_seed() -> &'static SeedArtifacts {
    static SEED: OnceLock<SeedArtifacts> = OnceLock::new();
    SEED.get_or_init(|| {
        build_seed(
            "rayon-seed",
            r#"[package]
name = "rayon-seed"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "rayon-seed"
path = "src/main.rs"

[dependencies]
rayon = "1"
"#,
            "fn seed_fn() { }\nfn main() { seed_fn(); }\n",
            &["--fn", "seed_fn", "--fn", "main"],
        )
    })
}

/// Return a seed with pre-compiled piano-runtime only (no external deps).
pub fn mini_seed() -> &'static SeedArtifacts {
    static SEED: OnceLock<SeedArtifacts> = OnceLock::new();
    SEED.get_or_init(|| {
        build_seed(
            "mini-seed",
            r#"[package]
name = "mini-seed"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "mini-seed"
path = "src/main.rs"
"#,
            "fn seed_fn() { }\nfn main() { seed_fn(); }\n",
            &["--fn", "seed_fn", "--fn", "main"],
        )
    })
}

fn build_seed(name: &str, cargo_toml: &str, main_rs: &str, piano_args: &[&str]) -> SeedArtifacts {
    use std::process::Command;

    let dir = tempfile::tempdir().expect("create seed tempdir");
    let project = dir.path().join(name);
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(project.join("Cargo.toml"), cargo_toml).unwrap();
    std::fs::write(project.join("src/main.rs"), main_rs).unwrap();

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let mut cmd = Command::new(piano_bin);
    cmd.args(["build"])
        .args(piano_args)
        .arg("--project")
        .arg(&project)
        .arg("--runtime-path")
        .arg(&runtime_path);

    let output = cmd.output().expect("failed to run piano build for seed");
    assert!(
        output.status.success(),
        "seed build failed for {name}:\nstderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout),
    );

    SeedArtifacts {
        target_piano: project.join("target/piano"),
        _dir: dir,
    }
}

/// Copy pre-compiled dep artifacts from a seed into a test project's
/// target/piano/ directory. Uses `cp -Rp` to preserve timestamps
/// (critical for cargo fingerprint validity).
pub fn prepopulate_deps(project_dir: &std::path::Path, seed: &SeedArtifacts) {
    use std::process::Command;

    let dst = project_dir.join("target/piano");
    std::fs::create_dir_all(dst.join("release")).unwrap();

    let src_release = seed.target_piano.join("release");
    let dst_release = dst.join("release");

    for dir_name in ["deps", ".fingerprint", "build"] {
        let src = src_release.join(dir_name);
        if src.exists() {
            let status = Command::new("cp")
                .arg("-Rp")
                .arg(&src)
                .arg(dst_release.join(dir_name))
                .status()
                .expect("cp -Rp failed");
            assert!(status.success(), "cp -Rp failed for {dir_name}");
        }
    }

    let rustc_info = seed.target_piano.join(".rustc_info.json");
    if rustc_info.exists() {
        std::fs::copy(&rustc_info, dst.join(".rustc_info.json")).unwrap();
    }
}
