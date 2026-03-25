//! Shared NDJSON parsing utilities for integration tests.
//!
//! Parses the NDJSON format written by piano-runtime:
//! - Header: `{"type":"header","names":{"0":"fn_name",...}}`
//! - Measurement: `{"span_id":N,"parent_span_id":N,"name_id":N,...}`
//! - Trailer: `{"type":"trailer","names":{"0":"fn_name",...}}`
//!
//! Self-attribution is computed from the span tree: for each span,
//! self = inclusive - sum(direct children's inclusive).

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;

/// Per-function aggregated data from NDJSON spans.
#[derive(Debug, Default)]
pub struct FnStats {
    pub calls: u64,
    pub self_ns: u64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
}

/// A single measurement span (one per completed function invocation).
struct Measurement {
    span_id: u64,
    parent_span_id: u64,
    name_id: u64,
    start_ns: u64,
    end_ns: u64,
    alloc_count: u64,
    alloc_bytes: u64,
}

/// Parse NDJSON content and aggregate per-function stats.
///
/// Parses the name table from header/trailer, parses measurement spans,
/// computes self-attribution (inclusive - children), and aggregates by
/// function name.
///
/// Returns a map from function name to aggregated `FnStats`.
pub fn aggregate_ndjson(content: &str) -> HashMap<String, FnStats> {
    let all_lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        !all_lines.is_empty(),
        "NDJSON should have at least a header line"
    );

    // Parse all lines: extract name table (prefer trailer over header)
    // and collect measurements.
    let mut header_names: Option<HashMap<String, String>> = None;
    let mut trailer_names: Option<HashMap<String, String>> = None;
    let mut measurements: Vec<Measurement> = Vec::new();
    let mut pre_aggregated: HashMap<u64, FnStats> = HashMap::new();

    for &line in &all_lines {
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Lines with a "type" field are header or trailer.
        if let Some(kind) = v.get("type").and_then(|t| t.as_str()) {
            if let Some(names_obj) = v.get("names").and_then(|n| n.as_object()) {
                let names: HashMap<String, String> = names_obj
                    .iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect();
                match kind {
                    "header" => header_names = Some(names),
                    "trailer" => trailer_names = Some(names),
                    _ => {}
                }
            }
            continue;
        }

        // Aggregated lines (default mode): have "calls" and "self_ns", no "span_id"
        if let Some(calls) = v.get("calls").and_then(|c| c.as_u64()) {
            let name_id = v.get("name_id").and_then(|n| n.as_u64()).unwrap_or(0);
            let agg = pre_aggregated.entry(name_id).or_insert_with(FnStats::default);
            agg.calls += calls;
            agg.self_ns += v.get("self_ns").and_then(|n| n.as_u64()).unwrap_or(0);
            agg.alloc_count += v.get("alloc_count").and_then(|n| n.as_u64()).unwrap_or(0);
            agg.alloc_bytes += v.get("alloc_bytes").and_then(|n| n.as_u64()).unwrap_or(0);
            continue;
        }

        // Raw span lines (--raw-spans mode): have "span_id"
        if let Some(span_id) = v.get("span_id").and_then(|s| s.as_u64()) {
            measurements.push(Measurement {
                span_id,
                parent_span_id: v
                    .get("parent_span_id")
                    .and_then(|s| s.as_u64())
                    .unwrap_or(0),
                name_id: v.get("name_id").and_then(|s| s.as_u64()).unwrap_or(0),
                start_ns: v.get("start_ns").and_then(|s| s.as_u64()).unwrap_or(0),
                end_ns: v.get("end_ns").and_then(|s| s.as_u64()).unwrap_or(0),
                alloc_count: v
                    .get("alloc_count")
                    .and_then(|s| s.as_u64())
                    .unwrap_or(0),
                alloc_bytes: v
                    .get("alloc_bytes")
                    .and_then(|s| s.as_u64())
                    .unwrap_or(0),
            });
        }
    }

    // Resolve name table: prefer trailer (authoritative), fall back to header.
    let raw_names = trailer_names
        .or(header_names)
        .expect("NDJSON should have a header or trailer with names");

    let fn_names = build_name_table(&raw_names);

    // If we have pre-aggregated data (default mode), use it directly.
    if !pre_aggregated.is_empty() {
        return pre_aggregated
            .into_iter()
            .filter_map(|(id, stats)| {
                let idx = id as usize;
                fn_names.get(idx).map(|name| (name.clone(), stats))
            })
            .collect();
    }

    // Otherwise, compute self-attribution from span tree (raw-spans mode).
    let span_index: HashMap<u64, usize> = measurements
        .iter()
        .enumerate()
        .map(|(i, m)| (m.span_id, i))
        .collect();

    let mut children_sums: HashMap<u64, (u64, u64, u64)> = HashMap::new();
    for m in &measurements {
        if m.parent_span_id != 0 && span_index.contains_key(&m.parent_span_id) {
            let entry = children_sums.entry(m.parent_span_id).or_default();
            entry.0 += m.end_ns.saturating_sub(m.start_ns);
            entry.1 += m.alloc_count;
            entry.2 += m.alloc_bytes;
        }
    }

    let mut stats_by_id: HashMap<u64, FnStats> = HashMap::new();
    for m in &measurements {
        let wall = m.end_ns.saturating_sub(m.start_ns);
        let (child_wall, child_ac, child_ab) =
            children_sums.get(&m.span_id).copied().unwrap_or_default();

        let agg = stats_by_id.entry(m.name_id).or_default();
        agg.calls += 1;
        agg.self_ns += wall.saturating_sub(child_wall);
        agg.alloc_count += m.alloc_count.saturating_sub(child_ac);
        agg.alloc_bytes += m.alloc_bytes.saturating_sub(child_ab);
    }

    stats_by_id
        .into_iter()
        .filter_map(|(id, stats)| {
            let idx = id as usize;
            fn_names.get(idx).map(|name| (name.clone(), stats))
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
    let mut pairs: Vec<(u64, String)> = raw
        .iter()
        .filter_map(|(k, v)| k.parse::<u64>().ok().map(|id| (id, v.clone())))
        .collect();
    pairs.sort_by_key(|(id, _)| *id);

    let max_id = pairs.last().map(|(id, _)| *id).unwrap_or(0);
    let mut names = vec![String::new(); (max_id + 1) as usize];
    for (id, name) in pairs {
        names[id as usize] = name;
    }
    names
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
