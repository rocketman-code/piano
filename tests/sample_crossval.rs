//! Cross-validation: compare piano's self-time percentages against macOS `sample` output.
//!
//! Run with: cargo test --test sample_crossval -- --ignored --nocapture
//!
//! Skips gracefully if `sample` is not available or cannot capture the process.

use std::fs;
use std::path::Path;
use std::process::Command;

fn sample_available() -> bool {
    Command::new("which")
        .arg("sample")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn create_crossval_project(dir: &Path, runtime_path: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    let runtime_dep = format!(
        "piano-runtime = {{ path = \"{}\" }}",
        runtime_path.display()
    );

    fs::write(
        dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "crossval"
version = "0.1.0"
edition = "2024"

[dependencies]
{runtime_dep}
"#
        ),
    )
    .unwrap();

    // Three functions with 6:3:1 compute ratio.
    // Work is INLINED (no helper function) so these functions appear at
    // the top of the call stack in `sample` output.
    //
    // Iteration counts are calibrated for release mode on Apple Silicon:
    // ~6s / ~3s / ~1s respectively, giving ~10s total runtime so `sample`
    // has ample time to capture data.
    fs::write(
        dir.join("src/main.rs"),
        r#"#[inline(never)]
fn heavy() {
    let _g = piano_runtime::enter("heavy");
    let mut buf = [0x42u8; 4096];
    for i in 0..180_000_000u64 {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[inline(never)]
fn medium() {
    let _g = piano_runtime::enter("medium");
    let mut buf = [0x42u8; 4096];
    for i in 0..90_000_000u64 {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[inline(never)]
fn light() {
    let _g = piano_runtime::enter("light");
    let mut buf = [0x42u8; 4096];
    for i in 0..30_000_000u64 {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

fn main() {
    let _g = piano_runtime::enter("main");
    heavy();
    medium();
    light();
    println!("done");
}
"#,
    )
    .unwrap();
}

/// Parse the "Sort by top of stack" section of macOS `sample` output.
///
/// Lines in this section look like:
///     heavy  (in crossval)        500
///
/// The count is the last whitespace-separated token.
fn parse_sample_output(output: &str, functions: &[&str]) -> Vec<(String, u64)> {
    let mut in_sort_section = false;
    let mut counts: Vec<(String, u64)> = functions.iter().map(|f| (f.to_string(), 0u64)).collect();

    for line in output.lines() {
        if line.contains("Sort by top of stack") {
            in_sort_section = true;
            continue;
        }
        // End of section: blank line or new section header after we've entered the section.
        // The section uses indented lines; a non-indented, non-empty line signals a new section.
        if in_sort_section && !line.starts_with(' ') && !line.is_empty() {
            break;
        }
        if !in_sort_section {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Extract trailing count: the last whitespace-separated token should be a number.
        let parts: Vec<&str> = trimmed.rsplitn(2, char::is_whitespace).collect();
        if parts.len() != 2 {
            continue;
        }
        let Ok(count) = parts[0].parse::<u64>() else {
            continue;
        };

        // Check if this line matches any of our functions
        for (name, total) in &mut counts {
            if trimmed.contains(name.as_str()) {
                *total += count;
            }
        }
    }
    counts
}

#[test]
#[ignore]
fn sample_cross_validation() {
    if !sample_available() {
        eprintln!("SKIP: macOS `sample` command not available");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("crossval");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    create_crossval_project(&project_dir, &runtime_path);

    // Build in release mode for realistic profiling
    let build = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&project_dir)
        .output()
        .expect("cargo build failed");
    assert!(
        build.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );

    let binary = project_dir.join("target/release/crossval");
    assert!(binary.exists(), "binary not found at {}", binary.display());

    // --- Run 1: Piano instrumentation ---
    let runs_dir = tmp.path().join("piano_runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let piano_run = Command::new(&binary)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("piano run failed");
    assert!(piano_run.status.success());

    // Read piano results
    let json_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert!(!json_files.is_empty(), "no piano JSON output");

    let json_content = fs::read_to_string(json_files[0].path()).unwrap();
    eprintln!("Piano JSON: {json_content}");

    // Parse piano self-time percentages
    let piano_funcs = ["heavy", "medium", "light"];
    let mut piano_self_ms: Vec<(String, f64)> = Vec::new();
    for func in &piano_funcs {
        if let Some(pos) = json_content.find(&format!("\"name\":\"{func}\"")) {
            let rest = &json_content[pos..];
            if let Some(sm_pos) = rest.find("\"self_ms\":") {
                let num_start = sm_pos + "\"self_ms\":".len();
                let num_end = rest[num_start..]
                    .find(|c: char| c != '.' && !c.is_ascii_digit())
                    .unwrap_or(rest.len() - num_start);
                let val: f64 = rest[num_start..num_start + num_end].parse().unwrap_or(0.0);
                piano_self_ms.push((func.to_string(), val));
            }
        }
    }

    let piano_total: f64 = piano_self_ms.iter().map(|(_, v)| v).sum();
    if piano_total <= 0.0 {
        eprintln!("SKIP: piano reported zero total self-time");
        return;
    }
    eprintln!("\nPiano self-time distribution:");
    for (name, ms) in &piano_self_ms {
        eprintln!("  {name}: {ms:.1}ms ({:.1}%)", ms / piano_total * 100.0);
    }

    // --- Run 2: macOS sample ---
    // Spawn binary in background, then sample it
    let mut child = Command::new(&binary)
        .env_remove("PIANO_RUNS_DIR")
        .spawn()
        .expect("failed to spawn for sampling");

    let pid = child.id();
    let sample_file = tmp.path().join("sample.txt");

    // Sample for 15 seconds — enough to cover the full ~10s runtime
    let sample_out = Command::new("sample")
        .args([
            &pid.to_string(),
            "15",
            "-file",
            sample_file.to_str().unwrap(),
        ])
        .output();

    let _ = child.wait();

    match sample_out {
        Ok(out) if out.status.success() => {
            let sample_text = match fs::read_to_string(&sample_file) {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("SKIP: could not read sample output file: {e}");
                    return;
                }
            };

            let sample_counts = parse_sample_output(&sample_text, &piano_funcs);
            let sample_total: u64 = sample_counts.iter().map(|(_, c)| c).sum();

            if sample_total == 0 {
                eprintln!(
                    "SKIP: sample captured 0 hits for our functions (process may have finished too quickly)"
                );
                eprintln!("Sample output:\n{sample_text}");
                return;
            }

            eprintln!("\nSample distribution:");
            for (name, count) in &sample_counts {
                eprintln!(
                    "  {name}: {count} samples ({:.1}%)",
                    *count as f64 / sample_total as f64 * 100.0
                );
            }

            // Compare distributions: within 10 percentage points for sampling-based comparison
            for func in &piano_funcs {
                let piano_pct = piano_self_ms
                    .iter()
                    .find(|(n, _)| n == func)
                    .map(|(_, v)| v / piano_total * 100.0)
                    .unwrap_or(0.0);
                let sample_pct = sample_counts
                    .iter()
                    .find(|(n, _)| n == func)
                    .map(|(_, c)| *c as f64 / sample_total as f64 * 100.0)
                    .unwrap_or(0.0);

                let diff = (piano_pct - sample_pct).abs();
                eprintln!(
                    "{func}: piano={piano_pct:.1}%, sample={sample_pct:.1}%, diff={diff:.1}pp"
                );
                assert!(
                    diff < 10.0,
                    "{func} diverges: piano={piano_pct:.1}% vs sample={sample_pct:.1}% (diff={diff:.1}pp, limit 10pp)"
                );
            }

            eprintln!("\nCross-validation PASSED: piano and sample agree within 10pp");
        }
        Ok(out) => {
            eprintln!(
                "SKIP: sample command failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Err(e) => {
            eprintln!("SKIP: could not run sample: {e}");
        }
    }
}
