//! Distribution accuracy: verify piano's reported self-time percentages
//! match a known compute ratio.
//!
//! Three functions do identical work per iteration with 6:3:1 iteration
//! counts. The true self-time distribution is 60%/30%/10%. Piano must
//! report percentages within 5 percentage points of the true ratio.
//!
//! This is the metrological calibration test: it validates that piano's
//! measurement apparatus does not distort the relative distribution of
//! work across functions.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_crossval_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "crossval"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "crossval"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // Three functions with 6:3:1 compute ratios.
    // Each does identical work per iteration (wrapping multiply chain).
    // #[inline(never)] prevents cross-function optimization.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"#[inline(never)]
fn heavy() {
    let mut x = 1u64;
    for i in 0..6_000_000u64 {
        x = x.wrapping_mul(i | 1).wrapping_add(i);
        std::hint::black_box(x);
    }
}

#[inline(never)]
fn medium() {
    let mut x = 1u64;
    for i in 0..3_000_000u64 {
        x = x.wrapping_mul(i | 1).wrapping_add(i);
        std::hint::black_box(x);
    }
}

#[inline(never)]
fn light() {
    let mut x = 1u64;
    for i in 0..1_000_000u64 {
        x = x.wrapping_mul(i | 1).wrapping_add(i);
        std::hint::black_box(x);
    }
}

fn main() {
    heavy();
    medium();
    light();
}
"#,
    )
    .unwrap();
}

#[test]
fn distribution_matches_known_compute_ratio() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("crossval");
    create_crossval_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("piano build failed");

    assert!(
        output.status.success(),
        "piano build failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let binary_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let run_output = Command::new(&binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let stats = common::aggregate_ndjson(&content);

    let heavy = stats.get("heavy").expect("heavy should be instrumented");
    let medium = stats.get("medium").expect("medium should be instrumented");
    let light = stats.get("light").expect("light should be instrumented");

    assert_eq!(heavy.calls, 1);
    assert_eq!(medium.calls, 1);
    assert_eq!(light.calls, 1);

    let total = heavy.self_ns + medium.self_ns + light.self_ns;
    assert!(total > 0, "total self-time must be nonzero");

    let heavy_pct = heavy.self_ns as f64 / total as f64 * 100.0;
    let medium_pct = medium.self_ns as f64 / total as f64 * 100.0;
    let light_pct = light.self_ns as f64 / total as f64 * 100.0;

    // True ratio: 6:3:1 = 60%/30%/10%
    // Tolerance: 5 percentage points
    let tolerance = 5.0;

    assert!(
        (heavy_pct - 60.0).abs() < tolerance,
        "heavy should be ~60%, got {heavy_pct:.1}% (self_ns={})",
        heavy.self_ns
    );
    assert!(
        (medium_pct - 30.0).abs() < tolerance,
        "medium should be ~30%, got {medium_pct:.1}% (self_ns={})",
        medium.self_ns
    );
    assert!(
        (light_pct - 10.0).abs() < tolerance,
        "light should be ~10%, got {light_pct:.1}% (self_ns={})",
        light.self_ns
    );
}
