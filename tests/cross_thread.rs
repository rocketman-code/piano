//! Integration test: verify cross-thread instrumentation captures all calls
//! from rayon par_iter and std::thread::scope with correct attribution.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_cross_thread_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "cross-thread-fixture"
version = "0.1.0"
edition = "2024"

[dependencies]
rayon = "1"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"use rayon::prelude::*;

fn main() {
    let items: Vec<u64> = (0..100).collect();

    // Pattern 1: rayon par_iter with instrumented function calls
    let results: Vec<u64> = items.par_iter().map(|&x| compute(x)).collect();
    println!("par_iter results: {}", results.len());

    // Pattern 2: std::thread::scope with instrumented work
    std::thread::scope(|s| {
        for chunk in items.chunks(25) {
            s.spawn(move || {
                for &x in chunk {
                    compute(x);
                }
            });
        }
    });
    println!("thread::scope done");
}

fn compute(x: u64) -> u64 {
    let mut result = x;
    for _ in 0..1000 {
        result = result.wrapping_mul(31).wrapping_add(7);
    }
    result
}
"#,
    )
    .unwrap();
}

#[test]
fn cross_thread_captures_all_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("cross-thread-fixture");
    create_cross_thread_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Run piano build on the fixture — instrument both compute and main.
    let piano_build = Command::new(piano_bin)
        .args(["build", "--fn", "compute", "--fn", "main", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&piano_build.stderr);
    let stdout = String::from_utf8_lossy(&piano_build.stdout);
    assert!(
        piano_build.status.success(),
        "piano build failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();

    // Run the instrumented binary.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Read the NDJSON output.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    // Verify compute function is captured.
    assert!(
        content.contains("\"compute\""),
        "should contain compute function. Got:\n{content}"
    );

    // Parse NDJSON to aggregate calls per function.
    let stats = common::aggregate_ndjson(&content);

    // compute is called 100 times in par_iter + 100 times in thread::scope = 200.
    let compute_calls = stats.get("compute").map(|s| s.calls).unwrap_or(0);
    assert_eq!(
        compute_calls, 200,
        "compute should be called 200 times (100 par_iter + 100 thread::scope), got {compute_calls}"
    );

    // Cross-thread wall-time non-propagation: main's self_ns must NOT be
    // reduced by child-thread compute time. Since compute runs on worker
    // threads (not as a same-thread child of main), main's self_ns equals
    // its full wall time. If this invariant were broken, main's self_ns
    // would be near zero.
    let main_self_ns = stats.get("main").map(|s| s.self_ns).unwrap_or(0);
    let compute_self_ns = stats.get("compute").map(|s| s.self_ns).unwrap_or(0);
    assert!(
        main_self_ns > compute_self_ns / 64,
        "main self_ns ({main_self_ns}) should be substantial relative to compute \
         self_ns ({compute_self_ns}) — cross-thread wall time must not be subtracted \
         from parent self-time"
    );
}
