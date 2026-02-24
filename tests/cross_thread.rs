//! Integration test: verify cross-thread instrumentation captures all calls
//! from rayon par_iter and std::thread::scope with correct attribution.

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

    // Read the JSON output (shutdown always writes JSON with cross-thread data).
    let json_files: Vec<_> = fs::read_dir(&runs_dir)
        .expect("runs dir should exist")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert!(!json_files.is_empty(), "should have JSON output files");

    let content = fs::read_to_string(json_files[0].path()).unwrap();

    // Verify compute function is captured.
    assert!(
        content.contains("\"compute\""),
        "should contain compute function. Got:\n{content}"
    );

    // compute is called 100 times in par_iter + 100 times in thread::scope = 200.
    let compute_calls = extract_field_u64(&content, "compute", "calls");
    assert_eq!(
        compute_calls, 200,
        "compute should be called 200 times (100 par_iter + 100 thread::scope), got {compute_calls}"
    );

    // After wall-time non-propagation: main self_ms ~ total_ms because
    // cross-thread children wall time is NOT subtracted from parent.
    let main_self = extract_field_f64(&content, "main", "self_ms");
    let main_total = extract_field_f64(&content, "main", "total_ms");
    assert!(
        main_self > main_total * 0.5,
        "main self_ms ({main_self}) should be close to total_ms ({main_total}) — no cross-thread wall subtraction"
    );
}

fn extract_field_u64(json: &str, function: &str, field: &str) -> u64 {
    let func_section = json
        .split(&format!("\"{function}\""))
        .nth(1)
        .unwrap_or_else(|| panic!("function {function} not found in JSON"));
    let value_str = func_section
        .split(&format!("\"{field}\":"))
        .nth(1)
        .unwrap_or_else(|| panic!("field {field} not found for {function}"))
        .split([',', '}'])
        .next()
        .unwrap();
    value_str
        .parse()
        .unwrap_or_else(|_| panic!("failed to parse {field}={value_str} for {function}"))
}

fn extract_field_f64(json: &str, function: &str, field: &str) -> f64 {
    let func_section = json
        .split(&format!("\"{function}\""))
        .nth(1)
        .unwrap_or_else(|| panic!("function {function} not found in JSON"));
    let value_str = func_section
        .split(&format!("\"{field}\":"))
        .nth(1)
        .unwrap_or_else(|| panic!("field {field} not found for {function}"))
        .split([',', '}'])
        .next()
        .unwrap();
    value_str
        .parse()
        .unwrap_or_else(|_| panic!("failed to parse {field}={value_str} for {function}"))
}
