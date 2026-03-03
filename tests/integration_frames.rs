//! End-to-end test for per-frame profiling pipeline.
//!
//! Creates a project with a main loop (simulating game frames), instruments it,
//! runs it, verifies NDJSON output, and checks that `piano report` and
//! `piano report --frames` produce expected output.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a project with a main loop that calls instrumented functions per frame.
fn create_frame_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "frame_test"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "frame_test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    // Simulate 5 frames
    for _ in 0..5 {
        update();
    }
    println!("done");
}

fn update() {
    // Do some work and allocate
    let v: Vec<u8> = vec![0u8; 256];
    std::hint::black_box(&v);
    physics();
}

fn physics() {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    std::hint::black_box(sum);
}
"#,
    )
    .unwrap();
}

#[test]
fn frame_pipeline_build_run_report() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("frame_test");
    create_frame_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with instrumentation on update and physics.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "update", "--fn", "physics", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "piano build failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

    // Run the instrumented binary.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run_output = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("done"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify an NDJSON file was written (frame boundaries produce NDJSON output).
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    // Header should have format_version 4 and NO functions (v4 = trailer).
    assert!(
        lines[0].contains("\"format_version\":4"),
        "header should have format_version 4"
    );
    // v4: functions are in the trailer (last line), not the header.
    let last_line = lines.last().unwrap();
    assert!(
        last_line.contains("\"functions\""),
        "trailer should have functions array"
    );
    assert!(
        last_line.contains("update"),
        "functions should include 'update'"
    );
    assert!(
        last_line.contains("physics"),
        "functions should include 'physics'"
    );

    // Should have frame lines (header + at least 5 frames + trailer).
    assert!(
        lines.len() >= 7,
        "expected header + 5 frames + trailer, got {} lines",
        lines.len()
    );

    // Verify `piano report` works with ndjson (default view).
    let report_output = Command::new(piano_bin)
        .args(["report"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report");

    assert!(
        report_output.status.success(),
        "piano report failed:\n{}",
        String::from_utf8_lossy(&report_output.stderr)
    );

    let report_stdout = String::from_utf8_lossy(&report_output.stdout);
    assert!(
        report_stdout.contains("update"),
        "report should show 'update'"
    );
    assert!(
        !report_stdout.contains("p50"),
        "default report should not show percentile columns"
    );

    // Verify `piano report --frames` shows per-frame breakdown.
    let frames_output = Command::new(piano_bin)
        .args(["report", "--frames"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report --frames");

    assert!(
        frames_output.status.success(),
        "piano report --frames failed:\n{}",
        String::from_utf8_lossy(&frames_output.stderr)
    );

    let frames_stdout = String::from_utf8_lossy(&frames_output.stdout);
    assert!(
        frames_stdout.contains("Frame"),
        "frames view should have Frame column"
    );
    assert!(
        frames_stdout.contains("frames"),
        "frames view should show frame count"
    );
}
