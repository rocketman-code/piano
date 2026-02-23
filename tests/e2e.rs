//! End-to-end test: create a project, instrument it, build it, run it, verify output.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a minimal Rust project that we can instrument.
fn create_mini_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "mini"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "mini"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = work();
    println!("result: {result}");
}

fn work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn full_pipeline_instrument_build_run_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("mini");
    create_mini_project(&project_dir);

    // Find the piano binary (built by cargo test).
    let piano_bin = env!("CARGO_BIN_EXE_piano");

    // Locate the piano-runtime source directory (sibling to the piano binary crate).
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Run `piano build --fn work --project <dir> --runtime-path <path>`.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
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

    // stdout should contain the path to the built binary.
    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

    // Run the instrumented binary with PIANO_RUNS_DIR pointing to a temp dir.
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

    // The program output should still work.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 499500"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify a run file was written (JSON or NDJSON depending on frame boundaries).
    let run_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "json" || ext == "ndjson")
        })
        .collect();

    assert!(
        !run_files.is_empty(),
        "expected at least one run file in {runs_dir:?}"
    );

    let content = fs::read_to_string(run_files[0].path()).unwrap();
    assert!(
        content.contains("\"work\"") || content.contains("work"),
        "output should contain instrumented function name 'work'"
    );
    assert!(
        content.contains("\"timestamp_ms\""),
        "output should contain timestamp_ms"
    );
    assert!(
        content.contains("\"run_id\":\""),
        "output should contain run_id"
    );

    // Verify `piano report` with latest-run consolidation.
    let report_output = Command::new(piano_bin)
        .args(["report"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report (latest)");

    assert!(
        report_output.status.success(),
        "piano report (latest) failed:\n{}",
        String::from_utf8_lossy(&report_output.stderr)
    );

    let report_stdout = String::from_utf8_lossy(&report_output.stdout);
    assert!(
        report_stdout.contains("work"),
        "report should show the 'work' function"
    );

    // Verify `piano report` can also read a specific file.
    let specific_report = Command::new(piano_bin)
        .args(["report"])
        .arg(run_files[0].path())
        .output()
        .expect("failed to run piano report (specific)");

    assert!(
        specific_report.status.success(),
        "piano report (specific) failed:\n{}",
        String::from_utf8_lossy(&specific_report.stderr)
    );
}
