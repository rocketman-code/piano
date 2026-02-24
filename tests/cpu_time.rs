//! Integration test: verify --cpu-time flag produces CPU time data in output.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a minimal Rust project with a compute-heavy function.
fn create_cpu_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "cpu_test"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "cpu_test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = compute();
    println!("result: {result}");
}

fn compute() -> u64 {
    let mut sum = 0u64;
    for i in 0..10_000 {
        sum = sum.wrapping_add(i * i);
    }
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn cpu_time_flag_produces_cpu_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("cpu_test");
    create_cpu_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with --cpu-time flag.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "compute", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .arg("--cpu-time")
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build --cpu-time failed:\nstderr: {stderr}\nstdout: {stdout}"
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

    // Program should still produce correct output.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result:"),
        "program should produce output, got: {program_stdout}"
    );

    // Verify run file was written.
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

    // Verify the run file contains CPU time data.
    let content = fs::read_to_string(run_files[0].path()).unwrap();
    let ext = run_files[0]
        .path()
        .extension()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    if ext == "ndjson" {
        // NDJSON: header should have has_cpu_time, entries should have csn field.
        assert!(
            content.contains("\"has_cpu_time\":true"),
            "NDJSON header should contain has_cpu_time flag. Got:\n{}",
            content.lines().next().unwrap_or("")
        );
        assert!(
            content.contains("\"csn\":"),
            "NDJSON entries should contain csn (cpu_self_ns) field"
        );
    } else {
        // JSON: should have cpu_self_ms field.
        assert!(
            content.contains("\"cpu_self_ms\":"),
            "JSON output should contain cpu_self_ms field"
        );
    }

    // Verify `piano report` shows CPU column.
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
        report_stdout.contains("CPU"),
        "report should show CPU column when cpu-time data is present. Got:\n{report_stdout}"
    );
    assert!(
        report_stdout.contains("compute"),
        "report should show the 'compute' function"
    );
}
