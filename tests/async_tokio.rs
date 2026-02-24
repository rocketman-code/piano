//! Integration test: verify piano can instrument, build, and run an async
//! tokio project. Async functions should be instrumented (not skipped), the
//! instrumented binary should run without panicking, and the profiling output
//! should contain the async function names.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a small tokio project with async functions.
fn create_async_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-test"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"async fn compute(x: u64) -> u64 {
    let mut sum = 0u64;
    for i in 0..x {
        sum += i;
    }
    sum
}

async fn orchestrate() -> u64 {
    let a = compute(1000).await;
    let b = compute(2000).await;
    a + b
}

#[tokio::main]
async fn main() {
    let result = orchestrate().await;
    println!("result: {result}");
}
"#,
    )
    .unwrap();
}

#[test]
fn async_tokio_pipeline() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-test");
    create_async_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Run piano build on the async project -- instrument all three functions.
    let output = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "compute",
            "--fn",
            "orchestrate",
            "--fn",
            "main",
            "--project",
        ])
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

    // The old behavior was to skip async functions with a warning.
    // Verify no "skipped" + "async" message appears in stderr.
    let stderr_lower = stderr.to_lowercase();
    assert!(
        !(stderr_lower.contains("skipped") && stderr_lower.contains("async")),
        "stderr should not contain async-skip warnings, got:\n{stderr}"
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
        "instrumented binary panicked or failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    // The program should still produce the correct result.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 2498500"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify run files were produced.
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

    // Read the output and verify async function names appear.
    let content = fs::read_to_string(run_files[0].path()).unwrap();
    assert!(
        content.contains("compute"),
        "output should contain instrumented async function 'compute'. Got:\n{content}"
    );
    assert!(
        content.contains("orchestrate"),
        "output should contain instrumented async function 'orchestrate'. Got:\n{content}"
    );
}
