//! Test: piano build succeeds on projects with strict lint configuration.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a project with `-Dunsafe_code` in `.cargo/config.toml`.
fn create_strict_lint_project(root: &Path) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::create_dir_all(root.join(".cargo")).unwrap();

    fs::write(
        root.join("Cargo.toml"),
        r#"[package]
name = "strict"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "strict"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        root.join(".cargo").join("config.toml"),
        r#"[target.'cfg(all())']
rustflags = ["-Dunsafe_code"]
"#,
    )
    .unwrap();

    fs::write(
        root.join("src").join("main.rs"),
        r#"fn main() {
    let result = compute();
    println!("result: {result}");
}

fn compute() -> u64 {
    (0..100).sum()
}
"#,
    )
    .unwrap();
}

#[test]
fn project_with_deny_unsafe_code_builds_successfully() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("strict");
    create_strict_lint_project(&project);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--fn", "compute", "--project"])
        .arg(&project)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build should succeed despite -Dunsafe_code:\nstderr: {stderr}\nstdout: {stdout}"
    );

    // Run the instrumented binary.
    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

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
        program_stdout.contains("result: 4950"),
        "program should produce correct output, got: {program_stdout}"
    );
}
