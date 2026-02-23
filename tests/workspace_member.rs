//! Test: piano build works on a workspace member that uses workspace inheritance.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a workspace with a member that uses `edition.workspace = true`.
fn create_workspace_project(root: &Path) {
    // Workspace root Cargo.toml
    fs::create_dir_all(root).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["crates/*"]
resolver = "2"

[workspace.package]
edition = "2024"
"#,
    )
    .unwrap();

    // Member crate
    let member = root.join("crates").join("demo");
    fs::create_dir_all(member.join("src")).unwrap();

    fs::write(
        member.join("Cargo.toml"),
        r#"[package]
name = "demo"
version = "0.1.0"
edition.workspace = true

[[bin]]
name = "demo"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        member.join("src").join("main.rs"),
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
fn workspace_member_with_inherited_fields_builds() {
    let tmp = tempfile::tempdir().unwrap();
    let ws_root = tmp.path().join("ws");
    create_workspace_project(&ws_root);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build the workspace member.
    let member_dir = ws_root.join("crates").join("demo");
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "compute", "--project"])
        .arg(&member_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build on workspace member failed:\nstderr: {stderr}\nstdout: {stdout}"
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

    // Verify run data was written.
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
        content.contains("compute"),
        "output should contain instrumented function name 'compute'"
    );
}
