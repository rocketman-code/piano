//! Integration test: piano-runtime compiles on Rust 1.56.
//!
//! piano-runtime is injected as a dependency of the target project, so it must
//! compile under the target's toolchain. This test pins Rust 1.56 (the edition
//! 2021 floor) to verify the runtime works on the oldest supported compiler.

use std::fs;
use std::path::Path;
use std::process::Command;

fn has_toolchain(version: &str) -> bool {
    Command::new("rustup")
        .args(["run", version, "rustc", "--version"])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn create_project_pinned_to(dir: &Path, rust_version: &str) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("rust-toolchain.toml"),
        format!(
            r#"[toolchain]
channel = "{rust_version}"
"#
        ),
    )
    .unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "msrv_test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "msrv_test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = compute();
    println!("result: {}", result);
}

fn compute() -> u64 {
    (0..100).sum()
}
"#,
    )
    .unwrap();
}

#[test]
fn piano_runtime_compiles_on_rust_1_56() {
    if !has_toolchain("1.56.0") {
        eprintln!("skipping: Rust 1.56.0 not installed (rustup toolchain install 1.56.0)");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("msrv_test");
    create_project_pinned_to(&project_dir, "1.56.0");

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--fn", "compute", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build failed on Rust 1.56:\nstderr: {stderr}\nstdout: {stdout}"
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
        "instrumented binary failed on Rust 1.56:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 4950"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify profiling data was produced.
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
        "expected profiling data on Rust 1.56"
    );
}
