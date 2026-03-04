//! Integration test: verify process::exit() inside an instrumented non-main
//! function still produces NDJSON output via the atexit handler.
//! The existing test (process_exit_preserves_profiling_data) tests exit in main.
//! This test covers exit in a helper function.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_exit_in_helper_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "exit-helper"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "exit-helper"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}

fn helper() {
    let sum = work();
    println!("sum: {sum}");
    std::process::exit(0);
}

fn main() {
    helper();
}
"#,
    )
    .unwrap();
}

#[test]
fn exit_in_non_main_preserves_profiling_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-helper");
    create_exit_in_helper_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Only instrument work (not helper or main) so that work's guard drops
    // before helper calls process::exit(0). The atexit handler then collects
    // the completed timing data.
    let build = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&build.stderr);
    let stdout = String::from_utf8_lossy(&build.stdout);
    assert!(
        build.status.success(),
        "piano build failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run.status.success(),
        "instrumented binary should exit cleanly:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr),
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    assert!(
        content.contains("work"),
        "profiling data should contain 'work' function:\n{content}"
    );
}
