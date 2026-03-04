//! Integration test: verify `async fn main() -> Result<...>` works through
//! the full piano pipeline. The rewriter's `__piano_result` binding logic
//! was a late bug fix (commit 9d938c5) and only had a unit test (rewrite.rs:1680).
//! This test compiles and runs the instrumented binary end-to-end.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_async_main_return_type_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-main-result"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-main-result"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"use std::io;

async fn do_work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let result = do_work().await;
    println!("result: {result}");
    Ok(())
}
"#,
    )
    .unwrap();
}

#[test]
fn async_main_with_return_type() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-main-result");
    create_async_main_return_type_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let build = Command::new(piano_bin)
        .args(["build", "--fn", "do_work", "--fn", "main", "--project"])
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

    let program_stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        program_stdout.contains("result: 499500"),
        "program should produce correct output, got: {program_stdout}"
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let stats = common::aggregate_ndjson(&content);

    assert!(
        stats.contains_key("do_work"),
        "output should contain do_work. Got:\n{content}"
    );
}
