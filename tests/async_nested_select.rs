//! Integration test: verify nested `tokio::select!` does not inflate parent
//! self-time. This was the original bug that motivated PianoFuture: when a
//! parent function uses `select!` over instrumented children, the parent's
//! self_ns should remain small since it does no computation of its own.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_nested_select_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "nested-select-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "nested-select-test"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    )
    .unwrap();

    // The program has a parent that uses select! over two children.
    // Each child sleeps for 50ms. The parent does no computation.
    // With the old phantom-based system, select! cancellation could inflate
    // the parent's self_ns. With PianoFuture, self_ns should be ~0.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use tokio::time::{sleep, Duration};

async fn child_a() -> &'static str {
    sleep(Duration::from_millis(50)).await;
    "a"
}

async fn child_b() -> &'static str {
    sleep(Duration::from_millis(60)).await;
    "b"
}

async fn parent_select() -> &'static str {
    tokio::select! {
        result = child_a() => result,
        result = child_b() => result,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // Run select multiple times to accumulate timing data.
    for _ in 0..5 {
        let result = parent_select().await;
        println!("winner: {result}");
    }
}
"#,
    )
    .unwrap();
}

#[test]
fn async_nested_select_self_time() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("nested-select-test");
    create_nested_select_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let build = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "child_a",
            "--fn",
            "child_b",
            "--fn",
            "parent_select",
            "--fn",
            "main",
            "--project",
        ])
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
        "instrumented binary failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr),
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let stats = common::aggregate_ndjson(&content);

    // All instrumented functions should appear.
    assert!(
        stats.contains_key("parent_select"),
        "output should contain parent_select. Got:\n{content}"
    );
    assert!(
        stats.contains_key("child_a"),
        "output should contain child_a. Got:\n{content}"
    );

    // parent_select does zero computation -- its body is just a select! macro.
    // Its self_ns should be much smaller than child_a's self_ns (~50ms of sleep).
    let parent = stats.get("parent_select").unwrap();
    let child = stats.get("child_a").unwrap();
    assert!(
        child.self_ns > 0,
        "child_a should have non-zero self_ns (it sleeps 50ms)"
    );
    assert!(
        parent.self_ns < child.self_ns,
        "parent_select.self_ns ({}) must be < child_a.self_ns ({}) -- \
         parent does no computation, select! overhead should be negligible",
        parent.self_ns,
        child.self_ns,
    );
}
