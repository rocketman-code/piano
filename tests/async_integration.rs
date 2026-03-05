//! Consolidated async integration tests.
//!
//! Tests in this module cover:
//! - async allocation tracking (alloc_count / alloc_bytes in NDJSON output)
//! - async main with return type (`async fn main() -> Result<...>`)
//! - nested `tokio::select!` self-time accuracy
//! - async self-time accuracy across thread migrations
//! - basic async tokio pipeline (instrument, build, run, verify output)
//!
//! All tests share a single pre-compiled tokio seed via `common::tokio_seed()`
//! so each `piano build` only recompiles the user crate (~0.7s vs ~3.5s).

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// Project scaffolding helpers
// ---------------------------------------------------------------------------

fn create_async_alloc_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-alloc-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-alloc-test"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    )
    .unwrap();

    // The program allocates in an async function with .await points.
    // Vec::with_capacity forces a real heap allocation.
    // Uses a sync wrapper that calls the async function via block_on,
    // ensuring the depth-0 guard boundary flushes FRAME_BUFFER to FRAMES
    // so NDJSON output is produced.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"
async fn allocating_work() -> Vec<u8> {
    let mut data = Vec::with_capacity(1024);
    data.extend_from_slice(&[1u8; 512]);
    tokio::task::yield_now().await;
    data.extend_from_slice(&[2u8; 512]);
    data
}

fn wrapper() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(allocating_work());
    println!("len: {}", result.len());
}

fn main() {
    wrapper();
}
"#,
    )
    .unwrap();
}

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
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
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

fn create_async_self_time_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-selftime"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-selftime"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    )
    .unwrap();

    // The program has a clear parent/child hierarchy:
    // - parent_fn calls expensive_child twice via .await
    // - expensive_child is a leaf that does heavy computation
    // - parent_fn's self_time should be negligible compared to its wall time
    //
    // Multiple tasks + yield_now maximize migration likelihood on multi-thread rt.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use tokio::task;

async fn expensive_child() -> u64 {
    use std::hint::black_box;
    let mut sum = 0u64;
    for i in 0..2_000_000u64 {
        sum = sum.wrapping_add(black_box(i));
    }
    // Yield to give the scheduler a migration opportunity
    task::yield_now().await;
    let mut sum2 = 0u64;
    for i in 0..2_000_000u64 {
        sum2 = sum2.wrapping_add(black_box(i));
    }
    sum.wrapping_add(sum2)
}

async fn parent_fn() -> u64 {
    let a = expensive_child().await;
    let b = expensive_child().await;
    a.wrapping_add(b)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // Spawn several concurrent tasks to increase scheduling pressure
    let mut handles = Vec::new();
    for _ in 0..4 {
        handles.push(task::spawn(parent_fn()));
    }
    let mut total = 0u64;
    for h in handles {
        total = total.wrapping_add(h.await.unwrap());
    }
    println!("total: {total}");
}
"#,
    )
    .unwrap();
}

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn async_alloc_tracking_pipeline() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-alloc-test");
    create_async_alloc_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::tokio_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with piano -- instrument allocating_work, wrapper, and main.
    // wrapper is the sync depth-0 boundary that triggers frame flush.
    let output = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "allocating_work",
            "--fn",
            "wrapper",
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
        "instrumented binary failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    // Program should produce correct output.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("len: 1024"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Find run output file.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    // NDJSON v4 format:
    //   Line 1 (header): {"format_version":4,"run_id":"...","timestamp_ms":...}
    //   Lines 2..N (frames): {"frame":0,"fns":[{"id":0,"calls":1,"self_ns":...,"ac":N,"ab":N,...}]}
    //   Last line (trailer): {"functions":["allocating_work","main"]}
    // "ac" = alloc_count, "ab" = alloc_bytes. Functions referenced by index from trailer.

    // v4: function names are in the trailer (last non-empty line), not the header.
    let all_lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    let trailer_line = all_lines
        .last()
        .expect("should have at least header + trailer");
    let trailer: serde_json::Value =
        serde_json::from_str(trailer_line).expect("trailer should be valid JSON");
    let fn_names = trailer
        .get("functions")
        .and_then(|f| f.as_array())
        .expect("trailer should have functions array");
    let alloc_work_id = fn_names
        .iter()
        .position(|n| n.as_str() == Some("allocating_work"))
        .expect("allocating_work should be in functions list");

    // Search frame lines (skip header and trailer) for an entry with that function id
    // and non-zero "ac".
    let frame_lines = &all_lines[1..all_lines.len() - 1];
    let has_alloc_data = frame_lines.iter().any(|line| {
        if let Ok(frame) = serde_json::from_str::<serde_json::Value>(line) {
            frame
                .get("fns")
                .and_then(|f| f.as_array())
                .map(|fns| {
                    fns.iter().any(|f| {
                        f.get("id").and_then(|id| id.as_u64()) == Some(alloc_work_id as u64)
                            && f.get("ac").and_then(|n| n.as_u64()).unwrap_or(0) > 0
                    })
                })
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        has_alloc_data,
        "allocating_work should have non-zero alloc_count (ac) in NDJSON output.\nContent:\n{content}"
    );
}

#[test]
fn async_main_with_return_type() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-main-result");
    create_async_main_return_type_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::tokio_seed());

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

#[test]
fn async_nested_select_self_time() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("nested-select-test");
    create_nested_select_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::tokio_seed());

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

#[test]
fn async_self_time_accuracy() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-selftime");
    create_async_self_time_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::tokio_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build: instrument parent_fn and expensive_child.
    let build = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "parent_fn",
            "--fn",
            "expensive_child",
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

    // Run the instrumented binary.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    let run_stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "instrumented binary failed:\nstdout: {}\nstderr: {run_stderr}",
        String::from_utf8_lossy(&run.stdout),
    );

    // Parse NDJSON output (always written by shutdown).
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let stats = common::aggregate_ndjson(&content);

    // Verify functions appear in the output.
    assert!(
        stats.contains_key("parent_fn"),
        "output should contain parent_fn. Got:\n{content}"
    );
    assert!(
        stats.contains_key("expensive_child"),
        "output should contain expensive_child. Got:\n{content}"
    );

    // Structural assertion derived from the program, not an arbitrary threshold.
    //
    // By construction:
    //   parent_fn does ZERO computation (body is just two .await calls)
    //   expensive_child does 4M wrapping_add iterations (black_box prevents folding)
    //
    // Therefore: parent_fn.self ~ 0  and  expensive_child.self >> 0
    // So: parent_fn.self_ns < expensive_child.self_ns  (always true if accounting works)
    //
    // PianoFuture carries the call stack inside each future's state machine,
    // so parent-child subtraction is correct regardless of thread migration.
    let child_self = stats.get("expensive_child").unwrap().self_ns;
    assert!(
        child_self > 0,
        "expensive_child should have non-zero self_ns (it does real work)"
    );

    let parent_stats = stats
        .get("parent_fn")
        .expect("parent_fn should appear in output");
    assert!(
        parent_stats.self_ns < child_self,
        "parent_fn.self_ns ({}) must be < expensive_child.self_ns \
         ({child_self}) -- parent does no computation, child does all of it",
        parent_stats.self_ns,
    );

    // PianoFuture carries real function names; no "<migrated>" bucket.
    assert!(
        !stats.contains_key("<migrated>"),
        "should not have a <migrated> bucket"
    );
}

#[test]
fn async_tokio_pipeline() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-test");
    create_async_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::tokio_seed());

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

    // Read the output and verify async function names appear.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("compute"),
        "output should contain instrumented async function 'compute'. Got:\n{content}"
    );
    assert!(
        content.contains("orchestrate"),
        "output should contain instrumented async function 'orchestrate'. Got:\n{content}"
    );
}
