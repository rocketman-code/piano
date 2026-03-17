//! Consolidated integration tests for minimal-dependency projects.
//!
//! Each test creates a small Rust project from scratch (no external deps beyond
//! piano-runtime), instruments it with `piano build`, runs the binary, and
//! verifies the output.  Grouping them in one file lets `cargo test` run all 7
//! in parallel (~1.5 s) instead of sequentially (~5.5 s).

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_cfg_gated_alloc_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "cfg-gated-alloc-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "cfg-gated-alloc-test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The user project declares a global allocator behind #[cfg(target_os = "foobar")].
    // This cfg never matches any real platform, so the allocator is compiled out.
    // Piano should inject a fallback PianoAllocator<System> behind the negated cfg.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use std::alloc::System;

#[cfg(target_os = "foobar")]
struct MyCustomAlloc;

#[cfg(target_os = "foobar")]
unsafe impl std::alloc::GlobalAlloc for MyCustomAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        System.dealloc(ptr, layout)
    }
}

#[cfg(target_os = "foobar")]
#[global_allocator]
static ALLOC: MyCustomAlloc = MyCustomAlloc;

fn do_allocs() -> usize {
    let mut total = 0usize;
    for i in 0..100 {
        let v: Vec<u8> = vec![0u8; (i + 1) * 64];
        total += v.len();
        std::hint::black_box(&v);
    }
    total
}

fn main() {
    let n = do_allocs();
    println!("total: {n}");
}
"#,
    )
    .unwrap();
}

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

/// Create a project with `[[bin]] path = "src/myapp/main.rs"`.
fn create_custom_bin_project(root: &Path) {
    fs::create_dir_all(root.join("src").join("myapp")).unwrap();

    fs::write(
        root.join("Cargo.toml"),
        r#"[package]
name = "myapp"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "myapp"
path = "src/myapp/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        root.join("src").join("myapp").join("main.rs"),
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

/// Create a project with a main loop that calls instrumented functions per frame.
fn create_frame_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "frame_test"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "frame_test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    // Simulate 5 frames
    for _ in 0..5 {
        update();
    }
    println!("done");
}

fn update() {
    // Do some work and allocate
    let v: Vec<u8> = vec![0u8; 256];
    std::hint::black_box(&v);
    physics();
}

fn physics() {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    std::hint::black_box(sum);
}
"#,
    )
    .unwrap();
}

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn cfg_gated_allocator_reports_nonzero() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("cfg-gated-alloc-test");
    create_cfg_gated_alloc_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with instrumentation on do_allocs and main.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "do_allocs", "--fn", "main", "--project"])
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
        program_stdout.contains("total:"),
        "program should produce output, got: {program_stdout}"
    );

    // Find run output file.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    // NDJSON format:
    //   Header: {"type":"header","bias_ns":N,"names":{"0":"do_allocs",...}}
    //   Measurement: {"span_id":N,...,"alloc_count":N,"alloc_bytes":N}
    //   Trailer: {"type":"trailer","bias_ns":N,"names":{"0":"do_allocs",...}}

    let stats = common::aggregate_ndjson(&content);

    let alloc_stats = stats
        .get("do_allocs")
        .expect("do_allocs should appear in output");

    assert!(
        alloc_stats.alloc_count > 0,
        "do_allocs should have non-zero alloc_count in NDJSON output.\n\
         This means the cfg-gated allocator fallback was not injected correctly.\n\
         Content:\n{content}"
    );
}

#[test]
fn cpu_time_flag_produces_cpu_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("cpu_test");
    create_cpu_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

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
    let run_file = common::largest_ndjson_file(&runs_dir);

    // Verify the run file contains CPU time data.
    let content = fs::read_to_string(&run_file).unwrap();

    // NDJSON measurements contain cpu_start_ns and cpu_end_ns fields.
    // With --cpu-time, at least one measurement should have non-zero cpu_end_ns.
    assert!(
        content.contains("\"cpu_start_ns\":"),
        "measurements should contain cpu_start_ns field. Got:\n{}",
        content.lines().next().unwrap_or("")
    );
    assert!(
        content.contains("\"cpu_end_ns\":"),
        "measurements should contain cpu_end_ns field"
    );

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

#[test]
fn custom_bin_path_produces_profiling_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myapp");
    create_custom_bin_project(&project);
    common::prepopulate_deps(&project, common::mini_seed());

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
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 4950"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify profiling data was actually written.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("compute"),
        "output should contain instrumented function name 'compute'"
    );
}

#[test]
fn exit_in_non_main_preserves_profiling_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-helper");
    create_exit_in_helper_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

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
    let stats = common::aggregate_ndjson(&content);

    assert!(
        stats.contains_key("work"),
        "output should contain work function. Got:\n{content}"
    );
}

#[test]
fn frame_pipeline_build_run_report() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("frame_test");
    create_frame_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with instrumentation on update and physics.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "update", "--fn", "physics", "--project"])
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

    // Run the instrumented binary with frame streaming enabled.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run_output = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .env("PIANO_STREAM_FRAMES", "1")
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("done"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify an NDJSON file was written.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    // NDJSON format: header has type + names, trailer has type + names.
    assert!(
        content.contains("\"type\":\"header\""),
        "output should contain NDJSON header"
    );
    assert!(
        content.contains("\"type\":\"trailer\""),
        "output should contain NDJSON trailer"
    );

    // Verify instrumented function names appear in the name table.
    let stats = common::aggregate_ndjson(&content);
    assert!(
        stats.contains_key("update"),
        "output should contain 'update'. Got keys: {:?}",
        stats.keys().collect::<Vec<_>>()
    );
    assert!(
        stats.contains_key("physics"),
        "output should contain 'physics'. Got keys: {:?}",
        stats.keys().collect::<Vec<_>>()
    );

    // Should have measurement spans: update is called 5 times, physics 5 times.
    assert!(
        stats["update"].calls == 5,
        "update should be called 5 times, got {}",
        stats["update"].calls
    );
    assert!(
        stats["physics"].calls == 5,
        "physics should be called 5 times, got {}",
        stats["physics"].calls
    );

    // Verify `piano report` works with ndjson (default view).
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
        report_stdout.contains("update"),
        "report should show 'update'"
    );
    assert!(
        !report_stdout.contains("p50"),
        "default report should not show percentile columns"
    );

    // Verify `piano report --frames` shows per-frame breakdown.
    let frames_output = Command::new(piano_bin)
        .args(["report", "--frames"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report --frames");

    assert!(
        frames_output.status.success(),
        "piano report --frames failed:\n{}",
        String::from_utf8_lossy(&frames_output.stderr)
    );

    let frames_stdout = String::from_utf8_lossy(&frames_output.stdout);
    assert!(
        frames_stdout.contains("Frame"),
        "frames view should have Frame column"
    );
    assert!(
        frames_stdout.contains("frames"),
        "frames view should show frame count"
    );
}

#[test]
fn project_with_deny_unsafe_code_builds_successfully() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("strict");
    create_strict_lint_project(&project);
    common::prepopulate_deps(&project, common::mini_seed());

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

#[test]
fn workspace_member_with_inherited_fields_builds() {
    let tmp = tempfile::tempdir().unwrap();
    let ws_root = tmp.path().join("ws");
    create_workspace_project(&ws_root);
    common::prepopulate_deps(&ws_root, common::mini_seed());

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
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("compute"),
        "output should contain instrumented function name 'compute'"
    );
}
