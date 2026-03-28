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

    // With --cpu-time, aggregate lines should contain cpu_self_ns.
    assert!(
        content.contains("\"cpu_self_ns\":"),
        "aggregates should contain cpu_self_ns field. Got:\n{}",
        content.lines().next().unwrap_or("")
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

// ---------------------------------------------------------------------------
// Integration glue: allocator case 2 (user has #[global_allocator], no cfg)
//
// User wraps System in their own allocator. Piano must detect it, wrap
// their allocator in PianoAllocator, and produce nonzero alloc tracking.
// ---------------------------------------------------------------------------

fn create_custom_allocator_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "custom-alloc"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "custom-alloc"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"use std::alloc::{GlobalAlloc, Layout, System};

struct MyAlloc;

unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: MyAlloc = MyAlloc;

fn do_allocs() -> usize {
    let mut total = 0usize;
    for i in 0..50 {
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

#[test]
fn custom_allocator_wrapped_reports_nonzero() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("custom-alloc");
    create_custom_allocator_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
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
    let run_output = Command::new(binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    let stats = common::aggregate_ndjson(&content);
    let do_allocs = stats
        .get("do_allocs")
        .expect("do_allocs should be in output");

    assert!(
        do_allocs.alloc_count > 0,
        "custom allocator wrapping must produce nonzero alloc_count, got 0"
    );
    assert!(
        do_allocs.alloc_bytes > 0,
        "custom allocator wrapping must produce nonzero alloc_bytes, got 0"
    );
}

// ---------------------------------------------------------------------------
// Integration glue: multi-file instrumentation
//
// Project with `mod db;` where functions in db.rs also need instrumentation.
// The wrapper must instrument BOTH main.rs and db.rs.
// ---------------------------------------------------------------------------

fn create_multi_file_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "multi-file"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "multi-file"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"mod db;

fn process() -> u64 {
    let mut sum = 0u64;
    for i in 0..100 {
        sum += i;
    }
    sum
}

fn main() {
    let a = process();
    let b = db::query();
    println!("results: {a} {b}");
}
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("db.rs"),
        r#"pub fn query() -> u64 {
    let mut sum = 0u64;
    for i in 0..200 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn multi_file_both_files_instrumented() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("multi-file");
    create_multi_file_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
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
    let run_output = Command::new(binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("results: 4950 19900"),
        "program should produce correct output, got: {program_stdout}"
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    let stats = common::aggregate_ndjson(&content);

    // Both process (main.rs) and query (db.rs) must be instrumented
    assert!(
        stats.contains_key("process"),
        "process() from main.rs must be instrumented. Found: {:?}",
        stats.keys().collect::<Vec<_>>()
    );
    assert!(
        stats.contains_key("db::query"),
        "db::query() from db.rs must be instrumented. Found: {:?}",
        stats.keys().collect::<Vec<_>>()
    );

    // Both must have been called
    let process = stats.get("process").unwrap();
    assert_eq!(process.calls, 1, "process should be called once");

    let query = stats.get("db::query").unwrap();
    assert_eq!(query.calls, 1, "db::query should be called once");
}

// ---------------------------------------------------------------------------
// Integration: multi-injection error line preservation
//
// Entry point gets multiple injections (guard, name table, allocator,
// lifecycle). A compilation error must show the ORIGINAL line number.
// ---------------------------------------------------------------------------

fn create_multi_injection_error_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "four-pass-error"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "four-pass-error"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The type error is on line 9 of the original source (let x: u32 = work()).
    // Multiple injections (guard in work, lifecycle in main, name table,
    // allocator) all affect the compiled file. Zero-shift + original-source
    // display must preserve line 9.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn work() -> u64 {
    let mut sum = 0u64;
    for i in 0..100 {
        sum += i;
    }
    sum
}
fn main() {
    let x: u32 = work();
    println!("{x}");
}
"#,
    )
    .unwrap();
}

#[test]
fn multi_injection_preserves_line_numbers() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("multi-inj-error");
    create_multi_injection_error_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    // Build should FAIL (type error: u64 assigned to u32)
    assert!(
        !output.status.success(),
        "build should fail due to type error"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The error should reference line 9 (the `let x: u32 = work();` line)
    // not a shifted line from the instrumented source.
    assert!(
        stderr.contains(":9:") || stderr.contains("main.rs:9"),
        "error should reference original line 9, got:\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// Integration glue: entry point NOT in targets
//
// User does `piano build --fn work` where work() is in db.rs but main.rs
// has no measured functions. The wrapper must still inject the lifecycle
// (name table, allocator, shutdown) into main.rs even though main.rs
// has zero measured functions.
// ---------------------------------------------------------------------------

fn create_entry_point_not_in_targets_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "ep-not-target"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "ep-not-target"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"mod worker;

fn main() {
    let result = worker::heavy_work();
    println!("result: {result}");
}
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("worker.rs"),
        r#"pub fn heavy_work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn entry_point_not_in_targets_still_instruments() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("ep-not-target");
    create_entry_point_not_in_targets_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Only instrument heavy_work, not main. main.rs is the entry point
    // but has no measured functions.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "heavy_work", "--project"])
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
    let run_output = Command::new(binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    let stats = common::aggregate_ndjson(&content);

    assert!(
        stats.contains_key("worker::heavy_work"),
        "heavy_work must be instrumented even though entry point is not a target. Found: {:?}",
        stats.keys().collect::<Vec<_>>()
    );

    let hw = stats.get("worker::heavy_work").unwrap();
    assert_eq!(hw.calls, 1, "heavy_work should be called once");
}

// ---------------------------------------------------------------------------
// Integration glue: multiple inner attributes in main()
//
// If main() has multiple #![...] inner attributes, the guard/shutdown
// injection must go AFTER all of them, not between them.
// ---------------------------------------------------------------------------

fn create_multi_inner_attr_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "multi-attr"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "multi-attr"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn work() -> u64 {
    #![allow(unused_variables)]
    #![allow(unused_assignments)]
    let mut x = 0u64;
    x = 42;
    x
}

fn main() {
    let result = work();
    println!("result: {result}");
}
"#,
    )
    .unwrap();
}

#[test]
fn multiple_inner_attributes_handled_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("multi-attr");
    create_multi_inner_attr_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build with multiple inner attrs failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();
    let run_output = Command::new(binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary with multiple inner attrs failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 42"),
        "program should produce correct output, got: {program_stdout}"
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    let stats = common::aggregate_ndjson(&content);
    assert!(
        stats.contains_key("work"),
        "work() with multiple inner attrs must be instrumented"
    );
}

// ---------------------------------------------------------------------------
// Integration glue: allocator with complex cfg predicate
//
// #[cfg(all(target_os = "linux", feature = "custom-alloc"))]
// Piano must negate this correctly for the fallback.
// ---------------------------------------------------------------------------

fn create_complex_cfg_allocator_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "complex-cfg-alloc"
version = "0.1.0"
edition = "2024"

[features]
custom-alloc = []

[[bin]]
name = "complex-cfg-alloc"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // Allocator behind #[cfg(all(...))] -- never active in test
    // (feature "custom-alloc" is not enabled)
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use std::alloc::{GlobalAlloc, Layout, System};

struct MyAlloc;

unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[cfg(all(target_os = "foobar", feature = "custom-alloc"))]
#[global_allocator]
static ALLOC: MyAlloc = MyAlloc;

fn do_allocs() -> usize {
    let mut total = 0usize;
    for i in 0..50 {
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

#[test]
fn complex_cfg_allocator_negation_works() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("complex-cfg-alloc");
    create_complex_cfg_allocator_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build with complex cfg allocator failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();
    let run_output = Command::new(binary_path)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary with complex cfg allocator failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    let runs_dir = project_dir.join("target/piano/runs");
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    let stats = common::aggregate_ndjson(&content);
    let do_allocs = stats
        .get("do_allocs")
        .expect("do_allocs should be in output");

    assert!(
        do_allocs.alloc_count > 0,
        "complex cfg allocator with fallback must produce nonzero alloc_count"
    );
}
