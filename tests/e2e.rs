//! End-to-end test: create a project, instrument it, build it, run it, verify output.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;
#[cfg(unix)]
use std::time::Duration;

/// Create a minimal Rust project that we can instrument.
fn create_mini_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "mini"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "mini"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = work();
    println!("result: {result}");
}

fn work() -> u64 {
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
fn full_pipeline_instrument_build_run_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("mini");
    create_mini_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    // Find the piano binary (built by cargo test).
    let piano_bin = env!("CARGO_BIN_EXE_piano");

    // Locate the piano-runtime source directory (sibling to the piano binary crate).
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Run `piano build --fn work --project <dir> --runtime-path <path>`.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
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

    // stdout should contain the path to the built binary.
    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

    // Run the instrumented binary with PIANO_RUNS_DIR pointing to a temp dir.
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

    // The program output should still work.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("result: 499500"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify a run file was written.
    let run_file = common::largest_ndjson_file(&runs_dir);

    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("\"work\"") || content.contains("work"),
        "output should contain instrumented function name 'work'"
    );
    assert!(
        content.contains("\"timestamp_ms\""),
        "output should contain timestamp_ms"
    );
    assert!(
        content.contains("\"run_id\":\""),
        "output should contain run_id"
    );

    // Verify `piano report` with latest-run consolidation.
    let report_output = Command::new(piano_bin)
        .args(["report"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report (latest)");

    assert!(
        report_output.status.success(),
        "piano report (latest) failed:\n{}",
        String::from_utf8_lossy(&report_output.stderr)
    );

    let report_stdout = String::from_utf8_lossy(&report_output.stdout);
    assert!(
        report_stdout.contains("work"),
        "report should show the 'work' function"
    );

    // Verify `piano report` can also read a specific file.
    let specific_report = Command::new(piano_bin)
        .args(["report"])
        .arg(&run_file)
        .output()
        .expect("failed to run piano report (specific)");

    assert!(
        specific_report.status.success(),
        "piano report (specific) failed:\n{}",
        String::from_utf8_lossy(&specific_report.stderr)
    );
}

#[test]
fn build_with_no_targets_instruments_all_functions() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("mini");
    create_mini_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // No --fn, --file, or --mod: should instrument everything.
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
        "piano build (no targets) failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    // Should have found both main and work (the two functions in mini project).
    assert!(
        stderr.contains("found 2 function(s)"),
        "should instrument both functions, got: {stderr}"
    );

    // Run the instrumented binary and verify it produces output.
    let binary_path = stdout.trim();
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
        program_stdout.contains("result: 499500"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify run file contains both functions.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(content.contains("\"main\""), "output should contain 'main'");
    assert!(content.contains("\"work\""), "output should contain 'work'");
}

#[test]
fn report_no_runs_shows_recovery_guidance() {
    let tmp = tempfile::tempdir().unwrap();
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let piano_bin = env!("CARGO_BIN_EXE_piano");

    let output = Command::new(piano_bin)
        .args(["report"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano report");

    assert!(
        !output.status.success(),
        "piano report with no runs should fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("piano profile"),
        "NoRuns error should include recovery guidance mentioning `piano profile`, got: {stderr}"
    );
}

#[test]
fn multi_file_same_name_gets_module_qualified_names() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("multimod");
    fs::create_dir_all(project_dir.join("src")).unwrap();

    fs::write(
        project_dir.join("Cargo.toml"),
        r#"[package]
name = "multimod"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "multimod"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        project_dir.join("src").join("main.rs"),
        r#"mod db;

fn main() {
    let a = process();
    let b = db::process();
    println!("{a} {b}");
}

fn process() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();

    fs::write(
        project_dir.join("src").join("db.rs"),
        r#"pub fn process() -> u64 {
    let mut sum = 0u64;
    for i in 0..500 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();

    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // No --fn flag: instrument everything.
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

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    let stats = common::aggregate_ndjson(&content);

    // The crate-root process() should appear as "process" (no module prefix).
    assert!(
        stats.contains_key("process"),
        "should have 'process' from crate root, got keys: {:?}",
        stats.keys().collect::<Vec<_>>()
    );
    assert!(
        stats["process"].calls >= 1,
        "process should have at least 1 call"
    );

    // The db module's process() should appear as "db::process".
    assert!(
        stats.contains_key("db::process"),
        "should have 'db::process' from db module, got keys: {:?}",
        stats.keys().collect::<Vec<_>>()
    );
    assert!(
        stats["db::process"].calls >= 1,
        "db::process should have at least 1 call"
    );
}

// ---------------------------------------------------------------------------
// Signal recovery
// ---------------------------------------------------------------------------

/// Create a project whose binary sleeps long enough for us to send SIGTERM.
#[cfg(unix)]
fn create_sleeping_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "sleeper"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "sleeper"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The binary calls `work()` (instrumented), then sleeps for 60 seconds.
    // We send SIGTERM while it sleeps; the signal handler should flush the
    // profiling data that `work()` already recorded.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let _ = work();
    // Sleep long enough for the test harness to send SIGTERM.
    std::thread::sleep(std::time::Duration::from_secs(60));
}

fn work() -> u64 {
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

/// Verify the full signal-recovery pipeline: instrumented binary receives
/// SIGTERM, the signal handler fires, and profiling data is flushed to disk.
#[cfg(unix)]
#[test]
fn signal_recovery_flushes_profiling_data_on_sigterm() {
    const SIGTERM: i32 = 15;

    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("sleeper");
    create_sleeping_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build the instrumented binary.
    let build_output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--fn", "main", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&build_output.stderr);
    let stdout = String::from_utf8_lossy(&build_output.stdout);
    assert!(
        build_output.status.success(),
        "piano build failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    let binary_path = stdout.trim();

    // Spawn the instrumented binary (it will sleep for 60s).
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let mut child = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .spawn()
        .expect("failed to spawn instrumented binary");

    // Give the binary time to start, call work(), and reach the sleep.
    std::thread::sleep(Duration::from_secs(2));

    // Send SIGTERM to the child process.
    let pid = child.id() as i32;
    let ret = unsafe { kill(pid, SIGTERM) };
    assert_eq!(ret, 0, "kill(2) should succeed");

    // Wait for the child to exit (should be quick after SIGTERM).
    let status = child.wait().expect("failed to wait on child");

    // On Unix, a process killed by SIGTERM exits via signal, not success.
    assert!(
        !status.success(),
        "process should have been terminated by signal, got: {status}"
    );

    // Verify that the signal handler flushed profiling data.
    let entries: Vec<_> = fs::read_dir(&runs_dir)
        .expect("runs dir should exist")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
        .collect();

    assert!(
        !entries.is_empty(),
        "signal handler should have flushed at least one .ndjson file to {runs_dir:?}"
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    assert!(
        !content.is_empty(),
        "NDJSON output should be non-empty after signal recovery"
    );
    assert!(
        content.contains("work"),
        "output should contain the instrumented function name 'work'"
    );
    assert!(
        content.contains("\"timestamp_ms\""),
        "output should contain timestamp_ms"
    );
}
