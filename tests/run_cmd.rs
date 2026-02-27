//! Integration tests for `piano profile` and `piano run`.

use std::fs;
use std::path::Path;
use std::process::Command;

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
fn run_command_builds_executes_and_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("mini");
    create_mini_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let runs_dir = tmp.path().join("runs");

    let output = Command::new(piano_bin)
        .args(["profile", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano profile");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano profile failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    // The program's own stdout should appear (inherited).
    assert!(
        stdout.contains("result: 499500"),
        "program output should appear, got: {stdout}"
    );

    // stderr should contain build info.
    assert!(
        stderr.contains("built:"),
        "should show built path on stderr, got: {stderr}"
    );

    // Report table goes to stdout (cmd_report uses print!).
    assert!(
        stdout.contains("work"),
        "report should appear on stdout with 'work' function, got: {stdout}"
    );
}

fn create_exit_one_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "exit-one"
version = "0.1.0"
edition = "2024"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    work();
    std::process::exit(1);
}

fn work() -> u64 {
    let mut sum = 0u64;
    for i in 0..100 {
        sum += i;
    }
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn run_command_warns_on_nonzero_exit_code() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-one");
    create_exit_one_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let runs_dir = tmp.path().join("runs");

    // Without --ignore-exit-code: should contain warning.
    let output = Command::new(piano_bin)
        .args(["profile", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano profile");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("exited with code"),
        "should warn about non-zero exit code without --ignore-exit-code, got: {stderr}"
    );
}

#[test]
fn run_command_ignore_exit_code_suppresses_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-one");
    create_exit_one_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let runs_dir = tmp.path().join("runs");

    // With --ignore-exit-code: warning should be suppressed.
    let output = Command::new(piano_bin)
        .args(["profile", "--fn", "work", "--ignore-exit-code", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano profile with --ignore-exit-code");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("exited with code"),
        "should NOT warn about exit code with --ignore-exit-code, got: {stderr}"
    );
}

#[test]
fn profile_suppresses_no_runs_error_on_nonzero_exit() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-one");
    create_exit_one_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Create the runs dir so cmd_report hits NoRuns (empty dir) not an IO error.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let output = Command::new(piano_bin)
        .args(["profile", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano profile");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should contain the exit code warning.
    assert!(
        stderr.contains("exited with code"),
        "should warn about non-zero exit, got: {stderr}"
    );

    // Should NOT contain the NoRuns error -- that's cascading noise.
    assert!(
        !stderr.contains("no piano runs found"),
        "should suppress NoRuns when program exited non-zero, got: {stderr}"
    );
}

fn create_echo_args_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "echo-args"
version = "0.1.0"
edition = "2024"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    println!("args: {}", args.join(" "));
}
"#,
    )
    .unwrap();
}

#[test]
fn run_command_passes_args_after_separator() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("echo-args");
    create_echo_args_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let runs_dir = tmp.path().join("runs");

    let output = Command::new(piano_bin)
        .args(["profile", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .args(["--", "--input", "data.csv", "--verbose"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano profile with args");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "piano profile with args failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    assert!(
        stdout.contains("args: --input data.csv --verbose"),
        "program should receive the passed args, got: {stdout}"
    );
}

#[test]
fn run_executes_last_built_binary() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("mini");
    create_mini_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Step 1: build
    let build_output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    assert!(
        build_output.status.success(),
        "piano build failed: {}",
        String::from_utf8_lossy(&build_output.stderr)
    );

    // Step 2: run (from project directory so target/piano/debug/ is found)
    let run_output = Command::new(piano_bin)
        .arg("run")
        .current_dir(&project_dir)
        .output()
        .expect("failed to run piano run");

    let stdout = String::from_utf8_lossy(&run_output.stdout);
    let stderr = String::from_utf8_lossy(&run_output.stderr);

    assert!(
        run_output.status.success(),
        "piano run failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    assert!(
        stdout.contains("result: 499500"),
        "program output should appear, got stdout: {stdout}"
    );

    assert!(
        stderr.contains("running:"),
        "should show which binary is running, got stderr: {stderr}"
    );
}

#[test]
fn run_passes_args_via_separator() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("echo-args");
    create_echo_args_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build first
    let build_output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    assert!(build_output.status.success());

    // Run with args
    let run_output = Command::new(piano_bin)
        .args(["run", "--", "--input", "data.csv", "--verbose"])
        .current_dir(&project_dir)
        .output()
        .expect("failed to run piano run with args");

    let stdout = String::from_utf8_lossy(&run_output.stdout);

    assert!(
        run_output.status.success(),
        "piano run with args failed: {}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    assert!(
        stdout.contains("args: --input data.csv --verbose"),
        "program should receive the passed args, got: {stdout}"
    );
}

#[test]
fn run_errors_when_no_binary_exists() {
    let tmp = tempfile::tempdir().unwrap();

    let piano_bin = env!("CARGO_BIN_EXE_piano");

    // Run from empty directory -- no target/piano/debug/
    let output = Command::new(piano_bin)
        .arg("run")
        .current_dir(tmp.path())
        .output()
        .expect("failed to run piano run");

    assert!(
        !output.status.success(),
        "piano run should fail when no binary exists"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("piano build"),
        "error should mention piano build, got: {stderr}"
    );
}
