//! Integration test for `piano profile` — build + execute + report in one step.

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
