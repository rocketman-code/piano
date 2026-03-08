//! Test: workspace projects where the binary lives in a member crate
//! and there is no `src/` directory at the workspace root (e.g. ripgrep).

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a workspace project with the binary in a member crate and no
/// root-level `src/` directory.
fn create_workspace_project(root: &Path) {
    // Workspace root Cargo.toml — no [package], just [workspace].
    fs::create_dir_all(root).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["crates/cli"]
resolver = "2"
"#,
    )
    .unwrap();

    // Member crate with a binary.
    let member = root.join("crates/cli");
    fs::create_dir_all(member.join("src")).unwrap();

    fs::write(
        member.join("Cargo.toml"),
        r#"[package]
name = "ws-cli"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "ws-cli"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        member.join("src/main.rs"),
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
fn workspace_binary_in_member_crate_builds_from_member_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path().join("ws");
    create_workspace_project(&workspace_root);

    let member_dir = workspace_root.join("crates/cli");
    common::prepopulate_deps(&member_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&member_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build from member dir failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    // Run the instrumented binary and verify profiling output.
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

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("\"work\"") || content.contains("work"),
        "output should contain instrumented function name 'work' (from member dir)"
    );
}

#[test]
fn workspace_binary_in_member_crate_builds_from_workspace_root() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path().join("ws");
    create_workspace_project(&workspace_root);

    // Prepopulate deps at the workspace root (target dir lives there).
    common::prepopulate_deps(&workspace_root, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // This was the failing case in #520: --project points at workspace root,
    // which has no src/ directory.
    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&workspace_root)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build from workspace root failed:\nstderr: {stderr}\nstdout: {stdout}"
    );

    // Run the instrumented binary and verify profiling output.
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

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("\"work\"") || content.contains("work"),
        "output should contain instrumented function name 'work' (from workspace root)"
    );
}

/// Create a workspace with two member crates (cli and lib-user), each with
/// its own binary.  This exercises the multi-package branch in build dispatch
/// that the single-member tests above skip.
fn create_multi_member_workspace(root: &Path) {
    fs::create_dir_all(root).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["crates/cli", "crates/lib-user"]
resolver = "2"
"#,
    )
    .unwrap();

    // First member: cli
    let cli = root.join("crates/cli");
    fs::create_dir_all(cli.join("src")).unwrap();
    fs::write(
        cli.join("Cargo.toml"),
        r#"[package]
name = "ws-cli"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "ws-cli"
path = "src/main.rs"
"#,
    )
    .unwrap();
    fs::write(
        cli.join("src/main.rs"),
        r#"fn main() {
    let result = cli_work();
    println!("cli: {result}");
}

fn cli_work() -> u64 {
    (0..500u64).sum()
}
"#,
    )
    .unwrap();

    // Second member: lib-user
    let lib_user = root.join("crates/lib-user");
    fs::create_dir_all(lib_user.join("src")).unwrap();
    fs::write(
        lib_user.join("Cargo.toml"),
        r#"[package]
name = "ws-lib-user"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "ws-lib-user"
path = "src/main.rs"
"#,
    )
    .unwrap();
    fs::write(
        lib_user.join("src/main.rs"),
        r#"fn main() {
    let result = lib_work();
    println!("lib: {result}");
}

fn lib_work() -> u64 {
    (0..200u64).sum()
}
"#,
    )
    .unwrap();
}

#[test]
fn multi_member_workspace_builds_with_bin_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path().join("multi-ws");
    create_multi_member_workspace(&workspace_root);

    common::prepopulate_deps(&workspace_root, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // From workspace root with --bin to select a specific member binary.
    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&workspace_root)
        .arg("--bin")
        .arg("ws-cli")
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build with --bin from workspace root failed:\nstderr: {stderr}\nstdout: {stdout}"
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

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("cli: 124750"),
        "program should produce correct output, got: {program_stdout}"
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("cli_work"),
        "output should contain instrumented function name 'cli_work'"
    );
}

#[test]
fn multi_member_workspace_builds_from_member_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path().join("multi-ws");
    create_multi_member_workspace(&workspace_root);

    let member_dir = workspace_root.join("crates/lib-user");
    common::prepopulate_deps(&member_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // From member directory without --bin: exercises find_current_package path.
    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&member_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build from member dir failed:\nstderr: {stderr}\nstdout: {stdout}"
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

    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("lib: 19900"),
        "program should produce correct output, got: {program_stdout}"
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();
    assert!(
        content.contains("lib_work"),
        "output should contain instrumented function name 'lib_work'"
    );
}
