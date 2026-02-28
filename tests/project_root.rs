//! Tests for project root auto-detection.

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_mini_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"mini\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
}

#[test]
fn finds_root_from_project_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let root = piano::build::find_project_root(&project).unwrap();
    assert_eq!(root, project.canonicalize().unwrap());
}

#[test]
fn finds_root_from_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let subdir = project.join("src");
    let root = piano::build::find_project_root(&subdir).unwrap();
    assert_eq!(root, project.canonicalize().unwrap());
}

#[test]
fn finds_root_from_nested_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let deep = project.join("src").join("nested");
    fs::create_dir_all(&deep).unwrap();

    let root = piano::build::find_project_root(&deep).unwrap();
    assert_eq!(root, project.canonicalize().unwrap());
}

#[test]
fn errors_when_no_cargo_toml() {
    let tmp = tempfile::tempdir().unwrap();
    let empty = tmp.path().join("empty");
    fs::create_dir_all(&empty).unwrap();

    let result = piano::build::find_project_root(&empty);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("no Cargo.toml found"),
        "error should mention Cargo.toml, got: {msg}"
    );
}

#[test]
fn build_auto_detects_project_from_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Run piano build from src/ subdirectory, without --project
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "main", "--runtime-path"])
        .arg(&runtime_path)
        .current_dir(project.join("src"))
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "piano build from subdirectory failed:\nstderr: {stderr}\nstdout: {stdout}"
    );
}

#[test]
fn report_works_from_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build and run to generate profiling data
    let output = Command::new(piano_bin)
        .args(["profile", "--fn", "main", "--project"])
        .arg(&project)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano profile");

    assert!(
        output.status.success(),
        "piano profile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Now run piano report from src/ subdirectory, without PIANO_RUNS_DIR
    let report_output = Command::new(piano_bin)
        .args(["report"])
        .current_dir(project.join("src"))
        .output()
        .expect("failed to run piano report");

    let stdout = String::from_utf8_lossy(&report_output.stdout);
    let stderr = String::from_utf8_lossy(&report_output.stderr);

    assert!(
        report_output.status.success(),
        "piano report from subdirectory failed:\nstderr: {stderr}\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("main"),
        "report should show 'main' function, got: {stdout}"
    );
}

#[test]
fn run_works_from_subdirectory() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("myproject");
    create_mini_project(&project);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build first
    let build_output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    assert!(
        build_output.status.success(),
        "piano build failed: {}",
        String::from_utf8_lossy(&build_output.stderr)
    );

    // Run from src/ subdirectory
    let run_output = Command::new(piano_bin)
        .arg("run")
        .current_dir(project.join("src"))
        .output()
        .expect("failed to run piano run");

    let stderr = String::from_utf8_lossy(&run_output.stderr);

    assert!(
        run_output.status.success(),
        "piano run from subdirectory failed:\nstderr: {stderr}"
    );
}
