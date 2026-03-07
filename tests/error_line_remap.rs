//! Integration test: verify compilation errors show original (un-shifted) line
//! numbers and do not leak piano_runtime internals.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

/// Create a project with a deliberate type error at a known line.
fn create_project_with_error(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "errtest"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "errtest"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The type error is on line 8 ("hello" assigned to i32).
    // Lines 1-7 are valid; line 8 has the error.
    // After instrumentation, piano injects a guard line inside work(),
    // which would shift line 8 to line 9+ without remapping.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = work();
    println!("result: {result}");
}

fn work() -> u64 {
    let mut sum: u64 = 0;
    let bad: i32 = "hello";
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn compilation_error_shows_original_line_numbers() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("errtest");
    create_project_with_error(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    // Build SHOULD fail (there's a type error).
    assert!(
        !output.status.success(),
        "piano build should have failed due to type error"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The error should reference line 8 (original line), not a shifted line.
    assert!(
        stderr.contains("src/main.rs:8:") || stderr.contains(":8:"),
        "error should reference original line 8, got:\n{stderr}"
    );

    // Piano internals should NOT appear in the error output.
    assert!(
        !stderr.contains("piano_runtime::"),
        "piano_runtime should be filtered from errors:\n{stderr}"
    );
    assert!(
        !stderr.contains("__piano_guard"),
        "__piano_guard should be filtered from errors:\n{stderr}"
    );
}
