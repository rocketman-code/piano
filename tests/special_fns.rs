//! Test that const, unsafe, and extern fn are skipped during instrumentation.
//! The instrumented project should build and run successfully, with only
//! normal functions appearing in the output.

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_project_with_special_fns(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "special"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "special"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"const fn fixed_size() -> usize { 42 }

unsafe fn dangerous() -> i32 { 99 }

extern "C" fn ffi_callback() -> i32 { 7 }

fn normal_work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum += i;
    }
    sum
}

fn main() {
    let a = fixed_size();
    let b = unsafe { dangerous() };
    let c = ffi_callback();
    let d = normal_work();
    println!("results: {a} {b} {c} {d}");
}
"#,
    )
    .unwrap();
}

#[test]
fn special_fns_are_skipped_during_instrumentation() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("special");
    create_project_with_special_fns(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with no target filter (instrument everything).
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

    // Run the instrumented binary.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let binary_path = stdout.trim();
    let run_output = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\n{}",
        String::from_utf8_lossy(&run_output.stderr)
    );

    // Program should produce correct output.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("results: 42 99 7 499500"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify run file contains normal_work but NOT const/unsafe/extern fns.
    let run_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "json" || ext == "ndjson")
        })
        .collect();

    assert!(!run_files.is_empty(), "expected at least one run file");

    let content = fs::read_to_string(run_files[0].path()).unwrap();
    assert!(
        content.contains("\"normal_work\""),
        "output should contain normal_work"
    );
    assert!(
        !content.contains("\"fixed_size\""),
        "output should NOT contain const fn fixed_size"
    );
    assert!(
        !content.contains("\"dangerous\""),
        "output should NOT contain unsafe fn dangerous"
    );
    assert!(
        !content.contains("\"ffi_callback\""),
        "output should NOT contain extern fn ffi_callback"
    );
}
