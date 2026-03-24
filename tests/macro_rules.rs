//! Test that macro_rules! definitions with fn items get instrumented correctly.

mod common;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;

fn create_project_with_macro_fns(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "macro-fns"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "macro-fns"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"macro_rules! setup_compute {
    () => {
        fn compute() -> u64 {
            let mut sum = 0u64;
            for i in 0..100 {
                sum += i;
            }
            sum
        }
    };
}

macro_rules! setup_pair {
    () => {
        fn alpha() -> u64 { 42 }
        fn beta() -> u64 { 99 }
    };
}

setup_compute!();
setup_pair!();

fn main() {
    let a = compute();
    let b = alpha();
    let c = beta();
    println!("results: {a} {b} {c}");
}
"#,
    )
    .unwrap();
}

#[test]
fn macro_generated_fns_appear_in_output() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("macro-fns");
    create_project_with_macro_fns(&project_dir);
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

    let runs_dir = project_dir.join("target/piano/runs");
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
        program_stdout.contains("results: 4950 42 99"),
        "program should produce correct output, got: {program_stdout}"
    );

    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

    assert!(content.contains("\"compute\""), "should contain 'compute'");
    assert!(content.contains("\"alpha\""), "should contain 'alpha'");
    assert!(content.contains("\"beta\""), "should contain 'beta'");
    assert!(!content.contains("\"main\""), "should NOT contain 'main'");
}

#[test]
fn instrumented_macro_output_is_valid_syntax() {
    let source = r#"
macro_rules! setup {
    () => {
        fn initialize() -> u64 {
            let mut sum = 0u64;
            for i in 0..100 {
                sum += i;
            }
            sum
        }
    };
}

setup!();

fn main() {
    let a = initialize();
    println!("result: {a}");
}
"#;

    let measured: HashMap<String, u32> =
        [("initialize".into(), 0), ("main".into(), 1)].into_iter().collect();
    let result = piano::rewrite::instrument_source(source, &measured, None)
        .expect("instrument_source should succeed");

    let re_parse = ra_ap_syntax::SourceFile::parse(
        &result.source, ra_ap_syntax::Edition::Edition2024,
    );
    let errors: Vec<_> = re_parse.errors().to_vec();
    assert!(
        errors.is_empty(),
        "instrumented output should be valid Rust. Errors: {:?}\nSource:\n{}",
        errors, result.source
    );

    assert!(
        result.source.contains("piano_runtime::enter(0)"),
        "should contain guard for initialize. Got:\n{}", result.source
    );
}

#[test]
fn macro_literal_fn_gets_guard() {
    let source = r#"
macro_rules! setup {
    () => {
        fn initialize() {
            let _ = 1;
        }
    };
}
setup!();
fn main() {}
"#;
    let measured: HashMap<String, u32> = [("initialize".into(), 0)].into_iter().collect();
    let result = piano::rewrite::instrument_source(source, &measured, None)
        .expect("instrument_source should succeed");

    assert!(
        result.source.contains("piano_runtime::enter(0)"),
        "literal fn in macro should get a guard. Got:\n{}", result.source
    );
}
