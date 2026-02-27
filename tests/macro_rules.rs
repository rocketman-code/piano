//! Test that macro_rules! definitions with fn items get instrumented correctly.
//! Covers the full pipeline (build + run + verify NDJSON output) and a
//! unit-level syntax validation test.

use std::collections::HashSet;
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
        r#"macro_rules! make_handler {
    ($name:ident) => {
        fn $name() -> u64 {
            let mut sum = 0u64;
            for i in 0..100 {
                sum += i;
            }
            sum
        }
    };
}

macro_rules! make_pair {
    ($a:ident, $b:ident) => {
        fn $a() -> u64 { 42 }
        fn $b() -> u64 { 99 }
    };
}

make_handler!(compute);
make_pair!(alpha, beta);

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

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with no target filter -- activates instrument_macros = true.
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

    // Program correctness: compute() = 4950, alpha() = 42, beta() = 99.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("results: 4950 42 99"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify run file contains the macro-generated function names.
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

    // Metavar-generated functions should appear by their expanded names.
    assert!(
        content.contains("\"compute\""),
        "output should contain macro-generated function 'compute'"
    );
    assert!(
        content.contains("\"alpha\""),
        "output should contain macro-generated function 'alpha'"
    );
    assert!(
        content.contains("\"beta\""),
        "output should contain macro-generated function 'beta'"
    );

    // main should also be instrumented (no target filter = instrument all).
    assert!(content.contains("\"main\""), "output should contain 'main'");
}

#[test]
fn instrumented_macro_output_is_valid_syntax() {
    // Use instrument_source directly and verify the output parses as valid Rust.
    let source = r#"
macro_rules! make_handler {
    ($name:ident) => {
        fn $name() -> u64 {
            let mut sum = 0u64;
            for i in 0..100 {
                sum += i;
            }
            sum
        }
    };
}

macro_rules! make_pair {
    ($a:ident, $b:ident) => {
        fn $a() -> u64 { 42 }
        fn $b() -> u64 { 99 }
    };
}

make_handler!(compute);
make_pair!(alpha, beta);

fn main() {
    let a = compute();
    let b = alpha();
    let c = beta();
    println!("results: {a} {b} {c}");
}
"#;

    let targets: HashSet<String> = HashSet::new();
    let result = piano::rewrite::instrument_source(source, &targets, true)
        .expect("instrument_source should succeed");

    // The instrumented source must parse as valid Rust.
    let parsed: Result<syn::File, _> = syn::parse_str(&result.source);
    assert!(
        parsed.is_ok(),
        "instrumented macro output should be valid Rust syntax. Error: {:?}\nSource:\n{}",
        parsed.err(),
        result.source
    );

    // Verify the guards were actually injected.
    assert!(
        result.source.contains("piano_runtime::enter"),
        "instrumented source should contain piano_runtime::enter guards.\nSource:\n{}",
        result.source
    );
}
