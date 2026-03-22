//! Test that macro_rules! definitions with fn items get instrumented correctly.
//! Covers the full pipeline (build + run + verify NDJSON output) and a
//! unit-level syntax validation test.

mod common;

use std::collections::{HashMap, HashSet};
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

    // Uses literal-name functions in macros (not metavar names) so they
    // get profiling guards. Metavar-name functions are skipped because
    // the function name is unknown at rewrite time.
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

    // Run the instrumented binary. The runs dir is hardcoded at build time
    // inside the project's target/piano/runs.
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

    // Program correctness: compute() = 4950, alpha() = 42, beta() = 99.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("results: 4950 42 99"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Verify run file contains the macro-generated function names.
    let run_file = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&run_file).unwrap();

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

    // main() is excluded from the name table -- lifecycle boundary, not a profiled function.
    assert!(
        !content.contains("\"main\""),
        "output should NOT contain 'main' (lifecycle boundary, excluded from name table)"
    );
}

#[test]
fn instrumented_macro_output_is_valid_syntax() {
    // Use instrument_source directly and verify the output parses as valid Rust.
    // Uses a literal-name function so profiling guards are injected.
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

    let targets: HashMap<String, u32> = HashMap::new();
    let all_instrumentable: HashSet<String> = HashSet::new();
    // Pre-scan to discover macro function names and assign IDs.
    let macro_names = piano::rewrite::discover_macro_fn_names(source, "").unwrap();
    let mut macro_name_ids: HashMap<String, u32> = HashMap::new();
    for (i, name) in macro_names.iter().enumerate() {
        macro_name_ids.insert(name.clone(), i as u32);
    }
    let result = piano::rewrite::instrument_source(source, &targets, &all_instrumentable, true, "", &macro_name_ids)
        .expect("instrument_source should succeed");

    // The instrumented source must parse as valid Rust.
    let re_parse = ra_ap_syntax::SourceFile::parse(&result.source, ra_ap_syntax::Edition::Edition2024);
    let errors: Vec<_> = re_parse.errors().to_vec();
    assert!(
        errors.is_empty(),
        "instrumented macro output should be valid Rust syntax. Errors: {:?}\nSource:\n{}",
        errors,
        result.source
    );

    // Verify profiling guards were injected (strip whitespace to tolerate prettyplease reformatting).
    let stripped: String = result.source.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        stripped.contains("__piano_ctx.enter("),
        "instrumented source should contain profiling guards.\nSource:\n{}",
        result.source
    );
    // Verify ctx parameter was injected.
    assert!(
        stripped.contains("__piano_ctx:piano_runtime::ctx::Ctx"),
        "instrumented source should contain ctx parameter.\nSource:\n{}",
        result.source
    );
}

#[test]
fn macro_literal_fn_gets_registered() {
    let source = r#"
macro_rules! setup {
    () => {
        fn initialize() {
            start();
        }
    };
}
setup!();
fn main() {}
"#;
    let targets: HashMap<String, u32> = HashMap::new();
    let all_instrumentable: HashSet<String> = HashSet::new();
    // Pre-scan to discover macro function names.
    let macro_names = piano::rewrite::discover_macro_fn_names(source, "").unwrap();
    let mut macro_name_ids: HashMap<String, u32> = HashMap::new();
    for (i, name) in macro_names.iter().enumerate() {
        macro_name_ids.insert(name.clone(), i as u32);
    }
    let result = piano::rewrite::instrument_source(source, &targets, &all_instrumentable, true, "", &macro_name_ids)
        .expect("instrument_source should succeed");

    assert!(
        result.macro_fn_names.contains(&"initialize".to_string()),
        "literal fn name should be collected. Got: {:?}",
        result.macro_fn_names,
    );

    let names: Vec<(u32, &str)> = result
        .macro_fn_names
        .iter()
        .enumerate()
        .map(|(i, n)| (i as u32, n.as_str()))
        .collect();
    let (registered_source, _map) =
        piano::rewrite::inject_registrations(&result.source, &names)
            .expect("inject_registrations should succeed");
    assert!(
        registered_source.contains(r#"(0, "initialize")"#),
        "literal macro fn should appear in name table. Got:\n{registered_source}"
    );
}
