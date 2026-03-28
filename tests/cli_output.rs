//! CLI interaction contract: test stderr messages, exit codes, and progress
//! output for build, run, report, diff, and tag commands.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn piano_bin() -> &'static str {
    env!("CARGO_BIN_EXE_piano")
}

fn runtime_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("piano-runtime")
}

// ---------------------------------------------------------------------------
// Tag CLI
// ---------------------------------------------------------------------------

#[test]
fn tag_list_no_runs_dir_shows_no_tags() {
    let tmp = tempfile::tempdir().unwrap();
    // Empty dir with no runs/ subdirectory at all
    let output = Command::new(piano_bin())
        .args(["tag"])
        .env("PIANO_RUNS_DIR", tmp.path().join("nonexistent"))
        .output()
        .expect("failed to run piano tag");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no tags saved"),
        "should show 'no tags saved' when runs dir doesn't exist: {stderr}"
    );
}

#[test]
fn tag_list_empty_tags_dir_shows_no_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(runs_dir.join("tags")).unwrap();
    // tags/ exists but is empty

    let output = Command::new(piano_bin())
        .args(["tag"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano tag");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no tags saved"),
        "should show 'no tags saved' when tags dir is empty: {stderr}"
    );
}

#[test]
fn tag_save_shows_confirmation() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("tag-save");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    // Build and run to produce NDJSON in target/piano/runs
    build_and_run(&project_dir);

    // Tag the latest run (current_dir for project root discovery)
    let output = Command::new(piano_bin())
        .args(["tag", "baseline"])
        .current_dir(&project_dir)
        .output()
        .expect("failed to run piano tag");

    assert!(
        output.status.success(),
        "piano tag should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("tagged 'baseline'"),
        "should show confirmation: {stderr}"
    );
}

#[test]
fn tag_list_shows_saved_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("tag-list");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    build_and_run(&project_dir);

    // Save a tag
    let tag_output = Command::new(piano_bin())
        .args(["tag", "v1"])
        .current_dir(&project_dir)
        .output()
        .expect("failed to run piano tag save");
    assert!(
        tag_output.status.success(),
        "tag save failed: {}",
        String::from_utf8_lossy(&tag_output.stderr)
    );

    // List tags
    let list_output = Command::new(piano_bin())
        .args(["tag"])
        .current_dir(&project_dir)
        .output()
        .expect("failed to run piano tag list");

    assert!(
        list_output.status.success(),
        "tag list failed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains("v1"),
        "tag list should include saved tag: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Report separator
// ---------------------------------------------------------------------------

#[test]
fn profile_shows_report_separator() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("report-sep");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let output = Command::new(piano_bin())
        .args(["profile", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano profile");

    assert!(
        output.status.success(),
        "profile should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--- profiling report ---"),
        "should show report separator: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Diff labels
// ---------------------------------------------------------------------------

#[test]
fn diff_auto_shows_comparing_labels() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("diff-labels");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let runs_dir = tmp.path().join("diff-runs");
    fs::create_dir_all(&runs_dir).unwrap();

    // Two runs to enable diff
    build_and_run_to(&project_dir, &runs_dir);
    std::thread::sleep(std::time::Duration::from_millis(10));
    build_and_run_to(&project_dir, &runs_dir);

    let output = Command::new(piano_bin())
        .args(["diff"])
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run piano diff");

    assert!(
        output.status.success(),
        "diff should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("comparing:"),
        "should show comparing labels: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// --threads fallback for single-thread data
// ---------------------------------------------------------------------------

#[test]
fn report_threads_single_thread_shows_fallback_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("threads-fb");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    // Build and run (produces single-thread NDJSON)
    let runs_dir = build_and_run(&project_dir);
    let ndjson_file = common::largest_ndjson_file(&runs_dir);

    // Report with --threads on single-thread data
    let output = Command::new(piano_bin())
        .args(["report", "--threads"])
        .arg(&ndjson_file)
        .output()
        .expect("failed to run piano report --threads");

    assert!(
        output.status.success(),
        "report --threads should succeed (with fallback): {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("per-thread data"),
        "should warn about missing per-thread data: {stderr}"
    );
    assert!(
        stderr.contains("aggregated view"),
        "should mention aggregated fallback: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Build progress
// ---------------------------------------------------------------------------

#[test]
fn build_shows_function_count() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("fn-count");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let output = Command::new(piano_bin())
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("found 1 function(s) across 1 file(s)"),
        "should show function count: {stderr}"
    );
}

#[test]
fn build_shows_pre_building_message() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("pre-build");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let output = Command::new(piano_bin())
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("pre-building piano-runtime"),
        "should show pre-building message: {stderr}"
    );
}

#[test]
fn build_shows_built_message() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("built-msg");
    create_runnable_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let output = Command::new(piano_bin())
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("built:"),
        "should show built message: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Diff format mismatch warning
// ---------------------------------------------------------------------------

#[test]
fn diff_mixed_formats_shows_warning() {
    let tmp = tempfile::tempdir().unwrap();

    // Create an NDJSON file
    let ndjson_path = tmp.path().join("run_a.ndjson");
    fs::write(
        &ndjson_path,
        concat!(
            r#"{"type":"header","run_id":"a","timestamp_ms":1000,"bias_ns":0,"cpu_bias_ns":0,"names":{"0":"work"}}"#,
            "\n",
            r#"{"thread":0,"name_id":0,"calls":1,"self_ns":1000000,"inclusive_ns":1000000,"cpu_self_ns":0,"alloc_count":0,"alloc_bytes":0,"free_count":0,"free_bytes":0}"#,
            "\n",
            r#"{"type":"trailer","bias_ns":0,"cpu_bias_ns":0,"names":{"0":"work"}}"#,
            "\n",
        ),
    )
    .unwrap();

    // Create a legacy JSON file (Run struct serialized directly)
    let json_path = tmp.path().join("run_b.json");
    fs::write(
        &json_path,
        r#"{"functions":[{"name":"work","calls":2,"self_ms":2.0}],"timestamp_ms":2000}"#,
    )
    .unwrap();

    let output = Command::new(piano_bin())
        .args(["diff"])
        .arg(&ndjson_path)
        .arg(&json_path)
        .output()
        .expect("failed to run piano diff");

    assert!(
        output.status.success(),
        "diff should succeed with mixed formats: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("different source formats"),
        "should warn about format mismatch: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Parse error warning
// ---------------------------------------------------------------------------

#[test]
fn build_warns_on_parse_errors_in_source() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("parse-err");
    fs::create_dir_all(project_dir.join("src")).unwrap();

    fs::write(
        project_dir.join("Cargo.toml"),
        r#"[package]
name = "parse-err"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "parse-err"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // A file with a syntax error that the error-recovering parser can handle.
    // The function `work` is valid; the trailing garbage triggers parse errors.
    fs::write(
        project_dir.join("src/main.rs"),
        "fn main() { let _ = work(); }\nfn work() -> u64 { 42 }\n)))\n",
    )
    .unwrap();
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let output = Command::new(piano_bin())
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("parse errors in") || stderr.contains("warning: parse errors"),
        "should warn about parse errors: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Wrapper stderr messages
// ---------------------------------------------------------------------------

#[test]
fn wrapper_no_args_shows_expected_rustc_path() {
    // Invoke piano in wrapper mode with no rustc path argument
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("nonexistent_config.json");
    let output = Command::new(piano_bin())
        .env("PIANO_WRAPPER_CONFIG", &config_path)
        .output()
        .expect("failed to run piano in wrapper mode");

    assert!(!output.status.success(), "wrapper with no args should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("expected rustc path"),
        "should mention expected rustc path: {stderr}"
    );
}

#[test]
fn wrapper_missing_config_shows_error() {
    // Invoke piano in wrapper mode with a rustc path but nonexistent config
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("nonexistent_config.json");
    let output = Command::new(piano_bin())
        .env("PIANO_WRAPPER_CONFIG", &config_path)
        .arg("rustc") // fake rustc path as argv[1]
        .arg("src/main.rs") // fake source file
        .output()
        .expect("failed to run piano in wrapper mode");

    assert!(
        !output.status.success(),
        "wrapper with bad config should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to read config"),
        "should mention config read failure: {stderr}"
    );
}

#[test]
fn wrapper_bad_rustc_path_shows_execute_error() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("config.json");
    fs::write(
        &config_path,
        r#"{"runtime_rlib":"/x","runtime_deps_dir":"/x","entry_point":{"source_path":"src/main.rs","name_table":[],"runs_dir":"/tmp","cpu_time":false},"targets":{}}"#,
    )
    .unwrap();

    let output = Command::new(piano_bin())
        .env("PIANO_WRAPPER_CONFIG", &config_path)
        .arg("/nonexistent/rustc")
        .output()
        .expect("failed to run piano in wrapper mode");

    assert!(
        !output.status.success(),
        "wrapper with bad rustc should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to execute"),
        "should mention execution failure: {stderr}"
    );
}

#[test]
fn wrapper_missing_source_shows_instrument_failure_and_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("config.json");
    fs::write(
        &config_path,
        r#"{"runtime_rlib":"/x","runtime_deps_dir":"/x","entry_point":{"source_path":"src/main.rs","name_table":[],"runs_dir":"/tmp","cpu_time":false},"targets":{"src/main.rs":{"work":0}}}"#,
    )
    .unwrap();

    // Run from a temp dir where src/main.rs does not exist
    let work_dir = tmp.path().join("empty");
    fs::create_dir_all(&work_dir).unwrap();

    let output = Command::new(piano_bin())
        .env("PIANO_WRAPPER_CONFIG", &config_path)
        .arg("/bin/true")
        .args(["--crate-name", "test", "src/main.rs"])
        .current_dir(&work_dir)
        .output()
        .expect("failed to run piano in wrapper mode");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to instrument"),
        "should mention instrumentation failure: {stderr}"
    );
    assert!(
        stderr.contains("compiling without instrumentation"),
        "should mention fallback: {stderr}"
    );
}

#[cfg(unix)]
#[test]
fn wrapper_fallback_passes_original_args_to_rustc() {
    let tmp = tempfile::tempdir().unwrap();
    let config_path = tmp.path().join("config.json");
    fs::write(
        &config_path,
        r#"{"runtime_rlib":"/x","runtime_deps_dir":"/x","entry_point":{"source_path":"src/main.rs","name_table":[],"runs_dir":"/tmp","cpu_time":false},"targets":{"src/main.rs":{"work":0}}}"#,
    )
    .unwrap();

    // Script that prints its args to a file so we can verify them
    let args_file = tmp.path().join("captured_args.txt");
    let fake_rustc = tmp.path().join("fake_rustc.sh");
    fs::write(
        &fake_rustc,
        format!("#!/bin/sh\necho \"$@\" > {}\n", args_file.display()),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();

    let work_dir = tmp.path().join("empty");
    fs::create_dir_all(&work_dir).unwrap();

    let output = Command::new(piano_bin())
        .env("PIANO_WRAPPER_CONFIG", &config_path)
        .arg(&fake_rustc)
        .args(["--edition", "2024", "--crate-name", "test", "src/main.rs"])
        .current_dir(&work_dir)
        .output()
        .expect("failed to run piano in wrapper mode");

    assert!(
        output.status.success(),
        "fallback should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify fake_rustc received the original args
    let captured = fs::read_to_string(&args_file).expect("fake_rustc should have written args");
    assert!(
        captured.contains("--edition 2024"),
        "should pass through original args: {captured}"
    );
    assert!(
        captured.contains("src/main.rs"),
        "should pass through source file: {captured}"
    );
}

// ---------------------------------------------------------------------------
// Overhead warning (>200 functions)
// ---------------------------------------------------------------------------

#[test]
fn build_warns_when_instrumenting_many_functions() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("many-fns");
    fs::create_dir_all(project_dir.join("src")).unwrap();

    fs::write(
        project_dir.join("Cargo.toml"),
        r#"[package]
name = "many-fns"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "many-fns"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // Generate a source file with 210 trivial functions
    let mut source = String::new();
    for i in 0..210 {
        source.push_str(&format!("fn func_{i}() -> u64 {{ {i} }}\n"));
    }
    source.push_str("fn main() {\n");
    source.push_str("    let mut sum = 0u64;\n");
    for i in 0..210 {
        source.push_str(&format!("    sum += func_{i}();\n"));
    }
    source.push_str("    println!(\"{sum}\");\n");
    source.push_str("}\n");

    fs::write(project_dir.join("src/main.rs"), &source).unwrap();
    common::prepopulate_deps(&project_dir, common::mini_seed());

    // Build without --fn (instruments all functions)
    let output = Command::new(piano_bin())
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");

    assert!(output.status.success(), "build should succeed");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("may add overhead"),
        "should warn about overhead for >200 functions: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_runnable_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "{name}"
path = "src/main.rs"
"#,
            name = dir.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
    fs::write(
        dir.join("src/main.rs"),
        r#"fn main() {
    let _ = work();
}

fn work() -> u64 {
    let mut sum = 0u64;
    for i in 0..1000 {
        sum = sum.wrapping_add(i);
    }
    sum
}
"#,
    )
    .unwrap();
}

fn build_and_run(project_dir: &Path) -> std::path::PathBuf {
    let runs_dir = project_dir.join("target/piano/runs");
    build_and_run_to(project_dir, &runs_dir);
    runs_dir
}

fn build_and_run_to(project_dir: &Path, runs_dir: &Path) {
    fs::create_dir_all(runs_dir).unwrap();

    let build = Command::new(piano_bin())
        .args(["build", "--fn", "work", "--project"])
        .arg(project_dir)
        .arg("--runtime-path")
        .arg(runtime_path())
        .output()
        .expect("failed to run piano build");
    assert!(
        build.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );

    let binary_path = String::from_utf8_lossy(&build.stdout).trim().to_string();
    let run = Command::new(&binary_path)
        .env("PIANO_RUNS_DIR", runs_dir)
        .output()
        .expect("failed to run binary");
    assert!(
        run.status.success(),
        "binary failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
}
