//! Test: programs that call process::exit() must produce valid profiling data.

mod common;

use std::fs;
use std::path::Path;

fn create_exit_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "exit-test"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "exit-test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let _ = work();
    std::process::exit(1);
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
fn process_exit_produces_valid_profiling_data() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-test");
    create_exit_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build instrumented binary.
    let build_output = std::process::Command::new(piano_bin)
        .args(["build", "--fn", "work", "--fn", "main", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("piano build failed");

    assert!(
        build_output.status.success(),
        "piano build failed:\n{}",
        String::from_utf8_lossy(&build_output.stderr)
    );

    let binary_path = String::from_utf8_lossy(&build_output.stdout)
        .trim()
        .to_string();
    assert!(
        Path::new(&binary_path).exists(),
        "binary not found: {binary_path}"
    );

    // Run the instrumented binary (it calls process::exit(1)).
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run_output = std::process::Command::new(&binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    let stderr = String::from_utf8_lossy(&run_output.stderr);

    // Must NOT contain TLS panic.
    assert!(
        !stderr.contains("cannot access a Thread Local Storage"),
        "TLS panic detected in stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("panic"),
        "unexpected panic in stderr:\n{stderr}"
    );

    // Must have produced NDJSON output.
    let ndjson_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
        .collect();

    assert!(
        !ndjson_files.is_empty(),
        "no NDJSON files written (runtime crashed without writing data)"
    );

    // Parse and verify the profiling data contains the "work" function.
    let ndjson_path = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&ndjson_path).unwrap();
    let stats = common::aggregate_ndjson(&content);

    assert!(
        stats.contains_key("work"),
        "profiling data should contain 'work' function, got: {stats:?}"
    );
    assert!(
        stats["work"].calls >= 1,
        "work() should have been called at least once"
    );

    // main() is the lifecycle boundary (creates root context) and is excluded
    // from the name table. It does not appear in the output even though --fn main was passed.
    assert!(
        !stats.contains_key("main"),
        "main should NOT appear in output (lifecycle boundary, excluded from name table)"
    );
}

/// Streaming mode (PIANO_STREAM_FRAMES=1) exercises the atexit path through
/// `stream_frame_to_writer` and `intern_name`, which use TLS (THREAD_INDEX,
/// NAME_CACHE). When `process::exit()` destroys TLS before the atexit handler
/// runs, these `.with()` calls would panic. The fix converts them to
/// `.try_with()` with graceful fallback.
#[test]
fn process_exit_streaming_no_tls_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("exit-stream-test");
    create_exit_project(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let build_output = std::process::Command::new(piano_bin)
        .args(["build", "--fn", "work", "--fn", "main", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("piano build failed");

    assert!(
        build_output.status.success(),
        "piano build failed:\n{}",
        String::from_utf8_lossy(&build_output.stderr)
    );

    let binary_path = String::from_utf8_lossy(&build_output.stdout)
        .trim()
        .to_string();

    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    // Run with PIANO_STREAM_FRAMES=1 to exercise the streaming atexit path.
    let run_output = std::process::Command::new(&binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .env("PIANO_STREAM_FRAMES", "1")
        .output()
        .expect("failed to run instrumented binary");

    let stderr = String::from_utf8_lossy(&run_output.stderr);

    // Must NOT contain TLS panic -- this is the core assertion for issue #518.
    assert!(
        !stderr.contains("cannot access a Thread Local Storage"),
        "TLS panic detected in streaming atexit path:\n{stderr}"
    );
    assert!(
        !stderr.contains("panic"),
        "unexpected panic in stderr:\n{stderr}"
    );

    // Must have produced NDJSON output.
    let ndjson_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
        .collect();

    assert!(
        !ndjson_files.is_empty(),
        "no NDJSON files written in streaming mode (runtime crashed without writing data)"
    );

    let ndjson_path = common::largest_ndjson_file(&runs_dir);
    let content = fs::read_to_string(&ndjson_path).unwrap();
    let stats = common::aggregate_ndjson(&content);

    assert!(
        stats.contains_key("work"),
        "streaming profiling data should contain 'work' function, got: {stats:?}"
    );
}
