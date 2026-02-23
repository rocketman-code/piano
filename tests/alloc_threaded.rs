//! Integration test: PianoAllocator in a multi-threaded program.
//!
//! The allocator must not crash when threads that performed allocations exit.
//! Previously, `track_alloc` accessed `STACK` (RefCell<Vec>) which has a
//! destructor — forbidden for global allocators on Rust < 1.93.1.
//!
//! This test pins Rust 1.91.0 (via rust-toolchain.toml in the test project)
//! because 1.93.1 relaxed the TLS restriction and silently masks the bug.
//! It also uses rayon (common real-world pattern) to exercise thread pool
//! allocation paths.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Check if a specific Rust toolchain is installed.
fn has_toolchain(version: &str) -> bool {
    Command::new("rustup")
        .args(["run", version, "rustc", "--version"])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn create_rayon_alloc_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    // Pin Rust 1.91.0 — this version has the strict TLS destructor check
    // that aborts if the global allocator uses TLS with destructors.
    fs::write(
        dir.join("rust-toolchain.toml"),
        r#"[toolchain]
channel = "1.91.0"
"#,
    )
    .unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "threaded_alloc_test"
version = "0.1.0"
edition = "2021"

[dependencies]
rayon = "1"

[[bin]]
name = "threaded_alloc_test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // Program that uses rayon thread pool with heap allocations.
    // This is representative of real programs like chainsaw that
    // use rayon for parallelism and pin older Rust toolchains.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    let threads = cpus.min(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .ok();

    rayon::scope(|s| {
        for i in 0..threads {
            s.spawn(move |_| {
                worker(i);
            });
        }
    });
    println!("ok");
}

fn worker(id: usize) {
    let mut vecs: Vec<Vec<u8>> = Vec::new();
    for j in 0..100 {
        vecs.push(vec![0u8; (id + 1) * (j + 1)]);
    }
    std::hint::black_box(&vecs);
}
"#,
    )
    .unwrap();
}

#[test]
fn rayon_program_with_alloc_tracking_does_not_crash_on_older_rust() {
    if !has_toolchain("1.91.0") {
        eprintln!("skipping: Rust 1.91.0 not installed (rustup toolchain install 1.91.0)");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("threaded_alloc_test");
    create_rayon_alloc_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with instrumentation on worker.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "worker", "--fn", "main", "--project"])
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

    let binary_path = stdout.trim();
    assert!(
        Path::new(binary_path).exists(),
        "built binary should exist at: {binary_path}"
    );

    // Run the instrumented binary — should NOT crash.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run_output = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    let run_stderr = String::from_utf8_lossy(&run_output.stderr);
    let run_stdout = String::from_utf8_lossy(&run_output.stdout);

    assert!(
        run_output.status.success(),
        "instrumented binary crashed (likely TLS destructor abort):\nstderr: {run_stderr}\nstdout: {run_stdout}"
    );

    assert!(
        run_stdout.contains("ok"),
        "program should produce correct output, got: {run_stdout}"
    );

    // Verify output file was produced with worker data.
    let json_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();

    assert!(
        !json_files.is_empty(),
        "expected at least one .json run file"
    );

    let content = fs::read_to_string(json_files[0].path()).unwrap();
    assert!(
        content.contains("worker"),
        "output should contain worker function data"
    );
}
