//! Integration test: verify allocation tracking works when the user's
//! `#[global_allocator]` is behind a `#[cfg(...)]` gate that does NOT match the
//! current platform.
//!
//! Piano should detect the cfg-gated allocator, skip wrapping it, and inject a
//! fallback `PianoAllocator<System>` behind the negated cfg. The NDJSON output
//! should report non-zero `alloc_count` ("ac" field) for functions that allocate.

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_cfg_gated_alloc_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "cfg-gated-alloc-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "cfg-gated-alloc-test"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The user project declares a global allocator behind #[cfg(target_os = "foobar")].
    // This cfg never matches any real platform, so the allocator is compiled out.
    // Piano should inject a fallback PianoAllocator<System> behind the negated cfg.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use std::alloc::System;

#[cfg(target_os = "foobar")]
struct MyCustomAlloc;

#[cfg(target_os = "foobar")]
unsafe impl std::alloc::GlobalAlloc for MyCustomAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        System.dealloc(ptr, layout)
    }
}

#[cfg(target_os = "foobar")]
#[global_allocator]
static ALLOC: MyCustomAlloc = MyCustomAlloc;

fn do_allocs() -> usize {
    let mut total = 0usize;
    for i in 0..100 {
        let v: Vec<u8> = vec![0u8; (i + 1) * 64];
        total += v.len();
        std::hint::black_box(&v);
    }
    total
}

fn main() {
    let n = do_allocs();
    println!("total: {n}");
}
"#,
    )
    .unwrap();
}

#[test]
fn cfg_gated_allocator_reports_nonzero() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("cfg-gated-alloc-test");
    create_cfg_gated_alloc_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with instrumentation on do_allocs and main.
    let output = Command::new(piano_bin)
        .args(["build", "--fn", "do_allocs", "--fn", "main", "--project"])
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

    // Run the instrumented binary.
    let runs_dir = tmp.path().join("runs");
    fs::create_dir_all(&runs_dir).unwrap();

    let run_output = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    assert!(
        run_output.status.success(),
        "instrumented binary failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    // Program should produce correct output.
    let program_stdout = String::from_utf8_lossy(&run_output.stdout);
    assert!(
        program_stdout.contains("total:"),
        "program should produce output, got: {program_stdout}"
    );

    // Find run output file (.ndjson preferred).
    let all_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let run_file = all_files
        .iter()
        .find(|p| p.extension().is_some_and(|ext| ext == "ndjson"))
        .or_else(|| {
            all_files
                .iter()
                .find(|p| p.extension().is_some_and(|ext| ext == "json"))
        })
        .unwrap_or_else(|| {
            panic!("should have .ndjson or .json output file. Files in runs dir: {all_files:?}")
        });

    let content = fs::read_to_string(run_file).unwrap();

    let is_ndjson = run_file.extension().is_some_and(|ext| ext == "ndjson");
    assert!(
        is_ndjson,
        "expected NDJSON output (frame-level data with alloc fields). Got .json. Files: {all_files:?}",
    );

    // NDJSON format:
    //   Line 1 (header): {"format_version":3,...,"functions":["do_allocs","main"]}
    //   Line 2+ (frames): {"frame":0,"fns":[{"id":0,"calls":1,"self_ns":...,"ac":N,"ab":N,...}]}
    // "ac" = alloc_count, "ab" = alloc_bytes.
    let mut lines = content.lines();
    let header_line = lines
        .next()
        .expect("NDJSON should have at least a header line");
    let header: serde_json::Value =
        serde_json::from_str(header_line).expect("header should be valid JSON");

    // Find the function index for "do_allocs" in the header's functions array.
    let fn_names = header
        .get("functions")
        .and_then(|f| f.as_array())
        .expect("header should have functions array");
    let do_allocs_id = fn_names
        .iter()
        .position(|n| n.as_str() == Some("do_allocs"))
        .expect("do_allocs should be in functions list");

    // Search frame lines for an entry with that function id and non-zero "ac".
    let has_alloc_data = lines.any(|line| {
        if let Ok(frame) = serde_json::from_str::<serde_json::Value>(line) {
            frame
                .get("fns")
                .and_then(|f| f.as_array())
                .map(|fns| {
                    fns.iter().any(|f| {
                        f.get("id").and_then(|id| id.as_u64()) == Some(do_allocs_id as u64)
                            && f.get("ac").and_then(|n| n.as_u64()).unwrap_or(0) > 0
                    })
                })
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        has_alloc_data,
        "do_allocs should have non-zero alloc_count (ac) in NDJSON output.\n\
         This means the cfg-gated allocator fallback was not injected correctly.\n\
         Content:\n{content}"
    );
}
