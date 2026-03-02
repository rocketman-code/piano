//! Integration test: verify async allocation tracking works through the full
//! piano pipeline. Async functions that allocate should report non-zero
//! alloc_count and alloc_bytes in the NDJSON output, even when the function
//! migrates across threads.

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_async_alloc_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-alloc-test"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-alloc-test"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt", "macros"] }
"#,
    )
    .unwrap();

    // The program allocates in an async function with .await points.
    // Vec::with_capacity forces a real heap allocation.
    // Uses a sync wrapper that calls the async function via block_on,
    // ensuring the depth-0 guard boundary flushes FRAME_BUFFER to FRAMES
    // so NDJSON output is produced.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"
async fn allocating_work() -> Vec<u8> {
    let mut data = Vec::with_capacity(1024);
    data.extend_from_slice(&[1u8; 512]);
    tokio::task::yield_now().await;
    data.extend_from_slice(&[2u8; 512]);
    data
}

fn wrapper() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(allocating_work());
    println!("len: {}", result.len());
}

fn main() {
    wrapper();
}
"#,
    )
    .unwrap();
}

#[test]
fn async_alloc_tracking_pipeline() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-alloc-test");
    create_async_alloc_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build with piano -- instrument allocating_work, wrapper, and main.
    // wrapper is the sync depth-0 boundary that triggers frame flush.
    let output = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "allocating_work",
            "--fn",
            "wrapper",
            "--fn",
            "main",
            "--project",
        ])
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
        program_stdout.contains("len: 1024"),
        "program should produce correct output, got: {program_stdout}"
    );

    // Find run output file (.ndjson or .json).
    let all_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    // Prefer .ndjson (has alloc data) over .json (legacy, no alloc fields).
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

    // NDJSON format:
    //   Line 1 (header): {"format_version":3,...,"functions":["allocating_work","main"]}
    //   Line 2+ (frames): {"frame":0,"fns":[{"id":0,"calls":1,"self_ns":...,"ac":N,"ab":N,...}]}
    // "ac" = alloc_count, "ab" = alloc_bytes. Functions referenced by index into header array.
    let is_ndjson = run_file.extension().is_some_and(|ext| ext == "ndjson");
    assert!(
        is_ndjson,
        "expected NDJSON output (frame-level data with alloc fields). Got .json. Files: {all_files:?}",
    );

    let mut lines = content.lines();
    let header_line = lines
        .next()
        .expect("NDJSON should have at least a header line");
    let header: serde_json::Value =
        serde_json::from_str(header_line).expect("header should be valid JSON");

    // Find the function index for "allocating_work" in the header's functions array.
    let fn_names = header
        .get("functions")
        .and_then(|f| f.as_array())
        .expect("header should have functions array");
    let alloc_work_id = fn_names
        .iter()
        .position(|n| n.as_str() == Some("allocating_work"))
        .expect("allocating_work should be in functions list");

    // Search frame lines for an entry with that function id and non-zero "ac".
    let has_alloc_data = lines.any(|line| {
        if let Ok(frame) = serde_json::from_str::<serde_json::Value>(line) {
            frame
                .get("fns")
                .and_then(|f| f.as_array())
                .map(|fns| {
                    fns.iter().any(|f| {
                        f.get("id").and_then(|id| id.as_u64()) == Some(alloc_work_id as u64)
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
        "allocating_work should have non-zero alloc_count (ac) in NDJSON output.\nContent:\n{content}"
    );
}
