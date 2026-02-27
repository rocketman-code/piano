//! Integration test: verify async self-time is accurate through the full
//! piano pipeline. The parent function's self_ms should be less than the
//! child's self_ms because the parent does no computation -- its body is
//! just two .await calls.
//!
//! This test is blind to the implementation: it does not reference any
//! internal types or mechanisms. It goes through piano build -> run ->
//! parse JSON output -> assert self-time relationship.

use std::fs;
use std::path::Path;
use std::process::Command;

fn create_async_self_time_project(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "async-selftime"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "async-selftime"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
"#,
    )
    .unwrap();

    // The program has a clear parent/child hierarchy:
    // - parent_fn calls expensive_child twice via .await
    // - expensive_child is a leaf that does heavy computation
    // - parent_fn's self_time should be negligible compared to its wall time
    //
    // Multiple tasks + yield_now maximize migration likelihood on multi-thread rt.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"use tokio::task;

async fn expensive_child() -> u64 {
    let mut sum = 0u64;
    for i in 0..2_000_000 {
        sum = sum.wrapping_add(i);
    }
    // Yield to give the scheduler a migration opportunity
    task::yield_now().await;
    let mut sum2 = 0u64;
    for i in 0..2_000_000 {
        sum2 = sum2.wrapping_add(i);
    }
    sum.wrapping_add(sum2)
}

async fn parent_fn() -> u64 {
    let a = expensive_child().await;
    let b = expensive_child().await;
    a.wrapping_add(b)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // Spawn several concurrent tasks to increase scheduling pressure
    let mut handles = Vec::new();
    for _ in 0..4 {
        handles.push(task::spawn(parent_fn()));
    }
    let mut total = 0u64;
    for h in handles {
        total = total.wrapping_add(h.await.unwrap());
    }
    println!("total: {total}");
}
"#,
    )
    .unwrap();
}

#[test]
fn async_self_time_accuracy() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("async-selftime");
    create_async_self_time_project(&project_dir);

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    // Build: instrument parent_fn and expensive_child.
    let build = Command::new(piano_bin)
        .args([
            "build",
            "--fn",
            "parent_fn",
            "--fn",
            "expensive_child",
            "--fn",
            "main",
            "--project",
        ])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    let stderr = String::from_utf8_lossy(&build.stderr);
    let stdout = String::from_utf8_lossy(&build.stdout);
    assert!(
        build.status.success(),
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

    let run = Command::new(binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");

    let run_stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "instrumented binary failed:\nstdout: {}\nstderr: {run_stderr}",
        String::from_utf8_lossy(&run.stdout),
    );

    // Parse JSON output (always written by shutdown).
    let json_files: Vec<_> = fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert!(
        !json_files.is_empty(),
        "expected JSON output in {runs_dir:?}"
    );

    let content = fs::read_to_string(json_files[0].path()).unwrap();

    // Verify functions appear in the output.
    assert!(
        content.contains("\"parent_fn\""),
        "output should contain parent_fn. Got:\n{content}"
    );
    assert!(
        content.contains("\"expensive_child\""),
        "output should contain expensive_child. Got:\n{content}"
    );

    // Structural assertion derived from the program, not an arbitrary threshold.
    //
    // By construction:
    //   parent_fn does ZERO computation (body is just two .await calls)
    //   expensive_child does 4M wrapping_add iterations
    //
    // Therefore: parent_fn.self ≈ 0  and  expensive_child.self >> 0
    // So: parent_fn.self_ms < expensive_child.self_ms  (always true if accounting works)
    //
    // With the bug (migrated guard sets self = elapsed):
    //   parent_fn.self ≈ parent_fn.total ≈ 2 * expensive_child.total
    //   which is GREATER than expensive_child.self  → assertion fails
    //
    // This assertion holds regardless of whether migration occurred:
    // - No migration: normal parent-child subtraction makes parent self ≈ 0
    // - Migration + fix: phantom subtraction makes parent self ≈ 0
    let child_self = extract_field(&content, "expensive_child", "self_ms")
        .expect("expensive_child should appear in output");
    assert!(
        child_self > 0.0,
        "expensive_child should have non-zero self_ms (it does real work)"
    );

    // Migrated guards now preserve the real function name, so parent_fn
    // always appears under its own name.
    if let Some(parent_self) = extract_field(&content, "parent_fn", "self_ms") {
        assert!(
            parent_self < child_self,
            "parent_fn.self_ms ({parent_self:.3}) must be < expensive_child.self_ms \
             ({child_self:.3}) -- parent does no computation, child does all of it"
        );
    }

    // Migrated guards now preserve their real function names, so there
    // should be no "<migrated>" bucket in the output.
    assert!(
        extract_field(&content, "<migrated>", "self_ms").is_none(),
        "should not have a <migrated> bucket -- migrated guards preserve real names"
    );
}

/// Extract a float field from the JSON output for a given function name.
/// Returns None if the function or field is not found.
fn extract_field(json: &str, function: &str, field: &str) -> Option<f64> {
    let func_section = json.split(&format!("\"{function}\"")).nth(1)?;
    let value_str = func_section
        .split(&format!("\"{field}\":"))
        .nth(1)?
        .split([',', '}'])
        .next()?;
    value_str.parse().ok()
}
