//! Integration test: verify compilation errors show original (un-shifted) line
//! numbers and do not leak piano_runtime internals.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

fn piano_build_stderr(project_name: &str, main_rs: &str, piano_args: &[&str]) -> String {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join(project_name);
    std::fs::create_dir_all(project_dir.join("src")).unwrap();
    std::fs::write(
        project_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "{project_name}"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "{project_name}"
path = "src/main.rs"
"#
        ),
    )
    .unwrap();
    std::fs::write(project_dir.join("src/main.rs"), main_rs).unwrap();
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .args(piano_args)
        .output()
        .expect("failed to run piano build");

    assert!(
        !output.status.success(),
        "{project_name}: build should fail (deliberate error)"
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Assert that stderr contains no piano injection artifacts.
fn assert_no_piano_artifacts(stderr: &str, test_name: &str) {
    let artifacts = [
        ("__piano_guard", "guard variable"),
        ("__piano_sink", "sink variable"),
        ("__piano_ts", "timestamp variable"),
        ("__piano_pid", "pid variable"),
        ("__piano_warned", "warned variable"),
        ("__piano_file", "file variable"),
        ("__piano_dir", "dir variable"),
        ("__piano_suffix", "suffix variable"),
        ("__piano_name", "name variable"),
        ("__piano_path", "path variable"),
        ("__piano_run_id", "run_id variable"),
        ("piano_runtime::", "runtime path"),
        ("enter_async(", "enter_async call"),
        ("PIANO_NAMES", "name table"),
        ("__PIANO_ALLOC", "allocator static"),
        ("ProfileSession", "session init"),
        ("PianoAllocator", "allocator wrapper"),
        ("PianoFuture", "future wrapper"),
    ];
    for (pattern, desc) in &artifacts {
        assert!(
            !stderr.contains(pattern),
            "{test_name}: {desc} leaked in error output.\nPattern: {pattern}\nStderr:\n{stderr}"
        );
    }
}

/// Create a project with a deliberate type error at a known line.
fn create_project_with_error(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();

    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "errtest"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "errtest"
path = "src/main.rs"
"#,
    )
    .unwrap();

    // The type error is on line 8 ("hello" assigned to i32).
    // Lines 1-7 are valid; line 8 has the error.
    // After instrumentation, piano injects a guard line inside work(),
    // which would shift line 8 to line 9+ without remapping.
    fs::write(
        dir.join("src").join("main.rs"),
        r#"fn main() {
    let result = work();
    println!("result: {result}");
}

fn work() -> u64 {
    let mut sum: u64 = 0;
    let bad: i32 = "hello";
    sum
}
"#,
    )
    .unwrap();
}

#[test]
fn compilation_error_shows_original_line_numbers() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path().join("errtest");
    create_project_with_error(&project_dir);
    common::prepopulate_deps(&project_dir, common::mini_seed());

    let piano_bin = env!("CARGO_BIN_EXE_piano");
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime_path = manifest_dir.join("piano-runtime");

    let output = Command::new(piano_bin)
        .args(["build", "--fn", "work", "--project"])
        .arg(&project_dir)
        .arg("--runtime-path")
        .arg(&runtime_path)
        .output()
        .expect("failed to run piano build");

    // Build SHOULD fail (there's a type error).
    assert!(
        !output.status.success(),
        "piano build should have failed due to type error"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The error should reference line 8 (original line), not a shifted line.
    assert!(
        stderr.contains("src/main.rs:8:") || stderr.contains(":8:"),
        "error should reference original line 8, got:\n{stderr}"
    );

    // Piano internals should NOT appear in the error output.
    assert!(
        !stderr.contains("piano_runtime::"),
        "piano_runtime should be filtered from errors:\n{stderr}"
    );
    assert!(
        !stderr.contains("__piano_guard"),
        "__piano_guard should be filtered from errors:\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// Per-injection-type error remapping tests
// ---------------------------------------------------------------------------

#[test]
fn sync_guard_cleaned_from_type_error() {
    let stderr = piano_build_stderr(
        "sync-guard-type",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "sync_guard_type");
    assert!(
        stderr.contains(":2:"),
        "error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn sync_guard_cleaned_from_unresolved_error() {
    let stderr = piano_build_stderr(
        "sync-guard-unresolved",
        "fn work() {\n    unknown_function();\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "sync_guard_unresolved");
    assert!(
        stderr.contains(":2:"),
        "error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn sync_guard_cleaned_from_borrow_error() {
    let stderr = piano_build_stderr(
        "sync-guard-borrow",
        "fn work() {\n    let mut v = vec![1, 2, 3];\n    let r = &v;\n    v.push(4);\n    println!(\"{:?}\", r);\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "sync_guard_borrow");
    assert!(
        stderr.contains(":4:"),
        "error should reference line 4 (push while borrow held)\nStderr:\n{stderr}"
    );
}

#[test]
fn async_wrapper_cleaned_from_type_error() {
    let stderr = piano_build_stderr(
        "async-wrapper-type",
        "async fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "async_wrapper_type");
    assert!(
        stderr.contains(":2:"),
        "error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn async_wrapper_cleaned_from_unresolved_error() {
    let stderr = piano_build_stderr(
        "async-wrapper-unresolved",
        "async fn work() {\n    unknown_thing();\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "async_wrapper_unresolved");
    assert!(
        stderr.contains(":2:"),
        "error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn impl_future_wrapper_does_not_corrupt_error_text() {
    let stderr = piano_build_stderr(
        "impl-future-type",
        "fn work() -> impl std::future::Future<Output = i32> {\n    async { let x: i32 = \"hello\"; x }\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "impl_future_type");
    assert!(
        stderr.contains(":2:"),
        "error should reference line 2\nStderr:\n{stderr}"
    );
    // Injection cleanup must not leave bare `)` that breaks parentheses balance.
    let open = stderr.chars().filter(|&c| c == '(').count();
    let close = stderr.chars().filter(|&c| c == ')').count();
    assert_eq!(
        open, close,
        "error parentheses should not be corrupted by injection cleanup\nStderr:\n{stderr}"
    );
}

#[test]
fn lifecycle_cleaned_from_main_type_error() {
    let stderr = piano_build_stderr(
        "lifecycle-main-type",
        "fn work() -> i32 { 1 }\nfn main() {\n    let x: i32 = \"hello\";\n}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "lifecycle_main_type");
    assert!(
        stderr.contains(":3:"),
        "error should reference line 3\nStderr:\n{stderr}"
    );
}

#[test]
fn lifecycle_cleaned_from_main_unresolved_error() {
    let stderr = piano_build_stderr(
        "lifecycle-main-unresolved",
        "fn work() { }\nfn main() {\n    nonexistent();\n}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "lifecycle_main_unresolved");
    assert!(
        stderr.contains(":3:"),
        "error should reference line 3\nStderr:\n{stderr}"
    );
}

#[test]
fn allocator_wrap_preserves_line_numbers() {
    let stderr = piano_build_stderr(
        "allocator-wrap-type",
        r#"use std::alloc::{GlobalAlloc, Layout};
struct MyAlloc;
unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, _: Layout) -> *mut u8 { std::ptr::null_mut() }
    unsafe fn dealloc(&self, _: *mut u8, _: Layout) {}
}
#[global_allocator]
static ALLOC: MyAlloc = MyAlloc;
fn work() -> i32 {
    let x: i32 = "hello";
    x
}
fn main() { work(); }
"#,
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "allocator_wrap_type");
    assert!(
        stderr.contains(":10:"),
        "error should reference line 10\nStderr:\n{stderr}"
    );
}

#[test]
fn cfg_allocator_preserves_line_numbers() {
    let stderr = piano_build_stderr(
        "cfg-alloc-type",
        r#"use std::alloc::{GlobalAlloc, Layout};
struct MyAlloc;
unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, _: Layout) -> *mut u8 { std::ptr::null_mut() }
    unsafe fn dealloc(&self, _: *mut u8, _: Layout) {}
}
#[cfg(feature = "custom-alloc")]
#[global_allocator]
static ALLOC: MyAlloc = MyAlloc;
fn work() -> i32 {
    let x: i32 = "hello";
    x
}
fn main() { work(); }
"#,
        &["--fn", "work"],
    );
    // work() starts on line 10. Error is on line 11.
    // If the allocator replace added lines, the error would be on line 12+.
    assert!(
        stderr.contains(":11:"),
        "error should reference original line 11, got:\n{stderr}"
    );
    assert_no_piano_artifacts(&stderr, "cfg_alloc_type");
}

#[test]
fn macro_guard_cleaned_from_type_error() {
    let stderr = piano_build_stderr(
        "macro-guard-type",
        r#"macro_rules! make_fn {
    ($name:ident) => {
        fn $name() -> i32 {
            let x: i32 = "hello";
            x
        }
    };
}
make_fn!(work);
fn main() { work(); }
"#,
        &["--fn", "work"],
    );
    // Macro errors reference the macro call site; line numbers vary by compiler.
    // The key property: no piano artifacts leak into the diagnostic.
    assert_no_piano_artifacts(&stderr, "macro_guard_type");
}

#[test]
fn piano_diagnostics_filtered_from_output() {
    let stderr = piano_build_stderr(
        "diagnostics-filtered",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "diagnostics_filtered");
    // Piano's injected __piano_guard would trigger "unused variable" if not filtered.
    assert!(
        !stderr.contains("unused variable"),
        "'unused variable' warning leaked (likely from piano internals)\nStderr:\n{stderr}"
    );
    // The actual user error should still be present.
    assert!(
        stderr.contains("mismatched types") || stderr.contains("expected"),
        "user error should still appear in stderr\nStderr:\n{stderr}"
    );
}

#[test]
fn sync_guard_cleaned_from_lifetime_error() {
    let stderr = piano_build_stderr(
        "sync-guard-lifetime",
        "fn work<'a>(x: &i32) -> &'a i32 {\n    x\n}\nfn main() { let n = 1; let _ = work(&n); }\n",
        &["--fn", "work"],
    );
    // Lifetime errors may reference multiple lines; just verify no artifacts.
    assert_no_piano_artifacts(&stderr, "sync_guard_lifetime");
}

#[test]
fn async_wrapper_cleaned_from_borrow_error() {
    let stderr = piano_build_stderr(
        "async-wrapper-borrow",
        "async fn work() {\n    let mut v = vec![1];\n    let r = &v;\n    v.push(2);\n    println!(\"{:?}\", r);\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "async_wrapper_borrow");
}

#[test]
fn multiple_injections_cleaned_from_combined_errors() {
    let stderr = piano_build_stderr(
        "multi-injection",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() {\n    let y: bool = 42i32;\n}\n",
        &["--fn", "work"],
    );
    // Both work() (guard injection) and main() (lifecycle injection) have errors.
    assert_no_piano_artifacts(&stderr, "multi_injection");
}
