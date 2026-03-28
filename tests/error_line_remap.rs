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
// Proof matrix: injection type x grammar production x diagnostic category
// ---------------------------------------------------------------------------

#[test]
fn proof_t1_sync_guard_type_mismatch() {
    let stderr = piano_build_stderr(
        "proof-t1",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T1");
    assert!(
        stderr.contains(":2:"),
        "T1: error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t2_sync_guard_unresolved_name() {
    let stderr = piano_build_stderr(
        "proof-t2",
        "fn work() {\n    unknown_function();\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T2");
    assert!(
        stderr.contains(":2:"),
        "T2: error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t3_sync_guard_borrow_error() {
    let stderr = piano_build_stderr(
        "proof-t3",
        "fn work() {\n    let mut v = vec![1, 2, 3];\n    let r = &v;\n    v.push(4);\n    println!(\"{:?}\", r);\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T3");
    assert!(
        stderr.contains(":4:"),
        "T3: error should reference line 4 (push while borrow held)\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t4_async_wrapper_type_mismatch() {
    let stderr = piano_build_stderr(
        "proof-t4",
        "async fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T4");
    assert!(
        stderr.contains(":2:"),
        "T4: error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t5_async_wrapper_unresolved_name() {
    let stderr = piano_build_stderr(
        "proof-t5",
        "async fn work() {\n    unknown_thing();\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T5");
    assert!(
        stderr.contains(":2:"),
        "T5: error should reference line 2\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t6_impl_future_type_mismatch_b2_regression() {
    let stderr = piano_build_stderr(
        "proof-t6",
        "fn work() -> impl std::future::Future<Output = i32> {\n    async { let x: i32 = \"hello\"; x }\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T6");
    assert!(
        stderr.contains(":2:"),
        "T6: error should reference line 2\nStderr:\n{stderr}"
    );
    // B2 regression: bare `)` corruption would break parentheses in error text.
    // Verify that if parentheses appear in the error, they are balanced.
    let open = stderr.chars().filter(|&c| c == '(').count();
    let close = stderr.chars().filter(|&c| c == ')').count();
    assert_eq!(
        open, close,
        "T6 B2 regression: unbalanced parentheses in error output\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t7_lifecycle_main_type_mismatch() {
    let stderr = piano_build_stderr(
        "proof-t7",
        "fn work() -> i32 { 1 }\nfn main() {\n    let x: i32 = \"hello\";\n}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T7");
    assert!(
        stderr.contains(":3:"),
        "T7: error should reference line 3\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t8_lifecycle_name_table_unresolved_in_main() {
    let stderr = piano_build_stderr(
        "proof-t8",
        "fn work() { }\nfn main() {\n    nonexistent();\n}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T8");
    assert!(
        stderr.contains(":3:"),
        "T8: error should reference line 3\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t9_allocator_wrap_type_mismatch() {
    let stderr = piano_build_stderr(
        "proof-t9",
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
    assert_no_piano_artifacts(&stderr, "T9");
    assert!(
        stderr.contains(":10:"),
        "T9: error should reference line 10\nStderr:\n{stderr}"
    );
}

// T10: cfg-gated allocator wrap + type mismatch after allocator
#[test]
fn proof_t10_cfg_allocator_type_mismatch() {
    let stderr = piano_build_stderr(
        "t10",
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
    // If the allocator Replace added lines, the error would be on line 12+.
    assert!(
        stderr.contains(":11:"),
        "T10: error should reference original line 11, got:\n{stderr}"
    );
    assert_no_piano_artifacts(&stderr, "T10");
}

#[test]
fn proof_t11_macro_expansion_guard_type_mismatch() {
    let stderr = piano_build_stderr(
        "proof-t11",
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
    assert_no_piano_artifacts(&stderr, "T11");
}

#[test]
fn proof_t12_internal_diagnostic_filtered() {
    let stderr = piano_build_stderr(
        "proof-t12",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() { work(); }\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T12");
    // Piano's injected __piano_guard would trigger "unused variable" if not filtered.
    assert!(
        !stderr.contains("unused variable"),
        "T12: 'unused variable' warning leaked (likely from piano internals)\nStderr:\n{stderr}"
    );
    // The actual user error should still be present.
    assert!(
        stderr.contains("mismatched types") || stderr.contains("expected"),
        "T12: user error should still appear in stderr\nStderr:\n{stderr}"
    );
}

#[test]
fn proof_t13_sync_guard_lifetime_error() {
    let stderr = piano_build_stderr(
        "proof-t13",
        "fn work<'a>(x: &i32) -> &'a i32 {\n    x\n}\nfn main() { let n = 1; let _ = work(&n); }\n",
        &["--fn", "work"],
    );
    // Lifetime errors may reference multiple lines; just verify no artifacts.
    assert_no_piano_artifacts(&stderr, "T13");
}

#[test]
fn proof_t14_async_borrow_error() {
    let stderr = piano_build_stderr(
        "proof-t14",
        "async fn work() {\n    let mut v = vec![1];\n    let r = &v;\n    v.push(2);\n    println!(\"{:?}\", r);\n}\nfn main() {}\n",
        &["--fn", "work"],
    );
    assert_no_piano_artifacts(&stderr, "T14");
}

#[test]
fn proof_t15_multiple_injections_two_errors() {
    let stderr = piano_build_stderr(
        "proof-t15",
        "fn work() -> i32 {\n    let x: i32 = \"hello\";\n    x\n}\nfn main() {\n    let y: bool = 42i32;\n}\n",
        &["--fn", "work"],
    );
    // Both work() (guard injection) and main() (lifecycle injection) have errors.
    assert_no_piano_artifacts(&stderr, "T15");
}
