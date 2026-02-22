# Piano Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build `piano`, a CLI that automates instrumentation-based profiling for Rust via AST rewriting.

**Architecture:** Workspace with two crates: `piano` (binary, orchestration) and `piano-runtime` (tiny timing library injected into user code). The binary parses user source with syn, rewrites target functions with RAII timing guards, copies source to a staging dir, builds with cached deps, and reports results from JSON output files.

**Tech Stack:** syn + quote (AST), clap (CLI), serde + serde_json (serialization), std::time::Instant (timing in runtime)

---

### Task 1: Workspace Scaffolding

**Files:**
- Modify: `Cargo.toml` (root workspace manifest)
- Create: `piano-runtime/Cargo.toml`
- Create: `piano-runtime/src/lib.rs`
- Modify: `src/lib.rs` -> delete, replace with `src/main.rs`

**Step 1: Restructure into workspace**

Convert the repo from a single lib crate to a workspace with a binary crate and a runtime library crate.

Root `Cargo.toml`:
```toml
[workspace]
members = ["piano-runtime"]

[package]
name = "piano"
version = "0.0.1"
edition = "2024"
description = "Automated instrumentation-based profiling for Rust"
license = "MIT"
repository = "https://github.com/rocketman-code/piano"
keywords = ["profiling", "instrumentation", "performance"]
categories = ["development-tools::profiling"]

[[bin]]
name = "cargo-piano"
path = "src/main.rs"

[dependencies]
clap = { version = "4", features = ["derive"] }
syn = { version = "2", features = ["full", "visit-mut"] }
quote = "1"
proc-macro2 = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`piano-runtime/Cargo.toml`:
```toml
[package]
name = "piano-runtime"
version = "0.0.1"
edition = "2024"
description = "Runtime collector for piano instrumentation"
license = "MIT"

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

`src/main.rs`:
```rust
fn main() {
    println!("piano");
}
```

`piano-runtime/src/lib.rs`:
```rust
// piano-runtime: timing collection for instrumented code
```

**Step 2: Verify it builds**

Run: `cargo build --workspace`
Expected: compiles cleanly

**Step 3: Commit**

```
feat: restructure as workspace with piano-runtime crate
```

---

### Task 2: Runtime Crate -- Core Timing

**Files:**
- Create: `piano-runtime/src/collector.rs`
- Modify: `piano-runtime/src/lib.rs`
- Create: `piano-runtime/tests/timing.rs`

**Step 1: Write the failing test**

`piano-runtime/tests/timing.rs`:
```rust
use piano_runtime::{enter, collect};
use std::thread;
use std::time::Duration;

#[test]
fn single_function_timing() {
    piano_runtime::reset();
    {
        let _guard = enter("my_func");
        thread::sleep(Duration::from_millis(10));
    }
    let results = collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "my_func");
    assert_eq!(results[0].calls, 1);
    assert!(results[0].total_ms >= 10.0);
}

#[test]
fn nested_function_self_time() {
    piano_runtime::reset();
    {
        let _outer = enter("outer");
        thread::sleep(Duration::from_millis(10));
        {
            let _inner = enter("inner");
            thread::sleep(Duration::from_millis(20));
        }
    }
    let results = collect();
    let outer = results.iter().find(|r| r.name == "outer").unwrap();
    let inner = results.iter().find(|r| r.name == "inner").unwrap();
    assert_eq!(outer.calls, 1);
    assert_eq!(inner.calls, 1);
    // outer self_time should exclude inner's time
    assert!(outer.self_ms < outer.total_ms);
    assert!(inner.self_ms >= 20.0);
}

#[test]
fn call_count_tracking() {
    piano_runtime::reset();
    for _ in 0..5 {
        let _guard = enter("repeated");
        thread::sleep(Duration::from_millis(1));
    }
    let results = collect();
    assert_eq!(results[0].calls, 5);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime`
Expected: FAIL -- `enter`, `collect`, `reset` not found

**Step 3: Implement the collector**

`piano-runtime/src/collector.rs`:
```rust
use serde::Serialize;
use std::cell::RefCell;
use std::time::Instant;

#[derive(Debug, Serialize)]
pub struct FunctionRecord {
    pub name: String,
    pub calls: u64,
    pub total_ms: f64,
    pub self_ms: f64,
}

struct StackEntry {
    name: &'static str,
    start: Instant,
    children_ms: f64,
}

struct RawRecord {
    name: &'static str,
    total_ms: f64,
    self_ms: f64,
}

thread_local! {
    static STACK: RefCell<Vec<StackEntry>> = RefCell::new(Vec::new());
    static RECORDS: RefCell<Vec<RawRecord>> = RefCell::new(Vec::new());
}

pub struct Guard {
    _private: (),
}

impl Drop for Guard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            let entry = stack.borrow_mut().pop().expect("piano: stack underflow");
            let elapsed_ms = entry.start.elapsed().as_secs_f64() * 1000.0;
            let self_ms = elapsed_ms - entry.children_ms;

            // attribute our time to parent as children_ms
            if let Some(parent) = stack.borrow_mut().last_mut() {
                parent.children_ms += elapsed_ms;
            }

            RECORDS.with(|records| {
                records.borrow_mut().push(RawRecord {
                    name: entry.name,
                    total_ms: elapsed_ms,
                    self_ms,
                });
            });
        });
    }
}

pub fn enter(name: &'static str) -> Guard {
    STACK.with(|stack| {
        stack.borrow_mut().push(StackEntry {
            name,
            start: Instant::now(),
            children_ms: 0.0,
        });
    });
    Guard { _private: () }
}

pub fn collect() -> Vec<FunctionRecord> {
    RECORDS.with(|records| {
        let records = records.borrow();
        let mut map: std::collections::HashMap<&str, FunctionRecord> =
            std::collections::HashMap::new();
        for rec in records.iter() {
            let entry = map.entry(rec.name).or_insert_with(|| FunctionRecord {
                name: rec.name.to_string(),
                calls: 0,
                total_ms: 0.0,
                self_ms: 0.0,
            });
            entry.calls += 1;
            entry.total_ms += rec.total_ms;
            entry.self_ms += rec.self_ms;
        }
        let mut result: Vec<FunctionRecord> = map.into_values().collect();
        result.sort_by(|a, b| b.self_ms.partial_cmp(&a.self_ms).unwrap());
        result
    })
}

pub fn reset() {
    STACK.with(|stack| stack.borrow_mut().clear());
    RECORDS.with(|records| records.borrow_mut().clear());
}
```

`piano-runtime/src/lib.rs`:
```rust
mod collector;

pub use collector::{enter, collect, reset, FunctionRecord, Guard};
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p piano-runtime`
Expected: 3 tests PASS

**Step 5: Commit**

```
feat(runtime): add core timing collector with RAII guard
```

---

### Task 3: Runtime Crate -- JSON Flush

**Files:**
- Modify: `piano-runtime/src/collector.rs`
- Modify: `piano-runtime/src/lib.rs`
- Create: `piano-runtime/tests/flush.rs`

**Step 1: Write the failing test**

`piano-runtime/tests/flush.rs`:
```rust
use piano_runtime::{enter, flush_to_file};
use std::thread;
use std::time::Duration;

#[test]
fn flush_writes_valid_json() {
    piano_runtime::reset();
    {
        let _guard = enter("test_fn");
        thread::sleep(Duration::from_millis(5));
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.json");
    flush_to_file(&path).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    let value: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert!(value["timestamp"].is_string());
    assert!(value["functions"].is_array());
    let funcs = value["functions"].as_array().unwrap();
    assert_eq!(funcs.len(), 1);
    assert_eq!(funcs[0]["name"], "test_fn");
}
```

Add `tempfile` as a dev-dependency in `piano-runtime/Cargo.toml`:
```toml
[dev-dependencies]
tempfile = "3"
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime flush`
Expected: FAIL -- `flush_to_file` not found

**Step 3: Implement flush_to_file**

Add to `piano-runtime/src/collector.rs`:
```rust
use serde::Serialize;
use std::io;
use std::path::Path;

#[derive(Serialize)]
struct RunReport {
    timestamp: String,
    functions: Vec<FunctionRecord>,
}

pub fn flush_to_file(path: &Path) -> io::Result<()> {
    let functions = collect();
    let report = RunReport {
        timestamp: now_iso8601(),
        functions,
    };
    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, json)
}

fn now_iso8601() -> String {
    // Simple UTC timestamp without chrono dependency
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let secs = duration.as_secs();
    // Format as seconds-based ID -- good enough for file naming
    format!("{secs}")
}
```

Export from `lib.rs`:
```rust
pub use collector::{enter, collect, reset, flush_to_file, FunctionRecord, Guard};
```

**Step 4: Run tests**

Run: `cargo test -p piano-runtime`
Expected: all PASS

**Step 5: Commit**

```
feat(runtime): add JSON flush for run results
```

---

### Task 4: Target Resolution

**Files:**
- Create: `src/resolve.rs`
- Create: `tests/resolve.rs`
- Modify: `src/main.rs`

**Step 1: Write the failing test**

Create a test fixture: a small Rust source file to parse. The test creates it in a tempdir.

`tests/resolve.rs`:
```rust
use piano::resolve::{resolve_targets, TargetSpec};
use std::fs;

#[test]
fn resolve_fn_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(
        src.join("lib.rs"),
        r#"
fn walk(root: &str) -> Vec<String> { vec![] }
fn load() -> i32 { 42 }
fn unrelated() {}
"#,
    )
    .unwrap();

    let targets = resolve_targets(&src, &[TargetSpec::Fn("walk".into())]).unwrap();
    assert_eq!(targets.len(), 1);
    assert!(targets[0].file.ends_with("lib.rs"));
    assert_eq!(targets[0].functions, vec!["walk"]);
}

#[test]
fn resolve_fn_substring_match() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(
        src.join("lib.rs"),
        r#"
fn walk_dir() {}
fn walk_file() {}
fn load() {}
"#,
    )
    .unwrap();

    let targets = resolve_targets(&src, &[TargetSpec::Fn("walk".into())]).unwrap();
    assert_eq!(targets[0].functions.len(), 2);
}

#[test]
fn resolve_file() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("walker.rs"), "fn a() {} fn b() {}").unwrap();
    fs::write(src.join("cache.rs"), "fn c() {}").unwrap();

    let targets =
        resolve_targets(&src, &[TargetSpec::File("walker.rs".into())]).unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].functions.len(), 2);
}

#[test]
fn resolve_mod() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let walker_dir = src.join("walker");
    fs::create_dir_all(&walker_dir).unwrap();
    fs::write(walker_dir.join("mod.rs"), "fn a() {}").unwrap();
    fs::write(walker_dir.join("util.rs"), "fn b() {} fn c() {}").unwrap();
    fs::write(src.join("cache.rs"), "fn d() {}").unwrap();

    let targets =
        resolve_targets(&src, &[TargetSpec::Mod("walker".into())]).unwrap();
    // Should find functions in walker/mod.rs and walker/util.rs
    let total_fns: usize = targets.iter().map(|t| t.functions.len()).sum();
    assert_eq!(total_fns, 3);
}
```

Add `tempfile` as a dev-dependency in root `Cargo.toml`.

**Step 2: Run test to verify it fails**

Run: `cargo test --test resolve`
Expected: FAIL -- module `resolve` not found

**Step 3: Implement target resolution**

`src/resolve.rs`:
```rust
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum TargetSpec {
    Fn(String),
    File(String),
    Mod(String),
}

#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub file: PathBuf,
    pub functions: Vec<String>,
}

pub fn resolve_targets(
    src_dir: &Path,
    specs: &[TargetSpec],
) -> Result<Vec<ResolvedTarget>, Box<dyn std::error::Error>> {
    let mut targets = Vec::new();

    for spec in specs {
        match spec {
            TargetSpec::Fn(pattern) => {
                targets.extend(resolve_fn_pattern(src_dir, pattern)?);
            }
            TargetSpec::File(filename) => {
                targets.extend(resolve_file(src_dir, filename)?);
            }
            TargetSpec::Mod(module) => {
                targets.extend(resolve_mod(src_dir, module)?);
            }
        }
    }

    // Merge targets that point to the same file
    merge_targets(&mut targets);
    Ok(targets)
}

fn resolve_fn_pattern(
    src_dir: &Path,
    pattern: &str,
) -> Result<Vec<ResolvedTarget>, Box<dyn std::error::Error>> {
    let mut targets = Vec::new();
    for entry in walk_rs_files(src_dir)? {
        let source = std::fs::read_to_string(&entry)?;
        let file = syn::parse_file(&source)?;
        let matching: Vec<String> = file
            .items
            .iter()
            .filter_map(|item| {
                if let syn::Item::Fn(f) = item {
                    let name = f.sig.ident.to_string();
                    if name.contains(pattern) {
                        return Some(name);
                    }
                }
                None
            })
            .collect();
        if !matching.is_empty() {
            targets.push(ResolvedTarget {
                file: entry,
                functions: matching,
            });
        }
    }
    Ok(targets)
}

fn resolve_file(
    src_dir: &Path,
    filename: &str,
) -> Result<Vec<ResolvedTarget>, Box<dyn std::error::Error>> {
    let target_path = find_file(src_dir, filename)?;
    let source = std::fs::read_to_string(&target_path)?;
    let file = syn::parse_file(&source)?;
    let functions = extract_all_fn_names(&file);
    Ok(vec![ResolvedTarget {
        file: target_path,
        functions,
    }])
}

fn resolve_mod(
    src_dir: &Path,
    module: &str,
) -> Result<Vec<ResolvedTarget>, Box<dyn std::error::Error>> {
    let mut targets = Vec::new();
    let mod_dir = src_dir.join(module);
    let mod_file = src_dir.join(format!("{module}.rs"));

    if mod_dir.is_dir() {
        for entry in walk_rs_files(&mod_dir)? {
            let source = std::fs::read_to_string(&entry)?;
            let file = syn::parse_file(&source)?;
            let functions = extract_all_fn_names(&file);
            if !functions.is_empty() {
                targets.push(ResolvedTarget {
                    file: entry,
                    functions,
                });
            }
        }
    } else if mod_file.is_file() {
        let source = std::fs::read_to_string(&mod_file)?;
        let file = syn::parse_file(&source)?;
        let functions = extract_all_fn_names(&file);
        if !functions.is_empty() {
            targets.push(ResolvedTarget {
                file: mod_file,
                functions,
            });
        }
    }

    Ok(targets)
}

fn extract_all_fn_names(file: &syn::File) -> Vec<String> {
    file.items
        .iter()
        .filter_map(|item| {
            if let syn::Item::Fn(f) = item {
                Some(f.sig.ident.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn walk_rs_files(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walk_rs_files(&path)?);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn find_file(src_dir: &Path, filename: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    for entry in walk_rs_files(src_dir)? {
        if entry.file_name().is_some_and(|n| n == filename) {
            return Ok(entry);
        }
    }
    Err(format!("file not found: {filename}").into())
}

fn merge_targets(targets: &mut Vec<ResolvedTarget>) {
    targets.sort_by(|a, b| a.file.cmp(&b.file));
    targets.dedup_by(|b, a| {
        if a.file == b.file {
            a.functions.extend(b.functions.drain(..));
            true
        } else {
            false
        }
    });
}
```

Export from `src/main.rs` (make it a bin+lib or add `src/lib.rs`):

Create `src/lib.rs`:
```rust
pub mod resolve;
```

`src/main.rs`:
```rust
fn main() {
    println!("piano");
}
```

**Step 4: Run tests**

Run: `cargo test --test resolve`
Expected: all PASS

**Step 5: Commit**

```
feat: add target resolution (--fn, --file, --mod matching)
```

---

### Task 5: AST Rewriter

**Files:**
- Create: `src/rewrite.rs`
- Create: `tests/rewrite.rs`
- Modify: `src/lib.rs`

**Step 1: Write the failing test**

`tests/rewrite.rs`:
```rust
use piano::rewrite::instrument_source;

#[test]
fn instruments_target_function() {
    let source = r#"
fn walk(root: &str) -> Vec<String> {
    let entries = read(root);
    entries
}

fn other() -> i32 {
    42
}
"#;

    let result = instrument_source(source, &["walk".to_string()]).unwrap();
    assert!(result.contains("piano_runtime::enter"));
    assert!(result.contains("\"walk\""));
    // other() should not be instrumented
    let other_pos = result.find("fn other").unwrap();
    let other_body = &result[other_pos..];
    assert!(!other_body.contains("piano_runtime"));
}

#[test]
fn instruments_multiple_functions() {
    let source = r#"
fn a() {}
fn b() {}
fn c() {}
"#;

    let result = instrument_source(source, &["a".to_string(), "b".to_string()]).unwrap();
    assert!(result.contains("piano_runtime::enter(\"a\")"));
    assert!(result.contains("piano_runtime::enter(\"b\")"));
    assert!(!result.contains("piano_runtime::enter(\"c\")"));
}

#[test]
fn preserves_function_signature() {
    let source = "fn walk(root: &str) -> Vec<String> { vec![] }";
    let result = instrument_source(source, &["walk".to_string()]).unwrap();
    assert!(result.contains("fn walk(root: &str) -> Vec<String>"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test rewrite`
Expected: FAIL -- module `rewrite` not found

**Step 3: Implement the rewriter**

`src/rewrite.rs`:
```rust
use proc_macro2::Span;
use quote::quote;
use syn::visit_mut::VisitMut;
use syn::{parse_file, ItemFn};

struct Instrumenter {
    targets: Vec<String>,
}

impl VisitMut for Instrumenter {
    fn visit_item_fn_mut(&mut self, node: &mut ItemFn) {
        let name = node.sig.ident.to_string();
        if self.targets.contains(&name) {
            let name_lit = syn::LitStr::new(&name, Span::call_site());
            let guard_stmt: syn::Stmt = syn::parse_quote! {
                let _piano_guard = piano_runtime::enter(#name_lit);
            };
            node.block.stmts.insert(0, guard_stmt);
        }
        // Continue visiting nested functions
        syn::visit_mut::visit_item_fn_mut(self, node);
    }
}

pub fn instrument_source(
    source: &str,
    targets: &[String],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut ast = parse_file(source)?;
    let mut instrumenter = Instrumenter {
        targets: targets.to_vec(),
    };
    instrumenter.visit_item_fn_mut_all(&mut ast);
    Ok(prettyplease::unparse(&ast))
}

// Visit all items in the file
impl Instrumenter {
    fn visit_item_fn_mut_all(&mut self, file: &mut syn::File) {
        for item in &mut file.items {
            if let syn::Item::Fn(f) = item {
                self.visit_item_fn_mut(f);
            }
        }
    }
}
```

Add `prettyplease` to root `Cargo.toml` dependencies:
```toml
prettyplease = "0.2"
```

Update `src/lib.rs`:
```rust
pub mod resolve;
pub mod rewrite;
```

**Step 4: Run tests**

Run: `cargo test --test rewrite`
Expected: all PASS

**Step 5: Commit**

```
feat: add AST rewriter for function instrumentation
```

---

### Task 6: Build Orchestrator

**Files:**
- Create: `src/build.rs`
- Create: `tests/build.rs`
- Modify: `src/lib.rs`

**Step 1: Write the failing test**

`tests/build.rs`:
```rust
use piano::build::prepare_staging;
use std::fs;

#[test]
fn staging_copies_src_and_cargo_toml() {
    let project = tempfile::tempdir().unwrap();
    let src = project.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("main.rs"), "fn main() {}").unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        r#"[package]
name = "example"
version = "0.1.0"
edition = "2024"
"#,
    )
    .unwrap();

    let staging = tempfile::tempdir().unwrap();
    prepare_staging(project.path(), staging.path()).unwrap();

    assert!(staging.path().join("src/main.rs").exists());
    let cargo_toml = fs::read_to_string(staging.path().join("Cargo.toml")).unwrap();
    assert!(cargo_toml.contains("piano-runtime"));
}

#[test]
fn staging_preserves_nested_src_structure() {
    let project = tempfile::tempdir().unwrap();
    let nested = project.path().join("src/walker");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("mod.rs"), "fn walk() {}").unwrap();
    fs::write(project.path().join("src/main.rs"), "fn main() {}").unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"ex\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();

    let staging = tempfile::tempdir().unwrap();
    prepare_staging(project.path(), staging.path()).unwrap();

    assert!(staging.path().join("src/walker/mod.rs").exists());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test build`
Expected: FAIL -- module `build` not found

**Step 3: Implement build orchestrator**

`src/build.rs`:
```rust
use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;

pub fn prepare_staging(project_root: &Path, staging_dir: &Path) -> io::Result<()> {
    // Copy src/ directory
    let src = project_root.join("src");
    let staging_src = staging_dir.join("src");
    copy_dir_recursive(&src, &staging_src)?;

    // Copy and modify Cargo.toml to add piano-runtime dependency
    let cargo_toml = fs::read_to_string(project_root.join("Cargo.toml"))?;
    let modified = inject_runtime_dependency(&cargo_toml);
    fs::write(staging_dir.join("Cargo.toml"), modified)?;

    // Copy Cargo.lock if it exists
    let lock = project_root.join("Cargo.lock");
    if lock.exists() {
        fs::copy(&lock, staging_dir.join("Cargo.lock"))?;
    }

    Ok(())
}

pub fn build_instrumented(
    staging_dir: &Path,
    original_target_dir: &Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let output = Command::new("cargo")
        .arg("build")
        .current_dir(staging_dir)
        .env("CARGO_TARGET_DIR", original_target_dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("build failed:\n{stderr}").into());
    }

    // Find the binary in the target dir
    let debug_dir = original_target_dir.join("debug");
    Ok(debug_dir)
}

fn inject_runtime_dependency(cargo_toml: &str) -> String {
    // Find the [dependencies] section and add piano-runtime
    // If no [dependencies] section, add one
    if cargo_toml.contains("[dependencies]") {
        cargo_toml.replacen(
            "[dependencies]",
            "[dependencies]\npiano-runtime = { path = \"piano-runtime\" }",
            1,
        )
    } else {
        format!("{cargo_toml}\n[dependencies]\npiano-runtime = {{ path = \"piano-runtime\" }}\n")
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
```

Update `src/lib.rs`:
```rust
pub mod build;
pub mod resolve;
pub mod rewrite;
```

**Step 4: Run tests**

Run: `cargo test --test build`
Expected: all PASS

**Step 5: Commit**

```
feat: add build orchestrator with staging and dep injection
```

---

### Task 7: Reporter

**Files:**
- Create: `src/report.rs`
- Create: `tests/report.rs`
- Modify: `src/lib.rs`

**Step 1: Write the failing test**

`tests/report.rs`:
```rust
use piano::report::{load_run, format_table, diff_runs};

#[test]
fn load_run_from_json() {
    let json = r#"{
        "timestamp": "1740000000",
        "functions": [
            {"name": "walk", "calls": 1, "total_ms": 100.0, "self_ms": 80.0},
            {"name": "load", "calls": 3, "total_ms": 60.0, "self_ms": 60.0}
        ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.json");
    std::fs::write(&path, json).unwrap();

    let run = load_run(&path).unwrap();
    assert_eq!(run.functions.len(), 2);
    assert_eq!(run.functions[0].name, "walk");
}

#[test]
fn format_table_sorts_by_self_time() {
    let json = r#"{
        "timestamp": "1740000000",
        "functions": [
            {"name": "fast", "calls": 1, "total_ms": 10.0, "self_ms": 10.0},
            {"name": "slow", "calls": 1, "total_ms": 100.0, "self_ms": 100.0}
        ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.json");
    std::fs::write(&path, json).unwrap();

    let run = load_run(&path).unwrap();
    let table = format_table(&run);
    let slow_pos = table.find("slow").unwrap();
    let fast_pos = table.find("fast").unwrap();
    assert!(slow_pos < fast_pos, "slow should appear first (higher self-time)");
}

#[test]
fn diff_shows_delta() {
    let run_a = r#"{
        "timestamp": "1",
        "functions": [
            {"name": "walk", "calls": 1, "total_ms": 100.0, "self_ms": 100.0}
        ]
    }"#;
    let run_b = r#"{
        "timestamp": "2",
        "functions": [
            {"name": "walk", "calls": 1, "total_ms": 80.0, "self_ms": 80.0}
        ]
    }"#;

    let dir = tempfile::tempdir().unwrap();
    let path_a = dir.path().join("a.json");
    let path_b = dir.path().join("b.json");
    std::fs::write(&path_a, run_a).unwrap();
    std::fs::write(&path_b, run_b).unwrap();

    let a = load_run(&path_a).unwrap();
    let b = load_run(&path_b).unwrap();
    let diff = diff_runs(&a, &b);
    assert!(diff.contains("walk"));
    assert!(diff.contains("-20")); // 80 - 100 = -20ms improvement
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test report`
Expected: FAIL -- module `report` not found

**Step 3: Implement reporter**

`src/report.rs`:
```rust
use piano_runtime::FunctionRecord;
use serde::Deserialize;
use std::io;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Run {
    pub timestamp: String,
    pub functions: Vec<FunctionRecord>,
}

pub fn load_run(path: &Path) -> Result<Run, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let run: Run = serde_json::from_str(&contents)?;
    Ok(run)
}

pub fn format_table(run: &Run) -> String {
    let mut funcs = run.functions.clone();
    funcs.sort_by(|a, b| b.self_ms.partial_cmp(&a.self_ms).unwrap());

    let mut out = String::new();
    out.push_str(&format!(
        "{:<30} {:>6} {:>10} {:>10}\n",
        "Function", "Calls", "Self", "Total"
    ));
    out.push_str(&"-".repeat(60));
    out.push('\n');

    for f in &funcs {
        out.push_str(&format!(
            "{:<30} {:>6} {:>9.1}ms {:>9.1}ms\n",
            f.name, f.calls, f.self_ms, f.total_ms,
        ));
    }
    out
}

pub fn diff_runs(a: &Run, b: &Run) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<30} {:>10} {:>10} {:>10}\n",
        "Function", "Before", "After", "Delta"
    ));
    out.push_str(&"-".repeat(65));
    out.push('\n');

    for fb in &b.functions {
        let fa = a.functions.iter().find(|f| f.name == fb.name);
        let (before, delta) = match fa {
            Some(fa) => (fa.self_ms, fb.self_ms - fa.self_ms),
            None => (0.0, fb.self_ms),
        };
        out.push_str(&format!(
            "{:<30} {:>9.1}ms {:>9.1}ms {:>+9.1}ms\n",
            fb.name, before, fb.self_ms, delta,
        ));
    }
    out
}
```

Note: `FunctionRecord` needs `Deserialize` and `Clone` derives added in `piano-runtime/src/collector.rs`:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionRecord { ... }
```

Update `src/lib.rs`:
```rust
pub mod build;
pub mod report;
pub mod resolve;
pub mod rewrite;
```

**Step 4: Run tests**

Run: `cargo test --test report`
Expected: all PASS

**Step 5: Commit**

```
feat: add reporter with table output and run diffing
```

---

### Task 8: CLI Integration

**Files:**
- Modify: `src/main.rs`

**Step 1: Implement the CLI**

`src/main.rs`:
```rust
use clap::{Parser, Subcommand};
use piano::build::{build_instrumented, prepare_staging};
use piano::report::{diff_runs, format_table, load_run};
use piano::resolve::{resolve_targets, TargetSpec};
use piano::rewrite::instrument_source;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "cargo-piano", bin_name = "cargo")]
struct Cargo {
    #[command(subcommand)]
    command: CargoCommand,
}

#[derive(Subcommand)]
enum CargoCommand {
    Piano(PianoArgs),
}

#[derive(Parser)]
struct PianoArgs {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Instrument target functions and build
    Build {
        /// Function names to instrument (substring match)
        #[arg(long = "fn", num_args = 1..)]
        functions: Vec<String>,

        /// Module names to instrument (all functions)
        #[arg(long = "mod", num_args = 1..)]
        modules: Vec<String>,

        /// File paths to instrument (all functions)
        #[arg(long = "file", num_args = 1..)]
        files: Vec<String>,
    },
    /// Show results from a profiling run
    Report {
        /// Run ID (defaults to latest)
        run_id: Option<String>,
    },
    /// Compare two profiling runs
    Diff {
        /// First run ID
        a: String,
        /// Second run ID
        b: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cargo::parse();
    let CargoCommand::Piano(args) = cli.command;

    match args.command {
        Command::Build {
            functions,
            modules,
            files,
        } => cmd_build(functions, modules, files),
        Command::Report { run_id } => cmd_report(run_id),
        Command::Diff { a, b } => cmd_diff(a, b),
    }
}

fn cmd_build(
    functions: Vec<String>,
    modules: Vec<String>,
    files: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let project_root = std::env::current_dir()?;
    let src_dir = project_root.join("src");

    // Build target specs
    let mut specs: Vec<TargetSpec> = Vec::new();
    for f in functions {
        specs.push(TargetSpec::Fn(f));
    }
    for m in modules {
        specs.push(TargetSpec::Mod(m));
    }
    for f in files {
        specs.push(TargetSpec::File(f));
    }

    if specs.is_empty() {
        return Err("specify at least one target: --fn, --mod, or --file".into());
    }

    // Resolve targets
    let targets = resolve_targets(&src_dir, &specs)?;
    if targets.is_empty() {
        return Err("no matching functions found".into());
    }

    let total_fns: usize = targets.iter().map(|t| t.functions.len()).sum();
    eprintln!("Instrumenting {total_fns} function(s) across {} file(s)", targets.len());

    // Prepare staging directory
    let staging = tempfile::tempdir()?;
    prepare_staging(&project_root, staging.path())?;

    // Rewrite instrumented files in staging
    for target in &targets {
        let rel = target.file.strip_prefix(&src_dir)?;
        let staged_file = staging.path().join("src").join(rel);
        let source = fs::read_to_string(&staged_file)?;
        let instrumented = instrument_source(&source, &target.functions)?;
        fs::write(&staged_file, instrumented)?;
    }

    // Build
    eprintln!("Building instrumented binary...");
    let target_dir = project_root.join("target");
    build_instrumented(staging.path(), &target_dir)?;

    eprintln!("Done. Run your binary and then use `cargo piano report` to see results.");
    Ok(())
}

fn cmd_report(run_id: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let runs_dir = runs_dir()?;
    let path = match run_id {
        Some(id) => runs_dir.join(format!("{id}.json")),
        None => latest_run(&runs_dir)?,
    };
    let run = load_run(&path)?;
    print!("{}", format_table(&run));
    Ok(())
}

fn cmd_diff(a: String, b: String) -> Result<(), Box<dyn std::error::Error>> {
    let runs_dir = runs_dir()?;
    let run_a = load_run(&runs_dir.join(format!("{a}.json")))?;
    let run_b = load_run(&runs_dir.join(format!("{b}.json")))?;
    print!("{}", diff_runs(&run_a, &run_b));
    Ok(())
}

fn runs_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let home = std::env::var("HOME")?;
    Ok(PathBuf::from(home).join(".cargo-piano/runs"))
}

fn latest_run(runs_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut entries: Vec<PathBuf> = fs::read_dir(runs_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    entries.sort();
    entries
        .last()
        .cloned()
        .ok_or_else(|| "no runs found -- run your instrumented binary first".into())
}
```

Add `tempfile` to root `Cargo.toml` dependencies (not just dev-deps, since build uses it at runtime):
```toml
tempfile = "3"
```

**Step 2: Verify it builds**

Run: `cargo build`
Expected: compiles cleanly

**Step 3: Smoke test**

Run: `cargo run -- piano --help`
Expected: shows subcommands (build, report, diff)

**Step 4: Commit**

```
feat: add CLI with build, report, and diff commands
```

---

### Task 9: End-to-End Integration Test

**Files:**
- Create: `tests/e2e.rs`

**Step 1: Write the integration test**

`tests/e2e.rs`:
```rust
use std::fs;
use std::process::Command;

#[test]
fn end_to_end_instrument_and_report() {
    // Create a tiny Rust project in a tempdir
    let project = tempfile::tempdir().unwrap();
    let src = project.path().join("src");
    fs::create_dir_all(&src).unwrap();

    fs::write(
        project.path().join("Cargo.toml"),
        r#"[package]
name = "test-project"
version = "0.1.0"
edition = "2024"
"#,
    )
    .unwrap();

    fs::write(
        src.join("main.rs"),
        r#"
fn slow_function() {
    std::thread::sleep(std::time::Duration::from_millis(50));
}

fn main() {
    slow_function();
}
"#,
    )
    .unwrap();

    // Run cargo piano build
    let piano_bin = env!("CARGO_BIN_EXE_cargo-piano");
    let output = Command::new(piano_bin)
        .args(["piano", "build", "--fn", "slow_function"])
        .current_dir(project.path())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Run the instrumented binary
    let binary = project.path().join("target/debug/test-project");
    let output = Command::new(&binary).output().unwrap();
    assert!(output.status.success());

    // Check that a run file was created
    let home = std::env::var("HOME").unwrap();
    let runs_dir = std::path::PathBuf::from(home).join(".cargo-piano/runs");
    assert!(runs_dir.exists(), "runs directory should exist after running instrumented binary");
}
```

**Step 2: Run the test**

Run: `cargo test --test e2e`
Expected: PASS (this exercises the full pipeline)

**Step 3: Commit**

```
test: add end-to-end integration test
```

---

### Task 10: Cleanup and Polish

**Step 1: Run full test suite and clippy**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, no warnings

**Step 2: Fix any issues found**

**Step 3: Update the crate description and ensure Cargo.toml metadata is complete**

**Step 4: Commit**

```
chore: cleanup and lint fixes
```
