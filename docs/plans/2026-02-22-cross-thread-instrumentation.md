# Cross-Thread Instrumentation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Capture ALL function call data from rayon/thread pool worker threads with correct parent-child attribution.

**Architecture:** Two orthogonal runtime changes (per-thread Arc storage + SpanContext auto-finalize) plus AST rewriter enhancements (concurrency detection + fork/adopt/shutdown injection). Each change is independently testable.

**Tech Stack:** Rust, syn (AST manipulation), std::sync (Arc, Mutex, LazyLock)

---

### Task 1: Per-thread Arc record storage

Replace TLS-only record storage with per-thread `Arc<Mutex<Vec<RawRecord>>>` registered in a global Vec. This ensures records from thread-pool workers (rayon, etc.) are always reachable even if TLS destructors never fire.

**Files:**
- Modify: `piano-runtime/src/collector.rs:22-27` (imports)
- Modify: `piano-runtime/src/collector.rs:52-100` (record storage)
- Modify: `piano-runtime/src/collector.rs:109-134` (Guard::drop)
- Modify: `piano-runtime/src/collector.rs:192-202` (collect/reset)
- Modify: `piano-runtime/src/collector.rs:257-276` (flush)

**Step 1: Write the failing test**

Add to `piano-runtime/src/collector.rs` tests module:

```rust
#[test]
fn records_from_other_threads_are_captured_via_shutdown() {
    reset();
    // Spawn a thread that does work, then joins.
    // With TLS-only storage, the thread's records would be lost
    // if TLS destructors don't fire (as with rayon workers).
    // With per-thread Arc storage, shutdown() can collect them.
    std::thread::scope(|s| {
        s.spawn(|| {
            let _g = enter("thread_work");
            burn_cpu(10_000);
        });
    });

    // shutdown() should collect records from all threads, including spawned ones.
    let records = collect_all();
    let thread_work = records.iter().find(|r| r.name == "thread_work");
    assert!(
        thread_work.is_some(),
        "thread_work should be captured via global registry. Got: {:?}",
        records.iter().map(|r| &r.name).collect::<Vec<_>>()
    );
    assert_eq!(thread_work.unwrap().calls, 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime records_from_other_threads -- --nocapture`
Expected: FAIL — `collect_all` doesn't exist yet

**Step 3: Implement per-thread Arc storage**

In `collector.rs`, add the global registry and refactor record storage:

```rust
// Global registry of per-thread record storage.
// Each thread registers its Arc on first enter(). shutdown() iterates all Arcs.
static THREAD_RECORDS: LazyLock<Mutex<Vec<Arc<Mutex<Vec<RawRecord>>>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

thread_local! {
    static STACK: RefCell<Vec<StackEntry>> = const { RefCell::new(Vec::new()) };
    static RECORDS: Arc<Mutex<Vec<RawRecord>>> = {
        let arc = Arc::new(Mutex::new(Vec::new()));
        THREAD_RECORDS.lock().unwrap_or_else(|e| e.into_inner()).push(Arc::clone(&arc));
        arc
    };
    static REGISTERED: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}
```

Update `Guard::drop()` to push to the Arc:

```rust
impl Drop for Guard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            let entry = stack.borrow_mut().pop();
            let Some(entry) = entry else {
                eprintln!("piano-runtime: guard dropped without matching stack entry (bug)");
                return;
            };
            let elapsed_ms = entry.start.elapsed().as_secs_f64() * 1000.0;
            let children_ms = entry.children_ms;

            if let Some(parent) = stack.borrow_mut().last_mut() {
                parent.children_ms += elapsed_ms;
            }

            RECORDS.with(|records| {
                records.lock().unwrap_or_else(|e| e.into_inner()).push(RawRecord {
                    name: entry.name,
                    elapsed_ms,
                    children_ms,
                });
            });
        });
    }
}
```

Add `collect_all()` that aggregates from all threads:

```rust
/// Collect records from ALL threads via the global registry.
/// This is the primary collection method — it captures data from
/// thread-pool workers whose TLS destructors may never fire.
pub fn collect_all() -> Vec<FunctionRecord> {
    let registry = THREAD_RECORDS.lock().unwrap_or_else(|e| e.into_inner());
    let mut all_raw: Vec<RawRecord> = Vec::new();
    for arc in registry.iter() {
        let records = arc.lock().unwrap_or_else(|e| e.into_inner());
        all_raw.extend(records.iter().cloned());
    }
    // Also include registered names from the current thread.
    let registered: Vec<&str> = REGISTERED
        .try_with(|reg| reg.borrow().clone())
        .unwrap_or_default();
    aggregate(&all_raw, &registered)
}
```

This requires making `RawRecord` `Clone`:

```rust
#[derive(Clone)]
struct RawRecord {
    name: &'static str,
    elapsed_ms: f64,
    children_ms: f64,
}
```

Remove `AutoFlushRecords` — it relied on TLS Drop which is exactly the mechanism that fails for thread pools. The `shutdown()` function (added in Task 3) replaces it.

Update `collect()` to delegate to `collect_all()` for backwards compatibility.

Update `reset()` to clear both the thread-local and global state.

Update `flush()` to use `collect_all()`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano-runtime records_from_other_threads -- --nocapture`
Expected: PASS

**Step 5: Run all existing tests to check for regressions**

Run: `cargo test -p piano-runtime -- --nocapture`
Expected: All tests pass. Some tests may need minor updates due to the storage change (e.g., `collect()` now goes through `collect_all()`).

**Step 6: Commit**

```
feat(runtime): replace TLS record storage with per-thread Arc registry

Records now live in Arc<Mutex<Vec<RawRecord>>> registered globally.
Each thread locks only its own Arc (zero contention). This ensures
thread-pool workers (rayon, etc.) never lose data even if their TLS
destructors don't fire.
```

---

### Task 2: SpanContext auto-finalize on Drop

Add `impl Drop for SpanContext` that performs the finalize logic automatically. This ensures correct cross-thread attribution on all return paths (early return, ?, panic) without requiring explicit `finalize()` calls.

**Files:**
- Modify: `piano-runtime/src/collector.rs:289-309` (SpanContext)

**Step 1: Write the failing test**

```rust
#[test]
fn span_context_auto_finalizes_on_drop() {
    reset();
    {
        let _parent = enter("auto_parent");
        burn_cpu(5_000);

        // fork + adopt, but do NOT call finalize() — rely on Drop.
        {
            let ctx = fork().expect("should have parent on stack");
            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("auto_child");
                    burn_cpu(20_000);
                }
            }
            // ctx drops here — should auto-finalize
        }
    }

    let records = collect();
    let parent = records.iter().find(|r| r.name == "auto_parent").unwrap();
    let child = records.iter().find(|r| r.name == "auto_child").unwrap();

    // Parent's self_ms should be less than total_ms because child time was attributed.
    assert!(
        parent.self_ms < parent.total_ms * 0.9,
        "auto-finalize should subtract child time: self={:.1}ms, total={:.1}ms",
        parent.self_ms, parent.total_ms
    );
    // Conservation check.
    let sum_self = parent.self_ms + child.self_ms;
    let error_pct = ((sum_self - parent.total_ms) / parent.total_ms).abs() * 100.0;
    assert!(
        error_pct < 10.0,
        "conservation: sum_self={sum_self:.1}ms, total={:.1}ms, error={error_pct:.1}%",
        parent.total_ms
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime span_context_auto_finalizes -- --nocapture`
Expected: FAIL — without auto-finalize, parent self_ms equals total_ms

**Step 3: Implement Drop for SpanContext**

```rust
impl Drop for SpanContext {
    fn drop(&mut self) {
        let children = *self.children_ms.lock().unwrap_or_else(|e| e.into_inner());
        STACK.with(|stack| {
            if let Some(top) = stack.borrow_mut().last_mut() {
                top.children_ms += children;
            }
        });
    }
}
```

Remove `#[must_use]` from SpanContext (Drop handles cleanup now).

Keep `finalize(self)` as a public method for explicit use — it consumes self, triggering Drop, which does the actual work. Change `finalize` to just drop self (since Drop now does the work):

```rust
impl SpanContext {
    /// Explicitly finalize cross-thread attribution.
    /// Equivalent to just dropping the SpanContext, but makes intent clear.
    pub fn finalize(self) {
        // Drop impl handles everything.
        drop(self);
    }
}
```

Wait — there's a subtlety. The current `finalize(self)` does the work, and then the value is dropped. If Drop also does the work, it would double-apply. Solution: use a flag.

```rust
pub struct SpanContext {
    parent_name: &'static str,
    children_ms: Arc<Mutex<f64>>,
    finalized: bool,
}

impl SpanContext {
    pub fn finalize(mut self) {
        self.apply_children();
        self.finalized = true;
    }

    fn apply_children(&self) {
        let children = *self.children_ms.lock().unwrap_or_else(|e| e.into_inner());
        STACK.with(|stack| {
            if let Some(top) = stack.borrow_mut().last_mut() {
                top.children_ms += children;
            }
        });
    }
}

impl Drop for SpanContext {
    fn drop(&mut self) {
        if !self.finalized {
            self.apply_children();
        }
    }
}
```

This way existing code calling `finalize()` still works, and code relying on Drop also works. No double-application.

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano-runtime span_context_auto_finalizes -- --nocapture`
Expected: PASS

**Step 5: Run all tests**

Run: `cargo test -p piano-runtime -- --nocapture`
Expected: All pass. Existing tests that call `ctx.finalize()` still work (finalize sets flag, Drop skips).

**Step 6: Commit**

```
feat(runtime): auto-finalize SpanContext on Drop

SpanContext now implements Drop to apply children_ms to the parent
stack entry. RAII guarantees correct attribution on all return paths.
Explicit finalize() still works (sets flag to prevent double-apply).
```

---

### Task 3: Add shutdown() and inject it at end of main

Add a `shutdown()` function to the runtime that collects and flushes all records from all threads. The AST rewriter injects a call to it at the end of `fn main()`.

**Files:**
- Modify: `piano-runtime/src/collector.rs` (add shutdown)
- Modify: `piano-runtime/src/lib.rs` (export shutdown)
- Modify: `src/rewrite.rs` (add inject_shutdown)
- Modify: `src/main.rs:196-202` (call inject_shutdown)

**Step 1: Write the failing test for shutdown()**

In `piano-runtime/src/collector.rs` tests:

```rust
#[test]
fn shutdown_writes_json_with_all_thread_data() {
    reset();
    std::thread::scope(|s| {
        s.spawn(|| {
            let _g = enter("shutdown_thread_work");
            burn_cpu(10_000);
        });
    });
    {
        let _g = enter("shutdown_main_work");
        burn_cpu(5_000);
    }

    let tmp = std::env::temp_dir().join(format!("piano_shutdown_{}", timestamp_ms()));
    std::fs::create_dir_all(&tmp).unwrap();
    unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
    shutdown();
    unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

    let files: Vec<_> = std::fs::read_dir(&tmp)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert!(!files.is_empty(), "shutdown should write JSON");

    let content = std::fs::read_to_string(files[0].path()).unwrap();
    assert!(content.contains("\"shutdown_thread_work\""), "should contain thread work: {content}");
    assert!(content.contains("\"shutdown_main_work\""), "should contain main work: {content}");

    let _ = std::fs::remove_dir_all(&tmp);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime shutdown_writes_json -- --nocapture`
Expected: FAIL — `shutdown` doesn't collect from global registry yet

**Step 3: Implement shutdown()**

```rust
/// Flush all collected timing data from ALL threads and write to disk.
///
/// This is the primary flush mechanism — it collects from the global
/// per-thread registry, so data from thread-pool workers is included.
/// Injected at the end of main() by the AST rewriter.
pub fn shutdown() {
    if cfg!(test) {
        return;
    }
    let records = collect_all();
    if records.is_empty() {
        return;
    }
    let Some(dir) = runs_dir() else { return };
    let path = dir.join(format!("{}.json", timestamp_ms()));
    let _ = write_json(&records, &path);
}
```

Export in `piano-runtime/src/lib.rs`:

```rust
pub use collector::{
    AdoptGuard, FunctionRecord, Guard, SpanContext, adopt, collect, collect_all,
    enter, flush, fork, init, register, reset, shutdown,
};
```

**Step 4: Write test for inject_shutdown in rewriter**

In `src/rewrite.rs` tests:

```rust
#[test]
fn injects_shutdown_at_end_of_main() {
    let source = r#"
fn main() {
    do_stuff();
}
"#;
    let result = inject_shutdown(source).unwrap();
    assert!(
        result.contains("piano_runtime::shutdown()"),
        "should inject shutdown. Got:\n{result}"
    );
    // shutdown should come after existing statements
    let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
    let do_stuff_pos = result.find("do_stuff()").unwrap();
    assert!(
        shutdown_pos > do_stuff_pos,
        "shutdown should come after existing code"
    );
}
```

**Step 5: Implement inject_shutdown in rewriter**

In `src/rewrite.rs`:

```rust
/// Inject `piano_runtime::shutdown()` as the last statement in `fn main`.
pub fn inject_shutdown(source: &str) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;
    let mut injector = ShutdownInjector;
    injector.visit_file_mut(&mut file);
    Ok(prettyplease::unparse(&file))
}

struct ShutdownInjector;

impl VisitMut for ShutdownInjector {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if node.sig.ident == "main" {
            let stmt: syn::Stmt = syn::parse_quote! {
                piano_runtime::shutdown();
            };
            node.block.stmts.push(stmt);
        }
        syn::visit_mut::visit_item_fn_mut(self, node);
    }
}
```

**Step 6: Wire inject_shutdown into cmd_build**

In `src/main.rs`, after `inject_registrations` (around line 196-202), add:

```rust
let rewritten = piano::rewrite::inject_shutdown(&rewritten).map_err(|source| {
    Error::ParseError {
        path: main_file.clone(),
        source,
    }
})?;
```

Update the import at line 15 to include `inject_shutdown`.

**Step 7: Run all tests**

Run: `cargo test -p piano-runtime -- --nocapture && cargo test -p piano -- --nocapture`
Expected: All pass

**Step 8: Commit**

```
feat(runtime): add shutdown() and inject at end of main

shutdown() collects records from all threads via the global registry
and writes a single JSON file. The AST rewriter injects the call
as the last statement in fn main().
```

---

### Task 4: Concurrency detection and fork/adopt injection

The AST rewriter detects known concurrency patterns in instrumented function bodies and injects `fork()` after the guard and `adopt()` into closures within concurrency expressions.

**Files:**
- Modify: `src/rewrite.rs` (Instrumenter: detect concurrency, inject fork/adopt)

**Step 1: Write failing test for par_iter detection**

```rust
#[test]
fn injects_fork_and_adopt_for_par_iter() {
    let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
    let targets: HashSet<String> = ["process_all".to_string()].into();
    let result = instrument_source(source, &targets).unwrap();

    assert!(
        result.contains("piano_runtime::enter(\"process_all\")"),
        "should have guard. Got:\n{result}"
    );
    assert!(
        result.contains("piano_runtime::fork()"),
        "should inject fork. Got:\n{result}"
    );
    assert!(
        result.contains("piano_runtime::adopt"),
        "should inject adopt in closure. Got:\n{result}"
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano injects_fork_and_adopt -- --nocapture`
Expected: FAIL — no fork/adopt injection yet

**Step 3: Write failing test for thread::spawn detection**

```rust
#[test]
fn injects_fork_and_adopt_for_thread_spawn() {
    let source = r#"
fn do_work() {
    std::thread::spawn(|| {
        heavy_computation();
    });
}
"#;
    let targets: HashSet<String> = ["do_work".to_string()].into();
    let result = instrument_source(source, &targets).unwrap();

    assert!(
        result.contains("piano_runtime::fork()"),
        "should inject fork. Got:\n{result}"
    );
    assert!(
        result.contains("piano_runtime::adopt"),
        "should inject adopt in spawn closure. Got:\n{result}"
    );
}
```

**Step 4: Write failing test for rayon::scope detection**

```rust
#[test]
fn injects_adopt_in_rayon_scope_spawn() {
    let source = r#"
fn parallel_work() {
    rayon::scope(|s| {
        s.spawn(|_| { work_a(); });
        s.spawn(|_| { work_b(); });
    });
}
"#;
    let targets: HashSet<String> = ["parallel_work".to_string()].into();
    let result = instrument_source(source, &targets).unwrap();

    assert!(
        result.contains("piano_runtime::fork()"),
        "should inject fork. Got:\n{result}"
    );
    assert!(
        result.contains("piano_runtime::adopt"),
        "should inject adopt. Got:\n{result}"
    );
}
```

**Step 5: Write failing test — no injection in non-target functions**

```rust
#[test]
fn no_fork_inject_in_non_target_function() {
    let source = r#"
fn not_targeted() {
    items.par_iter().map(|x| x).collect()
}
"#;
    let targets: HashSet<String> = HashSet::new();
    let result = instrument_source(source, &targets).unwrap();

    assert!(
        !result.contains("piano_runtime::fork()"),
        "should NOT inject fork in non-target function. Got:\n{result}"
    );
}
```

**Step 6: Implement concurrency detection**

Add to `src/rewrite.rs`:

```rust
/// Method names that indicate parallel iterator chains.
const PARALLEL_ITER_METHODS: &[&str] = &[
    "par_iter", "par_iter_mut", "into_par_iter",
    "par_bridge", "par_chunks", "par_chunks_mut", "par_windows",
];

/// Function/method path segments that indicate spawn/scope boundaries.
const SPAWN_FUNCTIONS: &[&str] = &[
    "spawn", "scope", "scope_fifo", "join",
];
```

Add a helper that walks an expression tree and checks if it contains concurrency calls:

```rust
/// Check if an expression contains a known concurrency method call.
fn contains_concurrency_call(expr: &syn::Expr) -> bool {
    // Recursively walk the expression looking for method calls
    // with names in PARALLEL_ITER_METHODS or SPAWN_FUNCTIONS.
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            if PARALLEL_ITER_METHODS.contains(&method.as_str())
                || SPAWN_FUNCTIONS.contains(&method.as_str())
            {
                return true;
            }
            // Check receiver and args recursively
            if contains_concurrency_call(&mc.receiver) {
                return true;
            }
            mc.args.iter().any(|arg| contains_concurrency_call(arg))
        }
        syn::Expr::Call(call) => {
            // Check for path calls like thread::spawn(), rayon::scope()
            if let syn::Expr::Path(path) = &*call.func {
                let last_seg = path.path.segments.last().map(|s| s.ident.to_string());
                if let Some(name) = last_seg {
                    if SPAWN_FUNCTIONS.contains(&name.as_str()) {
                        return true;
                    }
                }
            }
            call.args.iter().any(|arg| contains_concurrency_call(arg))
        }
        syn::Expr::Block(b) => b.block.stmts.iter().any(|s| stmt_contains_concurrency(s)),
        syn::Expr::Closure(c) => contains_concurrency_call(&c.body),
        _ => false,
    }
}

fn stmt_contains_concurrency(stmt: &syn::Stmt) -> bool {
    match stmt {
        syn::Stmt::Expr(e, _) => contains_concurrency_call(e),
        syn::Stmt::Local(local) => {
            local.init.as_ref().map_or(false, |init| contains_concurrency_call(&init.expr))
        }
        _ => false,
    }
}

fn block_contains_concurrency(block: &syn::Block) -> bool {
    block.stmts.iter().any(|s| stmt_contains_concurrency(s))
}
```

Add a helper to inject adopt into closures within concurrency expressions:

```rust
/// Walk an expression and inject adopt() at the start of closures that
/// are arguments to concurrency calls or methods chained after par_iter.
fn inject_adopt_in_concurrency_closures(expr: &mut syn::Expr, in_parallel_chain: bool) {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            let is_par = PARALLEL_ITER_METHODS.contains(&method.as_str());
            let is_spawn = SPAWN_FUNCTIONS.contains(&method.as_str());

            // Recurse into receiver first
            inject_adopt_in_concurrency_closures(&mut mc.receiver, in_parallel_chain);

            // If this method is in a parallel chain or is a spawn call,
            // inject adopt into closure args
            let chain_active = in_parallel_chain || is_par;
            if chain_active || is_spawn {
                for arg in &mut mc.args {
                    if let syn::Expr::Closure(closure) = arg {
                        inject_adopt_at_closure_start(closure);
                    }
                    // Also recurse into args (for nested concurrency)
                    inject_adopt_in_concurrency_closures(arg, false);
                }
            }

            // If receiver has par_iter, we're in a parallel chain
            // for subsequent methods
            if receiver_has_parallel_method(&mc.receiver) || in_parallel_chain {
                // Args of methods after par_iter get adopt
                if !is_par {
                    for arg in &mut mc.args {
                        if let syn::Expr::Closure(closure) = arg {
                            inject_adopt_at_closure_start(closure);
                        }
                    }
                }
            }
        }
        syn::Expr::Call(call) => {
            // Check for path calls like thread::spawn(|| ...)
            let is_spawn = if let syn::Expr::Path(path) = &*call.func {
                path.path.segments.last()
                    .map(|s| SPAWN_FUNCTIONS.contains(&s.ident.to_string().as_str()))
                    .unwrap_or(false)
            } else {
                false
            };
            if is_spawn {
                for arg in &mut call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        inject_adopt_at_closure_start(closure);
                    }
                }
            }
            // Recurse into args
            for arg in &mut call.args {
                inject_adopt_in_concurrency_closures(arg, false);
            }
        }
        syn::Expr::Block(b) => {
            for stmt in &mut b.block.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
        }
        _ => {}
    }
}

fn receiver_has_parallel_method(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            PARALLEL_ITER_METHODS.contains(&method.as_str())
                || receiver_has_parallel_method(&mc.receiver)
        }
        _ => false,
    }
}

fn inject_adopt_at_closure_start(closure: &mut syn::ExprClosure) {
    let adopt_stmt: syn::Stmt = syn::parse_quote! {
        let _piano_adopt = _piano_ctx.as_ref().map(|c| piano_runtime::adopt(c));
    };
    // Closure body may be a Block or a bare expression.
    match &mut *closure.body {
        syn::Expr::Block(block) => {
            block.block.stmts.insert(0, adopt_stmt);
        }
        other => {
            // Wrap bare expression in a block
            let existing = other.clone();
            *other = syn::parse_quote! {
                {
                    #adopt_stmt
                    #existing
                }
            };
        }
    }
}
```

Modify `Instrumenter::inject_guard` to also inject fork when concurrency is detected:

```rust
fn inject_guard(&self, block: &mut syn::Block, name: &str) {
    if !self.targets.contains(name) {
        return;
    }
    let guard_stmt: syn::Stmt = syn::parse_quote! {
        let _piano_guard = piano_runtime::enter(#name);
    };
    block.stmts.insert(0, guard_stmt);

    // If the function body contains concurrency calls, inject fork
    // and adopt in the relevant closures.
    if block_contains_concurrency(block) {
        let fork_stmt: syn::Stmt = syn::parse_quote! {
            let _piano_ctx = piano_runtime::fork();
        };
        // Insert after the guard (position 1)
        block.stmts.insert(1, fork_stmt);

        // Walk the remaining statements and inject adopt into closures
        for stmt in block.stmts.iter_mut().skip(2) {
            if let syn::Stmt::Expr(expr, _) = stmt {
                inject_adopt_in_concurrency_closures(expr, false);
            }
            if let syn::Stmt::Local(local) = stmt {
                if let Some(init) = &mut local.init {
                    inject_adopt_in_concurrency_closures(&mut init.expr, false);
                }
            }
        }
    }
}
```

**Step 7: Run all tests**

Run: `cargo test -p piano -- --nocapture`
Expected: All pass including the new fork/adopt injection tests

**Step 8: Commit**

```
feat(rewrite): detect concurrency boundaries and inject fork/adopt

The AST rewriter now detects par_iter, thread::spawn, rayon::scope,
and other concurrency patterns. Instrumented functions containing
these patterns get fork() after the guard and adopt() in worker
closures.
```

---

### Task 5: Integration test

Build a real Rust project with rayon, std::thread, and verify that piano captures ALL function calls with correct attribution.

**Files:**
- Create: `tests/integration/cross_thread/` (test project)
- Create: `tests/cross_thread_integration.rs` (integration test)

**Step 1: Create the test fixture project**

Create `tests/integration/cross_thread/Cargo.toml`:
```toml
[package]
name = "cross-thread-fixture"
version = "0.1.0"
edition = "2024"

[dependencies]
rayon = "1"
```

Create `tests/integration/cross_thread/src/main.rs`:
```rust
use rayon::prelude::*;

fn main() {
    let items: Vec<u64> = (0..100).collect();

    // Pattern 1: rayon par_iter with instrumented function calls
    let results: Vec<u64> = items.par_iter().map(|&x| compute(x)).collect();
    println!("par_iter results: {}", results.len());

    // Pattern 2: std::thread::scope with instrumented work
    std::thread::scope(|s| {
        for chunk in items.chunks(25) {
            s.spawn(|| {
                for &x in chunk {
                    compute(x);
                }
            });
        }
    });
    println!("thread::scope done");
}

fn compute(x: u64) -> u64 {
    // Burn some CPU to make timing measurable
    let mut result = x;
    for _ in 0..1000 {
        result = result.wrapping_mul(31).wrapping_add(7);
    }
    result
}
```

**Step 2: Write the integration test**

Create `tests/cross_thread_integration.rs`:

```rust
use std::process::Command;

#[test]
fn cross_thread_captures_all_calls() {
    let project_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = project_root.join("tests/integration/cross_thread");
    let piano_bin = project_root.join("target/debug/piano");

    // Build piano first
    let build_piano = Command::new("cargo")
        .args(["build", "--bin", "piano"])
        .current_dir(project_root)
        .output()
        .expect("failed to build piano");
    assert!(build_piano.status.success(), "piano build failed");

    // Use a temp dir for run output
    let tmp = tempfile::tempdir().unwrap();
    let runs_dir = tmp.path().join("runs");

    // Run piano build on the fixture
    let piano_build = Command::new(&piano_bin)
        .args(["build", "--fn", "compute", "--fn", "main", "--project"])
        .arg(&fixture)
        .arg("--runtime-path")
        .arg(project_root.join("piano-runtime"))
        .output()
        .expect("failed to run piano build");
    assert!(
        piano_build.status.success(),
        "piano build failed: {}",
        String::from_utf8_lossy(&piano_build.stderr)
    );

    let binary_path = String::from_utf8(piano_build.stdout).unwrap().trim().to_string();

    // Run the instrumented binary
    let run = Command::new(&binary_path)
        .env("PIANO_RUNS_DIR", &runs_dir)
        .output()
        .expect("failed to run instrumented binary");
    assert!(
        run.status.success(),
        "instrumented binary failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Read the JSON output
    let json_files: Vec<_> = std::fs::read_dir(&runs_dir)
        .expect("runs dir should exist")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert!(!json_files.is_empty(), "should have JSON output files");

    let content = std::fs::read_to_string(json_files[0].path()).unwrap();

    // Parse and verify
    assert!(content.contains("\"compute\""), "should contain compute function");

    // compute is called 100 times in par_iter + 100 times in thread::scope = 200
    // Extract calls count for compute
    let compute_calls_str = content
        .split("\"compute\"")
        .nth(1)
        .and_then(|s| s.split("\"calls\":").nth(1))
        .and_then(|s| s.split(',').next())
        .expect("should find compute calls");
    let compute_calls: u64 = compute_calls_str.parse().expect("should parse calls");
    assert_eq!(
        compute_calls, 200,
        "compute should be called 200 times (100 par_iter + 100 thread::scope), got {compute_calls}"
    );

    // main should have children_ms > 0 (i.e., self_ms < total_ms)
    // since compute runs as its children
    let main_self = extract_field(&content, "main", "self_ms");
    let main_total = extract_field(&content, "main", "total_ms");
    assert!(
        main_self < main_total,
        "main self_ms ({main_self}) should be less than total_ms ({main_total})"
    );
}

fn extract_field(json: &str, function: &str, field: &str) -> f64 {
    let func_section = json.split(&format!("\"{function}\"")).nth(1).unwrap();
    let value_str = func_section
        .split(&format!("\"{field}\":"))
        .nth(1)
        .unwrap()
        .split([',', '}'].as_ref())
        .next()
        .unwrap();
    value_str.parse().unwrap()
}
```

**Step 3: Run the integration test**

Run: `cargo test cross_thread_captures_all_calls -- --nocapture`
Expected: PASS — all 200 compute calls captured, main shows correct attribution

**Step 4: Commit**

```
test(integration): add cross-thread instrumentation integration test

Builds a real Rust project with rayon par_iter and std::thread::scope,
runs piano build on it, executes the instrumented binary, and verifies
all function calls are captured with correct attribution.
```

---

### Task 6: Timing accuracy verification

Verify that fork/adopt does NOT distort timing. Only instrumented function time
should be reported. The adopt mechanism must be invisible in the output.

**Files:**
- Modify: `piano-runtime/src/collector.rs` (add accuracy test)

**Step 1: Write the accuracy test**

Add to `piano-runtime/src/collector.rs` tests:

```rust
#[test]
fn fork_adopt_does_not_inflate_reported_times() {
    // Verify that fork/adopt overhead is NOT attributed to any function.
    // Only instrumented functions (via enter()) should appear in output.
    // The adopt mechanism itself should be invisible.
    reset();
    {
        let _parent = enter("timed_parent");
        burn_cpu(5_000); // ~1ms of parent work

        let ctx = fork().unwrap();

        // Simulate rayon: 4 children each doing work
        for _ in 0..4 {
            let _adopt = adopt(&ctx);
            {
                let _child = enter("timed_child");
                burn_cpu(10_000); // ~2ms per child
            }
        }
        // ctx auto-finalizes on drop
    }

    let records = collect_all();

    // Only "timed_parent" and "timed_child" should appear. No adopt/fork entries.
    let names: Vec<&str> = records.iter().map(|r| r.name.as_str()).collect();
    assert!(
        !names.iter().any(|n| n.contains("adopt") || n.contains("fork") || n.contains("piano")),
        "fork/adopt should not appear in output. Got: {:?}", names
    );

    let parent = records.iter().find(|r| r.name == "timed_parent").unwrap();
    let child = records.iter().find(|r| r.name == "timed_child").unwrap();

    // Parent should appear once, child 4 times
    assert_eq!(parent.calls, 1);
    assert_eq!(child.calls, 4);

    // Conservation: sum of self-times should approximate parent total
    let sum_self = parent.self_ms + child.self_ms;
    let error_pct = ((sum_self - parent.total_ms) / parent.total_ms).abs() * 100.0;
    assert!(
        error_pct < 10.0,
        "conservation violated: sum_self={sum_self:.1}ms, total={:.1}ms, error={error_pct:.1}%",
        parent.total_ms
    );

    // Parent self_ms should be much less than total (children subtracted)
    assert!(
        parent.self_ms < parent.total_ms * 0.5,
        "parent self ({:.1}) should be << total ({:.1}) since 4 children ran",
        parent.self_ms, parent.total_ms
    );
}
```

**Step 2: Run test**

Run: `cargo test -p piano-runtime fork_adopt_does_not_inflate -- --nocapture`
Expected: PASS

**Step 3: Commit**

```
test(runtime): verify fork/adopt does not distort reported timing
```

---

### Task 7: Cleanup and final verification

**Step 1: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Clean up benchmark directory**

Remove `benches/flush_strategy/` (was only for design exploration).

**Step 4: Revert Cargo.toml workspace exclude change**

Remove `"benches/flush_strategy"` from the exclude array in `Cargo.toml` (line 3).

**Step 5: Final commit**

```
chore: clean up flush strategy benchmarks
```
