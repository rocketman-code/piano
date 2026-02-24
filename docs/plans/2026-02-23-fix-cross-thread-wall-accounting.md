# Fix Cross-Thread Wall-Time Accounting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stop propagating wall time across thread boundaries in fork/adopt; only propagate CPU time.

**Architecture:** Remove the `children_ms` Arc from SpanContext and `ctx_children_ms` from AdoptGuard. AdoptGuard::drop() keeps CPU propagation only. SpanContext::apply_children() keeps CPU propagation only. Wall self_ms becomes elapsed minus same-thread children (never cross-thread). CPU self remains cpu_elapsed minus cpu_children (cross-thread, additive).

**Tech Stack:** Rust, piano-runtime crate, cargo test with `--features cpu-time`

---

### Task 1: Write failing test for wall-time non-propagation

**Files:**
- Modify: `piano-runtime/src/collector.rs:1556-1596` (test section)

**Step 1: Write the failing test**

Add this test at the end of the `mod tests` block in `collector.rs`. It verifies that fork/adopt does NOT subtract wall time from the parent's self_ms. Currently the test will fail because wall time IS subtracted (making self_ms near zero for parallel workloads).

```rust
#[test]
fn fork_adopt_does_not_subtract_wall_time_from_parent() {
    // Wall time should NOT be subtracted cross-thread.
    // Parent wall self = elapsed - same-thread children only.
    // The adopted child's wall time must not reduce parent's wall self.
    reset();
    {
        let _parent = enter("wall_parent");
        burn_cpu(5_000); // parent does some work itself

        let ctx = fork().unwrap();

        // Simulate a child thread doing significant work.
        // (Same thread for test simplicity — the mechanism is identical.)
        {
            let _adopt = adopt(&ctx);
            {
                let _child = enter("wall_child");
                burn_cpu(50_000); // child does much more work
            }
        }

        ctx.finalize();
    }

    let records = collect();
    let parent = records.iter().find(|r| r.name == "wall_parent").unwrap();
    let child = records.iter().find(|r| r.name == "wall_child").unwrap();

    // Parent's wall self_ms should be approximately parent.total_ms
    // because the child's wall time should NOT be subtracted.
    // (Only same-thread children reduce wall self. The adopted child
    // runs in a synthetic scope that doesn't count as a same-thread child.)
    //
    // Before fix: parent.self_ms is near zero (child wall subtracted).
    // After fix: parent.self_ms ~ parent.total_ms (no cross-thread subtraction).
    assert!(
        parent.self_ms > child.self_ms * 0.5,
        "parent wall self ({:.3}ms) should NOT be reduced by cross-thread child wall ({:.3}ms). \
         parent.total={:.3}ms",
        parent.self_ms,
        child.self_ms,
        parent.total_ms,
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime --features cpu-time fork_adopt_does_not_subtract_wall_time_from_parent -- --nocapture`

Expected: FAIL — parent.self_ms is near zero because child wall time is currently subtracted.

**Step 3: Commit the failing test**

```bash
git add piano-runtime/src/collector.rs
git commit -m "test(runtime): add failing test for cross-thread wall-time non-propagation"
```

---

### Task 2: Remove wall-time propagation from SpanContext and AdoptGuard

**Files:**
- Modify: `piano-runtime/src/collector.rs:697-840`

**Step 1: Remove `children_ms` from SpanContext**

In the `SpanContext` struct (line ~697), remove the `children_ms` field:

```rust
// BEFORE:
pub struct SpanContext {
    parent_name: &'static str,
    children_ms: Arc<Mutex<f64>>,
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc<Mutex<u64>>,
    finalized: bool,
}

// AFTER:
pub struct SpanContext {
    parent_name: &'static str,
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc<Mutex<u64>>,
    finalized: bool,
}
```

**Step 2: Update `SpanContext::apply_children()`**

Remove the wall-time propagation. Keep CPU-time only:

```rust
// BEFORE:
fn apply_children(&self) {
    let children = *self.children_ms.lock().unwrap_or_else(|e| e.into_inner());
    #[cfg(feature = "cpu-time")]
    let children_cpu = *self
        .children_cpu_ns
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.children_ms += children;
            #[cfg(feature = "cpu-time")]
            {
                top.cpu_children_ns += children_cpu;
            }
        }
    });
}

// AFTER:
fn apply_children(&self) {
    #[cfg(feature = "cpu-time")]
    {
        let children_cpu = *self
            .children_cpu_ns
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        STACK.with(|stack| {
            if let Some(top) = stack.borrow_mut().last_mut() {
                top.cpu_children_ns += children_cpu;
            }
        });
    }
}
```

**Step 3: Remove `ctx_children_ms` from AdoptGuard**

```rust
// BEFORE:
pub struct AdoptGuard {
    ctx_children_ms: Arc<Mutex<f64>>,
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc<Mutex<u64>>,
}

// AFTER:
pub struct AdoptGuard {
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc<Mutex<u64>>,
}
```

**Step 4: Update `AdoptGuard::drop()`**

Remove wall-time propagation. Keep CPU only:

```rust
// BEFORE (inside STACK.with closure):
let elapsed_ms = entry.start.elapsed().as_secs_f64() * 1000.0;

// Propagate this thread's total wall time back to the parent context.
let mut children = self
    .ctx_children_ms
    .lock()
    .unwrap_or_else(|e| e.into_inner());
*children += elapsed_ms;

// Propagate this thread's CPU time back to the parent context.
#[cfg(feature = "cpu-time")]
{
    let cpu_elapsed_ns =
        crate::cpu_clock::cpu_now_ns().saturating_sub(entry.cpu_start_ns);
    let mut cpu_children = self
        .ctx_children_cpu_ns
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *cpu_children += cpu_elapsed_ns;
}

// AFTER (inside STACK.with closure):
// Only propagate CPU time back to the parent context.
// Wall time is NOT propagated cross-thread (it's not additive for parallel work).
#[cfg(feature = "cpu-time")]
{
    let cpu_elapsed_ns =
        crate::cpu_clock::cpu_now_ns().saturating_sub(entry.cpu_start_ns);
    let mut cpu_children = self
        .ctx_children_cpu_ns
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *cpu_children += cpu_elapsed_ns;
}
```

**Step 5: Update `fork()` constructor**

Remove `children_ms` initialization:

```rust
// BEFORE:
Some(SpanContext {
    parent_name: top.name,
    children_ms: Arc::new(Mutex::new(0.0)),
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc::new(Mutex::new(0)),
    finalized: false,
})

// AFTER:
Some(SpanContext {
    parent_name: top.name,
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc::new(Mutex::new(0)),
    finalized: false,
})
```

**Step 6: Update `adopt()` constructor**

Remove `ctx_children_ms` from AdoptGuard construction:

```rust
// BEFORE:
AdoptGuard {
    ctx_children_ms: Arc::clone(&ctx.children_ms),
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc::clone(&ctx.children_cpu_ns),
}

// AFTER:
AdoptGuard {
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc::clone(&ctx.children_cpu_ns),
}
```

**Step 7: Run the new test to verify it passes**

Run: `cargo test -p piano-runtime --features cpu-time fork_adopt_does_not_subtract_wall_time_from_parent -- --nocapture`

Expected: PASS

**Step 8: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "fix(runtime): stop propagating wall time across thread boundaries

Wall time is not additive for parallel work. 8 workers running 44ms each
sum to 347ms children, but actual elapsed was 44ms. Only CPU time
(which IS additive) crosses thread boundaries via adopt."
```

---

### Task 3: Fix existing tests that assume cross-thread wall subtraction

**Files:**
- Modify: `piano-runtime/src/collector.rs` (tests section)

Several existing tests assert that `parent.self_ms < parent.total_ms` after fork/adopt (assuming wall time was subtracted). These need updating.

**Step 1: Update `fork_adopt_propagates_child_time_to_parent`**

This test currently asserts parent self < total (wall subtracted). After the fix, wall self equals total for the parent (no cross-thread subtraction). Replace the wall-time assertions with CPU-time assertions:

```rust
#[test]
fn fork_adopt_propagates_child_time_to_parent() {
    reset();
    {
        let _parent = enter("parent_fn");
        burn_cpu(5_000);

        let ctx = fork().expect("should have parent on stack");

        // Simulate a child thread (same thread for test simplicity).
        {
            let _adopt = adopt(&ctx);
            {
                let _child = enter("child_fn");
                burn_cpu(20_000);
            }
        }

        ctx.finalize();
    }

    let records = collect();
    let parent = records.iter().find(|r| r.name == "parent_fn").unwrap();
    let child = records.iter().find(|r| r.name == "child_fn").unwrap();

    // Parent's wall total should include the child's wall time (it elapsed
    // during the parent's scope), but self_ms should NOT have child wall
    // subtracted (no cross-thread wall subtraction).
    assert!(
        parent.total_ms > child.total_ms,
        "parent total ({:.1}ms) should exceed child total ({:.1}ms)",
        parent.total_ms,
        child.total_ms
    );

    // Wall self_ms ~ total_ms (no cross-thread subtraction).
    // Allow some tolerance for same-thread overhead.
    assert!(
        parent.self_ms > parent.total_ms * 0.5,
        "parent wall self ({:.3}ms) should be close to total ({:.3}ms) — no cross-thread wall subtraction",
        parent.self_ms,
        parent.total_ms,
    );
}
```

**Step 2: Update `cross_thread_fork_adopt_propagates`**

This test asserts `parent.self_ms < parent.total_ms` after a real cross-thread spawn. Update:

```rust
#[test]
fn cross_thread_fork_adopt_propagates() {
    reset();
    {
        let _parent = enter("parent_fn");
        burn_cpu(5_000);

        let ctx = fork().expect("should have parent on stack");

        thread::scope(|s| {
            s.spawn(|| {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("thread_child");
                    burn_cpu(10_000);
                }
            });
        });

        ctx.finalize();
    }

    let records = collect();
    let parent = records.iter().find(|r| r.name == "parent_fn").unwrap();

    // collect() is thread-local so we can only see the parent.
    // Wall self_ms should NOT be reduced by cross-thread child.
    assert_eq!(parent.calls, 1);
    assert!(
        parent.self_ms > parent.total_ms * 0.5,
        "parent wall self ({:.3}ms) should be close to total ({:.3}ms) — no cross-thread wall subtraction",
        parent.self_ms,
        parent.total_ms,
    );
}
```

**Step 3: Update `span_context_auto_finalizes_on_drop`**

Same pattern — remove assertion that `parent.self_ms < parent.total_ms * 0.9` and the conservation check that assumes cross-thread wall subtraction:

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
    let _child = records.iter().find(|r| r.name == "auto_child").unwrap();

    // Auto-finalize should work (no crash, valid timing).
    // Wall self_ms is NOT reduced by cross-thread child wall time.
    assert_eq!(parent.calls, 1);
    assert!(parent.total_ms > 0.0);
    assert!(
        parent.self_ms > parent.total_ms * 0.5,
        "auto_parent wall self ({:.3}ms) should be close to total ({:.3}ms)",
        parent.self_ms,
        parent.total_ms,
    );
}
```

**Step 4: Run all tests**

Run: `cargo test -p piano-runtime --features cpu-time -- --nocapture`

Expected: ALL pass

**Step 5: Also run without cpu-time feature to check non-cpu-time build**

Run: `cargo test -p piano-runtime -- --nocapture`

Expected: ALL pass (AdoptGuard becomes an empty struct, apply_children is a no-op)

**Step 6: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "test(runtime): update fork/adopt tests for wall-time non-propagation"
```

---

### Task 4: Add CPU-time cross-thread propagation test

**Files:**
- Modify: `piano-runtime/src/collector.rs` (tests section)

**Step 1: Write a test that verifies CPU self IS reduced by cross-thread children**

```rust
#[cfg(feature = "cpu-time")]
#[test]
fn cpu_time_propagated_across_threads_via_adopt() {
    // CPU time IS additive across threads and should be propagated.
    // Parent's cpu_self should be reduced by adopted children's CPU usage.
    reset();
    {
        let _parent = enter("cpu_parent");
        burn_cpu(5_000); // parent does some CPU work

        let ctx = fork().unwrap();

        {
            let _adopt = adopt(&ctx);
            {
                let _child = enter("cpu_child");
                burn_cpu(50_000); // child does much more CPU work
            }
        }

        ctx.finalize();
    }

    let records = collect();
    let parent = records.iter().find(|r| r.name == "cpu_parent").unwrap();

    // Parent's cpu_self_ms should be less than its wall self_ms because
    // the child's CPU time was subtracted from the parent's CPU budget.
    assert!(
        parent.cpu_self_ms < parent.self_ms * 0.8,
        "parent cpu_self ({:.3}ms) should be much less than wall self ({:.3}ms) — \
         child CPU was subtracted",
        parent.cpu_self_ms,
        parent.self_ms,
    );
}
```

**Step 2: Run test**

Run: `cargo test -p piano-runtime --features cpu-time cpu_time_propagated_across_threads_via_adopt -- --nocapture`

Expected: PASS

**Step 3: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "test(runtime): verify CPU time propagation across thread boundaries"
```

---

### Task 5: CLI warning for parallel code without --cpu-time

**Files:**
- Modify: `src/rewrite.rs` (expose concurrency detection result)
- Modify: `src/main.rs:211-229` (emit warning after rewriting)

The rewriter already detects concurrency via `block_contains_concurrency()`. We need
to surface which functions have concurrency and what patterns were found, then warn
the user in the CLI if `--cpu-time` is not enabled.

**Step 1: Add return type to `instrument_source` that reports concurrency**

In `src/rewrite.rs`, change `instrument_source` to return concurrency metadata alongside
the rewritten source. Add a struct:

```rust
pub struct InstrumentResult {
    pub source: String,
    /// Functions that contain concurrency patterns, with the pattern name.
    /// e.g. [("concurrent_discover", "rayon::scope"), ("process_all", "par_iter")]
    pub concurrency: Vec<(String, String)>,
}
```

Update `instrument_source` to return `Result<InstrumentResult, syn::Error>`.
The `Instrumenter` collects concurrency info as it walks functions.

**Step 2: Emit warning in CLI**

In `src/main.rs`, after the rewriting loop (line ~229), check if any concurrency was
detected and `!cpu_time`:

```rust
if !cpu_time {
    let concurrent_fns: Vec<(String, String)> = /* collected from all InstrumentResults */;
    if !concurrent_fns.is_empty() {
        if concurrent_fns.len() == 1 {
            let (func, pattern) = &concurrent_fns[0];
            eprintln!(
                "warning: {} uses {} but --cpu-time is not enabled.", func, pattern
            );
            eprintln!(
                "         Wall time will include time blocked waiting for workers."
            );
            eprintln!(
                "         Re-run with --cpu-time to separate CPU from wall time."
            );
        } else {
            eprintln!("warning: parallel code profiled without --cpu-time:");
            for (func, pattern) in &concurrent_fns {
                eprintln!("           {} ({})", func, pattern);
            }
            eprintln!(
                "         Wall time will include time blocked waiting for workers."
            );
            eprintln!(
                "         Re-run with --cpu-time to separate CPU from wall time."
            );
        }
    }
}
```

**Step 3: Run the full test suite**

Run: `cargo test`

Expected: ALL pass. Existing tests don't check stderr for this warning.

**Step 4: Commit**

```bash
git add src/rewrite.rs src/main.rs
git commit -m "feat(cli): warn when profiling parallel code without --cpu-time"
```

---

### Task 6: Verify on chainsaw (integration test)

**Step 1: Rebuild piano**

Run: `cargo build --features cpu-time`

Expected: Clean build, no warnings

**Step 2: Build instrumented chainsaw**

Run: `cargo run --features cpu-time -- build /path/to/chainsaw`

(Use the same chainsaw path from previous session — check `scripts/profile-chainsaw.sh` for the path)

**Step 3: Run instrumented chainsaw on typeorm**

Run: `./target/release/chainsaw trace /path/to/typeorm/src/index.ts`

Expected: `concurrent_discover` now shows non-zero Self time (should be ~40-50ms wall in debug build). CPU Self should be ~0ms (it's an orchestrator that sleeps). No 0.0us values for functions that clearly run.

**Step 4: Commit any final adjustments**

```bash
git add -A
git commit -m "fix(runtime): validate cross-thread accounting on chainsaw"
```
