# Per-Frame Timing and Allocation Tracking Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add per-frame timing granularity and zero-distortion allocation tracking to piano, enabling game developers to see per-frame breakdowns, percentiles, and memory allocation stats without any manual annotation.

**Architecture:** The runtime switches from immediate aggregation to per-invocation recording with frame-boundary aggregation. A global allocator wrapper tracks allocations per function scope and subtracts its own overhead from timing. The JSON output moves to NDJSON v2 format with frame-level summaries. The reporting side gains percentile columns, a --frames view, and alloc stats.

**Tech Stack:** Rust, std::alloc::GlobalAlloc, thread-local storage, NDJSON, syn/quote for AST rewriting.

**Design doc:** `docs/plans/2026-02-22-per-frame-alloc-tracking-design.md`

---

### Task 1: Extend StackEntry and Add InvocationRecord

**Files:**
- Modify: `piano-runtime/src/collector.rs:46-57`
- Test: `piano-runtime/src/collector.rs` (inline tests)

**Step 1: Write the failing test**

Add to the test module in `piano-runtime/src/collector.rs`:

```rust
#[test]
fn invocation_records_capture_depth() {
    reset();
    {
        let _outer = enter("outer");
        burn_cpu(5_000);
        {
            let _inner = enter("inner");
            burn_cpu(5_000);
        }
    }
    let invocations = collect_invocations();
    let outer_inv = invocations.iter().find(|r| r.name == "outer").unwrap();
    let inner_inv = invocations.iter().find(|r| r.name == "inner").unwrap();
    assert_eq!(outer_inv.depth, 0);
    assert_eq!(inner_inv.depth, 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime invocation_records_capture_depth -- --nocapture`
Expected: FAIL with "cannot find function `collect_invocations`"

**Step 3: Implement the new data structures**

In `piano-runtime/src/collector.rs`:

1. Add fields to `StackEntry`:
```rust
struct StackEntry {
    name: &'static str,
    start: Instant,
    children_ms: f64,
    overhead_ns: u128,     // alloc tracking overhead to subtract
    alloc_count: u32,      // allocations during this scope
    alloc_bytes: u64,      // bytes allocated during this scope
    free_count: u32,       // frees during this scope
    free_bytes: u64,       // bytes freed during this scope
    depth: u16,            // call stack depth (0 = outermost)
}
```

2. Add `InvocationRecord`:
```rust
pub struct InvocationRecord {
    pub name: &'static str,
    pub start_ns: u64,
    pub elapsed_ns: u64,
    pub self_ns: u64,
    pub alloc_count: u32,
    pub alloc_bytes: u64,
    pub free_count: u32,
    pub free_bytes: u64,
    pub depth: u16,
}
```

3. Add a process-start `Instant` for relative timestamps:
```rust
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
```

4. Update `enter()` to set depth from current stack length and initialize new fields to zero.

5. Update `Guard::drop` to compute `self_ns` with overhead subtraction:
```rust
let elapsed_ns = entry.start.elapsed().as_nanos() as u64;
let children_ns = (entry.children_ms * 1_000_000.0) as u64;
let self_ns = elapsed_ns.saturating_sub(children_ns).saturating_sub(entry.overhead_ns as u64);
let start_ns = entry.start.duration_since(*EPOCH).as_nanos() as u64;
```

6. Replace `RawRecord` with `InvocationRecord` in `RECORDS`. Keep `RawRecord` as a private type or migrate fully.

7. Add `collect_invocations()` that returns the raw `Vec<InvocationRecord>` (not aggregated).

8. Keep existing `collect()` working by aggregating from invocations (backward compat for existing tests).

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano-runtime invocation_records_capture_depth -- --nocapture`
Expected: PASS

**Step 5: Run full test suite**

Run: `cargo test -p piano-runtime`
Expected: All existing tests PASS (backward compatibility)

**Step 6: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "feat(runtime): add InvocationRecord with depth and alloc fields"
```

---

### Task 2: Add PianoAllocator

**Files:**
- Create: `piano-runtime/src/alloc.rs`
- Modify: `piano-runtime/src/lib.rs`
- Test: `piano-runtime/src/alloc.rs` (inline tests)

**Step 1: Write the failing test**

Create `piano-runtime/src/alloc.rs` with test only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enter, reset, collect_invocations};

    #[test]
    fn allocator_tracks_alloc_count_and_bytes() {
        reset();
        {
            let _g = enter("alloc_test");
            // Force some heap allocations
            let v: Vec<u8> = vec![0u8; 1024];
            std::hint::black_box(&v);
        }
        let invocations = collect_invocations();
        let rec = invocations.iter().find(|r| r.name == "alloc_test").unwrap();
        assert!(rec.alloc_count > 0, "should have tracked allocations");
        assert!(rec.alloc_bytes >= 1024, "should have tracked at least 1024 bytes");
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime allocator_tracks_alloc_count_and_bytes -- --nocapture`
Expected: FAIL (module not found or alloc_count is 0)

**Step 3: Implement PianoAllocator**

In `piano-runtime/src/alloc.rs`:

```rust
use std::alloc::{GlobalAlloc, Layout};
use std::time::Instant;

use crate::collector::STACK;

/// Thread-local re-entrancy guard to prevent infinite recursion.
/// When the tracking code itself allocates, we must not re-enter tracking.
thread_local! {
    static IN_ALLOC_TRACKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A global allocator wrapper that tracks allocation counts and bytes
/// per instrumented function scope, with zero timing distortion.
///
/// Wraps any inner `GlobalAlloc`. Measures its own bookkeeping overhead
/// and subtracts it from the current function's timing via `overhead_ns`.
pub struct PianoAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> PianoAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for PianoAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        track_alloc(layout.size() as u64);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        track_dealloc(layout.size() as u64);
    }
}

fn track_alloc(bytes: u64) {
    let ok = IN_ALLOC_TRACKING.try_with(|flag| {
        if flag.get() {
            return false;
        }
        flag.set(true);
        true
    });
    if ok != Ok(true) {
        return;
    }

    let t0 = Instant::now();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.alloc_count += 1;
            top.alloc_bytes += bytes;
        }
    });
    let overhead = t0.elapsed().as_nanos();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.overhead_ns += overhead;
        }
    });

    let _ = IN_ALLOC_TRACKING.try_with(|flag| flag.set(false));
}

fn track_dealloc(bytes: u64) {
    let ok = IN_ALLOC_TRACKING.try_with(|flag| {
        if flag.get() {
            return false;
        }
        flag.set(true);
        true
    });
    if ok != Ok(true) {
        return;
    }

    let t0 = Instant::now();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.free_count += 1;
            top.free_bytes += bytes;
        }
    });
    let overhead = t0.elapsed().as_nanos();
    STACK.with(|stack| {
        if let Some(top) = stack.borrow_mut().last_mut() {
            top.overhead_ns += overhead;
        }
    });

    let _ = IN_ALLOC_TRACKING.try_with(|flag| flag.set(false));
}
```

Note: `STACK` needs to be made `pub(crate)` in `collector.rs` for `alloc.rs` to access it.

Update `piano-runtime/src/lib.rs`:
```rust
mod alloc;
mod collector;

pub use alloc::PianoAllocator;
pub use collector::{
    AdoptGuard, FunctionRecord, Guard, InvocationRecord, SpanContext,
    adopt, collect, collect_invocations, enter, flush, fork, init, register, reset,
};
```

**Step 4: Run test to verify it passes**

Note: This test will only pass when `PianoAllocator` is actually set as the global allocator in the test binary. Since we can't set a global allocator per-test, we need to either:
- Use a separate test binary with `#[global_allocator]`, or
- Test the tracking functions directly by calling `track_alloc` manually

Adjust the test to call `track_alloc` directly:

```rust
#[test]
fn track_alloc_updates_stack_entry() {
    reset();
    {
        let _g = enter("alloc_test");
        // Simulate allocator tracking calls
        track_alloc(1024);
        track_alloc(512);
        track_dealloc(256);
    }
    let invocations = collect_invocations();
    let rec = invocations.iter().find(|r| r.name == "alloc_test").unwrap();
    assert_eq!(rec.alloc_count, 2);
    assert_eq!(rec.alloc_bytes, 1536);
    assert_eq!(rec.free_count, 1);
    assert_eq!(rec.free_bytes, 256);
}
```

Run: `cargo test -p piano-runtime track_alloc_updates_stack_entry -- --nocapture`
Expected: PASS

**Step 5: Write overhead subtraction test**

```rust
#[test]
fn alloc_tracking_does_not_distort_timing() {
    reset();
    // Measure CPU work WITHOUT any alloc tracking
    let baseline_start = std::time::Instant::now();
    crate::tests::burn_cpu(50_000);
    let baseline_ns = baseline_start.elapsed().as_nanos() as u64;

    // Now measure WITH alloc tracking overhead injected
    {
        let _g = enter("timed_fn");
        crate::tests::burn_cpu(50_000);
        // Simulate 10K allocations worth of tracking overhead
        for _ in 0..10_000 {
            track_alloc(64);
        }
    }
    let invocations = collect_invocations();
    let rec = invocations.iter().find(|r| r.name == "timed_fn").unwrap();

    // self_ns should be close to baseline (overhead subtracted)
    // Allow 20% tolerance for measurement noise
    let ratio = rec.self_ns as f64 / baseline_ns as f64;
    assert!(
        (0.8..1.2).contains(&ratio),
        "self_ns ({}) should be close to baseline ({}) but ratio was {:.2}",
        rec.self_ns, baseline_ns, ratio
    );
}
```

Run: `cargo test -p piano-runtime alloc_tracking_does_not_distort_timing -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add piano-runtime/src/alloc.rs piano-runtime/src/lib.rs piano-runtime/src/collector.rs
git commit -m "feat(runtime): add PianoAllocator with zero-distortion overhead subtraction"
```

---

### Task 3: Frame Boundary Detection and FrameSummary Aggregation

**Files:**
- Modify: `piano-runtime/src/collector.rs`
- Test: `piano-runtime/src/collector.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn frame_boundary_aggregation() {
    reset();
    // Simulate 3 frames: depth-0 function called 3 times
    for frame in 0..3u32 {
        let _outer = enter("update");
        burn_cpu(5_000);
        {
            let _inner = enter("physics");
            burn_cpu(5_000);
        }
    }
    let frames = collect_frames();
    assert_eq!(frames.len(), 3, "should have 3 frames");
    for frame in &frames {
        let update = frame.iter().find(|s| s.name == "update").unwrap();
        assert_eq!(update.calls, 1);
        assert!(update.self_ns > 0);
        let physics = frame.iter().find(|s| s.name == "physics").unwrap();
        assert_eq!(physics.calls, 1);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime frame_boundary_aggregation -- --nocapture`
Expected: FAIL with "cannot find function `collect_frames`"

**Step 3: Implement FrameSummary and frame detection**

Add to `piano-runtime/src/collector.rs`:

```rust
/// Per-function summary within a single frame.
#[derive(Debug, Clone)]
pub struct FrameFnSummary {
    pub name: &'static str,
    pub calls: u32,
    pub self_ns: u64,
    pub alloc_count: u32,
    pub alloc_bytes: u64,
    pub free_count: u32,
    pub free_bytes: u64,
}
```

Add new thread-local state:
```rust
thread_local! {
    // Invocations accumulated within the current frame (cleared on frame boundary)
    static FRAME_BUFFER: RefCell<Vec<InvocationRecord>> = RefCell::new(Vec::new());
    // Completed frame summaries
    static FRAMES: RefCell<Vec<Vec<FrameFnSummary>>> = RefCell::new(Vec::new());
}
```

In `Guard::drop`, after creating the `InvocationRecord`:
- Push to `FRAME_BUFFER` instead of `RECORDS`
- If this record has `depth == 0`, that's a frame boundary:
  - Aggregate all records in `FRAME_BUFFER` into `Vec<FrameFnSummary>` (one per unique function name)
  - Push to `FRAMES`
  - Clear `FRAME_BUFFER`

Add `collect_frames()`:
```rust
pub fn collect_frames() -> Vec<Vec<FrameFnSummary>> {
    FRAMES.with(|f| f.borrow().clone())
}
```

Update `reset()` to also clear `FRAME_BUFFER` and `FRAMES`.

Keep `collect()` working: derive aggregated `FunctionRecord` from frames + any remaining unflushed invocations.

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano-runtime frame_boundary_aggregation -- --nocapture`
Expected: PASS

**Step 5: Write test for non-frame fallback**

```rust
#[test]
fn non_frame_workload_still_collects() {
    reset();
    // All calls at depth > 0 relative to each other, or
    // simulate a library without a main loop: just flat calls
    {
        let _a = enter("parse");
        burn_cpu(5_000);
    }
    {
        let _b = enter("resolve");
        burn_cpu(5_000);
    }
    // No depth-0 "frame" function, so frames should be empty
    let frames = collect_frames();
    assert!(frames.is_empty(), "no frames for non-frame workload");

    // But aggregate collect() should still work
    let records = collect();
    assert_eq!(records.len(), 2);
}
```

Run: `cargo test -p piano-runtime non_frame_workload_still_collects -- --nocapture`
Expected: PASS

**Step 6: Run full test suite**

Run: `cargo test -p piano-runtime`
Expected: All tests PASS

**Step 7: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "feat(runtime): add frame boundary detection and per-frame aggregation"
```

---

### Task 4: v2 NDJSON Writer

**Files:**
- Modify: `piano-runtime/src/collector.rs`
- Test: `piano-runtime/src/collector.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn write_ndjson_v2_format() {
    reset();
    for _ in 0..2 {
        let _outer = enter("update");
        burn_cpu(5_000);
        {
            let _inner = enter("physics");
            burn_cpu(5_000);
        }
    }

    let tmp = std::env::temp_dir().join(format!("piano_ndjson_{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();

    unsafe { std::env::set_var("PIANO_RUNS_DIR", &tmp) };
    flush();
    unsafe { std::env::remove_var("PIANO_RUNS_DIR") };

    let files: Vec<_> = std::fs::read_dir(&tmp)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ndjson"))
        .collect();
    assert!(!files.is_empty(), "should write .ndjson file");

    let content = std::fs::read_to_string(files[0].path()).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    // First line is header
    assert!(lines[0].contains("\"format_version\":2"));
    assert!(lines[0].contains("\"functions\""));

    // Remaining lines are frames
    assert!(lines.len() >= 3, "header + 2 frames");
    assert!(lines[1].contains("\"frame\":0"));
    assert!(lines[2].contains("\"frame\":1"));

    let _ = std::fs::remove_dir_all(&tmp);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano-runtime write_ndjson_v2_format -- --nocapture`
Expected: FAIL (no .ndjson files written)

**Step 3: Implement v2 writer**

Add `write_ndjson` function to `collector.rs`:

```rust
fn write_ndjson(
    frames: &[Vec<FrameFnSummary>],
    fn_names: &[&'static str],
    path: &std::path::Path,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = std::fs::File::create(path)?;
    let ts = timestamp_ms();
    let run_id = &*RUN_ID;

    // Header line: metadata + function name table
    write!(f, "{{\"format_version\":2,\"run_id\":\"{run_id}\",\"timestamp_ms\":{ts},\"functions\":[")?;
    for (i, name) in fn_names.iter().enumerate() {
        if i > 0 { write!(f, ",")?; }
        let name = name.replace('\\', "\\\\").replace('"', "\\\"");
        write!(f, "\"{name}\"")?;
    }
    writeln!(f, "]}}")?;

    // One line per frame
    for (frame_idx, frame) in frames.iter().enumerate() {
        write!(f, "{{\"frame\":{frame_idx},\"fns\":[")?;
        for (i, s) in frame.iter().enumerate() {
            if i > 0 { write!(f, ",")?; }
            let fn_id = fn_names.iter().position(|&n| n == s.name).unwrap_or(0);
            write!(
                f,
                "{{\"id\":{fn_id},\"calls\":{},\"self_ns\":{},\"ac\":{},\"ab\":{},\"fc\":{},\"fb\":{}}}",
                s.calls, s.self_ns, s.alloc_count, s.alloc_bytes, s.free_count, s.free_bytes
            )?;
        }
        writeln!(f, "]}}")?;
    }
    Ok(())
}
```

Update `AutoFlushRecords::drop` and `flush()`:
- If frames are present, write `.ndjson` using `write_ndjson`
- If no frames (non-frame workload), fall back to v1 JSON via `write_json`

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano-runtime write_ndjson_v2_format -- --nocapture`
Expected: PASS

**Step 5: Verify existing flush test still passes**

Run: `cargo test -p piano-runtime flush_writes_valid_json -- --nocapture`
Expected: PASS (non-frame workloads still write v1 JSON)

**Step 6: Commit**

```bash
git add piano-runtime/src/collector.rs
git commit -m "feat(runtime): add v2 NDJSON writer for frame-level data"
```

---

### Task 5: Inject Global Allocator via AST Rewriting

**Files:**
- Modify: `src/rewrite.rs`
- Modify: `src/main.rs:164-203` (cmd_build)
- Test: `src/rewrite.rs` (inline tests)

**Step 1: Write the failing test**

Add to test module in `src/rewrite.rs`:

```rust
#[test]
fn injects_global_allocator() {
    let source = r#"
fn main() {
    println!("hello");
}
"#;
    let result = inject_global_allocator(source, None).unwrap();
    assert!(
        result.contains("#[global_allocator]"),
        "should inject global_allocator attribute. Got:\n{result}"
    );
    assert!(
        result.contains("PianoAllocator"),
        "should use PianoAllocator. Got:\n{result}"
    );
    assert!(
        result.contains("std::alloc::System"),
        "should wrap System allocator. Got:\n{result}"
    );
}

#[test]
fn wraps_existing_global_allocator() {
    let source = r#"
use std::alloc::System;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
    let result = inject_global_allocator(source, Some("System")).unwrap();
    assert!(
        result.contains("PianoAllocator"),
        "should wrap existing allocator. Got:\n{result}"
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano inject_global_allocator -- --nocapture`
Expected: FAIL with "cannot find function `inject_global_allocator`"

**Step 3: Implement inject_global_allocator**

Add to `src/rewrite.rs`:

```rust
/// Inject a `#[global_allocator]` static using `PianoAllocator` wrapping System.
///
/// If `existing_allocator_type` is provided, the source already has a
/// `#[global_allocator]` that should be wrapped rather than replaced.
pub fn inject_global_allocator(
    source: &str,
    existing_allocator_type: Option<&str>,
) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;

    match existing_allocator_type {
        None => {
            // Add a new #[global_allocator] static at the top of the file
            let item: syn::Item = syn::parse_quote! {
                #[global_allocator]
                static _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>
                    = piano_runtime::PianoAllocator::new(std::alloc::System);
            };
            file.items.insert(0, item);
        }
        Some(_) => {
            // Find and wrap the existing #[global_allocator] static
            // Replace its type with PianoAllocator<OriginalType> and
            // wrap its initializer with PianoAllocator::new(original_init)
            for item in &mut file.items {
                if let syn::Item::Static(static_item) = item {
                    let has_global_alloc = static_item.attrs.iter().any(|a| {
                        a.path().is_ident("global_allocator")
                    });
                    if has_global_alloc {
                        let orig_ty = &static_item.ty;
                        let orig_expr = &static_item.expr;
                        static_item.ty = Box::new(syn::parse_quote! {
                            piano_runtime::PianoAllocator<#orig_ty>
                        });
                        static_item.expr = Box::new(syn::parse_quote! {
                            piano_runtime::PianoAllocator::new(#orig_expr)
                        });
                        break;
                    }
                }
            }
        }
    }

    Ok(prettyplease::unparse(&file))
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p piano inject_global_allocator -- --nocapture`
Expected: PASS

**Step 5: Wire into cmd_build**

In `src/main.rs`, after the existing registration injection (line ~203), add:

```rust
// Inject global allocator for allocation tracking.
if main_file.exists() {
    let main_source = std::fs::read_to_string(&main_file)
        .map_err(|source| Error::RunReadError {
            path: main_file.clone(),
            source,
        })?;

    // Check if source already has a #[global_allocator]
    let existing = if main_source.contains("#[global_allocator]") {
        Some("existing") // the inject function will find and wrap it
    } else {
        None
    };

    let rewritten = inject_global_allocator(&main_source, existing)
        .map_err(|source| Error::ParseError {
            path: main_file.clone(),
            source,
        })?;
    std::fs::write(&main_file, rewritten)?;
}
```

**Step 6: Commit**

```bash
git add src/rewrite.rs src/main.rs
git commit -m "feat(rewrite): inject PianoAllocator via AST rewriting"
```

---

### Task 6: Parse v2 NDJSON in Report

**Files:**
- Modify: `src/report.rs`
- Test: `src/report.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn load_v2_ndjson_run() {
    let dir = TempDir::new().unwrap();
    let content = r#"{"format_version":2,"run_id":"test_1","timestamp_ms":1000,"functions":["update","physics"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":2000000,"ac":10,"ab":4096,"fc":8,"fb":3072},{"id":1,"calls":1,"self_ns":1000000,"ac":0,"ab":0,"fc":0,"fb":0}]}
{"frame":1,"fns":[{"id":0,"calls":1,"self_ns":2100000,"ac":12,"ab":5000,"fc":10,"fb":4000},{"id":1,"calls":1,"self_ns":950000,"ac":0,"ab":0,"fc":0,"fb":0}]}
"#;
    fs::write(dir.path().join("1000.ndjson"), content).unwrap();

    let run = load_latest_run(dir.path()).unwrap();
    assert_eq!(run.functions.len(), 2);

    let update = run.functions.iter().find(|f| f.name == "update").unwrap();
    assert_eq!(update.calls, 2);
    // total self_ms should be (2000000 + 2100000) / 1_000_000 = 4.1ms
    assert!((update.self_ms - 4.1).abs() < 0.01);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano load_v2_ndjson_run -- --nocapture`
Expected: FAIL (ndjson files not recognized)

**Step 3: Implement v2 parsing**

Add to `src/report.rs`:

1. New structs for v2 format:
```rust
#[derive(Debug, serde::Deserialize)]
struct NdjsonHeader {
    format_version: u32,
    run_id: Option<String>,
    timestamp_ms: u128,
    functions: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct NdjsonFrame {
    frame: usize,
    fns: Vec<NdjsonFnEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct NdjsonFnEntry {
    id: usize,
    calls: u64,
    self_ns: u64,
    #[serde(default)]
    ac: u32,
    #[serde(default)]
    ab: u64,
    #[serde(default)]
    fc: u32,
    #[serde(default)]
    fb: u64,
}
```

2. New struct for enriched run data:
```rust
pub struct FrameData {
    pub frames: Vec<Vec<FrameFnEntry>>,
    pub fn_names: Vec<String>,
}

pub struct FrameFnEntry {
    pub fn_id: usize,
    pub calls: u64,
    pub self_ns: u64,
    pub alloc_count: u32,
    pub alloc_bytes: u64,
}
```

3. Add alloc fields to `FnEntry`:
```rust
pub struct FnEntry {
    pub name: String,
    pub calls: u64,
    pub total_ms: f64,
    pub self_ms: f64,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
}
```

4. Parsing function `load_ndjson` that reads header + frame lines, returns `Run` (aggregated) + `FrameData`.

5. Update `collect_run_files` to also find `.ndjson` files.

6. Update `load_run`, `load_latest_run` to handle `.ndjson` extension.

7. Aggregate v2 data into existing `Run` format for backward compat with `format_table` and `diff_runs`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano load_v2_ndjson_run -- --nocapture`
Expected: PASS

**Step 5: Verify all existing report tests still pass**

Run: `cargo test -p piano report -- --nocapture`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/report.rs
git commit -m "feat(report): parse v2 NDJSON format with frame-level data"
```

---

### Task 7: Enhanced Report Table with Percentiles and Alloc Stats

**Files:**
- Modify: `src/report.rs`
- Test: `src/report.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn format_table_shows_percentiles_and_allocs() {
    let frame_data = FrameData {
        fn_names: vec!["update".into(), "physics".into()],
        frames: vec![
            vec![
                FrameFnEntry { fn_id: 0, calls: 1, self_ns: 2_000_000, alloc_count: 10, alloc_bytes: 4096 },
                FrameFnEntry { fn_id: 1, calls: 1, self_ns: 1_000_000, alloc_count: 0, alloc_bytes: 0 },
            ],
            vec![
                FrameFnEntry { fn_id: 0, calls: 1, self_ns: 8_000_000, alloc_count: 50, alloc_bytes: 16384 },
                FrameFnEntry { fn_id: 1, calls: 1, self_ns: 1_100_000, alloc_count: 0, alloc_bytes: 0 },
            ],
        ],
    };
    let table = format_table_v2(&frame_data);
    assert!(table.contains("p50"), "should have p50 column");
    assert!(table.contains("p99"), "should have p99 column");
    assert!(table.contains("Allocs"), "should have allocs column");
    assert!(table.contains("update"), "should list update");
    assert!(table.contains("physics"), "should list physics");
    // Footer with frame summary
    assert!(table.contains("frames"), "should have frame count in footer");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano format_table_shows_percentiles -- --nocapture`
Expected: FAIL

**Step 3: Implement format_table_v2**

```rust
pub fn format_table_v2(frame_data: &FrameData) -> String {
    // For each function: collect per-frame self_ns, compute totals + percentiles
    // Sort by total self-time descending
    // Format table with columns: Function, Calls, Self Time, p50, p99, Allocs, Bytes
    // Footer: N frames | Xms avg | Xms p99 | N spikes (>2x avg)
    // ...
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn format_ns(ns: u64) -> String {
    let us = ns as f64 / 1_000.0;
    if us < 1000.0 {
        format!("{:.1}us", us)
    } else {
        format!("{:.2}ms", us / 1_000.0)
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano format_table_shows_percentiles -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/report.rs
git commit -m "feat(report): add percentile and alloc columns to default table"
```

---

### Task 8: Per-Frame View (--frames)

**Files:**
- Modify: `src/report.rs`
- Modify: `src/main.rs`
- Test: `src/report.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn format_frames_table_shows_per_frame_breakdown() {
    let frame_data = FrameData {
        fn_names: vec!["update".into(), "physics".into()],
        frames: vec![
            vec![
                FrameFnEntry { fn_id: 0, calls: 1, self_ns: 2_000_000, alloc_count: 10, alloc_bytes: 4096 },
                FrameFnEntry { fn_id: 1, calls: 1, self_ns: 1_000_000, alloc_count: 0, alloc_bytes: 0 },
            ],
            vec![
                FrameFnEntry { fn_id: 0, calls: 1, self_ns: 8_000_000, alloc_count: 50, alloc_bytes: 16384 },
                FrameFnEntry { fn_id: 1, calls: 1, self_ns: 1_100_000, alloc_count: 0, alloc_bytes: 0 },
            ],
        ],
    };
    let table = format_frames_table(&frame_data);
    assert!(table.contains("Frame"), "should have Frame column header");
    // Frame 0 and Frame 1 should appear
    assert!(table.contains(" 1 ") || table.contains(" 1\t"), "should show frame 1");
    assert!(table.contains(" 2 ") || table.contains(" 2\t"), "should show frame 2");
    // Spike detection
    assert!(table.contains("spike") || table.contains("<<"), "should flag the spike frame");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano format_frames_table -- --nocapture`
Expected: FAIL

**Step 3: Implement format_frames_table**

```rust
pub fn format_frames_table(frame_data: &FrameData) -> String {
    // Columns: Frame, Total, [one column per function], Allocs, Bytes
    // Each row is one frame
    // Mark frames where total > 2x median as spikes
    // ...
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p piano format_frames_table -- --nocapture`
Expected: PASS

**Step 5: Wire --frames into CLI**

In `src/main.rs`, add `--frames` flag to Report command:
```rust
Report {
    run: Option<PathBuf>,
    #[arg(long)]
    all: bool,
    /// Show per-frame breakdown.
    #[arg(long)]
    frames: bool,
},
```

Update `cmd_report` to call `format_frames_table` when `--frames` is passed.

**Step 6: Commit**

```bash
git add src/report.rs src/main.rs
git commit -m "feat(report): add --frames view with per-frame breakdown and spike detection"
```

---

### Task 9: Enhanced Diff with Alloc Deltas

**Files:**
- Modify: `src/report.rs`
- Test: `src/report.rs` (inline tests)

**Step 1: Write the failing test**

```rust
#[test]
fn diff_shows_alloc_deltas() {
    let a = Run {
        run_id: None,
        timestamp_ms: 1000,
        functions: vec![FnEntry {
            name: "walk".into(),
            calls: 3,
            total_ms: 12.0,
            self_ms: 10.0,
            alloc_count: 100,
            alloc_bytes: 8192,
        }],
    };
    let b = Run {
        run_id: None,
        timestamp_ms: 2000,
        functions: vec![FnEntry {
            name: "walk".into(),
            calls: 3,
            total_ms: 9.0,
            self_ms: 8.0,
            alloc_count: 50,
            alloc_bytes: 4096,
        }],
    };
    let diff = diff_runs(&a, &b);
    assert!(diff.contains("Allocs"), "should have Allocs column header");
    assert!(diff.contains("-50"), "should show alloc count delta");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p piano diff_shows_alloc_deltas -- --nocapture`
Expected: FAIL (FnEntry doesn't have alloc fields yet, or diff doesn't show them)

**Step 3: Implement**

Update `diff_runs` to include alloc delta columns:
```rust
pub fn diff_runs(a: &Run, b: &Run) -> String {
    // Existing time columns + new: Allocs Before, Allocs After, Allocs Delta
    // ...
}
```

**Step 4: Run test to verify it passes + existing diff test**

Run: `cargo test -p piano diff_shows -- --nocapture`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/report.rs
git commit -m "feat(report): add allocation deltas to diff view"
```

---

### Task 10: Integration Test

**Files:**
- Create: `tests/integration_v2.rs`

**Step 1: Write end-to-end test**

Create a test that verifies the full pipeline:
1. Create a small test project with a loop (simulating frames)
2. Run `piano build --fn` on it with `--runtime-path` pointing to local piano-runtime
3. Execute the instrumented binary
4. Run `piano report` and verify output has percentile columns
5. Run `piano report --frames` and verify per-frame output

This test can mirror the patterns in `tests/` (check existing integration tests for structure).

```rust
// tests/integration_v2.rs
// End-to-end test: build, run, report with v2 format
// Verify:
//   - .ndjson file is created in PIANO_RUNS_DIR
//   - piano report shows percentile columns
//   - piano report --frames shows per-frame breakdown
```

**Step 2: Run test**

Run: `cargo test -p piano --test integration_v2 -- --nocapture`
Expected: PASS

**Step 3: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add tests/integration_v2.rs
git commit -m "test: add end-to-end integration test for v2 per-frame profiling"
```

---

### Task 11: Clean Up Benchmark Files

**Files:**
- Remove: `benches/alloc_overhead/` directory
- Modify: `Cargo.toml` (remove workspace exclude)

**Step 1: Remove benchmark files**

```bash
rm -rf benches/alloc_overhead
```

**Step 2: Remove workspace exclude**

In `Cargo.toml`, remove the `exclude = ["benches/alloc_overhead"]` line.

**Step 3: Commit**

```bash
git add -A benches/ Cargo.toml
git commit -m "chore: remove temporary allocation overhead benchmark"
```
