# Per-Frame Timing and Allocation Tracking

## Problem

Piano currently captures wall-clock timing as aggregate summaries (total calls, total_ms, self_ms per function). Game developers need:
- Per-frame granularity: which frames spiked and why
- Memory allocation tracking: which functions allocate the most
- Percentile breakdowns: p50/p99 per function across frames

All without manual annotation. Zero config.

## Design Constraints

- Zero timing distortion from ground truth. Total runtime overhead is acceptable (program runs slower), but the profiling data must accurately reflect real function timing. Allocation tracking overhead must not inflate function timings.
- Zero user configuration for alloc tracking. `piano build --fn update` enables both timing and allocation tracking automatically.
- Zero runtime dependencies (no serde, no compression, no third-party crates in piano-runtime).
- Backward compatible with v1 JSON format on the reporting side.

## Benchmark Data

Per-allocation overhead measured across three strategies:

| Strategy | Per-alloc overhead |
|---|---|
| Counts + bytes | 2.5ns |
| Counts + bytes + peak | 2.7ns |
| Full alloc log | 3.4ns |

CPU-heavy workloads see ~0% overhead. Alloc-heavy workloads (10K allocs) see 13-36% total runtime overhead. All strategies are in the same ballpark -- the dominant cost is TLS access + re-entrancy guard, not bookkeeping.

Timing distortion at 3.4ns/alloc: a function doing 50K allocs gets inflated by ~170us relative to one doing 100 allocs. This is eliminated by the overhead subtraction mechanism.

## Architecture

### Distortion-Free Timing

The global allocator wrapper measures its own bookkeeping time and subtracts it from function timing. Same pattern as `children_ms`:

```rust
// In the global allocator:
unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    let ptr = self.inner.alloc(layout);
    let t0 = Instant::now();
    // ... tracking bookkeeping ...
    let overhead = t0.elapsed();
    STACK.with(|s| {
        if let Some(top) = s.borrow_mut().last_mut() {
            top.overhead_ns += overhead.as_nanos();
        }
    });
    ptr
}

// In Guard::drop:
// self_ns = elapsed_ns - children_ns - overhead_ns
```

The overhead measurement itself (~40ns for two Instant::now calls) is self-correcting -- it's included in the overhead that gets subtracted.

### Frame Boundary Detection

The outermost instrumented function (depth=0) defines frame boundaries. Each depth=0 invocation = one frame. No explicit `--frame` flag needed.

For non-frame workloads (no depth=0 function detected, e.g. library code or programs without a main loop), the runtime falls back to current aggregate behavior.

### Memory-Efficient Data Model

Aggregate at frame boundaries, not at process exit.

When a depth=0 guard drops:
1. All invocation records since the last frame boundary belong to this frame
2. Aggregate into `FrameSummary { fn_id, calls, self_ns, alloc_count, alloc_bytes }` (20 bytes per function)
3. Clear the per-invocation buffer
4. Push frame summary

Memory comparison for 10 functions, 50 calls/frame, 3600 frames (60s at 60fps):
- Naive per-invocation: 10 x 50 x 3600 x 72B = 126MB
- Frame aggregation: buffer(~36KB) + 10 x 20B x 3600 = ~756KB

~170x reduction. Memory bounded per-frame, grows linearly only with frame count.

### Allocator Wrapper Injection

`PianoAllocator<A: GlobalAlloc>` lives in `piano-runtime/src/alloc.rs`. Wraps `System` by default. Uses re-entrancy guard to prevent infinite recursion from tracking allocations caused by tracking.

AST rewriting (`rewrite.rs`) injects into crate root:

```rust
#[global_allocator]
static _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>
    = piano_runtime::PianoAllocator::new(std::alloc::System);
```

If the user already has a `#[global_allocator]`, piano wraps theirs instead of `System`.

### JSON Output Format (v2)

NDJSON (newline-delimited JSON). Header line with metadata + function table, then one line per frame summary.

```json
{"format_version":2,"run_id":"12345_1740000000000","timestamp_ms":1740000000123,"functions":["update","physics_step","Parser::parse_node"]}
{"frame":0,"fns":[{"id":0,"calls":1,"self_ns":1890000,"ac":34,"ab":8192},{"id":1,"calls":1,"self_ns":420000,"ac":0,"ab":0}]}
{"frame":1,"fns":[{"id":0,"calls":1,"self_ns":1920000,"ac":30,"ab":7200},{"id":1,"calls":1,"self_ns":415000,"ac":0,"ab":0}]}
```

Streaming writes. No need to hold everything for a closing bracket. Partial writes recoverable on crash.

v1 aggregate format still supported for reading on the report side.

### Reporting

Default `piano report` enhanced with percentile and alloc columns:

```
Function                   Calls   Self Time   p50      p99      Allocs    Bytes
------------------------------------------------------------------------------------------
Parser::parse_node          3000     145.2ms   38.1us   412.3us    6000    1.9MB
update                        60      12.1ms  198.3us   310.5us     340   96.0KB
physics_step                  60       8.3ms  135.2us   182.1us       0      0B

60 frames | 16.7ms avg | 18.2ms p99 | 3 spikes (>2x avg)
```

New `piano report --frames` for per-frame breakdown:

```
Frame    Total     update    parse_node   physics    Allocs    Bytes
----------------------------------------------------------------------
   1    16.2ms     2.31ms       1.42ms     1.05ms       112    32KB
   2    16.0ms     2.15ms       1.38ms     1.12ms       108    31KB
   3    24.8ms     8.41ms       1.35ms     1.08ms       340    96KB  <- spike
```

`piano diff` gains alloc delta columns.

## Changes by File

### piano-runtime

- `collector.rs`: `StackEntry` gains `alloc_count`, `alloc_bytes`, `free_count`, `free_bytes`, `overhead_ns`, `depth`. New `InvocationRecord` replaces `RawRecord`. New `FrameSummary` struct. Frame boundary detection on depth=0 guard drop. v2 NDJSON writer. Fallback to aggregate behavior for non-frame workloads.
- `alloc.rs` (new): `PianoAllocator<A>` with re-entrancy guard, overhead measurement, stack entry updates.
- `lib.rs`: re-export `PianoAllocator`.

### piano

- `rewrite.rs`: inject `#[global_allocator]` declaration into crate root. Detect and wrap existing global allocators.
- `report.rs`: parse v2 NDJSON alongside v1 JSON. Percentile computation from frame summaries. Spike detection (>2x median). New `format_frames_table`. Enhanced `format_table` with percentile/alloc columns. Enhanced `diff_runs` with alloc deltas.
- `main.rs`: `report` gains `--frames` flag.

### Unchanged

- `resolve.rs` -- target resolution unchanged
- `build.rs` -- staging/build pipeline unchanged
- User-facing `build` CLI -- zero new flags
- Zero-dependency constraint on piano-runtime
