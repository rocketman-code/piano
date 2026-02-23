# Cross-Thread Instrumentation Design

## Problem

Piano loses all function call data from rayon/thread pool worker threads.

Root causes:
1. Data loss: worker thread TLS (thread-local storage) never flushes because rayon
   threads don't terminate -- they stay in the pool. AutoFlushRecords::drop() never fires.
2. No attribution: calls on worker threads aren't connected to the spawning function as
   children. The parent's self_ms includes all wall-clock time.

Evidence from real usage (chainsaw profiled with piano):
- `concurrent_discover` shows 217.959ms self_ms (should be near zero -- it just spawns work)
- `OsVfs::metadata` shows 6 calls (only main-thread calls; thousands from rayon workers missing)
- `ImportResolver::resolve` shows 0 calls (all calls happen on rayon workers)
- Only 1 JSON file produced (main thread only; no worker thread data)

## Solution

Two orthogonal changes that together solve both problems.

### Change 1: Per-thread Arc record storage

Replace TLS-only RefCell<Vec<RawRecord>> with per-thread Arc<Mutex<Vec<RawRecord>>>
registered in a global Vec. Each thread locks only its own Mutex (zero contention).

Benchmark data (Apple Silicon):

```
                          Single-thread    Multi-thread (8 threads)
A: TLS RefCell (current)     ~3 ns/push    N/A (loses rayon data)
B: Per-thread Arc<Mutex>     ~8 ns/push    ~1 ns/push
C: Global Mutex              ~7 ns/push    ~12 ns/push
D: TLS + batch drain         ~4 ns/push    ~0.4 ns/push (loses up to N records)

Instant::now + elapsed      ~43 ns/push    (unavoidable timing cost per call)
```

Option B is the fastest 100%-correct approach. The ~5ns overhead is 11% of the 43ns
Instant::now cost already baked into every Guard::drop(). Options C and D are
disqualified (contention and data loss, respectively).

Implementation:
- Static global registry: LazyLock<Mutex<Vec<Arc<Mutex<Vec<RawRecord>>>>>>
- Each thread registers its Arc on first enter() call
- Guard::drop() pushes records to thread-local Arc (uncontended lock)
- Inject piano_runtime::shutdown() at end of main() via AST rewriter
- shutdown() iterates all registered Arcs, aggregates, and writes JSON

### Change 2: Concurrency boundary detection + fork/adopt injection

The AST rewriter detects known concurrency patterns and injects fork/adopt.

Known concurrency methods (parallel iterators):
- par_iter, par_iter_mut, into_par_iter, par_bridge
- par_chunks, par_chunks_mut, par_windows

Known spawn/scope calls:
- thread::spawn, thread::scope
- rayon::scope, rayon::scope_fifo, rayon::join
- tokio::spawn
- crossbeam::scope

Detection: walk method call chains. When a chain contains a concurrency method
(e.g., par_iter), all closures in methods above it in the chain receive adopt
injection. For spawn/scope calls, direct closure arguments receive adopt injection.

SpanContext gets impl Drop for auto-finalize. This is more correct than explicit
finalize() because RAII covers all return paths (early return, ?, panic unwind).
The explicit finalize(self) method remains available for manual use.

Injected code for an instrumented function containing par_iter:

```rust
fn concurrent_discover(paths: &[PathBuf]) -> Vec<FileInfo> {
    let _piano_guard = piano_runtime::enter("concurrent_discover");
    let _piano_ctx = piano_runtime::fork();
    paths.par_iter()
         .map(|path| {
             let _piano_adopt = _piano_ctx.as_ref()
                 .map(|c| piano_runtime::adopt(c));
             let metadata = OsVfs::metadata(path);
             FileInfo { path, metadata }
         })
         .collect()
    // _piano_ctx drops -> auto-finalize (children_ms flows to parent)
    // _piano_guard drops -> records timing with correct self_ms
}
```

Drop order is correct: _piano_ctx drops first (finalizing children time into
parent stack entry), then _piano_guard drops (computing self_ms with the
now-correct children_ms).

## Design decisions

Why per-thread Arc instead of global Mutex:
- Zero contention on the hot path (each thread only locks its own Mutex)
- 8ns vs 12ns at 8 threads; gap widens with more threads
- Global Mutex serializes all threads through one lock

Why auto-finalize on Drop instead of explicit finalize:
- RAII covers all return paths (early return, ?, panic)
- AST rewriter can't inject explicit finalize at every return point
- Same performance (identical logic, just triggered by Drop)
- More correct: impossible to forget

Why pattern matching on method names instead of type resolution:
- syn operates at syntax level, no type info available
- False positive cost is zero: fork/adopt on a non-concurrent closure
  correctly attributes the closure's work as "children" which is
  subtracted from parent self_ms and captured by child function guards
- Known method names (par_iter, par_bridge, etc.) are effectively unique
  to their respective concurrency frameworks

## Testing

Integration test that builds a real Rust project with:
- rayon par_iter calling instrumented functions (thousands of calls)
- std::thread::scope with spawned instrumented work
- std::thread::spawn fire-and-forget

Assertions:
- ALL function calls from ALL threads appear in the JSON output
- Call counts match expected values
- Conservation: parent total_ms >= sum of children self_ms
- Attribution: concurrent parent shows children_ms > 0 (not all self_ms)
- No data loss: number of JSON records covers all thread pools
