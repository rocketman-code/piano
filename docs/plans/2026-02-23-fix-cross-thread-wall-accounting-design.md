# Fix Cross-Thread Wall-Time Accounting

## Problem

AdoptGuard::drop() propagates each worker's wall time back to the parent's
children_ms via SpanContext. When 8 parallel rayon workers each run for ~44ms
wall, children_ms sums to ~347ms. Parent elapsed is ~44ms.
self = max(44 - 347, 0) = 0.

This causes `concurrent_discover` to report Self=0.0us -- clearly wrong since
the function blocks for ~44ms waiting for workers.

## Oracle validation

Three established profilers confirm the correct behavior:

1. Instruments (xctrace Time Profiler): profiles per-thread independently. Main
   thread shows 6ms CPU, workers show 52ms total CPU across 8 threads. Never
   sums worker time into the main thread.

2. /usr/bin/sample: main thread blocked 15ms in LockLatch::wait_and_reset.
   Each worker ~19 samples (19ms CPU). Per-thread call trees, no cross-thread
   merging.

3. /usr/bin/time: real=10ms, user=20ms, sys=20ms (release build). Confirms
   ~4x parallelism -- CPU time is additive, wall time is not.

No established profiler sums parallel children's wall times and subtracts from
a parent. Wall time is not additive across parallel threads. CPU time is.

## Solution

Remove wall-time propagation from AdoptGuard. Only propagate CPU time across
thread boundaries.

Wall self_ms = elapsed - same-thread children (never cross-thread).
CPU self_ns = cpu_elapsed - cpu_children (cross-thread OK, CPU is additive).

### Changes

1. SpanContext: remove children_ms field (Arc<Mutex<f64>>)
2. AdoptGuard: remove ctx_children_ms field
3. AdoptGuard::drop(): remove wall-time propagation, keep CPU-time propagation
4. SpanContext::apply_children(): remove wall-time propagation, keep CPU-time
5. fork(): remove children_ms initialization

### Result

For concurrent_discover on chainsaw:
- Before: Self=0.0us (wall children 347ms > elapsed 44ms, clamped to 0)
- After: Self=~44ms wall (blocked waiting for workers), CPU Self=~0ms (correct)
- Matches: Instruments, sample, /usr/bin/time

### Design decisions

Why not keep wall-time in a separate "parallel_children" field:
- 347ms "total parallel wall time" is misleading (actual elapsed was 44ms)
- No oracle profiler shows this metric
- Adds complexity without actionable information

Why CPU time is correct to propagate:
- CPU time is physically additive: 8 cores x 5ms each = 40ms total CPU
- This matches pprof's model (2 goroutines x 1s = 2s reported)
- clock_gettime(CLOCK_THREAD_CPUTIME_ID) measures actual on-CPU time per thread
