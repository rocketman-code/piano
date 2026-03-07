#[cfg(feature = "cpu-time")]
use std::sync::{Arc, Mutex};

use super::name_table::{intern_name, lookup_name, pack_name_depth, unpack_depth, unpack_name_id};
use super::{
    flush_records_buf, stream_frame, with_stack_mut, with_stack_ref, StackEntry, FRAME_BUFFER,
};

/// Context for propagating parent-child CPU timing across thread boundaries.
///
/// Created by `fork()` on the parent thread, passed to child threads via
/// `adopt()`. When the child completes, its CPU time is accumulated
/// in `children_cpu_ns` which the parent reads back via Drop (or explicit `finalize()`).
/// Wall time is NOT propagated cross-thread (it's not additive for parallel work).
#[non_exhaustive]
pub struct SpanContext {
    parent_name: &'static str,
    #[cfg(feature = "cpu-time")]
    children_cpu_ns: Arc<Mutex<u64>>,
    finalized: bool,
}

impl SpanContext {
    /// Explicitly finalize cross-thread attribution.
    /// Equivalent to dropping the SpanContext, but makes intent clear.
    pub fn finalize(mut self) {
        self.apply_children();
        self.finalized = true;
    }

    fn apply_children(&self) {
        #[cfg(feature = "cpu-time")]
        {
            let children_cpu = *self
                .children_cpu_ns
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            with_stack_mut(|s| {
                if let Some(top) = s.last_mut() {
                    top.cpu_children_ns += children_cpu;
                }
            });
        }
    }
}

impl Drop for SpanContext {
    fn drop(&mut self) {
        if !self.finalized {
            self.apply_children();
        }
    }
}

/// RAII guard for cross-thread adoption. Pops the synthetic parent on drop
/// and propagates CPU time back to the parent's `SpanContext`.
#[must_use = "dropping AdoptGuard immediately records ~0ms; bind it with `let _guard = ...`"]
#[non_exhaustive]
pub struct AdoptGuard {
    #[cfg(feature = "cpu-time")]
    cpu_start_ns: u64,
    #[cfg(feature = "cpu-time")]
    ctx_children_cpu_ns: Arc<Mutex<u64>>,
}

impl Drop for AdoptGuard {
    fn drop(&mut self) {
        // Restore the parent's saved alloc counters (same pattern as Guard::drop).
        // The adopted scope's alloc data isn't recorded into an InvocationRecord,
        // but the restore is necessary for correct nesting.
        with_stack_mut(|s| {
            let entry = match s.pop() {
                Some(e) => e,
                None => return,
            };

            let _ = crate::alloc::ALLOC_COUNTERS.try_with(|cell| {
                cell.set(entry.saved_alloc);
            });

            // The synthetic adopt entry is at depth 0. When it drops, any
            // pending FRAME_BUFFER data from child functions (which ran at
            // depth 1+) must be flushed to FRAMES -- same as a normal
            // depth-0 guard drop. Without this, worker-thread frame data
            // would be silently lost.
            if unpack_depth(entry.packed) == 0 {
                flush_records_buf();
                FRAME_BUFFER.with(|buf| {
                    let b = buf.borrow();
                    if !b.is_empty() {
                        stream_frame(&b);
                    }
                    drop(b);
                    buf.borrow_mut().clear();
                });
            }

            // Propagate this thread's CPU time back to the parent context.
            #[cfg(feature = "cpu-time")]
            {
                let cpu_elapsed_ns =
                    crate::cpu_clock::cpu_now_ns().saturating_sub(self.cpu_start_ns);
                let mut cpu_children = self
                    .ctx_children_cpu_ns
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *cpu_children += cpu_elapsed_ns;
            }
        });
    }
}

/// Capture the current stack top as a cross-thread span context.
///
/// Returns `None` if the call stack is empty (no active span to fork from).
/// Pass the returned context to child threads via `adopt()`.
pub fn fork() -> Option<SpanContext> {
    with_stack_ref(|s| {
        let top = s.last()?;
        Some(SpanContext {
            parent_name: lookup_name(unpack_name_id(top.packed)),
            #[cfg(feature = "cpu-time")]
            children_cpu_ns: Arc::new(Mutex::new(0)),
            finalized: false,
        })
    })
}

/// Adopt a parent span context on a child thread.
///
/// Pushes a synthetic parent entry so that `enter()`/`Guard::drop()` on this
/// thread correctly attributes children time. Returns an `AdoptGuard` that
/// propagates CPU time back to the parent on drop.
pub fn adopt(ctx: &SpanContext) -> AdoptGuard {
    // Save current alloc counters and zero them, same as enter().
    let saved_alloc = crate::alloc::ALLOC_COUNTERS
        .try_with(|cell| {
            let snap = cell.get();
            cell.set(crate::alloc::AllocSnapshot::new());
            snap
        })
        .unwrap_or_default();

    #[cfg(feature = "cpu-time")]
    let cpu_start_ns = crate::cpu_clock::cpu_now_ns();

    with_stack_mut(|s| {
        let depth = s.len() as u16;
        s.push(StackEntry {
            start_tsc: 0,
            children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_children_ns: 0,
            #[cfg(feature = "cpu-time")]
            cpu_start_ns,
            saved_alloc,
            packed: pack_name_depth(intern_name(ctx.parent_name), depth),
        });
    });

    AdoptGuard {
        #[cfg(feature = "cpu-time")]
        cpu_start_ns,
        #[cfg(feature = "cpu-time")]
        ctx_children_cpu_ns: Arc::clone(&ctx.children_cpu_ns),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::{burn_cpu, collect, collect_frames, enter, reset};
    use serial_test::serial;
    use std::thread;

    #[test]
    #[serial]
    fn fork_returns_none_with_empty_stack() {
        reset();
        assert!(fork().is_none(), "fork should return None with empty stack");
    }

    #[test]
    #[serial]
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

        // Both recorded with correct call counts.
        assert_eq!(parent.calls, 1);
        assert_eq!(child.calls, 1);
        // Parent total exceeds child total.
        assert!(
            parent.total_ms > child.total_ms,
            "parent total ({:.1}ms) should exceed child total ({:.1}ms)",
            parent.total_ms,
            child.total_ms
        );

        // Wall self no longer reduced by cross-thread children.
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
            parent.total_ms
        );
    }

    #[test]
    #[serial]
    fn adopt_without_child_work_adds_minimal_overhead() {
        reset();
        {
            let _parent = enter("overhead_parent");
            let ctx = fork().unwrap();
            {
                let _adopt = adopt(&ctx);
                // No work on child thread.
            }
            ctx.finalize();
        }

        let records = collect();
        let parent = records
            .iter()
            .find(|r| r.name == "overhead_parent")
            .unwrap();
        // Parent should still have valid timing.
        assert!(parent.calls == 1);
        assert!(parent.total_ms >= 0.0);
    }

    #[test]
    #[serial]
    fn multiple_children_accumulate_in_parent() {
        reset();
        {
            let _parent = enter("multi_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            // Simulate 3 child threads.
            for _ in 0..3 {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("worker");
                    burn_cpu(10_000);
                }
            }

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "multi_parent").unwrap();
        let worker = records.iter().find(|r| r.name == "worker").unwrap();

        assert_eq!(parent.calls, 1, "parent should have 1 call");
        assert_eq!(worker.calls, 3, "should have 3 worker calls");
    }

    #[test]
    #[serial]
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
        // Wall self no longer reduced by cross-thread children.
        assert_eq!(parent.calls, 1);
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
            parent.total_ms
        );
    }

    #[test]
    #[serial]
    fn span_context_auto_finalizes_on_drop() {
        reset();
        {
            let _parent = enter("auto_parent");
            burn_cpu(5_000);

            // fork + adopt, but do NOT call finalize() -- rely on Drop.
            {
                let ctx = fork().expect("should have parent on stack");
                {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("auto_child");
                        burn_cpu(20_000);
                    }
                }
                // ctx drops here -- should auto-finalize
            }
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "auto_parent").unwrap();

        // Wall self no longer reduced by cross-thread children.
        assert!(
            parent.self_ms > parent.total_ms * 0.5,
            "parent self ({:.1}ms) should not be reduced by cross-thread child wall. total={:.1}ms",
            parent.self_ms,
            parent.total_ms
        );
    }

    #[test]
    #[serial]
    fn fork_adopt_does_not_inflate_reported_times() {
        // Verify that fork/adopt overhead is NOT attributed to any function.
        // Only instrumented functions (via enter()) should appear in output.
        reset();
        {
            let _parent = enter("timed_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            // Simulate rayon: 4 children each doing work
            for _ in 0..4 {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("timed_child");
                    burn_cpu(10_000);
                }
            }
            // ctx auto-finalizes on drop
        }

        // No cross-thread spawning here, so thread-local collect() is sufficient
        // and avoids picking up stale records from other threads in parallel tests.
        let records = collect();

        // Only "timed_parent" and "timed_child" should appear. No adopt/fork entries.
        let names: Vec<&str> = records.iter().map(|r| r.name.as_str()).collect();
        assert!(
            !names
                .iter()
                .any(|n| n.contains("adopt") || n.contains("fork") || n.contains("piano")),
            "fork/adopt should not appear in output. Got: {names:?}",
        );

        let parent = records.iter().find(|r| r.name == "timed_parent").unwrap();
        let child = records.iter().find(|r| r.name == "timed_child").unwrap();

        // Parent should appear once, child 4 times.
        assert_eq!(parent.calls, 1);
        assert_eq!(child.calls, 4);
    }

    #[cfg(feature = "cpu-time")]
    #[test]
    #[serial]
    fn cpu_time_propagated_across_threads_via_adopt() {
        reset();
        {
            let _parent = enter("cpu_parent");
            burn_cpu(5_000); // parent's own work

            let ctx = fork().expect("should have parent on stack");

            thread::scope(|s| {
                s.spawn(|| {
                    let _adopt = adopt(&ctx);
                    {
                        let _child = enter("cpu_child");
                        burn_cpu(50_000); // much more child CPU
                    }
                });
            });

            ctx.finalize();
        }

        let records = collect();
        let parent = records
            .iter()
            .find(|r| r.name == "cpu_parent")
            .expect("cpu_parent not found");

        // Key insight: after the wall-time fix, parent.self_ms is large because
        // wall time is NOT subtracted cross-thread. But parent.cpu_self_ms should
        // be small because CPU time IS propagated across thread boundaries via
        // fork/adopt, so the child's CPU time was subtracted from the parent's
        // CPU budget.
        eprintln!(
            "cpu_parent: self_ms={:.3}, cpu_self_ms={:.3}, total_ms={:.3}",
            parent.self_ms, parent.cpu_self_ms, parent.total_ms
        );
        assert!(
            parent.cpu_self_ms < parent.self_ms * 0.8,
            "cpu_self_ms ({:.3}) should be significantly less than self_ms ({:.3}) \
             because child CPU time is propagated cross-thread but wall time is not",
            parent.cpu_self_ms,
            parent.self_ms,
        );
    }

    #[test]
    #[serial]
    fn fork_adopt_does_not_subtract_wall_time_from_parent() {
        // Wall time should NOT be subtracted cross-thread.
        // Parent wall self = elapsed - same-thread children only.
        reset();
        {
            let _parent = enter("wall_parent");
            burn_cpu(5_000);

            let ctx = fork().unwrap();

            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("wall_child");
                    burn_cpu(50_000);
                }
            }

            ctx.finalize();
        }

        let records = collect();
        let parent = records.iter().find(|r| r.name == "wall_parent").unwrap();
        let child = records.iter().find(|r| r.name == "wall_child").unwrap();

        // After fix: parent.self_ms ~ parent.total_ms (no cross-thread wall subtraction).
        assert!(
            parent.self_ms > child.self_ms * 0.5,
            "parent wall self ({:.3}ms) should NOT be reduced by cross-thread child wall ({:.3}ms). \
             parent.total={:.3}ms",
            parent.self_ms,
            child.self_ms,
            parent.total_ms,
        );
    }

    #[test]
    #[serial]
    fn drop_cold_frame_boundary_with_adopt_context() {
        // Kills: collector.rs:968 replace == with !=
        //        collector.rs:969 replace || with && and == with !=
        // When fork/adopt places an entry at depth 0, real functions run at
        // depth 1+. The frame boundary fires when all remaining entries are
        // depth 0 (remaining_all_base), OR when the dropped entry itself is
        // depth 0. We verify frames are produced correctly in both scenarios.
        reset();

        // Scenario 1: normal depth-0 drop produces a frame.
        {
            let _g = enter("fb_normal");
            burn_cpu(1_000);
        }
        let frames = collect_frames();
        let normal_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fb_normal"))
            .collect();
        assert_eq!(
            normal_frames.len(),
            1,
            "depth-0 drop should produce exactly 1 frame"
        );

        // Scenario 2: adopt context -- child at depth 1 should produce frame
        // data when it drops (since remaining entries are all depth 0).
        {
            let _parent = enter("fb_adopt_parent");
            let ctx = fork().unwrap();
            {
                let _adopt = adopt(&ctx);
                {
                    let _child = enter("fb_adopt_child");
                    burn_cpu(1_000);
                }
            }
            ctx.finalize();
        }
        let frames = collect_frames();
        let adopt_frames: Vec<_> = frames
            .iter()
            .filter(|f| f.iter().any(|s| s.name == "fb_adopt_child"))
            .collect();
        assert!(
            !adopt_frames.is_empty(),
            "adopt context child should produce frame data"
        );
    }
}
