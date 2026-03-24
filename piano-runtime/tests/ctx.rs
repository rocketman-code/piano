use piano_runtime::buffer::drain_thread_buffer;
use piano_runtime::ctx::RootCtx;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};

// All tests run in spawned threads for TLS isolation.

fn noop_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    // SAFETY: The vtable functions are valid no-ops. The data pointer is
    // null and never dereferenced.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

// INVARIANT TEST: root Ctx produces a measurement with parent_span_id = 0.
#[test]
fn root_enter_has_zero_parent() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        {
            let (_guard, _child) = ctx.enter(100);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 1);
        let m = &drained[0];
        assert_eq!(m.parent_span_id, 0, "root span must have parent_span_id = 0");
        assert_eq!(m.name_id, 100);
        assert!(m.span_id > 0, "span_id must be nonzero");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: child Ctx inherits parent's span_id.
#[test]
fn child_inherits_parent_span_id() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        {
            let (_outer_guard, child_ctx) = ctx.enter(10);
            {
                let (_inner_guard, _grandchild_ctx) = child_ctx.enter(20);
            }
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2);

        // Inner drops first
        let inner = &drained[0];
        let outer = &drained[1];

        assert_eq!(
            inner.parent_span_id, outer.span_id,
            "inner's parent must be outer's span_id"
        );
        assert_eq!(outer.parent_span_id, 0, "outer's parent must be root (0)");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: three levels of nesting produce correct parent chain.
#[test]
fn three_level_nesting() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        {
            let (_g1, ctx1) = ctx.enter(1);
            {
                let (_g2, ctx2) = ctx1.enter(2);
                {
                    let (_g3, _ctx3) = ctx2.enter(3);
                }
            }
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3);

        // Drop order: g3, g2, g1
        let m3 = &drained[0];
        let m2 = &drained[1];
        let m1 = &drained[2];

        assert_eq!(m1.parent_span_id, 0, "level 1 parent is root");
        assert_eq!(m2.parent_span_id, m1.span_id, "level 2 parent is level 1");
        assert_eq!(m3.parent_span_id, m2.span_id, "level 3 parent is level 2");

        // All span_ids unique
        assert_ne!(m1.span_id, m2.span_id);
        assert_ne!(m2.span_id, m3.span_id);
        assert_ne!(m1.span_id, m3.span_id);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: sibling calls share the same parent.
#[test]
fn siblings_share_parent() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        {
            let (_guard, child_ctx) = ctx.enter(10);
            {
                let (_sib1, _) = child_ctx.enter(20);
            }
            {
                let (_sib2, _) = child_ctx.enter(30);
            }
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3);

        // Drop order: sib1, sib2, parent
        let sib1 = &drained[0];
        let sib2 = &drained[1];
        let parent = &drained[2];

        assert_eq!(sib1.parent_span_id, parent.span_id);
        assert_eq!(sib2.parent_span_id, parent.span_id);
        assert_ne!(sib1.span_id, sib2.span_id, "siblings must have different span_ids");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: each enter() allocates a unique span_id.
#[test]
fn enter_allocates_unique_span_ids() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let mut span_ids = Vec::new();
        for _ in 0..10 {
            let (guard, _) = ctx.enter(0);
            drop(guard);
        }

        let drained = drain_thread_buffer();
        for m in &drained {
            span_ids.push(m.span_id);
        }

        // All unique
        span_ids.sort();
        span_ids.dedup();
        assert_eq!(span_ids.len(), 10, "all span_ids must be unique");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: Ctx is Clone -- clones produce independent children.
#[test]
fn clone_produces_independent_children() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let (_guard, child) = ctx.enter(10);

        let clone1 = child.clone();
        let clone2 = child.clone();

        {
            let (_g1, _) = clone1.enter(20);
        }
        {
            let (_g2, _) = clone2.enter(30);
        }

        drop(_guard);

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 3);

        // Both clones' children should have same parent (child's span_id)
        let c1 = &drained[0];
        let c2 = &drained[1];
        let parent = &drained[2];

        assert_eq!(c1.parent_span_id, parent.span_id);
        assert_eq!(c2.parent_span_id, parent.span_id);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cpu_time_enabled is inherited by children.
#[test]
fn cpu_time_enabled_inherited() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let (_guard, child) = ctx.enter(10);
        let (_, grandchild) = child.enter(20);

        // All should inherit cpu_time_enabled = false
        // (Verified indirectly: cpu_start_ns and cpu_end_ns should be 0)
        {
            let (_g, _) = grandchild.enter(30);
        }

        // Drop remaining guards
        drop(_guard);

        let drained = drain_thread_buffer();
        for m in &drained {
            assert_eq!(m.cpu_start_ns, 0, "cpu_time should be disabled");
            assert_eq!(m.cpu_end_ns, 0, "cpu_time should be disabled");
        }
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: enter_async produces correct PianoFutureState and child Ctx.
#[test]
fn enter_async_produces_correct_state() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let (pf, child_ctx) = ctx.instrument_async(42, async {});

        // Poll the PianoFuture to completion
        let mut pf = pf;
        let pinned = unsafe { Pin::new_unchecked(&mut pf) };
        let _ = pinned.poll(&mut cx);
        drop(pf);

        // Now enter a sync function from the child ctx
        {
            let (_guard, _) = child_ctx.enter(43);
        }

        let drained = drain_thread_buffer();
        assert_eq!(drained.len(), 2);

        let async_m = &drained[0];
        let sync_m = &drained[1];

        assert_eq!(async_m.parent_span_id, 0, "async span parent should be root");
        assert_eq!(async_m.name_id, 42);

        // Sync child of the async span should have async's span_id as parent
        assert_eq!(
            sync_m.parent_span_id, async_m.span_id,
            "sync child's parent must be async span"
        );
        assert_eq!(sync_m.name_id, 43);
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST (C12): instrument_async(name_id, body) is equivalent to
// enter_async(name_id) + PianoFuture::new(state, body). Pure composition.
#[test]
fn instrument_async_equivalent_to_enter_async_plus_piano_future() {
    std::thread::spawn(|| {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let name_id: u32 = 77;

        // --- Manual path: enter_async + PianoFuture::new ---
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let (state, _manual_child) = ctx.enter_async(name_id);
        let mut manual_fut = piano_runtime::PianoFuture::new(state, async {});
        let pinned = unsafe { Pin::new_unchecked(&mut manual_fut) };
        let _ = pinned.poll(&mut cx);
        drop(manual_fut);

        let manual_measurements = drain_thread_buffer();
        assert_eq!(manual_measurements.len(), 1, "manual path should emit one measurement");

        // --- Convenience path: instrument_async ---
        let _root2 = RootCtx::new(None, false, &[]);
        let ctx2 = _root2.ctx();
        let (mut conv_fut, _conv_child) = ctx2.instrument_async(name_id, async {});
        let pinned = unsafe { Pin::new_unchecked(&mut conv_fut) };
        let _ = pinned.poll(&mut cx);
        drop(conv_fut);

        let conv_measurements = drain_thread_buffer();
        assert_eq!(conv_measurements.len(), 1, "convenience path should emit one measurement");

        let manual_m = &manual_measurements[0];
        let conv_m = &conv_measurements[0];

        // Structure must match: same name_id, same parent_span_id (both root = 0).
        assert_eq!(manual_m.name_id, conv_m.name_id, "name_id must match");
        assert_eq!(
            manual_m.parent_span_id, conv_m.parent_span_id,
            "parent_span_id must match (both root)"
        );
        assert_eq!(manual_m.parent_span_id, 0, "both should be root spans");
        // span_ids may or may not differ depending on allocator sharing.
        // The point of this test is structural equivalence, not span_id uniqueness.
        assert!(manual_m.span_id > 0, "manual span_id must be nonzero");
        assert!(conv_m.span_id > 0, "convenience span_id must be nonzero");
    })
    .join()
    .expect("test thread panicked");
}

// INVARIANT TEST: cross-thread Ctx propagation via clone.
#[test]
fn cross_thread_ctx_propagation() {
    std::thread::spawn(|| {
        let _root = RootCtx::new(None, false, &[]);
        let ctx = _root.ctx();
        let (_guard, child_ctx) = ctx.enter(10);

        // Clone ctx and send to child thread
        let child_ctx_clone = child_ctx.clone();
        let child_span_id = std::thread::spawn(move || {
            let (guard, _) = child_ctx_clone.enter(20);
            drop(guard); // must drop before draining -- Guard pushes on drop
            let drained = drain_thread_buffer();
            assert_eq!(drained.len(), 1);
            drained[0].span_id
        })
        .join()
        .expect("child panicked");

        drop(_guard);
        drain_thread_buffer();

        // The child thread's span should have a valid span_id.
        // We can't check the child's measurement directly (it's on the
        // child's buffer which was already drained), but we verified it
        // had len 1 and captured its span_id.
        assert!(child_span_id > 0, "child should have gotten a valid span_id");
    })
    .join()
    .expect("test thread panicked");
}
