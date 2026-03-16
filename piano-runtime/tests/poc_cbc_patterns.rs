//! PoC: CBC API injection patterns
//!
//! Each test is a hand-written "golden sample" of what the rewriter will
//! produce. If these compile and pass, the injection patterns are proven
//! correct against the CBC runtime API.
//!
//! Claim coverage (compilation patterns):
//!   GI1, GI4, GI5, GI6, GI8, GI9 — guard injection, async wrapping,
//!     pass-through, parameter position, inner-attr placement
//!   GI2 — inner function instrumentation, closure exclusion
//!   GI7 — unsafe fn instrumentation (not skipped)
//!   NT1, NT6, NT8 — name table format, IDs from 0, file-level const
//!   NT9 — proven by construction (PIANO_NAMES excludes main)
//!   LI1, LI4 — Ctx::new, enter shadows
//!   LI7 — non-move closure borrows ctx (Sync)
//!   LI8 — move closure pre-clone in for-loop
//!   LI10 — __piano_ prefix collision safety
//!   AI2, AI3 — allocator const wrapping (System case)
//!   T1, T3 — all functions receive ctx; pass-through = zero overhead
//!   T4 — runtime agnostic to pass-through vs measured
//!
//! Behavioral correctness in codegen_patterns.rs:
//!   LI2, LI3, LI4, LI6 — clone propagation, root drop, parent chains
//!   NT5 — NDJSON output format, L8 — header crash-recovery contract

use piano_runtime::ctx::Ctx;
use piano_runtime::PianoAllocator;
use std::alloc::System;

// --- NT1 + NT6 + NT8: name table as file-level const, IDs from 0 ---

const PIANO_NAMES: &[(u32, &str)] = &[
    (0, "measured_sync"),
    (1, "measured_async"),
    (2, "inner_fn"),
    (3, "unsafe_measured"),
];

// --- GI4 + GI8 + LI4: sync guard injection, ctx as last param, enter shadows ---

fn measured_sync(x: i32, __piano_ctx: Ctx) -> i32 {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
    // User's original body follows. __piano_ctx shadows the parameter
    // so any call sites below see the child context.
    x + 1
}

// --- GI5 + GI1: async fn whole-body wrapping ---

async fn measured_async(x: i32, __piano_ctx: Ctx) -> i32 {
    let (__piano_state, __piano_ctx) = __piano_ctx.enter_async(1);
    piano_runtime::PianoFuture::new(__piano_state, async move {
        // User's original body inside the future wrapper.
        // __piano_ctx is captured by async move, survives thread migration.
        let _ = &__piano_ctx;
        x * 2
    })
    .await
}

// --- GI6 + T3: pass-through function (receives ctx, no guard) ---

fn pass_through(x: i32, __piano_ctx: Ctx) -> i32 {
    // No guard, no timing, zero profiling overhead.
    // Only exists to forward ctx to measured callees.
    measured_sync(x, __piano_ctx.clone())
}

// --- GI2: inner function instrumented ---

fn outer_with_inner(x: i32, __piano_ctx: Ctx) -> i32 {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);

    // Inner fn item — gets its own ctx parameter and guard
    fn inner_fn(y: i32, __piano_ctx: Ctx) -> i32 {
        let (__piano_guard, __piano_ctx) = __piano_ctx.enter(2);
        y * 3
    }

    // Call inner fn, forwarding ctx
    inner_fn(x, __piano_ctx.clone())
}

// --- GI2: closure NOT instrumented (no ctx, no guard) ---

fn with_closure(items: &[i32], __piano_ctx: Ctx) -> Vec<i32> {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
    // Closure has no __piano_ctx param. Its execution time
    // is attributed to the enclosing function's self-time.
    items.iter().map(|x| x + 1).collect()
}

// --- GI7: unsafe fn IS instrumented ---

unsafe fn unsafe_measured(ptr: *const i32, __piano_ctx: Ctx) -> i32 {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(3);
    // Guard is pure safe code. unsafe is a caller contract,
    // not a body restriction.
    unsafe { *ptr }
}

// --- LI7: non-move closure borrows ctx (Ctx is Sync) ---

fn with_non_move_closure(__piano_ctx: Ctx) {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
    let closure = || {
        // Borrows __piano_ctx from enclosing scope.
        // Ctx is Sync, so shared reference is valid.
        measured_sync(42, __piano_ctx.clone())
    };
    let _ = closure();
}

// --- LI8: move closure in for-loop gets pre-clone ---

fn with_move_closure_loop(__piano_ctx: Ctx) {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
    let handles: Vec<_> = (0..3)
        .map(|i| {
            // Pre-clone inside the loop body so each closure
            // captures its own copy. Can't move the same value twice.
            let __piano_ctx = __piano_ctx.clone();
            std::thread::spawn(move || {
                measured_sync(i, __piano_ctx.clone())
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
}

// --- LI10: __piano_ prefix doesn't collide with user variables ---

fn user_has_ctx_variable(__piano_ctx: Ctx) -> i32 {
    let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
    // User's original code used a variable named `ctx`.
    // The rewriter renamed it to avoid collision with __piano_ctx.
    // This test proves the __piano_ namespace doesn't interfere.
    let ctx = 42; // user's variable, not piano's
    ctx + 1
}

// --- AI3 (no-allocator case): inject PianoAllocator<System> ---

// This is what the rewriter produces when no #[global_allocator] exists.
// Commented out because only one #[global_allocator] per binary,
// but the pattern compiles:
// #[global_allocator]
// static _PIANO_ALLOC: PianoAllocator<System> = PianoAllocator::new(System);

// --- Tests ---

#[test]
fn sync_guard_injection_compiles_and_runs() {
    // GI4 + GI8 + LI4: sync function with guard
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = measured_sync(10, ctx);
    assert_eq!(result, 11);
}

#[test]
fn async_whole_body_wrapping_compiles_and_runs() {
    // GI1 + GI5: async fn wrapped in PianoFuture
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(measured_async(5, ctx));
    assert_eq!(result, 10);
}

#[test]
fn pass_through_no_guard_compiles_and_runs() {
    // GI6 + T3 + T4: pass-through forwards ctx, runtime doesn't know
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = pass_through(7, ctx);
    assert_eq!(result, 8);
}

#[test]
fn inner_function_instrumented() {
    // GI2: inner fn gets its own guard and ctx
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = outer_with_inner(4, ctx);
    assert_eq!(result, 12);
}

#[test]
fn closure_not_instrumented() {
    // GI2: closure has no ctx, attributed to enclosing function
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = with_closure(&[1, 2, 3], ctx);
    assert_eq!(result, vec![2, 3, 4]);
}

#[test]
fn unsafe_fn_instrumented() {
    // GI7: unsafe fn gets guard (safe code inside unsafe fn is fine)
    let val: i32 = 99;
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = unsafe { unsafe_measured(&val, ctx) };
    assert_eq!(result, 99);
}

#[test]
fn non_move_closure_borrows_ctx() {
    // LI7: Ctx is Sync, closure can borrow
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    with_non_move_closure(ctx); // must not panic
}

#[test]
fn move_closure_loop_preclone() {
    // LI8: each iteration gets its own clone
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    with_move_closure_loop(ctx); // must not panic
}

#[test]
fn piano_prefix_no_collision() {
    // LI10: __piano_ prefix doesn't interfere with user code
    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = user_has_ctx_variable(ctx);
    assert_eq!(result, 43);
}

#[test]
fn name_table_format() {
    // NT1 + NT6 + NT8: const table with u32 IDs from 0
    assert_eq!(PIANO_NAMES[0], (0, "measured_sync"));
    assert_eq!(PIANO_NAMES[1], (1, "measured_async"));
    assert_eq!(PIANO_NAMES[2], (2, "inner_fn"));
    assert_eq!(PIANO_NAMES[3], (3, "unsafe_measured"));
}

#[test]
fn allocator_wrapping_const() {
    // AI2 + AI3: PianoAllocator::new is const, wraps System
    const _: PianoAllocator<System> = PianoAllocator::new(System);
    // Compiles — const fn requirement met
}

#[test]
fn ctx_is_send_and_sync() {
    // T1 + LI6 + LI7: Ctx can be sent and shared
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<Ctx>();
    assert_sync::<Ctx>();
}

// --- GI9: guard injection after inner attributes ---

#[test]
fn inner_attrs_before_guard() {
    // GI9: guard must go AFTER inner attributes, not before.
    // This test simulates a function with #![allow(unused)] —
    // the guard line comes after the attribute.
    // The function below is what the rewriter would produce:

    #[allow(unused_variables)]
    fn with_inner_attr(x: i32, __piano_ctx: Ctx) -> i32 {
        #![allow(unused)]
        // Guard appears here, AFTER the inner attr
        let (__piano_guard, __piano_ctx) = __piano_ctx.enter(0);
        let unused = 42;
        x
    }

    let ctx = Ctx::new(None, false, PIANO_NAMES);
    let result = with_inner_attr(5, ctx);
    assert_eq!(result, 5);
}
