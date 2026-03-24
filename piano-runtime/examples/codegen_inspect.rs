// Codegen inspection targets for ASM verification tests.
//
// Each function is #[inline(never)] so cargo-asm can extract it as a
// standalone symbol. Functions fall into three groups:
//
// 1. Positive controls -- real piano patterns that MUST exhibit the
//    claimed structural property.
// 2. Negative controls -- deliberately broken patterns that MUST NOT
//    exhibit the property. If the verification tool passes a negative
//    control, the tool itself is unsound.
// 3. Metrological controls -- validate that the tool can detect the
//    presence/absence of specific instructions.
//
// Compiled with `--features _test_internals` so private modules are
// accessible.

use piano_runtime::ctx::{Ctx, RootCtx};
use piano_runtime::piano_future::{PianoFuture, PianoFutureState};
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};

// ---------------------------------------------------------------------------
// TM6 positive control: real Guard::stamp + Guard::drop
//
// stamp(): compiler_fence(SeqCst) then rdtsc -- TSC read is the LAST
//          thing stamp does.
// drop():  rdtsc then compiler_fence(SeqCst) then bookkeeping -- TSC
//          read is the FIRST thing drop does.
//
// Between the two rdtsc reads (start and end), only user code runs.
// ---------------------------------------------------------------------------
#[inline(never)]
pub fn tm6_positive(ctx: &Ctx) -> u64 {
    let (__g, _ctx) = ctx.enter(1);
    black_box(42u64)
}

// ---------------------------------------------------------------------------
// TM6 negative control: broken measurement window
//
// Inserts black_box "bookkeeping" between the fence and the TSC read,
// breaking the tight window property. The verification tool MUST
// detect that this function has extra instructions between the fence
// and rdtsc.
// ---------------------------------------------------------------------------
#[inline(never)]
pub fn tm6_negative() -> u64 {
    use core::sync::atomic::{compiler_fence, Ordering};
    compiler_fence(Ordering::SeqCst);
    // "Bookkeeping" between fence and TSC read -- breaks TM6
    let garbage = black_box(123u64);
    let ticks = piano_runtime::time::read();
    black_box(garbage.wrapping_add(ticks))
}

// ---------------------------------------------------------------------------
// TM7 positive control: enter/stamp split
//
// Ctx::enter() is #[inline(always)], calling enter_inner() (regular fn)
// then stamp(). In the ASM:
//   - enter_inner must appear as a `call` instruction
//   - rdtsc must appear inline (from stamp())
// ---------------------------------------------------------------------------
#[inline(never)]
pub fn tm7_positive(ctx: &Ctx) -> u64 {
    let (__g, _ctx) = ctx.enter(1);
    black_box(99u64)
}

// ---------------------------------------------------------------------------
// TM7 negative control: everything inlined (no call to enter_inner)
//
// Directly calls time::read() inline -- no function call at all.
// The verification tool must see that there is NO `call` to enter_inner.
// ---------------------------------------------------------------------------
#[inline(never)]
pub fn tm7_negative() -> u64 {
    // Just an inline rdtsc with no function call -- violates the
    // "enter_inner is a call instruction" property.
    let ticks = piano_runtime::time::read();
    black_box(ticks)
}

// ---------------------------------------------------------------------------
// TM8 positive control: PianoFuture::poll CPU ordering
//
// Wraps a trivial future with PianoFuture (cpu_time_enabled = true) and
// polls it once. PianoFuture::poll is generic, so this monomorphization
// creates a concrete symbol the verification tool can inspect.
//
// The claim: cpu_now_ns() calls (clock_gettime) bracket the inner poll,
// with compiler_fence markers separating bookkeeping from measurement.
// In the ASM, clock_gettime must appear twice, and #MEMBARRIER must
// appear between the pre-poll bookkeeping and the first clock_gettime.
// ---------------------------------------------------------------------------
struct ReadyFuture(u64);

impl Future for ReadyFuture {
    type Output = u64;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u64> {
        Poll::Ready(self.0)
    }
}

#[inline(never)]
pub fn tm8_positive(state: PianoFutureState) -> Poll<u64> {
    let mut fut = PianoFuture::new(state, ReadyFuture(42));
    // SAFETY: fut is a local, never moved after pinning.
    let pinned = unsafe { Pin::new_unchecked(&mut fut) };
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    pinned.poll(&mut cx)
}

// ---------------------------------------------------------------------------
// TM8 negative control: clock_gettime with no fence between bookkeeping
//
// Calls cpu_now_ns() twice but with black_box "bookkeeping" between
// the fence position and the clock read. The verification tool must
// detect that bookkeeping intrudes into the measurement window.
// ---------------------------------------------------------------------------
#[inline(never)]
pub fn tm8_negative() -> u64 {
    use core::sync::atomic::{compiler_fence, Ordering};
    // "Pre-poll bookkeeping" then fence, but bookkeeping AFTER the fence
    compiler_fence(Ordering::SeqCst);
    let garbage = black_box(999u64);
    let cpu_start = piano_runtime::cpu_clock::cpu_now_ns();
    // Simulate inner poll
    let result = black_box(42u64);
    let cpu_end = piano_runtime::cpu_clock::cpu_now_ns();
    compiler_fence(Ordering::SeqCst);
    black_box(garbage + cpu_start + result + cpu_end)
}

// ---------------------------------------------------------------------------
// Metrological controls: prove the tool can detect rdtsc / call / fence
// ---------------------------------------------------------------------------

// Must contain exactly one rdtsc and zero calls to enter_inner.
// Uses wrapping_add(1) to prevent merging with tm7_negative.
#[inline(never)]
pub fn metrology_has_rdtsc() -> u64 {
    let ticks = piano_runtime::time::read();
    black_box(ticks.wrapping_add(1))
}

// Must contain zero rdtsc instructions.
#[inline(never)]
pub fn metrology_no_rdtsc() -> u64 {
    black_box(42u64)
}

// Must contain a #MEMBARRIER annotation.
#[inline(never)]
pub fn metrology_has_fence() -> u64 {
    use core::sync::atomic::{compiler_fence, Ordering};
    compiler_fence(Ordering::SeqCst);
    black_box(77u64)
}

// Must contain zero #MEMBARRIER annotations.
#[inline(never)]
pub fn metrology_no_fence() -> u64 {
    black_box(88u64)
}

fn noop_waker() -> std::task::Waker {
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(p, &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

fn main() {
    // Prevent DCE. Each function must be reachable.
    let root = RootCtx::new(None, true, &[]);
    let ctx = root.ctx();
    black_box(tm6_positive(&ctx));
    black_box(tm6_negative());
    black_box(tm7_positive(&ctx));
    black_box(tm7_negative());
    let (state, _ctx) = ctx.enter_async(1);
    let _ = black_box(tm8_positive(state));
    black_box(tm8_negative());
    black_box(metrology_has_rdtsc());
    black_box(metrology_no_rdtsc());
    black_box(metrology_has_fence());
    black_box(metrology_no_fence());
}
