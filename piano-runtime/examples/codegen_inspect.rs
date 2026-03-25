// Codegen inspection targets for ASM verification tests.
// This example runs on the dev toolchain (1.88), not on MSRV (1.59).
#![allow(clippy::incompatible_msrv)]
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
//
// IMPORTANT: every function body must be unique. LLVM merges identical
// functions in release mode, making cargo-asm unable to find the symbol.
#![allow(unused)]

use piano_runtime::time::read;
use std::hint::black_box;

// ---------------------------------------------------------------------------
// Metrological controls
// ---------------------------------------------------------------------------

/// Contains a TSC read (rdtsc on x86, mrs cntvct_el0 on aarch64).
#[inline(never)]
pub fn metrology_has_rdtsc() -> u64 {
    read()
}

/// Does NOT contain a TSC read.
#[inline(never)]
pub fn metrology_no_rdtsc() -> u64 {
    black_box(100) + black_box(1)
}

/// Contains a compiler fence (#MEMBARRIER in LLVM asm output).
#[inline(never)]
pub fn metrology_has_fence() -> u64 {
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
    black_box(1)
}

/// Does NOT contain a compiler fence.
#[inline(never)]
pub fn metrology_no_fence() -> u64 {
    black_box(200) + black_box(2)
}

// ---------------------------------------------------------------------------
// TM6: Guard measurement window tightness
//
// enter() inlines stamp() which does fence -> TSC (tight).
// Guard::drop inlines to TSC -> fence (tight).
// ---------------------------------------------------------------------------

/// Positive: enter(1) produces fence-then-TSC (stamp) and TSC-then-fence (drop).
#[inline(never)]
pub fn tm6_positive() -> u64 {
    let _g = piano_runtime::enter(1);
    black_box(42)
}

/// Negative: bookkeeping between fence and TSC (broken pattern).
#[inline(never)]
pub fn tm6_negative() -> u64 {
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
    black_box(99u64);
    read()
}

// ---------------------------------------------------------------------------
// TM7: enter/stamp split (code size)
//
// enter() inlines to: call Guard::create (not inlined) + inline rdtsc.
// Guard::create holds the heavy bookkeeping. stamp() is one inline TSC read.
// ---------------------------------------------------------------------------

/// Positive: enter(2) = call to Guard::create + inline TSC from stamp().
/// Uses name_id=2 (different from tm6_positive's 1) to prevent LLVM merging.
#[inline(never)]
pub fn tm7_positive() -> u64 {
    let _g = piano_runtime::enter(2);
    black_box(77)
}

/// Negative: inline TSC only, no function call.
#[inline(never)]
pub fn tm7_negative() -> u64 {
    let t = read();
    black_box(t)
}

// ---------------------------------------------------------------------------
// TM8: PianoFuture::poll CPU time ordering
//
// Poll structure: fence -> cpu_now_ns -> (inner poll) -> cpu_now_ns -> fence
// ---------------------------------------------------------------------------

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

struct ReadyFuture(u64);

impl Future for ReadyFuture {
    type Output = u64;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u64> {
        Poll::Ready(self.0)
    }
}

fn noop_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone_fn(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, no_op, no_op, no_op);
    // SAFETY: vtable functions are valid no-ops.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Positive: PianoFuture::poll has fence -> cpu_now_ns -> poll -> cpu_now_ns -> fence.
#[inline(never)]
pub fn tm8_positive() -> u64 {
    let mut fut = piano_runtime::enter_async(1, ReadyFuture(42));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: fut is local and never moved after pinning.
    let pinned = unsafe { Pin::new_unchecked(&mut fut) };
    match pinned.poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => 0,
    }
}

/// Negative: bookkeeping between fence and cpu_now_ns (broken pattern).
#[inline(never)]
pub fn tm8_negative() -> u64 {
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
    black_box(99u64);
    piano_runtime::cpu_clock::cpu_now_ns()
}

fn main() {
    // Functions are called to prevent dead-code elimination.
    black_box(metrology_has_rdtsc());
    black_box(metrology_no_rdtsc());
    black_box(metrology_has_fence());
    black_box(metrology_no_fence());
    black_box(tm6_positive());
    black_box(tm6_negative());
    black_box(tm7_positive());
    black_box(tm7_negative());
    black_box(tm8_positive());
    black_box(tm8_negative());
}
