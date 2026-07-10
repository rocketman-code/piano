//! Sync function instrumentation -- RAII sentinel.
//!
//! Guard is the Drop-vehicle for the spec-derived SyncExecution state:
//! created when entering a profiled function, dropped when exiting. The
//! entered state is produced by the enter_sync operation (out-of-line
//! bookkeeping, then the inline stamp writes the start timestamp last);
//! Drop hands the state to the exit_sync operation, which computes
//! self_ns = inclusive_ns - children_ns and aggregates into the
//! per-thread FnAgg vec.
//!
//! Children within the Guard's scope report their inclusive time via the
//! per-thread accumulator. The callee's drop scope is fully contained in
//! the caller's (Rust Reference, Destructors, Drop scopes), so nested
//! children report before the parent reads; reverse declaration order
//! additionally orders sentinels sharing one scope.
//!
//! Invariants:
//! - Guard is !Send. Alloc deltas are computed on the same thread as
//!   creation. Enforcement: PhantomData<*const ()>.
//! - Profiler bookkeeping allocs are excluded from user counts.
//!   Enforcement: ProfilerBookkeeping proof token inside the operations.
//! - Guard never panics. All arithmetic uses saturating_sub.

use core::sync::atomic::{compiler_fence, Ordering};

use crate::generated::piano_runtime::sync_execution::{ops, SyncExecution};
use crate::session::ProfileSession;
use crate::time::read;
use crate::NameId;
use std::marker::PhantomData;

/// RAII sentinel for sync function instrumentation.
///
/// Created by `piano_runtime::enter(name_id)`. Dropped at function exit.
/// Holds the entered SyncExecution; None means profiling is inactive and
/// drop is a no-op. The state type itself derives Clone, so the Drop
/// obligation lives here, on the vehicle, never on the state.
pub struct Guard {
    exec: Option<SyncExecution>,
    _not_send: PhantomData<*const ()>,
}

/// Enter a profiled function. Returns a Guard that aggregates on drop.
///
/// Reads profiling context from &'static ProfileSession.
/// If profiling is not active, returns an inactive Guard whose drop is a
/// no-op. The enter_sync operation does the out-of-line bookkeeping; the
/// TSC read stays inline here and lands last via the stamp.
#[inline(always)]
pub fn enter(name_id: u32) -> Guard {
    if ProfileSession::get().is_none() {
        return Guard {
            exec: None,
            _not_send: PhantomData,
        };
    }
    let id = NameId::from_raw(name_id);
    let unstamped = ops::enter_sync(&id);
    compiler_fence(Ordering::SeqCst);
    let exec = ops::stamped(unstamped, read());
    crate::inflight::enter(id, *exec.start_ticks());
    Guard {
        exec: Some(exec),
        _not_send: PhantomData,
    }
}

impl Drop for Guard {
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(exec) = self.exec.take() {
            let _exited = ops::exit_sync(exec);
        }
    }
}
