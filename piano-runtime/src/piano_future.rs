use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::alloc::{AllocSnapshot, ALLOC_COUNTERS};
use crate::collector::{StackEntry, STACK};

/// Future wrapper that carries the profiling call stack inside the future's
/// state machine. On each poll(), pushes saved entries onto the thread-local
/// STACK, polls the inner future, then splits them off. This makes the stack
/// correct by construction regardless of which thread the executor polls from,
/// eliminating the need for phantom-based migration repair.
pub struct PianoFuture<F> {
    inner: F,
    saved_entries: Vec<StackEntry>,
    base_depth: Option<usize>,
    alloc_carry: AllocSnapshot,
    #[cfg(feature = "cpu-time")]
    cpu_accumulated_ns: u64,
}

impl<F: Future> PianoFuture<F> {
    /// Wrap an inner future for profiling. The inner future should contain
    /// the `enter()` guard as its first statement.
    #[inline]
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            saved_entries: Vec::new(),
            base_depth: None,
            alloc_carry: AllocSnapshot::new(),
            #[cfg(feature = "cpu-time")]
            cpu_accumulated_ns: 0,
        }
    }
}

impl<F: Future> Future for PianoFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: We only project Pin to the `inner` field (which may be
        // !Unpin). All other fields (Vec, Option, AllocSnapshot, u64) are
        // Unpin and accessed via &mut. We never move `inner` out of self.
        let this = unsafe { self.get_unchecked_mut() };

        // --- Install phase ---

        // Save this thread's alloc counters (restored on exit).
        let thread_alloc = ALLOC_COUNTERS
            .try_with(|cell| cell.get())
            .unwrap_or_default();

        // Install our accumulated allocs so new allocs build on previous
        // segments. enter() will save this and zero, so the function body
        // sees carry + new allocs across polls.
        let _ = ALLOC_COUNTERS.try_with(|cell| {
            cell.set(this.alloc_carry);
        });
        this.alloc_carry = AllocSnapshot::new();

        // Push saved entries onto the thread-local STACK.
        let base = STACK.with(|stack| {
            let mut s = stack.borrow_mut();
            let base = *this.base_depth.get_or_insert(s.len());

            #[cfg(feature = "cpu-time")]
            {
                // Fix up cpu_start_ns on our bottom entry to this thread's
                // CPU clock. Previous segments' CPU time is in
                // cpu_accumulated_ns.
                if let Some(first) = this.saved_entries.first_mut() {
                    first.cpu_start_ns = crate::cpu_clock::cpu_now_ns() - this.cpu_accumulated_ns;
                    this.cpu_accumulated_ns = 0;
                }
            }

            s.extend(this.saved_entries.drain(..));
            base
        });

        // --- Poll phase ---

        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        // --- Save phase ---

        #[cfg(feature = "cpu-time")]
        {
            // Accumulate CPU time for this poll segment before splitting off.
            STACK.with(|stack| {
                let s = stack.borrow();
                if let Some(entry) = s.get(base) {
                    let cpu_now = crate::cpu_clock::cpu_now_ns();
                    this.cpu_accumulated_ns = cpu_now.saturating_sub(entry.cpu_start_ns);
                }
            });
        }

        // Capture alloc state: carry forward for next poll (or discard if
        // Ready, since Guard already consumed and recorded the allocs).
        this.alloc_carry = ALLOC_COUNTERS
            .try_with(|cell| cell.get())
            .unwrap_or_default();

        // Restore this thread's alloc counters.
        let _ = ALLOC_COUNTERS.try_with(|cell| {
            cell.set(thread_alloc);
        });

        // Split off our entries from the STACK.
        STACK.with(|stack| {
            this.saved_entries = stack.borrow_mut().split_off(base);
        });

        result
    }
}

/// When a PianoFuture is dropped without completing (e.g. cancelled by
/// `select!`), push saved entries back onto the STACK so Guards inside
/// `inner` can find their entries when they drop. Rust drops fields in
/// declaration order after Drop::drop returns, so `inner` (containing the
/// Guards) drops after this runs.
impl<F> Drop for PianoFuture<F> {
    fn drop(&mut self) {
        if !self.saved_entries.is_empty() {
            STACK.with(|stack| {
                stack.borrow_mut().extend(self.saved_entries.drain(..));
            });
        }
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PianoFuture<core::future::Ready<()>>>();
    }
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector;

    #[cfg(not(miri))]
    fn run<F: Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// Minimal waker-based test that exercises the unsafe Pin projection
    /// in PianoFuture::poll without pulling in tokio. Finishes in
    /// milliseconds under Miri.
    #[test]
    fn piano_future_pin_projection_safety() {
        use core::task::{RawWaker, RawWakerVTable, Waker};

        fn noop_raw_waker() -> RawWaker {
            fn no_op(_: *const ()) {}
            fn clone(p: *const ()) -> RawWaker {
                RawWaker::new(p, &VTABLE)
            }
            const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
            RawWaker::new(core::ptr::null(), &VTABLE)
        }

        collector::reset();

        // A future that yields once then completes.
        let mut yielded = false;
        let inner = core::future::poll_fn(move |cx| {
            if !yielded {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                let _guard = collector::enter("pin_safe");
                collector::register("pin_safe");
                Poll::Ready(())
            }
        });

        let mut fut = PianoFuture::new(inner);
        // SAFETY: we never move `fut` after pinning.
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };

        let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
        let mut cx = Context::from_waker(&waker);

        // First poll: Pending (inner yields)
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        // Second poll: Ready (inner completes)
        assert!(fut.as_mut().poll(&mut cx).is_ready());

        let records = collector::collect_all();
        assert!(
            records.iter().any(|r| r.name == "pin_safe"),
            "pin_safe should appear in records after Pin-projected polling"
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_basic_enter_drop() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_basic");
                collector::register("pf_basic");
            })
            .await;
        });
        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "pf_basic"));
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_stack_restored_after_yield() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_yield");
                collector::register("pf_yield");
                STACK.with(|s| assert_eq!(s.borrow().len(), 1));
                tokio::task::yield_now().await;
                // After yield + restore, entry should still be on stack
                STACK.with(|s| assert_eq!(s.borrow().len(), 1));
            })
            .await;
        });
        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "pf_yield"));
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_nested_parent_child() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_outer");
                collector::register("pf_outer");

                PianoFuture::new(async {
                    let _guard = collector::enter("pf_inner");
                    collector::register("pf_inner");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                })
                .await;
            })
            .await;
        });
        let records = collector::collect_all();
        let outer = records.iter().find(|r| r.name == "pf_outer").unwrap();
        let inner = records.iter().find(|r| r.name == "pf_inner").unwrap();
        // Inner's total_ms should be ~50ms
        assert!(
            inner.total_ms > 40.0,
            "inner total_ms too low: {}",
            inner.total_ms
        );
        // Outer's self_ms should NOT include inner's ~50ms
        assert!(
            outer.self_ms < 30.0,
            "outer self_ms ({}) should be small (not include inner's 50ms)",
            outer.self_ms
        );
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_stack_isolation_between_tasks() {
        collector::reset();
        run(async {
            let a = tokio::spawn(PianoFuture::new(async {
                let _guard = collector::enter("task_a");
                collector::register("task_a");
                for _ in 0..10 {
                    STACK.with(|s| {
                        let stack = s.borrow();
                        assert!(stack.iter().all(|e| e.name != "task_b"));
                    });
                    tokio::task::yield_now().await;
                }
            }));
            let b = tokio::spawn(PianoFuture::new(async {
                let _guard = collector::enter("task_b");
                collector::register("task_b");
                for _ in 0..10 {
                    STACK.with(|s| {
                        let stack = s.borrow();
                        assert!(stack.iter().all(|e| e.name != "task_a"));
                    });
                    tokio::task::yield_now().await;
                }
            }));
            a.await.unwrap();
            b.await.unwrap();
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_cancelled_by_select() {
        // When select! cancels a branch, the PianoFuture is dropped without
        // completing. The Drop impl must push saved entries back onto the
        // STACK so Guards can pop cleanly.
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_select_parent");
                collector::register("pf_select_parent");

                tokio::select! {
                    _ = PianoFuture::new(async {
                        let _guard = collector::enter("pf_winner");
                        collector::register("pf_winner");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }) => {}
                    _ = PianoFuture::new(async {
                        let _guard = collector::enter("pf_loser");
                        collector::register("pf_loser");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }) => {}
                }
            })
            .await;
        });
        // Should not panic. Both winner and loser should appear in records.
        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "pf_select_parent"));
        assert!(records.iter().any(|r| r.name == "pf_winner"));
        // The STACK should be clean after completion.
        STACK.with(|s| assert_eq!(s.borrow().len(), 0));
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_alloc_tracking() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_alloc");
                collector::register("pf_alloc");
                ALLOC_COUNTERS.with(|cell| {
                    let mut snap = cell.get();
                    snap.alloc_count += 5;
                    snap.alloc_bytes += 500;
                    cell.set(snap);
                });
                tokio::task::yield_now().await;
                // More allocs after yield (possibly on different thread)
                ALLOC_COUNTERS.with(|cell| {
                    let mut snap = cell.get();
                    snap.alloc_count += 3;
                    snap.alloc_bytes += 300;
                    cell.set(snap);
                });
            })
            .await;
        });
        let records = collector::collect_invocations();
        let rec = records.iter().find(|r| r.name == "pf_alloc").unwrap();
        assert_eq!(rec.alloc_count, 8, "expected 5+3=8 allocs");
        assert_eq!(rec.alloc_bytes, 800, "expected 500+300=800 bytes");
    }

    #[cfg(not(miri))]
    #[test]
    fn piano_future_with_fork_adopt() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_fork_parent");
                collector::register("pf_fork_parent");
                collector::register("pf_fork_child");

                let ctx = collector::fork().expect("should have parent on stack");
                std::thread::scope(|s| {
                    s.spawn(|| {
                        let _adopt = collector::adopt(&ctx);
                        let _child_guard = collector::enter("pf_fork_child");
                        collector::burn_cpu(20_000);
                    });
                });
                collector::burn_cpu(2_000);
            })
            .await;
        });
        let records = collector::collect_all();
        assert!(
            records.iter().any(|r| r.name == "pf_fork_parent"),
            "parent should appear in records"
        );
        assert!(
            records.iter().any(|r| r.name == "pf_fork_child"),
            "child should appear in records"
        );

        let parent = records.iter().find(|r| r.name == "pf_fork_parent").unwrap();
        let child = records.iter().find(|r| r.name == "pf_fork_child").unwrap();
        // Parent total_ms includes blocking on std::thread::scope, so it
        // should exceed the child's total_ms.
        assert!(
            parent.total_ms > child.total_ms,
            "parent total_ms ({:.3}) should exceed child total_ms ({:.3})",
            parent.total_ms,
            child.total_ms,
        );
        // Child should have measurable wall time from burn_cpu(20_000).
        assert!(
            child.total_ms > 0.5,
            "child total_ms ({:.3}) should reflect actual CPU work",
            child.total_ms,
        );
    }
}
