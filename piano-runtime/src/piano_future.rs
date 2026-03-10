use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::alloc::{AllocSnapshot, ALLOC_COUNTERS};
#[cfg(test)]
use crate::collector::with_stack_ref;
use crate::collector::{with_stack_mut, StackEntry};

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
    tsc_accumulated: u64,
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
            tsc_accumulated: 0,
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

        // Save this thread's alloc counters and install our carry in one
        // TLS lookup (halves ALLOC_COUNTERS try_with calls: 4 -> 2).
        let thread_alloc = ALLOC_COUNTERS
            .try_with(|cell| {
                let saved = cell.get();
                cell.set(this.alloc_carry);
                saved
            })
            .unwrap_or_default();
        this.alloc_carry = AllocSnapshot::new();

        // Push saved entries onto the thread-local STACK.
        let base = with_stack_mut(|s| {
            let base = *this.base_depth.get_or_insert(s.len());

            #[cfg(feature = "cpu-time")]
            {
                if let Some(first) = this.saved_entries.first_mut() {
                    first.cpu_start_ns = crate::cpu_clock::cpu_now_ns() - this.cpu_accumulated_ns;
                    this.cpu_accumulated_ns = 0;
                }
            }

            if let Some(first) = this.saved_entries.first_mut() {
                first.start_tsc = crate::tsc::read().wrapping_sub(this.tsc_accumulated);
                this.tsc_accumulated = 0;
            }

            s.extend_from_slice(&this.saved_entries);
            this.saved_entries.clear();
            base
        });

        // --- Poll phase ---

        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let result = inner.poll(cx);

        // --- Save phase ---

        // Capture alloc carry and restore thread's counters in one TLS lookup.
        this.alloc_carry = ALLOC_COUNTERS
            .try_with(|cell| {
                let carry = cell.get();
                cell.set(thread_alloc);
                carry
            })
            .unwrap_or_default();

        // Save our entries from the STACK, reusing saved_entries' buffer
        // from the previous poll to avoid allocating on every yield.
        with_stack_mut(|s| {
            #[cfg(feature = "cpu-time")]
            {
                if let Some(entry) = s.get(base) {
                    let cpu_now = crate::cpu_clock::cpu_now_ns();
                    this.cpu_accumulated_ns = cpu_now.saturating_sub(entry.cpu_start_ns);
                }
            }

            if let Some(entry) = s.get(base) {
                this.tsc_accumulated = crate::tsc::read().wrapping_sub(entry.start_tsc);
            }

            this.saved_entries.clear();
            this.saved_entries.extend_from_slice(&s[base..]);
            s.truncate(base);
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
            if let Some(first) = self.saved_entries.first_mut() {
                first.start_tsc = crate::tsc::read().wrapping_sub(self.tsc_accumulated);
                self.tsc_accumulated = 0;
            }
            with_stack_mut(|s| {
                s.extend_from_slice(&self.saved_entries);
                self.saved_entries.clear();
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
    use crate::collector::{lookup_name, unpack_name_id};

    fn run<F: Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

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

    #[test]
    fn piano_future_stack_restored_after_yield() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("pf_yield");
                collector::register("pf_yield");
                with_stack_ref(|s| assert_eq!(s.len(), 1));
                tokio::task::yield_now().await;
                // After yield + restore, entry should still be on stack
                with_stack_ref(|s| assert_eq!(s.len(), 1));
            })
            .await;
        });
        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "pf_yield"));
    }

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

    #[test]
    fn piano_future_stack_isolation_between_tasks() {
        collector::reset();
        run(async {
            let a = tokio::spawn(PianoFuture::new(async {
                let _guard = collector::enter("task_a");
                collector::register("task_a");
                for _ in 0..10 {
                    with_stack_ref(|s| {
                        assert!(s
                            .iter()
                            .all(|e| lookup_name(unpack_name_id(e.packed)) != "task_b"));
                    });
                    tokio::task::yield_now().await;
                }
            }));
            let b = tokio::spawn(PianoFuture::new(async {
                let _guard = collector::enter("task_b");
                collector::register("task_b");
                for _ in 0..10 {
                    with_stack_ref(|s| {
                        assert!(s
                            .iter()
                            .all(|e| lookup_name(unpack_name_id(e.packed)) != "task_a"));
                    });
                    tokio::task::yield_now().await;
                }
            }));
            a.await.unwrap();
            b.await.unwrap();
        });
    }

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
        with_stack_ref(|s| assert_eq!(s.len(), 0));
    }

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

    #[test]
    #[serial_test::serial]
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

        let child = records.iter().find(|r| r.name == "pf_fork_child").unwrap();
        // Child should have at least 1 call recorded.
        assert_eq!(child.calls, 1, "child should have 1 call recorded");
    }

    #[test]
    fn piano_future_split_index_correctness() {
        // Verifies that the stack split in poll() correctly separates
        // this future's entries from entries below it on the stack.
        // Regression test: cargo-mutants found that replacing `-` with
        // `+` or `/` in the split arithmetic produced no test failure.
        collector::reset();
        run(async {
            // Outer future pushes one entry onto the stack.
            PianoFuture::new(async {
                let _outer = collector::enter("split_outer");
                collector::register("split_outer");

                // Inner future pushes another entry. After yield,
                // the inner future should save exactly 2 entries
                // (split_outer + split_inner) -- not more, not fewer.
                PianoFuture::new(async {
                    let _inner = collector::enter("split_inner");
                    collector::register("split_inner");

                    // Before yield: stack should have 2 entries
                    with_stack_ref(|s| {
                        assert_eq!(s.len(), 2, "stack should have outer + inner before yield");
                    });

                    tokio::task::yield_now().await;

                    // After yield + restore: stack should still have 2 entries
                    with_stack_ref(|s| {
                        assert_eq!(s.len(), 2, "stack should have outer + inner after yield");
                    });
                })
                .await;
            })
            .await;
        });

        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "split_outer"));
        assert!(records.iter().any(|r| r.name == "split_inner"));
    }

    /// Verifies that cpu_accumulated_ns is correctly subtracted from cpu_now_ns
    /// during the install phase. If `-` is mutated to `+` or `/`, the
    /// cpu_start_ns would be wrong, causing CPU time to be grossly over- or
    /// under-counted across yields.
    #[cfg(feature = "cpu-time")]
    #[test]
    fn piano_future_cpu_time_across_yields() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("cpu_yield");
                collector::register("cpu_yield");

                // Do some CPU work before yield
                collector::burn_cpu(10_000);
                tokio::task::yield_now().await;

                // Do more CPU work after yield
                collector::burn_cpu(10_000);
                tokio::task::yield_now().await;

                // Final burst
                collector::burn_cpu(10_000);
            })
            .await;
        });

        let records = collector::collect_all();
        let rec = records.iter().find(|r| r.name == "cpu_yield").unwrap();

        // CPU time should reflect actual compute across all three bursts.
        // burn_cpu(10_000) takes measurable CPU time. Total should be > 0.
        assert!(
            rec.cpu_self_ms > 0.0,
            "cpu_self_ms should be positive after CPU work: got {:.3}ms",
            rec.cpu_self_ms,
        );

        // If `-` were `+`, cpu_start_ns would be cpu_now + accumulated,
        // which is far in the future. The elapsed CPU time would wrap to
        // a huge value or be negative (saturated to 0).
        // If `-` were `/`, cpu_start_ns would be cpu_now / accumulated
        // which is near 0, making elapsed CPU time equal to cpu_now (~huge).
        // Either way, the result would be wildly different from wall time.
        // CPU time should not exceed wall time (it's a single thread).
        assert!(
            rec.cpu_self_ms < rec.total_ms * 2.0,
            "cpu_self_ms ({:.3}) should not grossly exceed total_ms ({:.3}) -- arithmetic bug?",
            rec.cpu_self_ms,
            rec.total_ms,
        );
    }

    /// Verifies that tsc_accumulated is correctly subtracted from tsc::read()
    /// during the install phase. If the subtraction is missing or wrong, the
    /// virtual start_tsc would not encode prior poll time, causing self_ms
    /// for async functions to include suspension gaps.
    #[test]
    fn piano_future_wall_time_across_yields() {
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _guard = collector::enter("wall_yield");
                collector::register("wall_yield");

                // Do some CPU work before yield
                collector::burn_cpu(10_000);
                // Sleep introduces a suspension gap that should NOT appear in self_ms
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                // More CPU work after yield
                collector::burn_cpu(10_000);
            })
            .await;
        });

        let records = collector::collect_all();
        let rec = records.iter().find(|r| r.name == "wall_yield").unwrap();

        // total_ms should include the 50ms sleep (latency).
        assert!(
            rec.total_ms > 40.0,
            "total_ms should include sleep: got {:.1}ms",
            rec.total_ms,
        );

        // self_ms should NOT include the 50ms sleep (active time only).
        // The gap between total_ms and self_ms should be at least 30ms
        // (most of the 50ms sleep). Using a ratio avoids absolute thresholds
        // that break on slow CI runners where burn_cpu takes longer.
        let gap = rec.total_ms - rec.self_ms;
        assert!(
            gap > 30.0,
            "suspension gap should be excluded from self_ms: gap={:.1}ms \
             (total={:.1}ms, self={:.1}ms)",
            gap,
            rec.total_ms,
            rec.self_ms,
        );
    }

    #[test]
    fn piano_future_drop_restores_stack_for_cancelled_guards() {
        // Verifies that PianoFuture::drop pushes saved_entries back onto
        // the STACK so Guards inside the cancelled future can pop cleanly.
        // Regression test: cargo-mutants found that replacing Drop::drop
        // body with () produced no test failure.
        collector::reset();
        run(async {
            PianoFuture::new(async {
                let _parent = collector::enter("drop_parent");
                collector::register("drop_parent");
                collector::register("drop_cancelled");

                tokio::select! {
                    _ = PianoFuture::new(async {
                        let _fast = collector::enter("drop_fast");
                        collector::register("drop_fast");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }) => {}
                    _ = PianoFuture::new(async {
                        let _cancelled = collector::enter("drop_cancelled");
                        // This guard must be dropped cleanly after PianoFuture::drop
                        // restores saved_entries to the stack.
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }) => {}
                }

                // After select!, the stack should only contain the parent entry.
                // If Drop::drop is a no-op, the cancelled guard can't find its
                // entry and either panics or corrupts the stack.
                with_stack_ref(|s| {
                    assert_eq!(
                        s.len(),
                        1,
                        "stack should have only parent after select! (got {})",
                        s.len()
                    );
                    assert_eq!(lookup_name(unpack_name_id(s[0].packed)), "drop_parent");
                });
            })
            .await;
        });

        // Both branches should appear in records (winner completed, loser dropped).
        let records = collector::collect_all();
        assert!(records.iter().any(|r| r.name == "drop_parent"));
        assert!(records.iter().any(|r| r.name == "drop_fast"));
        // Stack should be clean.
        with_stack_ref(|s| assert_eq!(s.len(), 0));
    }
}
