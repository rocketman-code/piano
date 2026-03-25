//! Micro-benchmark: measures PianoFuture per-poll overhead at varying nesting depths.

use std::alloc::System;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use piano_runtime::alloc::PianoAllocator;
use piano_runtime::ctx::Ctx;
use piano_runtime::piano_future::PianoFuture;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: PianoAllocator<System> = PianoAllocator::new(System);

static NAMES: &[(u32, &str)] = &[
    (1, "l1"),
    (2, "l2"),
    (3, "l3"),
    (4, "l4"),
    (5, "l5"),
    (6, "l6"),
    (7, "l7"),
    (8, "l8"),
    (9, "l9"),
    (10, "l10"),
];

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn fresh_ctx() -> Ctx {
    Ctx::new(None, false, NAMES)
}

/// Bare yield_now in a loop -- the baseline.
async fn bare_yields(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

/// Single PianoFuture wrapping yields.
async fn instrumented_depth_1(ctx: Ctx, n: usize) {
    let (state, _child_ctx) = ctx.enter_async(1);
    PianoFuture::new(state, async move {
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn instrumented_depth_5(ctx: Ctx, n: usize) {
    let (s1, c1) = ctx.enter_async(1);
    PianoFuture::new(s1, async move {
        let (s2, c2) = c1.enter_async(2);
        PianoFuture::new(s2, async move {
            let (s3, c3) = c2.enter_async(3);
            PianoFuture::new(s3, async move {
                let (s4, c4) = c3.enter_async(4);
                PianoFuture::new(s4, async move {
                    let (s5, _c5) = c4.enter_async(5);
                    PianoFuture::new(s5, async move {
                        for _ in 0..n {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await;
                })
                .await;
            })
            .await;
        })
        .await;
    })
    .await;
}

async fn instrumented_depth_10(ctx: Ctx, n: usize) {
    let (s1, c1) = ctx.enter_async(1);
    PianoFuture::new(s1, async move {
        let (s2, c2) = c1.enter_async(2);
        PianoFuture::new(s2, async move {
            let (s3, c3) = c2.enter_async(3);
            PianoFuture::new(s3, async move {
                let (s4, c4) = c3.enter_async(4);
                PianoFuture::new(s4, async move {
                    let (s5, c5) = c4.enter_async(5);
                    PianoFuture::new(s5, async move {
                        let (s6, c6) = c5.enter_async(6);
                        PianoFuture::new(s6, async move {
                            let (s7, c7) = c6.enter_async(7);
                            PianoFuture::new(s7, async move {
                                let (s8, c8) = c7.enter_async(8);
                                PianoFuture::new(s8, async move {
                                    let (s9, c9) = c8.enter_async(9);
                                    PianoFuture::new(s9, async move {
                                        let (s10, _c10) = c9.enter_async(10);
                                        PianoFuture::new(s10, async move {
                                            for _ in 0..n {
                                                tokio::task::yield_now().await;
                                            }
                                        })
                                        .await;
                                    })
                                    .await;
                                })
                                .await;
                            })
                            .await;
                        })
                        .await;
                    })
                    .await;
                })
                .await;
            })
            .await;
        })
        .await;
    })
    .await;
}

fn bench_yield_overhead(c: &mut Criterion) {
    let rt = rt();
    let yields = 1000;

    let mut group = c.benchmark_group("piano_future_per_yield");

    group.bench_function("bare_yield", |b| {
        b.iter(|| {
            rt.block_on(async {
                bare_yields(black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_1", |b| {
        b.iter(|| {
            let ctx = fresh_ctx();
            rt.block_on(async {
                instrumented_depth_1(ctx, black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_5", |b| {
        b.iter(|| {
            let ctx = fresh_ctx();
            rt.block_on(async {
                instrumented_depth_5(ctx, black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_10", |b| {
        b.iter(|| {
            let ctx = fresh_ctx();
            rt.block_on(async {
                instrumented_depth_10(ctx, black_box(yields)).await;
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_yield_overhead);
criterion_main!(benches);
