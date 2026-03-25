//! Micro-benchmark: measures enter_async per-poll overhead at varying nesting depths.

use std::alloc::System;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use piano_runtime::alloc::PianoAllocator;
use piano_runtime::session::ProfileSession;
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

fn init_session() {
    // ProfileSession::init is idempotent (Once-guarded internally
    // via CalibrationData::calibrate). Safe to call from multiple benches.
    ProfileSession::init(None, false, NAMES, "bench", 0);
}

/// Bare yield_now in a loop -- the baseline.
async fn bare_yields(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

/// Single enter_async wrapping yields.
async fn instrumented_depth_1(n: usize) {
    piano_runtime::enter_async(1, async move {
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn instrumented_depth_5(n: usize) {
    piano_runtime::enter_async(1, async move {
        piano_runtime::enter_async(2, async move {
            piano_runtime::enter_async(3, async move {
                piano_runtime::enter_async(4, async move {
                    piano_runtime::enter_async(5, async move {
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

async fn instrumented_depth_10(n: usize) {
    piano_runtime::enter_async(1, async move {
        piano_runtime::enter_async(2, async move {
            piano_runtime::enter_async(3, async move {
                piano_runtime::enter_async(4, async move {
                    piano_runtime::enter_async(5, async move {
                        piano_runtime::enter_async(6, async move {
                            piano_runtime::enter_async(7, async move {
                                piano_runtime::enter_async(8, async move {
                                    piano_runtime::enter_async(9, async move {
                                        piano_runtime::enter_async(10, async move {
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
    init_session();
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
            rt.block_on(async {
                instrumented_depth_1(black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_5", |b| {
        b.iter(|| {
            rt.block_on(async {
                instrumented_depth_5(black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_10", |b| {
        b.iter(|| {
            rt.block_on(async {
                instrumented_depth_10(black_box(yields)).await;
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_yield_overhead);
criterion_main!(benches);
