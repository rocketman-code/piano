//! Micro-benchmark: PianoFuture overhead per yield at varying nesting depths.
//!
//! Measures the cost of extend_from_slice/truncate on each poll() relative to a bare
//! tokio::task::yield_now().

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Bare yield_now in a loop -- the baseline.
async fn bare_yields(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

/// Single PianoFuture wrapping yields.
async fn instrumented_depth_1(n: usize) {
    piano_runtime::PianoFuture::new(async move {
        let _guard = piano_runtime::enter("depth1");
        piano_runtime::register("depth1");
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn instrumented_depth_5(n: usize) {
    piano_runtime::PianoFuture::new(async move {
        let _guard = piano_runtime::enter("l1");
        piano_runtime::register("l1");
        piano_runtime::PianoFuture::new(async move {
            let _guard = piano_runtime::enter("l2");
            piano_runtime::register("l2");
            piano_runtime::PianoFuture::new(async move {
                let _guard = piano_runtime::enter("l3");
                piano_runtime::register("l3");
                piano_runtime::PianoFuture::new(async move {
                    let _guard = piano_runtime::enter("l4");
                    piano_runtime::register("l4");
                    piano_runtime::PianoFuture::new(async move {
                        let _guard = piano_runtime::enter("l5");
                        piano_runtime::register("l5");
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
    piano_runtime::PianoFuture::new(async move {
        let _guard = piano_runtime::enter("l1");
        piano_runtime::register("l1");
        piano_runtime::PianoFuture::new(async move {
            let _guard = piano_runtime::enter("l2");
            piano_runtime::register("l2");
            piano_runtime::PianoFuture::new(async move {
                let _guard = piano_runtime::enter("l3");
                piano_runtime::register("l3");
                piano_runtime::PianoFuture::new(async move {
                    let _guard = piano_runtime::enter("l4");
                    piano_runtime::register("l4");
                    piano_runtime::PianoFuture::new(async move {
                        let _guard = piano_runtime::enter("l5");
                        piano_runtime::register("l5");
                        piano_runtime::PianoFuture::new(async move {
                            let _guard = piano_runtime::enter("l6");
                            piano_runtime::register("l6");
                            piano_runtime::PianoFuture::new(async move {
                                let _guard = piano_runtime::enter("l7");
                                piano_runtime::register("l7");
                                piano_runtime::PianoFuture::new(async move {
                                    let _guard = piano_runtime::enter("l8");
                                    piano_runtime::register("l8");
                                    piano_runtime::PianoFuture::new(async move {
                                        let _guard = piano_runtime::enter("l9");
                                        piano_runtime::register("l9");
                                        piano_runtime::PianoFuture::new(async move {
                                            let _guard = piano_runtime::enter("l10");
                                            piano_runtime::register("l10");
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
            piano_runtime::reset();
            rt.block_on(async {
                instrumented_depth_1(black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_5", |b| {
        b.iter(|| {
            piano_runtime::reset();
            rt.block_on(async {
                instrumented_depth_5(black_box(yields)).await;
            });
        });
    });

    group.bench_function("depth_10", |b| {
        b.iter(|| {
            piano_runtime::reset();
            rt.block_on(async {
                instrumented_depth_10(black_box(yields)).await;
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_yield_overhead);
criterion_main!(benches);
