//! Accuracy validation: compute-bound workload with known ratios.
//!
//! Run with: cargo test -p piano-runtime --test accuracy -- --ignored --nocapture
#![allow(clippy::incompatible_msrv)] // tests run on dev toolchain, not published MSRV

/// CPU-bound workload: wrapping arithmetic over a buffer.
fn burn_cpu(iterations: u64) {
    let mut buf = [0x42u8; 4096];
    for i in 0..iterations {
        for b in &mut buf {
            *b = b.wrapping_add(i as u8).wrapping_mul(31);
        }
    }
    std::hint::black_box(&buf);
}

#[test]
#[ignore]
fn compute_ratio_accuracy() {
    piano_runtime::reset();

    {
        let _main = piano_runtime::enter("bench_main");
        {
            let _heavy = piano_runtime::enter("heavy");
            burn_cpu(100_000);
        }
        {
            let _light = piano_runtime::enter("light");
            burn_cpu(10_000);
        }
    }

    let records = piano_runtime::collect();
    let heavy = records.iter().find(|r| r.name == "heavy").unwrap();
    let light = records.iter().find(|r| r.name == "light").unwrap();

    let ratio = heavy.self_ms / light.self_ms;
    let expected_ratio = 10.0;
    let error_pct = ((ratio - expected_ratio) / expected_ratio).abs() * 100.0;

    eprintln!(
        "heavy: {:.3}ms, light: {:.3}ms",
        heavy.self_ms, light.self_ms
    );
    eprintln!("ratio: {ratio:.2} (expected {expected_ratio:.1}, error {error_pct:.1}%)");

    assert!(
        error_pct < 5.0,
        "ratio {ratio:.2} deviates from expected {expected_ratio:.1} by {error_pct:.1}% (limit 5%)"
    );

    piano_runtime::reset();
}

#[test]
#[ignore]
fn compute_three_way_ratio() {
    piano_runtime::reset();

    {
        let _main = piano_runtime::enter("ratio_main");
        {
            let _a = piano_runtime::enter("ratio_a");
            burn_cpu(60_000);
        }
        {
            let _b = piano_runtime::enter("ratio_b");
            burn_cpu(30_000);
        }
        {
            let _c = piano_runtime::enter("ratio_c");
            burn_cpu(10_000);
        }
    }

    let records = piano_runtime::collect();
    let a = records.iter().find(|r| r.name == "ratio_a").unwrap();
    let b = records.iter().find(|r| r.name == "ratio_b").unwrap();
    let c = records.iter().find(|r| r.name == "ratio_c").unwrap();

    let total_self = a.self_ms + b.self_ms + c.self_ms;
    let a_pct = a.self_ms / total_self * 100.0;
    let b_pct = b.self_ms / total_self * 100.0;
    let c_pct = c.self_ms / total_self * 100.0;

    eprintln!(
        "a: {a_pct:.1}% (expect ~60%), b: {b_pct:.1}% (expect ~30%), c: {c_pct:.1}% (expect ~10%)"
    );

    // 60:30:10 ratio -> 60%, 30%, 10% of self-time
    assert!(
        (a_pct - 60.0).abs() < 5.0,
        "a should be ~60%, got {a_pct:.1}%"
    );
    assert!(
        (b_pct - 30.0).abs() < 5.0,
        "b should be ~30%, got {b_pct:.1}%"
    );
    assert!(
        (c_pct - 10.0).abs() < 5.0,
        "c should be ~10%, got {c_pct:.1}%"
    );

    piano_runtime::reset();
}
