//! Kani proof harnesses for bounded model checking of piano-runtime arithmetic invariants.
//!
//! Run with: cargo kani -p piano-runtime

/// I4: Stack depth cast to u16 -- verify truncation behavior.
#[kani::proof]
fn proof_depth_u16_truncation() {
    let depth: usize = kani::any();
    kani::assume(depth <= 70_000);
    let depth_u16 = depth as u16;
    if depth <= u16::MAX as usize {
        assert_eq!(depth_u16 as usize, depth, "no truncation for valid depths");
    }
    // For depth > u16::MAX, truncation is documented behavior
}

/// I11: elapsed_ns uses wrapping_sub + u128 -- no panic, no UB.
#[kani::proof]
fn proof_elapsed_ns_no_overflow() {
    let start: u64 = kani::any();
    let end: u64 = kani::any();
    let numer: u64 = kani::any();
    let denom: u64 = kani::any();
    kani::assume(denom > 0);
    kani::assume(numer > 0);
    kani::assume(numer <= 10_000_000_000);
    kani::assume(denom <= 10_000_000_000);

    let ticks = end.wrapping_sub(start);
    let result = (ticks as u128 * numer as u128 / denom as u128) as u64;
    let _ = result; // no panic = proof passes
}

/// I32: synthesize ms-to-ns: (self_ms * 1_000_000.0).max(0.0) as u64
#[kani::proof]
fn proof_synthesize_ms_to_ns_non_negative() {
    let self_ms: f64 = kani::any();
    kani::assume(self_ms >= -1e12 && self_ms <= 1e12);
    kani::assume(!self_ms.is_nan());
    let ns = (self_ms * 1_000_000.0).max(0.0) as u64;
    // max(0.0) guarantees non-negative input to cast
    // Rust >= 1.45 saturating f64->u64 cast guarantees no UB
    let _ = ns;
}

/// I46: CPU time accumulation uses saturating_sub -- never underflows.
#[kani::proof]
fn proof_cpu_time_saturating_sub() {
    let cpu_now: u64 = kani::any();
    let cpu_start: u64 = kani::any();
    let result = cpu_now.saturating_sub(cpu_start);
    if cpu_now >= cpu_start {
        assert_eq!(result, cpu_now - cpu_start);
    } else {
        assert_eq!(result, 0);
    }
}
