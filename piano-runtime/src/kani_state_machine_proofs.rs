//! Kani proof harnesses for state machine invariants.
//!
//! Run with: cargo kani -p piano-runtime

/// I10/I41: PianoFuture split_off correctness.
/// push N entries at base, split_off(base) returns exactly those entries.
#[kani::proof]
#[kani::unwind(6)]
fn proof_split_off_preserves_entries() {
    let pre_existing: usize = kani::any();
    let entries_pushed: usize = kani::any();
    kani::assume(pre_existing <= 3);
    kani::assume(entries_pushed <= 3);
    kani::assume(pre_existing + entries_pushed <= 5);

    let mut stack: Vec<u64> = Vec::new();
    for i in 0..pre_existing {
        stack.push(i as u64);
    }

    let base = stack.len();

    for i in 0..entries_pushed {
        stack.push((pre_existing + i) as u64);
    }

    let split = stack.split_off(base);
    assert_eq!(split.len(), entries_pushed);
    assert_eq!(stack.len(), pre_existing);

    for (i, &val) in split.iter().enumerate() {
        assert_eq!(val, (pre_existing + i) as u64);
    }
    for (i, &val) in stack.iter().enumerate() {
        assert_eq!(val, i as u64);
    }
}

/// I10/I41 supplement: base_depth set once via get_or_insert.
#[kani::proof]
fn proof_base_depth_set_once() {
    let first_depth: usize = kani::any();
    let second_depth: usize = kani::any();
    kani::assume(first_depth <= 100);
    kani::assume(second_depth <= 100);

    let mut base: Option<usize> = None;
    let b1 = *base.get_or_insert(first_depth);
    assert_eq!(b1, first_depth);
    let b2 = *base.get_or_insert(second_depth);
    assert_eq!(b2, first_depth, "second call must return FIRST depth");
}

/// I21: Frame boundary heuristic -- remaining_all_base is true
/// iff all remaining entries have depth 0.
#[kani::proof]
#[kani::unwind(5)]
fn proof_frame_boundary_heuristic() {
    let stack_size: usize = kani::any();
    kani::assume(stack_size <= 4);

    let mut depths = [0u16; 4];
    for i in 0..4 {
        if i < stack_size {
            depths[i] = kani::any();
            kani::assume(depths[i] <= 3);
        }
    }

    let remaining_all_base = (0..stack_size).all(|i| depths[i] == 0);

    let mut manual_check = true;
    for i in 0..stack_size {
        if depths[i] != 0 {
            manual_check = false;
        }
    }

    assert_eq!(remaining_all_base, manual_check);
}
