// ASM verification tests for measurement window claims (TM6, TM7, TM8).
//
// These tests extract assembly from codegen_inspect.rs example functions
// using cargo-show-asm and verify structural properties of the generated
// code. Each claim has a positive control (real pattern, must pass) and
// a negative control (deliberately broken, must fail) to prove the
// verification tool itself is sound.
//
// Platform support: x86_64 (rdtsc) and aarch64 (mrs cntvct_el0).
// Requires: cargo-show-asm (`cargo install cargo-show-asm`).
//
// Architecture:
//   Metrological controls first -- prove the tool can detect instructions.
//   Then TM6/TM7/TM8 -- each with positive + negative control.

use std::process::Command;

// ---- Platform-specific instruction patterns --------------------------------

/// The TSC read instruction for the current platform.
#[cfg(target_arch = "x86_64")]
const TSC_PATTERN: &str = "rdtsc";

#[cfg(target_arch = "aarch64")]
const TSC_PATTERN: &str = "cntvct_el0";

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const TSC_PATTERN: &str = "__UNSUPPORTED_ARCH__";

/// The compiler fence annotation emitted by LLVM.
const FENCE_PATTERN: &str = "#MEMBARRIER";

/// Substring that identifies a call to Guard::create in the ASM.
/// enter() is #[inline(always)], Guard::create is NOT inlined.
const GUARD_CREATE_CALL: &str = "Guard::create";

/// Substring that identifies a call to cpu_now_ns in the ASM.
const CPU_NOW_CALL: &str = "cpu_now_ns";

// ---- ASM extraction --------------------------------------------------------

/// Extract the assembly for a function from the codegen_inspect example.
/// Returns the full ASM text. Panics if cargo-asm fails.
fn extract_asm(function_name: &str) -> String {
    let symbol = format!("codegen_inspect::{}", function_name);
    extract_asm_symbol(&symbol)
}

/// Extract the assembly for an arbitrary symbol from the codegen_inspect example.
fn extract_asm_symbol(symbol: &str) -> String {
    let output = Command::new("cargo")
        .args([
            "asm",
            "-p",
            "piano-runtime",
            "--example",
            "codegen_inspect",
            "--features",
            "_test_internals",
            symbol,
            "--intel",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run cargo asm -- is cargo-show-asm installed?");

    assert!(
        output.status.success(),
        "cargo asm failed for {}: {}",
        symbol,
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("non-UTF8 ASM output")
}

/// Filter ASM lines to only instruction lines (skip directives, labels,
/// section headers, CFI, and blank lines). This is what we analyze.
fn instruction_lines(asm: &str) -> Vec<&str> {
    asm.lines()
        .map(|l| l.trim())
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with('.')
                && !l.ends_with(':')
                && !l.starts_with("//")
                && !l.starts_with(';')
        })
        .collect()
}

/// Count occurrences of a pattern in the instruction stream.
fn count_pattern(lines: &[&str], pattern: &str) -> usize {
    lines.iter().filter(|l| l.contains(pattern)).count()
}

/// Find all line indices (0-based) where pattern appears.
fn find_indices(lines: &[&str], pattern: &str) -> Vec<usize> {
    lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.contains(pattern))
        .map(|(i, _)| i)
        .collect()
}

/// True if any line between start (exclusive) and end (exclusive)
/// matches the pattern.
fn has_between(lines: &[&str], start: usize, end: usize, pattern: &str) -> bool {
    if start + 1 >= end {
        return false;
    }
    lines[start + 1..end].iter().any(|l| l.contains(pattern))
}

/// Count non-trivial instructions between two indices (exclusive).
/// Excludes #APP/#NO_APP markers and rdtsc materialization (shl, or).
fn count_nontrivial_between(lines: &[&str], start: usize, end: usize) -> usize {
    if start + 1 >= end {
        return 0;
    }
    lines[start + 1..end]
        .iter()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with("#APP")
                && !t.starts_with("#NO_APP")
                && !t.starts_with("shl")
                && !t.starts_with("or ")
        })
        .count()
}

/// Format a range of lines for diagnostic output.
fn format_line_range(lines: &[&str], start: usize, end: usize) -> String {
    lines[start..=end.min(lines.len() - 1)]
        .iter()
        .enumerate()
        .map(|(i, l)| format!("  [{}] {}", start + i, l))
        .collect::<Vec<_>>()
        .join("\n")
}

/// True if any line between start (exclusive) and end (exclusive)
/// contains a "call" instruction that is NOT to the given excluded symbol.
fn has_non_excluded_call_between(
    lines: &[&str],
    start: usize,
    end: usize,
    exclude: &[&str],
) -> bool {
    if start + 1 >= end {
        return false;
    }
    lines[start + 1..end].iter().any(|l| {
        let trimmed = l.trim();
        // "call" instruction: starts with "call" or contains "\tcall"
        let is_call = trimmed.starts_with("call") || trimmed.contains("\tcall");
        if !is_call {
            return false;
        }
        // Exclude specific allowed calls
        !exclude.iter().any(|ex| trimmed.contains(ex))
    })
}

// ---- Metrological controls -------------------------------------------------
//
// Before testing any claim, prove the tool itself is sound.
// Positive control: function with known instruction -> tool detects it.
// Negative control: function without that instruction -> tool rejects it.

#[test]
fn metrology_rdtsc_detection() {
    if cfg!(not(any(target_arch = "x86_64", target_arch = "aarch64"))) {
        return; // fallback platforms don't use hardware TSC
    }

    // Positive: metrology_has_rdtsc must contain the TSC instruction.
    let asm = extract_asm("metrology_has_rdtsc");
    let lines = instruction_lines(&asm);
    let count = count_pattern(&lines, TSC_PATTERN);
    assert!(
        count >= 1,
        "metrology_has_rdtsc: expected TSC instruction ({}), found {} in:\n{}",
        TSC_PATTERN,
        count,
        asm
    );

    // Negative: metrology_no_rdtsc must NOT contain the TSC instruction.
    let asm = extract_asm("metrology_no_rdtsc");
    let lines = instruction_lines(&asm);
    let count = count_pattern(&lines, TSC_PATTERN);
    assert_eq!(
        count, 0,
        "metrology_no_rdtsc: expected 0 TSC instructions, found {} in:\n{}",
        count, asm
    );
}

#[test]
fn metrology_fence_detection() {
    // Positive: metrology_has_fence must contain MEMBARRIER.
    let asm = extract_asm("metrology_has_fence");
    let lines = instruction_lines(&asm);
    let count = count_pattern(&lines, FENCE_PATTERN);
    assert!(
        count >= 1,
        "metrology_has_fence: expected {}, found {} in:\n{}",
        FENCE_PATTERN,
        count,
        asm
    );

    // Negative: metrology_no_fence must NOT contain MEMBARRIER.
    let asm = extract_asm("metrology_no_fence");
    let lines = instruction_lines(&asm);
    let count = count_pattern(&lines, FENCE_PATTERN);
    assert_eq!(
        count, 0,
        "metrology_no_fence: expected 0 {}, found {} in:\n{}",
        FENCE_PATTERN, count, asm
    );
}

// ---- TM6: Guard measurement window tightness ------------------------------
//
// Claim: stamp() does fence-then-TSC (TSC is last). drop() does
//        TSC-then-fence (TSC is first). Between the two TSC reads,
//        only user code runs -- no bookkeeping.
//
// stamp() and drop() are both #[inline(always)]. stamp() inlines into
// tm6_positive (the caller of enter()). drop() inlines into
// drop_in_place<Guard> (a separate symbol the linker emits for Drop).
// We verify both symbols.
//
// What we verify in ASM:
// (a) In tm6_positive: a MEMBARRIER followed by a TSC read with NO
//     call instructions between (stamp: fence -> rdtsc, tight).
// (b) In drop_in_place<Guard>: a TSC read followed by a MEMBARRIER
//     with NO bookkeeping calls between (drop: rdtsc -> fence, tight).
// (c) Negative control: tm6_negative has bookkeeping between fence
//     and TSC read, proving the tool detects violations.

/// The symbol name for Guard's drop_in_place.
const GUARD_DROP_SYMBOL: &str = "core::ptr::drop_in_place<piano_runtime::guard::Guard>";

#[test]
fn tm6_stamp_fence_then_tsc() {
    if cfg!(not(any(target_arch = "x86_64", target_arch = "aarch64"))) {
        return;
    }

    // --- Positive: stamp pattern in tm6_positive ---
    let asm = extract_asm("tm6_positive");
    let lines = instruction_lines(&asm);

    let fences = find_indices(&lines, FENCE_PATTERN);
    let tscs = find_indices(&lines, TSC_PATTERN);

    assert!(
        !fences.is_empty(),
        "tm6_positive: must contain a fence (from stamp)"
    );
    assert!(
        !tscs.is_empty(),
        "tm6_positive: must contain a TSC read (from stamp)"
    );

    // Guard::create must be called BEFORE the fence (bookkeeping done first).
    let create_indices = find_indices(&lines, GUARD_CREATE_CALL);
    assert!(
        !create_indices.is_empty(),
        "tm6_positive: Guard::create call not found"
    );
    let last_create = *create_indices.last().unwrap();

    // The stamp fence comes after Guard::create returns.
    let stamp_fence = fences
        .iter()
        .find(|&&f| f > last_create)
        .expect("tm6_positive: no fence after Guard::create");
    let stamp_tsc = tscs
        .iter()
        .find(|&&t| t > *stamp_fence)
        .expect("tm6_positive: no TSC after stamp fence");

    // Between stamp fence and stamp TSC: no call instructions.
    assert!(
        !has_non_excluded_call_between(&lines, *stamp_fence, *stamp_tsc, &[]),
        "tm6_positive: call found between stamp fence and TSC\n\
         lines[{}..{}]:\n{}",
        stamp_fence,
        stamp_tsc,
        format_line_range(&lines, *stamp_fence, *stamp_tsc)
    );

    // --- Negative: tm6_negative has bookkeeping between fence and TSC ---
    let asm = extract_asm("tm6_negative");
    let lines = instruction_lines(&asm);
    let fences = find_indices(&lines, FENCE_PATTERN);
    let tscs = find_indices(&lines, TSC_PATTERN);

    assert!(!fences.is_empty(), "tm6_negative: must contain a fence");
    assert!(!tscs.is_empty(), "tm6_negative: must contain a TSC read");

    let fence_idx = fences[0];
    let tsc_idx = *tscs
        .iter()
        .find(|&&t| t > fence_idx)
        .expect("tm6_negative: no TSC after fence");

    let intervening = count_nontrivial_between(&lines, fence_idx, tsc_idx);
    assert!(
        intervening > 0,
        "tm6_negative: expected bookkeeping between fence and TSC, \
         but window is tight (tool unsound)\n\
         lines[{}..{}]:\n{}",
        fence_idx,
        tsc_idx,
        format_line_range(&lines, fence_idx, tsc_idx)
    );
}

#[test]
fn tm6_drop_tsc_then_fence() {
    if cfg!(not(any(target_arch = "x86_64", target_arch = "aarch64"))) {
        return;
    }

    // Guard::drop is #[inline(always)] into drop_in_place<Guard>.
    let asm = extract_asm_symbol(GUARD_DROP_SYMBOL);
    let lines = instruction_lines(&asm);

    let fences = find_indices(&lines, FENCE_PATTERN);
    let tscs = find_indices(&lines, TSC_PATTERN);

    assert!(
        !tscs.is_empty(),
        "Guard drop_in_place: must contain a TSC read"
    );
    assert!(
        !fences.is_empty(),
        "Guard drop_in_place: must contain a fence"
    );

    // Drop pattern: TSC read comes FIRST, fence comes AFTER.
    let first_tsc = tscs[0];
    let next_fence = fences
        .iter()
        .find(|&&f| f > first_tsc)
        .expect("Guard drop_in_place: no fence after TSC read");

    // Between TSC and fence: no bookkeeping calls allowed.
    // The fence immediately follows the TSC materialization (shl+or on x86).
    let bookkeeping_patterns = [
        "snapshot_alloc",
        "push_measurement",
        "ReentrancyGuard",
        "cpu_now_ns",
    ];
    for pattern in &bookkeeping_patterns {
        assert!(
            !has_between(&lines, first_tsc, *next_fence, pattern),
            "Guard drop_in_place: {} found between TSC and fence\n\
             lines[{}..{}]:\n{}",
            pattern,
            first_tsc,
            next_fence,
            format_line_range(&lines, first_tsc, *next_fence)
        );
    }

    // All bookkeeping must come AFTER the fence.
    let has_bookkeeping_after = bookkeeping_patterns
        .iter()
        .any(|p| lines[*next_fence..].iter().any(|l| l.contains(p)));
    assert!(
        has_bookkeeping_after,
        "Guard drop_in_place: no bookkeeping found after fence \
         (expected ReentrancyGuard, snapshot, etc.)"
    );
}

// ---- TM7: enter/stamp split (code size) ------------------------------------
//
// Claim: Ctx::enter() inlines to a call Guard::create() + inline rdtsc.
//        Guard::create is NOT inlined (one shared copy via function call).
//
// What we verify:
// (a) Positive: tm7_positive has a `call` to Guard::create AND an
//     inline TSC read (rdtsc/cntvct).
// (b) Negative: tm7_negative has an inline TSC read but NO call to
//     Guard::create.

#[test]
fn tm7_enter_stamp_split() {
    if cfg!(not(any(target_arch = "x86_64", target_arch = "aarch64"))) {
        return;
    }

    // Positive: must have both a call to Guard::create AND inline TSC.
    let asm = extract_asm("tm7_positive");
    let lines = instruction_lines(&asm);

    let has_create_call = lines
        .iter()
        .any(|l| l.contains(GUARD_CREATE_CALL) && (l.contains("call") || l.contains("bl")));
    let has_inline_tsc = count_pattern(&lines, TSC_PATTERN) >= 1;

    assert!(
        has_create_call,
        "tm7_positive: Guard::create must appear as a call instruction\nASM:\n{}",
        asm
    );
    assert!(
        has_inline_tsc,
        "tm7_positive: TSC read must be inlined (from stamp())\nASM:\n{}",
        asm
    );

    // Negative: inline TSC but NO call to Guard::create.
    let asm = extract_asm("tm7_negative");
    let lines = instruction_lines(&asm);

    let has_create_call = lines
        .iter()
        .any(|l| l.contains(GUARD_CREATE_CALL) && (l.contains("call") || l.contains("bl")));
    let has_inline_tsc = count_pattern(&lines, TSC_PATTERN) >= 1;

    assert!(
        !has_create_call,
        "tm7_negative: must NOT have a call to Guard::create (everything is inline)\nASM:\n{}",
        asm
    );
    assert!(
        has_inline_tsc,
        "tm7_negative: must still have an inline TSC read\nASM:\n{}",
        asm
    );
}

// ---- TM8: PianoFuture::poll CPU ordering -----------------------------------
//
// Claim: cpu_now_ns() calls bracket the inner poll with compiler_fence
//        between bookkeeping and measurement.
//
// What we verify:
// (a) Positive: tm8_positive (which inlines PianoFuture::poll) has:
//     - A MEMBARRIER before the first cpu_now_ns call
//     - Two cpu_now_ns calls (before and after inner poll)
//     - A MEMBARRIER after the second cpu_now_ns call
// (b) Negative: tm8_negative has bookkeeping (black_box) between
//     the fence and the first cpu_now_ns call.

#[test]
fn tm8_piano_future_poll_cpu_ordering() {
    // TM8 uses clock_gettime (via cpu_now_ns), not TSC -- runs on any unix.
    if cfg!(not(unix)) {
        return;
    }

    // Positive control: real PianoFuture::poll pattern.
    let asm = extract_asm("tm8_positive");
    let lines = instruction_lines(&asm);

    let fences = find_indices(&lines, FENCE_PATTERN);
    let cpu_calls = find_indices(&lines, CPU_NOW_CALL);

    // Must have at least 2 fences and at least 2 cpu_now_ns calls.
    assert!(
        fences.len() >= 2,
        "tm8_positive: need >= 2 fences, got {}\nASM:\n{}",
        fences.len(),
        asm
    );
    assert!(
        cpu_calls.len() >= 2,
        "tm8_positive: need >= 2 cpu_now_ns calls, got {}\nASM:\n{}",
        cpu_calls.len(),
        asm
    );

    // Structural ordering: fence -> cpu_now_ns -> cpu_now_ns -> fence.
    // Find the first fence that precedes a cpu_now_ns call.
    let pre_fence = fences
        .iter()
        .find(|&&f| cpu_calls.iter().any(|&c| c > f))
        .expect("tm8_positive: no fence before cpu_now_ns");
    let first_cpu = cpu_calls
        .iter()
        .find(|&&c| c > *pre_fence)
        .expect("tm8_positive: no cpu_now_ns after pre-fence");
    let second_cpu = cpu_calls
        .iter()
        .find(|&&c| c > *first_cpu)
        .expect("tm8_positive: no second cpu_now_ns after first");
    let post_fence = fences
        .iter()
        .find(|&&f| f > *second_cpu)
        .expect("tm8_positive: no fence after second cpu_now_ns");

    // The ordering fence < first_cpu < second_cpu < post_fence is the claim.
    assert!(
        *pre_fence < *first_cpu && *first_cpu < *second_cpu && *second_cpu < *post_fence,
        "tm8_positive: ordering violation: fence@{} < cpu@{} < cpu@{} < fence@{}",
        pre_fence,
        first_cpu,
        second_cpu,
        post_fence
    );

    // Between the pre-fence and first cpu_now_ns, no bookkeeping calls
    // (snapshot_alloc_counters, push_measurement, etc.) are allowed.
    // Only the branch check for cpu_time_enabled (cmp + je) is OK.
    assert!(
        !has_between(&lines, *pre_fence, *first_cpu, "snapshot_alloc"),
        "tm8_positive: snapshot_alloc_counters found between fence and cpu_now_ns"
    );
    assert!(
        !has_between(&lines, *pre_fence, *first_cpu, "push_measurement"),
        "tm8_positive: push_measurement found between fence and cpu_now_ns"
    );

    // Negative control: tm8_negative has bookkeeping between fence and cpu_now_ns.
    let asm = extract_asm("tm8_negative");
    let lines = instruction_lines(&asm);

    let fences = find_indices(&lines, FENCE_PATTERN);
    let cpu_calls = find_indices(&lines, CPU_NOW_CALL);

    assert!(!fences.is_empty(), "tm8_negative: must contain a fence");
    assert!(
        !cpu_calls.is_empty(),
        "tm8_negative: must contain cpu_now_ns calls"
    );

    // In the negative control, there MUST be non-trivial instructions
    // (the black_box bookkeeping) between the first fence and the
    // first cpu_now_ns call.
    let first_fence = fences[0];
    let first_cpu = cpu_calls
        .iter()
        .find(|&&c| c > first_fence)
        .expect("tm8_negative: no cpu_now_ns after fence");

    let intervening: Vec<&str> = lines[first_fence + 1..*first_cpu]
        .iter()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with("#APP") && !t.starts_with("#NO_APP")
        })
        .copied()
        .collect();

    assert!(
        !intervening.is_empty(),
        "tm8_negative: expected bookkeeping between fence and cpu_now_ns, \
         but window is tight (tool failed to detect the broken pattern)\n\
         lines[{}..{}]:\n{}",
        first_fence,
        first_cpu,
        lines[first_fence..*first_cpu + 1]
            .iter()
            .enumerate()
            .map(|(i, l)| format!("  [{}] {}", first_fence + i, l))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
