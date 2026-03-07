/// Tracks text injections into a source file for line-number remapping.
///
/// Each injection records its byte offset in the original source and the
/// number of newlines in the injected text. Given a line number in the
/// rewritten source, `remap_line` subtracts the injected newlines that
/// precede it to recover the original line number.
#[derive(Default)]
pub struct SourceMap {
    /// Sorted by `original_line` ascending.
    /// Each entry: (original_line, offset, none_span).
    /// - `offset`: total newlines injected (used for line shifting).
    /// - `none_span`: how many lines return None (injected content).
    ///   When injected text ends with `\n`, none_span = offset - 1
    ///   because the trailing newline terminates the last injected line
    ///   and the next line is original code.
    injections: Vec<(u32, u32, u32)>,
    /// Chained maps from subsequent rewrite passes. Each map's injection
    /// lines are in the output coordinate space of the previous map.
    /// Remapping walks the chain in reverse (last applied → first applied).
    chain: Vec<SourceMap>,
}

impl SourceMap {
    pub fn new() -> Self {
        Self {
            injections: Vec::new(),
            chain: Vec::new(),
        }
    }

    /// Record an injection at `original_line`.
    ///
    /// - `offset`: number of newlines in the injected text (line shift).
    /// - `none_span`: number of lines that should return None (injected
    ///   content lines). Equals `offset` for texts not ending with `\n`,
    ///   or `offset - 1` for texts ending with `\n`.
    ///
    /// Callers must insert in ascending `original_line` order (which
    /// `StringInjector::apply` guarantees since it processes sorted offsets).
    pub fn record(&mut self, original_line: u32, offset: u32, none_span: u32) {
        debug_assert!(
            none_span <= offset,
            "none_span ({none_span}) must not exceed offset ({offset})",
        );
        debug_assert!(
            self.injections
                .last()
                .is_none_or(|&(prev, _, _)| original_line >= prev),
            "SourceMap::record called out of order: {original_line} after {:?}",
            self.injections.last(),
        );
        self.injections.push((original_line, offset, none_span));
    }

    /// Total lines added by all injections at or before `rewritten_line`.
    fn cumulative_offset(&self, rewritten_line: u32) -> u32 {
        let mut shift = 0u32;
        for &(orig_line, offset, _) in &self.injections {
            let rewritten_injection_line = orig_line + shift;
            if rewritten_injection_line >= rewritten_line {
                break;
            }
            shift += offset;
        }
        shift
    }

    /// Chain a subsequent rewrite pass's SourceMap after this one.
    ///
    /// `other` was computed relative to the output of `self` (i.e., its
    /// injection lines are in self's output coordinate space). To remap a
    /// final-output line back to the original, we first undo `other`'s
    /// injections (getting a line in self's output space), then undo
    /// `self`'s injections (getting the original line).
    pub fn merge(&mut self, other: SourceMap) {
        if other.injections.is_empty() {
            return;
        }
        // Store chained maps for multi-step remapping.
        self.chain.push(other);
    }

    /// Map a rewritten-source line number back to the original line number.
    /// Returns None if the line falls within injected text (not user code).
    ///
    /// When chained maps exist (from `merge`), walks the chain in reverse:
    /// first undoes the last pass's injections, then the previous pass's, etc.
    #[must_use]
    pub fn remap_line(&self, rewritten_line: u32) -> Option<u32> {
        // Walk chain in reverse: last-applied map first.
        let mut line = rewritten_line;
        for chained in self.chain.iter().rev() {
            line = chained.remap_line_single(line)?;
        }
        self.remap_line_single(line)
    }

    /// Remap using only this map's injections (no chain traversal).
    fn remap_line_single(&self, rewritten_line: u32) -> Option<u32> {
        let shift = self.cumulative_offset(rewritten_line);
        if shift == 0 {
            return Some(rewritten_line);
        }
        let original = rewritten_line.checked_sub(shift)?;
        let mut running_offset = 0u32;
        for &(orig_line, offset, none_span) in &self.injections {
            let rw_start = orig_line + running_offset;
            if rewritten_line > rw_start && rewritten_line <= rw_start + none_span {
                return None;
            }
            running_offset += offset;
        }
        Some(original)
    }
}

/// Collects text injections at byte offsets and applies them to source text.
///
/// Usage:
/// 1. Call `insert(byte_offset, text)` for each injection point (any order).
/// 2. Call `apply(source)` to produce the modified source + SourceMap.
#[derive(Default)]
pub struct StringInjector {
    entries: Vec<(usize, String)>,
}

impl StringInjector {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record an injection at the given byte offset in the original source.
    pub fn insert(&mut self, byte_offset: usize, text: impl Into<String>) {
        self.entries.push((byte_offset, text.into()));
    }

    /// Apply all injections to `source`, returning the modified source and
    /// a SourceMap for line-number remapping.
    ///
    /// Injections are applied in offset order. Each byte of the original
    /// source is copied exactly once (slice-copy pattern).
    pub fn apply(mut self, source: &str) -> (String, SourceMap) {
        self.entries.sort_by_key(|&(offset, _)| offset);

        let total_inject: usize = self.entries.iter().map(|(_, t)| t.len()).sum();
        let mut result = String::with_capacity(source.len() + total_inject);
        let mut map = SourceMap::new();
        let mut cursor = 0usize;

        for (offset, text) in &self.entries {
            debug_assert!(
                *offset >= cursor && *offset <= source.len(),
                "StringInjector: offset {offset} out of range (cursor={cursor}, len={})",
                source.len(),
            );
            result.push_str(&source[cursor..*offset]);
            let newline_count = text.bytes().filter(|&b| b == b'\n').count() as u32;
            if newline_count > 0 {
                let line_at_offset =
                    source[..*offset].bytes().filter(|&b| b == b'\n').count() as u32 + 1;
                let none_span = if text.ends_with('\n') {
                    newline_count - 1
                } else {
                    newline_count
                };
                map.record(line_at_offset, newline_count, none_span);
            }
            result.push_str(text);
            cursor = *offset;
        }
        result.push_str(&source[cursor..]);

        (result, map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_injections_identity() {
        let map = SourceMap::new();
        assert_eq!(map.remap_line(1), Some(1));
        assert_eq!(map.remap_line(10), Some(10));
        assert_eq!(map.remap_line(100), Some(100));
    }

    #[test]
    fn single_injection_at_top() {
        let mut map = SourceMap::new();
        map.record(1, 2, 2);
        assert_eq!(map.remap_line(1), Some(1));
        assert_eq!(map.remap_line(2), None);
        assert_eq!(map.remap_line(3), None);
        assert_eq!(map.remap_line(4), Some(2));
        assert_eq!(map.remap_line(5), Some(3));
    }

    #[test]
    fn two_injections_at_different_lines() {
        let mut map = SourceMap::new();
        map.record(5, 1, 1);
        map.record(15, 1, 1);
        assert_eq!(map.remap_line(4), Some(4));
        assert_eq!(map.remap_line(5), Some(5));
        assert_eq!(map.remap_line(6), None);
        assert_eq!(map.remap_line(7), Some(6));
        assert_eq!(map.remap_line(16), Some(15));
        assert_eq!(map.remap_line(17), None);
        assert_eq!(map.remap_line(18), Some(16));
    }

    #[test]
    fn three_line_injection_for_concurrent() {
        let mut map = SourceMap::new();
        map.record(10, 3, 3);
        assert_eq!(map.remap_line(10), Some(10));
        assert_eq!(map.remap_line(11), None);
        assert_eq!(map.remap_line(12), None);
        assert_eq!(map.remap_line(13), None);
        assert_eq!(map.remap_line(14), Some(11));
    }

    #[test]
    fn string_injector_single_injection() {
        let source = "fn main() {\n    println!(\"hello\");\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(11, "\n    let _guard = enter();");
        let (result, map) = inj.apply(source);

        assert!(result.contains("let _guard = enter();"));
        assert_eq!(map.remap_line(1), Some(1));
        assert_eq!(map.remap_line(2), None);
        assert_eq!(map.remap_line(3), Some(2));
    }

    #[test]
    fn string_injector_multiple_injections() {
        let source = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(8, "\n    guard_a;");
        inj.insert(25, "\n    guard_b;");
        let (result, map) = inj.apply(source);

        assert!(result.contains("guard_a"));
        assert!(result.contains("guard_b"));
        assert_eq!(map.remap_line(3), Some(2));
        assert_eq!(map.remap_line(5), Some(4));
        assert_eq!(map.remap_line(7), Some(5));
    }

    #[test]
    fn string_injector_preserves_formatting() {
        let source = "fn weird(  ) {\n  x  +  y\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(14, "\n  let _g = 1;");
        let (result, _) = inj.apply(source);

        assert!(result.starts_with("fn weird(  ) {\n"));
        assert!(result.contains("  x  +  y\n"));
    }

    #[test]
    fn merge_chains_sequential_maps() {
        // Step 1: inject 1 line at original line 5
        let mut a = SourceMap::new();
        a.record(5, 1, 1);
        // In a's output: line 5 = orig, line 6 = injected, line 7 = orig 6

        // Step 2 (applied to a's output): inject 2 lines at line 3
        let mut b = SourceMap::new();
        b.record(3, 2, 2);

        a.merge(b);
        // Final output: lines 1-2 original, line 3 original, lines 4-5 injected (b),
        // lines 6 = orig 4, line 7 = orig 5, line 8 = injected (a), line 9 = orig 6
        assert_eq!(a.remap_line(2), Some(2));
        assert_eq!(a.remap_line(3), Some(3));
        assert_eq!(a.remap_line(4), None); // b's injection
        assert_eq!(a.remap_line(5), None); // b's injection
        assert_eq!(a.remap_line(6), Some(4));
        assert_eq!(a.remap_line(7), Some(5));
        assert_eq!(a.remap_line(8), None); // a's injection
        assert_eq!(a.remap_line(9), Some(6));
    }

    #[test]
    fn trailing_newline_does_not_swallow_next_line() {
        // Injection text ending with \n: the line AFTER the injection is original
        // code, not injected. It should return Some, not None.
        let source = "fn main() {\n    body();\n}\n";
        let mut inj = StringInjector::new();
        // Inject 2-newline text ending with \n at byte 0 (before fn main)
        inj.insert(0, "\n// injected\n");
        let (result, map) = inj.apply(source);

        // Result:
        //   line 1: "" (empty, before first \n of injected text)
        //   line 2: "// injected" (injected content)
        //   line 3: "fn main() {" (ORIGINAL line 1)
        //   line 4: "    body();" (original line 2)
        //   line 5: "}" (original line 3)
        assert!(result.starts_with("\n// injected\nfn main()"));

        // Line 3 is original content — must map to Some(1), not None.
        assert_eq!(
            map.remap_line(3),
            Some(1),
            "line after trailing \\n should be original"
        );
        // Lines 1-2 are injected range
        assert_eq!(map.remap_line(2), None, "injected line should be None");
        // Line 4 is original line 2
        assert_eq!(map.remap_line(4), Some(2));
    }

    #[test]
    fn trailing_newline_allocator_style() {
        // Simulates the global allocator injection: 4 newlines, ending with \n.
        let source = "fn main() {\n    x();\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(0, "\n#[alloc]\nstatic A: T\n    = T::new();\n");
        let (result, map) = inj.apply(source);

        // Result:
        //   1: ""
        //   2: "#[alloc]"
        //   3: "static A: T"
        //   4: "    = T::new();"
        //   5: "fn main() {"    <-- ORIGINAL line 1
        //   6: "    x();"        <-- original line 2
        //   7: "}"               <-- original line 3
        assert!(result.contains("fn main()"));

        assert_eq!(
            map.remap_line(5),
            Some(1),
            "fn main should map to original line 1"
        );
        assert_eq!(map.remap_line(6), Some(2));
        assert_eq!(map.remap_line(7), Some(3));
        // Injected lines
        assert_eq!(map.remap_line(2), None);
        assert_eq!(map.remap_line(3), None);
        assert_eq!(map.remap_line(4), None);
    }

    #[test]
    fn non_trailing_newline_preserves_none_range() {
        // Injection NOT ending with \n: the last injected content shares
        // a line with original code. That line should be None.
        let source = "fn main() {\n    println!(\"hello\");\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(11, "\n    let _guard = enter();");
        let (result, map) = inj.apply(source);

        assert!(result.contains("let _guard = enter();"));
        // Line 2 has injected content — should be None
        assert_eq!(map.remap_line(2), None);
        // Line 1 is original
        assert_eq!(map.remap_line(1), Some(1));
        // Line 3 is original line 2
        assert_eq!(map.remap_line(3), Some(2));
    }

    #[test]
    fn full_pipeline_remaps_main_signature() {
        use crate::rewrite::{
            AllocatorKind, inject_global_allocator, inject_registrations, inject_shutdown,
            instrument_source,
        };
        let source = "fn main() {\n    let result = work();\n    println!(\"result: {result}\");\n}\n\nfn work() -> u64 {\n    let mut sum: u64 = 0;\n    let bad: i32 = \"hello\";\n    sum\n}\n";
        let targets: std::collections::HashSet<String> = ["work".to_string()].into_iter().collect();

        let result = instrument_source(source, &targets, false).unwrap();
        let mut map = result.source_map;
        let mut current = result.source;

        let (s, m) = inject_registrations(&current, &["work".to_string()]).unwrap();
        map.merge(m);
        current = s;
        let (s, m) = inject_global_allocator(&current, AllocatorKind::Absent).unwrap();
        map.merge(m);
        current = s;
        let (s, m) = inject_shutdown(&current, None).unwrap();
        map.merge(m);

        // fn main() should remap to original line 1
        let main_line = s
            .lines()
            .enumerate()
            .find(|(_, l)| l.contains("fn main()"))
            .map(|(i, _)| (i + 1) as u32)
            .unwrap();
        assert_eq!(
            map.remap_line(main_line),
            Some(1),
            "fn main() at rewritten line {main_line} should map to original line 1"
        );

        // Body lines should still be correct
        let bad_line = s
            .lines()
            .enumerate()
            .find(|(_, l)| l.contains("bad"))
            .map(|(i, _)| (i + 1) as u32)
            .unwrap();
        assert_eq!(map.remap_line(bad_line), Some(8));
    }

    #[test]
    fn merge_full_pipeline_remaps_correctly() {
        use crate::rewrite::{
            AllocatorKind, inject_global_allocator, inject_registrations, inject_shutdown,
            instrument_source,
        };
        let source = "fn main() {\n    let result = work();\n    println!(\"result: {result}\");\n}\n\nfn work() -> u64 {\n    let mut sum: u64 = 0;\n    let bad: i32 = \"hello\";\n    sum\n}\n";
        let targets: std::collections::HashSet<String> = ["work".to_string()].into_iter().collect();

        let result = instrument_source(source, &targets, false, "").unwrap();
        let mut map = result.source_map;
        let mut current = result.source;

        let (s, m) = inject_registrations(&current, &["work".to_string()]).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_global_allocator(&current, AllocatorKind::Absent).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_shutdown(&current, None).unwrap();
        map.merge(m);

        // "let bad: i32" is on original line 8.
        // Find it in the final source and verify remapping.
        let bad_line = s
            .lines()
            .enumerate()
            .find(|(_, l)| l.contains("bad"))
            .map(|(i, _)| (i + 1) as u32)
            .unwrap();
        assert_eq!(map.remap_line(bad_line), Some(8));
    }
}
