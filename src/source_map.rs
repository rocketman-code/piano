/// Tracks text injections into a source file for line-number remapping.
///
/// Each injection records the original line it was inserted at and how
/// many newlines it added. `remap_line` subtracts the cumulative newlines
/// preceding the query line to recover the original line number.
///
/// Multi-pass rewrites are flattened at merge time: `merge()` transforms
/// the incoming map's coordinates through this map and appends, keeping
/// a single sorted list. No chain, no recursion.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SourceMap {
    /// Sorted by original_line ascending.
    /// Each entry: (original_line, newlines_added).
    injections: Vec<(u32, u32)>,
}

impl SourceMap {
    pub fn new() -> Self {
        Self {
            injections: Vec::new(),
        }
    }

    /// Record an injection at `original_line` that added `newlines` lines.
    ///
    /// Callers must insert in ascending `original_line` order (which
    /// `StringInjector::apply` guarantees since it processes sorted offsets).
    pub fn record(&mut self, original_line: u32, newlines: u32) {
        debug_assert!(
            self.injections
                .last()
                .is_none_or(|&(prev, _)| original_line >= prev),
            "SourceMap::record called out of order: {original_line} after {:?}",
            self.injections.last(),
        );
        if newlines > 0 {
            self.injections.push((original_line, newlines));
        }
    }

    /// Flatten another pass's SourceMap into this one.
    ///
    /// `other` was computed in this map's output coordinate space (i.e.,
    /// its injection lines account for this map's shifts). We transform
    /// each of `other`'s injections back to original coordinates and
    /// insert them into our list, maintaining sorted order.
    pub fn merge(&mut self, other: SourceMap) {
        for &(pass2_line, newlines) in &other.injections {
            // Transform pass2_line (in our output space) back to original space.
            let original = self.remap_line(pass2_line);
            self.injections.push((original, newlines));
        }
        // Re-sort to maintain the ascending invariant.
        self.injections.sort_by_key(|&(line, _)| line);
    }

    /// Map a rewritten-source line number back to the original line number.
    ///
    /// Lines within an injected region map to the injection point's
    /// original line (best approximation for piano-generated code).
    /// Lines in original code map to the exact original line.
    #[must_use]
    pub fn remap_line(&self, rewritten_line: u32) -> u32 {
        let mut shift = 0u32;
        for &(orig_line, newlines) in &self.injections {
            let rw_pos = orig_line + shift;
            if rewritten_line <= rw_pos {
                // Before this injection
                return rewritten_line.saturating_sub(shift);
            }
            if rewritten_line <= rw_pos + newlines {
                // Within this injection's added lines
                return orig_line;
            }
            shift += newlines;
        }
        // After all injections
        rewritten_line.saturating_sub(shift)
    }
}

/// Collects text injections and replacements at byte offsets, applies them
/// to source text in one pass.
///
/// Usage:
/// 1. Call `insert(offset, text)` and/or `replace(start, end, text)`.
/// 2. Call `apply(source)` to produce the modified source + SourceMap.
#[derive(Default)]
pub struct StringInjector {
    entries: Vec<Entry>,
}

#[derive(Debug)]
enum Entry {
    /// Insert text at byte offset. Original bytes at offset are preserved.
    Insert(usize, String),
    /// Replace source[start..end] with text. Original bytes are skipped.
    Replace(usize, usize, String),
}

impl Entry {
    fn sort_key(&self) -> (usize, u8) {
        // Insert before Replace at the same position
        match self {
            Entry::Insert(offset, _) => (*offset, 0),
            Entry::Replace(start, _, _) => (*start, 1),
        }
    }
}

impl StringInjector {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record an injection at the given byte offset in the original source.
    pub fn insert(&mut self, byte_offset: usize, text: impl Into<String>) {
        self.entries.push(Entry::Insert(byte_offset, text.into()));
    }

    /// Replace source[start..end] with the given text.
    /// The original bytes in [start..end) are skipped in the output.
    pub fn replace(&mut self, start: usize, end: usize, text: impl Into<String>) {
        debug_assert!(end >= start, "replace: end ({end}) < start ({start})");
        self.entries.push(Entry::Replace(start, end, text.into()));
    }

    /// Apply all injections and replacements to `source`, returning the
    /// modified source and a SourceMap for line-number remapping.
    ///
    /// Entries are applied in offset order. Each byte of the original
    /// source is copied at most once.
    pub fn apply(mut self, source: &str) -> (String, SourceMap) {
        self.entries.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

        let mut result = String::with_capacity(source.len() * 2);
        let mut map = SourceMap::new();
        let mut cursor = 0usize;

        for entry in &self.entries {
            let (start, end, text) = match entry {
                Entry::Insert(offset, text) => (*offset, *offset, text.as_str()),
                Entry::Replace(start, end, text) => (*start, *end, text.as_str()),
            };

            debug_assert!(
                start >= cursor && end <= source.len(),
                "StringInjector: range [{start}..{end}) out of bounds \
                 (cursor={cursor}, len={})",
                source.len(),
            );

            // Emit original source up to this entry's start
            result.push_str(&source[cursor..start]);

            // Compute line-number effects
            let deleted_newlines =
                source[start..end].bytes().filter(|&b| b == b'\n').count() as u32;
            let added_newlines = text.bytes().filter(|&b| b == b'\n').count() as u32;
            let net = added_newlines.saturating_sub(deleted_newlines);

            if net > 0 {
                let line_at_start =
                    source[..start].bytes().filter(|&b| b == b'\n').count() as u32 + 1;
                map.record(line_at_start, net);
            }

            // Emit replacement/injection text
            result.push_str(text);

            // Advance cursor past the replaced region (or stay for insert)
            cursor = end;
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
        assert_eq!(map.remap_line(1), 1);
        assert_eq!(map.remap_line(10), 10);
        assert_eq!(map.remap_line(100), 100);
    }

    #[test]
    fn single_injection_shifts_subsequent_lines() {
        let mut map = SourceMap::new();
        map.record(1, 2);
        assert_eq!(map.remap_line(1), 1); // before injection
        assert_eq!(map.remap_line(2), 1); // injected region
        assert_eq!(map.remap_line(3), 1); // injected region
        assert_eq!(map.remap_line(4), 2); // first real line after
        assert_eq!(map.remap_line(5), 3);
    }

    #[test]
    fn two_injections_at_different_lines() {
        let mut map = SourceMap::new();
        map.record(5, 1);
        map.record(15, 1);
        assert_eq!(map.remap_line(4), 4);
        assert_eq!(map.remap_line(5), 5);
        assert_eq!(map.remap_line(6), 5); // injected
        assert_eq!(map.remap_line(7), 6);
        assert_eq!(map.remap_line(16), 15);
        assert_eq!(map.remap_line(17), 15); // injected
        assert_eq!(map.remap_line(18), 16);
    }

    #[test]
    fn three_line_injection() {
        let mut map = SourceMap::new();
        map.record(10, 3);
        assert_eq!(map.remap_line(10), 10);
        assert_eq!(map.remap_line(11), 10); // injected
        assert_eq!(map.remap_line(12), 10); // injected
        assert_eq!(map.remap_line(13), 10); // injected
        assert_eq!(map.remap_line(14), 11);
    }

    #[test]
    fn string_injector_single_injection() {
        let source = "fn main() {\n    println!(\"hello\");\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(11, "\n    let _guard = enter();");
        let (result, map) = inj.apply(source);

        assert!(result.contains("let _guard = enter();"));
        assert_eq!(map.remap_line(1), 1);
        assert_eq!(map.remap_line(2), 1); // injected
        assert_eq!(map.remap_line(3), 2);
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
        assert_eq!(map.remap_line(3), 2);
        assert_eq!(map.remap_line(5), 4);
        assert_eq!(map.remap_line(7), 5);
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
    fn flat_merge_equivalent_to_chain() {
        let mut map = SourceMap::new();
        map.record(5, 1);

        let mut pass2 = SourceMap::new();
        pass2.record(3, 2);

        map.merge(pass2);

        assert_eq!(map.remap_line(1), 1);
        assert_eq!(map.remap_line(2), 2);
        assert_eq!(map.remap_line(3), 3);
        assert_eq!(map.remap_line(6), 4);
        assert_eq!(map.remap_line(7), 5);
        assert_eq!(map.remap_line(9), 6);
    }

    #[test]
    fn merge_four_passes() {
        // Simulates entry-point: guard + registrations + allocator + shutdown
        let mut map = SourceMap::new();
        map.record(5, 1); // guard

        let mut p2 = SourceMap::new();
        p2.record(1, 3); // registrations at top
        map.merge(p2);

        let mut p3 = SourceMap::new();
        p3.record(1, 4); // allocator at top
        map.merge(p3);

        let mut p4 = SourceMap::new();
        p4.record(1, 6); // shutdown at top
        map.merge(p4);

        // Line 1 is deep in injected territory
        // A line way past all injections should map back correctly
        let shift = 1 + 3 + 4 + 6; // 14 total injected lines
        assert_eq!(map.remap_line(20 + shift), 20);
    }

    #[test]
    fn allocator_style_injection() {
        let source = "fn main() {\n    x();\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(0, "\n#[alloc]\nstatic A: T\n    = T::new();\n");
        let (result, map) = inj.apply(source);

        assert!(result.contains("fn main()"));
        // 4 newlines injected before line 1; fn main is now line 5
        assert_eq!(map.remap_line(5), 1);
        assert_eq!(map.remap_line(6), 2);
        assert_eq!(map.remap_line(7), 3);
    }

    // === replace() tests ===

    #[test]
    fn replace_removes_original_text() {
        let source = "aaa\nbbb\nccc\n";
        let mut inj = StringInjector::new();
        // Replace "bbb\n" (bytes 4..8) with "XXX\n"
        inj.replace(4, 8, "XXX\n");
        let (result, _) = inj.apply(source);
        assert_eq!(result, "aaa\nXXX\nccc\n");
    }

    #[test]
    fn replace_with_more_lines() {
        // Replace 1-line region with 3-line replacement (net +2)
        let source = "line1\nline2\nline3\n";
        let mut inj = StringInjector::new();
        // Replace "line2\n" (bytes 6..12) with "new_a\nnew_b\nnew_c\n"
        inj.replace(6, 12, "new_a\nnew_b\nnew_c\n");
        let (result, map) = inj.apply(source);

        assert_eq!(result, "line1\nnew_a\nnew_b\nnew_c\nline3\n");
        // line1 is line 1, unchanged
        assert_eq!(map.remap_line(1), 1);
        // new_a/new_b/new_c are lines 2-4 (injected, map to line 2)
        assert_eq!(map.remap_line(2), 2);
        assert_eq!(map.remap_line(3), 2);
        assert_eq!(map.remap_line(4), 2);
        // line3 is now line 5, was originally line 3
        assert_eq!(map.remap_line(5), 3);
    }

    #[test]
    fn replace_allocator_wrapping() {
        // Simulates wrapping an existing #[global_allocator] static
        let source = "#[global_allocator]\nstatic ALLOC: MyAlloc = MyAlloc;\n\nfn main() {}\n";
        let mut inj = StringInjector::new();
        // Replace the entire allocator item (bytes 0..51)
        let item_end = source.find(";\n").unwrap() + 2;
        inj.replace(0, item_end, "#[global_allocator]\nstatic ALLOC: PianoAllocator<MyAlloc>\n    = PianoAllocator::new(MyAlloc);\n");
        let (result, map) = inj.apply(source);

        assert!(result.contains("PianoAllocator<MyAlloc>"));
        assert!(result.contains("fn main()"));
        // fn main is on line 5 in output (3-line replacement + blank line)
        // Original fn main was on line 4
        assert_eq!(map.remap_line(5), 4);
    }

    #[test]
    fn insert_and_replace_in_same_pass() {
        // Guard insertion + allocator replacement in one pass
        let source = "#[global_allocator]\nstatic A: M = M;\n\nfn work() {\n    1\n}\n";
        let mut inj = StringInjector::new();
        // Replace allocator (bytes 0..37)
        let alloc_end = source.find(";\n").unwrap() + 2;
        inj.replace(
            0,
            alloc_end,
            "#[global_allocator]\nstatic A: P<M> = P::new(M);\n",
        );
        // Insert guard after work()'s opening brace
        let brace = source.find("{\n    1").unwrap();
        inj.insert(brace + 1, "\nlet _g = enter(0);");
        let (result, _) = inj.apply(source);

        assert!(result.contains("P<M>"));
        assert!(result.contains("let _g = enter(0);"));
        assert!(result.contains("    1\n"));
    }
}
