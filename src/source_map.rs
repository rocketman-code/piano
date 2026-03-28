/// Collects text injections and replacements at byte offsets, applies them
/// to source text in one pass.
///
/// Usage:
/// 1. Call `insert(offset, text)` and/or `replace(start, end, text)`.
/// 2. Call `apply(source)` to produce the modified source.
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
    /// modified source.
    ///
    /// Entries are applied in offset order. Each byte of the original
    /// source is copied at most once.
    pub fn apply(mut self, source: &str) -> String {
        self.entries.sort_by_key(|a| a.sort_key());

        let mut result = String::with_capacity(source.len() * 2);
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

            // Emit replacement/injection text
            result.push_str(text);

            // Advance cursor past the replaced region (or stay for insert)
            cursor = end;
        }

        result.push_str(&source[cursor..]);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_injector_single_injection() {
        let source = "fn main() {\n    println!(\"hello\");\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(11, " let _guard = enter();");
        let result = inj.apply(source);

        assert!(result.contains("let _guard = enter();"));
    }

    #[test]
    fn string_injector_multiple_injections() {
        let source = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(8, " guard_a;");
        inj.insert(25, " guard_b;");
        let result = inj.apply(source);

        assert!(result.contains("guard_a"));
        assert!(result.contains("guard_b"));
    }

    #[test]
    fn string_injector_preserves_formatting() {
        let source = "fn weird(  ) {\n  x  +  y\n}\n";
        let mut inj = StringInjector::new();
        inj.insert(14, " let _g = 1;");
        let result = inj.apply(source);

        assert!(result.starts_with("fn weird(  ) {"));
        assert!(result.contains("  x  +  y\n"));
    }

    // === replace() tests ===

    #[test]
    fn replace_removes_original_text() {
        let source = "aaa\nbbb\nccc\n";
        let mut inj = StringInjector::new();
        inj.replace(4, 8, "XXX\n");
        let result = inj.apply(source);
        assert_eq!(result, "aaa\nXXX\nccc\n");
    }

    #[test]
    fn replace_allocator_wrapping() {
        let source = "#[global_allocator]\nstatic ALLOC: MyAlloc = MyAlloc;\n\nfn main() {}\n";
        let mut inj = StringInjector::new();
        let item_end = source.find(";\n").unwrap() + 2;
        inj.replace(0, item_end, "#[global_allocator]\nstatic ALLOC: PianoAllocator<MyAlloc>\n    = PianoAllocator::new(MyAlloc);\n");
        let result = inj.apply(source);

        assert!(result.contains("PianoAllocator<MyAlloc>"));
        assert!(result.contains("fn main()"));
    }

    #[test]
    fn insert_and_replace_in_same_pass() {
        let source = "#[global_allocator]\nstatic A: M = M;\n\nfn work() {\n    1\n}\n";
        let mut inj = StringInjector::new();
        let alloc_end = source.find(";\n").unwrap() + 2;
        inj.replace(
            0,
            alloc_end,
            "#[global_allocator]\nstatic A: P<M> = P::new(M);\n",
        );
        let brace = source.find("{\n    1").unwrap();
        inj.insert(brace + 1, " let _g = enter(0);");
        let result = inj.apply(source);

        assert!(result.contains("P<M>"));
        assert!(result.contains("let _g = enter(0);"));
    }
}
