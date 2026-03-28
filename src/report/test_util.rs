/// Visible character count, ignoring ANSI escape sequences.
pub(crate) fn visible_len(s: &str) -> usize {
    let mut len = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if c == '\x1b' {
            in_escape = true;
        } else if in_escape {
            if c == 'm' {
                in_escape = false;
            }
        } else {
            len += 1;
        }
    }
    len
}

/// Assert all content lines in a table have equal visible width.
pub(crate) fn assert_aligned(table: &str, label: &str) {
    let content_lines: Vec<&str> = table
        .lines()
        .filter(|l| !l.is_empty() && !l.contains("hidden"))
        .collect();
    assert!(
        content_lines.len() >= 3,
        "[{label}] need header + separator + data, got {}",
        content_lines.len()
    );
    let expected = visible_len(content_lines[0]);
    for (i, line) in content_lines.iter().enumerate() {
        assert_eq!(
            visible_len(line),
            expected,
            "[{label}] line {i} visible width {} != header width {expected}\n  line: {line:?}",
            visible_len(line)
        );
    }
}
