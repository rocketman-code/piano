/// Gate test: enforces spec constraints on production source code
/// and test isolation rules.
///
/// This test is the authority. If it fails, the code is wrong -- not
/// this test. Do not add exceptions. Fix the code.
///
/// Source code (src/) forbidden patterns:
///   - pub fn that contains "reset" or "clear" (test-only mutation)
///   - comments referencing renamed functions (stale references)
///   - #[doc(hidden)] pub fn (backdoor exports for test convenience)
///   - #[cfg(test)] blocks in production source (test-only code paths)
///   - unsafe block/impl without a preceding // SAFETY: comment
///
/// Test code (tests/) forbidden patterns:
///   - drain_all_buffers usage outside buffer.rs (global mutation race)
const RENAMED_FNS: &[(&str, &str)] = &[
    ("reset()", "discard()"),
    ("reset_all()", "discard_all()"),
    ("clear_runs_dir()", "unset_runs_dir()"),
];

#[test]
fn no_test_backdoors_in_source() {
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let test_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut violations = Vec::new();

    // --- Source code checks ---
    visit_rs_files(&src_dir, &mut |path, contents| {
        for (line_num, line) in contents.lines().enumerate() {
            let n = line_num + 1;
            let trimmed = line.trim();
            let rel = path.strip_prefix(&src_dir).unwrap_or(path);

            // No pub fn with "reset" or "clear"
            if trimmed.starts_with("pub fn ") {
                let fn_name = trimmed
                    .strip_prefix("pub fn ")
                    .unwrap()
                    .split('(')
                    .next()
                    .unwrap_or("");
                if fn_name.contains("reset") || fn_name.contains("clear") {
                    violations.push(format!(
                        "{}:{}: test-only mutation function '{}' -- \
                         use delta-based tests instead",
                        rel.display(), n, fn_name,
                    ));
                }
            }

            // No stale function references in comments
            if trimmed.starts_with("//") {
                for &(old, new) in RENAMED_FNS {
                    if trimmed.contains(old) {
                        violations.push(format!(
                            "{}:{}: stale function reference '{}' in comment \
                             -- was renamed to '{}'",
                            rel.display(), n, old, new,
                        ));
                    }
                }
            }

            // No #[doc(hidden)] pub fn
            if trimmed == "#[doc(hidden)]" {
                let remaining: Vec<&str> = contents
                    .lines()
                    .skip(line_num + 1)
                    .filter(|l| {
                        let t = l.trim();
                        !t.is_empty() && !t.starts_with("#[")
                    })
                    .take(1)
                    .collect();
                if let Some(next) = remaining.first() {
                    if next.trim().starts_with("pub fn ") {
                        violations.push(format!(
                            "{}:{}: #[doc(hidden)] pub fn -- no backdoor \
                             exports for test convenience",
                            rel.display(), n,
                        ));
                    }
                }
            }

            // No #[cfg(test)] in production source
            if trimmed.starts_with("#[cfg(test)]") {
                violations.push(format!(
                    "{}:{}: #[cfg(test)] in production source -- tests \
                     belong in tests/, not in src/",
                    rel.display(), n,
                ));
            }

            // Every unsafe block/impl must have a // SAFETY: comment
            if (trimmed.starts_with("unsafe {")
                || trimmed.starts_with("unsafe impl")
                || trimmed.contains("unsafe {"))
                && !trimmed.contains("allow")
            {
                let start = line_num.saturating_sub(5);
                let preceding: Vec<&str> = contents
                    .lines()
                    .skip(start)
                    .take(line_num - start)
                    .collect();
                let has_safety = preceding.iter().any(|l| {
                    let t = l.trim().to_uppercase();
                    t.contains("SAFETY:")
                });
                if !has_safety {
                    violations.push(format!(
                        "{}:{}: unsafe block without // SAFETY: comment -- \
                         document why the invariants are upheld",
                        rel.display(), n,
                    ));
                }
            }
        }
    });

    // --- Test code checks ---
    visit_rs_files(&test_dir, &mut |path, contents| {
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        // buffer_global.rs tests drain_all_buffers itself; this file references it
        if filename == "buffer_global.rs" || filename == "no_test_backdoors.rs" {
            return;
        }
        for (line_num, line) in contents.lines().enumerate() {
            let n = line_num + 1;
            let trimmed = line.trim();
            if trimmed.contains("drain_all_buffers") && !trimmed.starts_with("//") {
                violations.push(format!(
                    "tests/{filename}:{n}: drain_all_buffers is a global mutation -- \
                     use drain_thread_buffer for test isolation",
                ));
            }
        }
    });

    if !violations.is_empty() {
        let msg = violations.join("\n  ");
        panic!(
            "\n\nSpec violation: forbidden patterns found.\n\n\
             Found {} violation(s):\n  {}\n\n\
             Fix the code, not this test. See spec constraints in cbc-spec.md.\n",
            violations.len(),
            msg,
        );
    }
}

fn visit_rs_files(
    dir: &std::path::Path,
    cb: &mut dyn FnMut(&std::path::Path, &str),
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            visit_rs_files(&path, cb);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                cb(&path, &contents);
            }
        }
    }
}
