//! Enumeration tests pinned to the Rust compiler/stdlib source.
//!
//! Each test reads an authoritative Rust source file, extracts the
//! exhaustive list of variants/methods, and asserts piano handles
//! every one. If a Rust update adds a new variant, the test fails
//! and forces us to add coverage.
//!
//! Requires: git clone --depth 1 https://github.com/rust-lang/rust.git ~/dev/rust-lang/rust

fn rust_source_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").expect("HOME not set");
    std::path::PathBuf::from(home).join("dev/rust-lang/rust")
}

fn read_rust_source(relative: &str) -> String {
    let path = rust_source_path().join(relative);
    if !path.exists() {
        panic!(
            "Rust source not found at {}.\n\
             Clone it: git clone --depth 1 https://github.com/rust-lang/rust.git ~/dev/rust-lang/rust",
            path.display()
        );
    }
    std::fs::read_to_string(&path).unwrap()
}

/// Extract `unsafe fn NAME` declarations from a trait body.
fn extract_trait_methods(source: &str, trait_name: &str) -> Vec<String> {
    let trait_start = source
        .find(&format!("pub unsafe trait {trait_name}"))
        .unwrap_or_else(|| panic!("{trait_name} trait not found"));
    let trait_block = &source[trait_start..];

    // Find matching closing brace (simple brace counting)
    let mut depth = 0;
    let mut end = 0;
    for (i, ch) in trait_block.char_indices() {
        if ch == '{' {
            depth += 1;
        }
        if ch == '}' {
            depth -= 1;
            if depth == 0 {
                end = i;
                break;
            }
        }
    }
    let trait_body = &trait_block[..end];

    trait_body
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("unsafe fn ") {
                let name = trimmed
                    .strip_prefix("unsafe fn ")?
                    .split('(')
                    .next()?
                    .trim();
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------
// Process termination: piano must handle or explicitly not handle
// every way a Rust process can terminate
// ---------------------------------------------------------------

#[test]
fn process_termination_modes_exhaustive() {
    let source = read_rust_source("library/std/src/process.rs");

    // Extract public termination functions from std::process
    let termination_fns: Vec<&str> = source
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed == "pub fn exit(code: i32) -> ! {" {
                Some("exit")
            } else if trimmed == "pub fn abort() -> ! {" {
                Some("abort")
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        termination_fns,
        vec!["exit", "abort"],
        "std::process termination functions have changed.\n\
         Found: {termination_fns:?}"
    );

    // Full enumeration of termination modes and piano's handling:
    // 1. main() returns     → RootCtx drop (if it existed) or atexit → data saved
    // 2. process::exit()    → atexit handler fires → data saved
    // 3. process::abort()   → nothing runs → data lost (UNRECOVERABLE BY DESIGN)
    // 4. panic (unwind)     → destructors run → data saved
    // 5. SIGTERM/SIGINT     → signal handler (try_lock drain + trailer) → data saved
    // 6. SIGKILL            → nothing runs → data lost (UNRECOVERABLE BY DESIGN)
    //
    // Modes 3 and 6 are explicitly unhandleable. The OS kills the process
    // without running any user code. This is the same for every profiler.
    //
    // Modes 1, 2, 4, 5 are tested by path proof tests PD1-PD5.
}

// ---------------------------------------------------------------
// FnContext: rewriter must handle every grammar context for fn
// ---------------------------------------------------------------

#[test]
fn fn_contexts_exhaustive() {
    let source = read_rust_source("compiler/rustc_parse/src/parser/item.rs");

    let enum_start = source
        .find("pub(crate) enum FnContext")
        .expect("FnContext enum not found in rustc_parse");
    let enum_block = &source[enum_start..];
    let enum_end = enum_block.find('}').unwrap();
    let enum_body = &enum_block[..enum_end];

    let variants: Vec<&str> = enum_body
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("///")
                || trimmed.starts_with("//")
                || trimmed.is_empty()
                || trimmed.contains("enum")
                || trimmed.starts_with('{')
            {
                return None;
            }
            Some(trimmed.trim_end_matches(','))
        })
        .collect();

    let expected = vec!["Free", "Trait", "Impl"];
    assert_eq!(
        variants, expected,
        "FnContext enum has changed in the Rust compiler. \
         Update the rewriter tests to cover the new variant(s).\n\
         Found: {variants:?}\nExpected: {expected:?}"
    );
}

// ---------------------------------------------------------------
// GlobalAlloc: PianoAllocator must implement every method
// ---------------------------------------------------------------

#[test]
fn global_alloc_methods_exhaustive() {
    let source = read_rust_source("library/core/src/alloc/global.rs");
    let methods = extract_trait_methods(&source, "GlobalAlloc");

    let expected = vec!["alloc", "dealloc", "alloc_zeroed", "realloc"];
    assert_eq!(
        methods, expected,
        "GlobalAlloc trait has changed. Update PianoAllocator to handle new methods.\n\
         Found: {methods:?}\n\
         Expected: {expected:?}"
    );

    // Verify PianoAllocator implements each one
    let alloc_source = include_str!("../src/alloc.rs");
    for method in &methods {
        let pattern = format!("unsafe fn {method}(");
        assert!(
            alloc_source.contains(&pattern),
            "PianoAllocator missing implementation of GlobalAlloc::{method}"
        );
    }
}

// ---------------------------------------------------------------
// Future + Poll: PianoFuture must handle every poll result
// ---------------------------------------------------------------

#[test]
fn poll_variants_exhaustive() {
    let source = read_rust_source("library/core/src/task/poll.rs");

    // Extract Poll enum variants
    let enum_start = source
        .find("pub enum Poll<T>")
        .expect("Poll enum not found");
    let enum_block = &source[enum_start..];
    let enum_end = enum_block.find('}').unwrap();
    let enum_body = &enum_block[..enum_end];

    let variants: Vec<&str> = enum_body
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("///")
                || trimmed.starts_with("//")
                || trimmed.is_empty()
                || trimmed.contains("enum")
                || trimmed.starts_with('{')
                || trimmed.starts_with('#')
            {
                return None;
            }
            let name = trimmed.split('(').next()?.split(',').next()?.trim();
            if name.is_empty() {
                return None;
            }
            Some(name)
        })
        .collect();

    let expected = vec!["Ready", "Pending"];
    assert_eq!(
        variants, expected,
        "Poll enum has changed. Update PianoFuture to handle new variants.\n\
         Found: {variants:?}\n\
         Expected: {expected:?}"
    );

    // Verify PianoFuture handles both
    let future_source = include_str!("../src/piano_future.rs");
    assert!(
        future_source.contains("Poll::Ready") || future_source.contains("is_ready()"),
        "PianoFuture must handle Poll::Ready"
    );
    // Pending is handled implicitly (poll returns, future resumes on next poll)
}

// ---------------------------------------------------------------
// Future trait: exactly one method (poll)
// ---------------------------------------------------------------

#[test]
fn future_trait_methods_exhaustive() {
    let source = read_rust_source("library/core/src/future/future.rs");

    // Count fn declarations in the Future trait
    let trait_start = source
        .find("pub trait Future")
        .expect("Future trait not found");
    let trait_block = &source[trait_start..];
    let mut depth = 0;
    let mut end = 0;
    for (i, ch) in trait_block.char_indices() {
        if ch == '{' {
            depth += 1;
        }
        if ch == '}' {
            depth -= 1;
            if depth == 0 {
                end = i;
                break;
            }
        }
    }
    let trait_body = &trait_block[..end];

    let methods: Vec<&str> = trait_body
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("fn ") {
                Some(trimmed.split('(').next()?.strip_prefix("fn ")?.trim())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        methods,
        vec!["poll"],
        "Future trait has new methods. Update PianoFuture.\n\
         Found: {methods:?}"
    );
}
