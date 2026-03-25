//! Property-based tests for the rewriter.
//!
//! Uses proptest to generate diverse Rust function signatures with randomized
//! decorators, then verifies the rewriter produces valid instrumented output.

use std::collections::HashMap;

use piano::rewrite::instrument_source;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Valid Rust identifier (lowercase, starts with letter).
fn ident() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{1,15}".prop_filter("must not be a keyword", |s| {
        !matches!(
            s.as_str(),
            "fn" | "let"
                | "mut"
                | "pub"
                | "use"
                | "mod"
                | "if"
                | "else"
                | "for"
                | "while"
                | "loop"
                | "return"
                | "match"
                | "impl"
                | "struct"
                | "enum"
                | "trait"
                | "type"
                | "const"
                | "static"
                | "async"
                | "await"
                | "unsafe"
                | "extern"
                | "crate"
                | "self"
                | "super"
                | "where"
                | "true"
                | "false"
                | "as"
                | "in"
                | "ref"
                | "move"
                | "dyn"
                | "break"
                | "continue"
                | "yield"
                | "box"
                | "become"
                | "do"
                | "final"
                | "macro"
                | "override"
                | "priv"
                | "try"
                | "abstract"
                | "virtual"
        )
    })
}

/// One of: nothing, `const`, `async`, `unsafe`, `extern "C"`.
fn fn_decorator() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        5 => Just(""),
        1 => Just("const "),
        2 => Just("async "),
        1 => Just("unsafe "),
        1 => Just("extern \"C\" "),
    ]
}

/// Whether a function is instrumentable (not const, not extern non-Rust).
fn is_instrumentable(decorator: &str) -> bool {
    !decorator.contains("const") && !decorator.contains("extern")
}

/// Generate a single function with a random name and decorator.
fn rust_fn() -> impl Strategy<Value = (String, String, bool)> {
    (ident(), fn_decorator()).prop_map(|(name, decorator)| {
        let instrumentable = is_instrumentable(decorator);
        let body = if decorator.contains("const") {
            format!("{decorator}fn {name}() -> u32 {{ 42 }}")
        } else {
            format!("{decorator}fn {name}() {{ let _ = 1; }}")
        };
        (name, body, instrumentable)
    })
}

/// Generate a file with 1-5 functions, all unique names.
fn rust_file() -> impl Strategy<Value = (Vec<String>, String, Vec<String>)> {
    prop::collection::vec(rust_fn(), 1..=5).prop_map(|fns| {
        let mut seen = std::collections::HashSet::new();
        let mut names = Vec::new();
        let mut bodies = Vec::new();
        let mut instrumentable_names = Vec::new();

        for (name, body, instrumentable) in fns {
            if seen.contains(&name) {
                continue;
            }
            seen.insert(name.clone());
            names.push(name.clone());
            bodies.push(body);
            if instrumentable {
                instrumentable_names.push(name);
            }
        }

        let source = bodies.join("\n");
        (names, source, instrumentable_names)
    })
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    /// Instrumented output must always parse as valid Rust.
    #[test]
    fn output_parses_as_valid_rust((names, source, _instrumentable) in rust_file()) {
        if !ra_ap_syntax::SourceFile::parse(&source, ra_ap_syntax::Edition::Edition2024).errors().is_empty() {
            return Ok(());
        }
        let measured: HashMap<String, u32> = names.iter().enumerate()
            .map(|(i, n)| (n.clone(), i as u32)).collect();
        let result = instrument_source(&source, &measured, None)
            .expect("instrument_source should succeed on valid Rust input");
        let re_parse = ra_ap_syntax::SourceFile::parse(&result.source, ra_ap_syntax::Edition::Edition2024);
        let errors: Vec<_> = re_parse.errors().to_vec();
        prop_assert!(
            errors.is_empty(),
            "output failed to parse:\n{}\nerrors: {:?}",
            result.source,
            errors
        );
    }

    /// Every targeted instrumentable function should have a guard injected.
    #[test]
    fn targeted_functions_get_guards((names, source, instrumentable) in rust_file()) {
        if !ra_ap_syntax::SourceFile::parse(&source, ra_ap_syntax::Edition::Edition2024).errors().is_empty() {
            return Ok(());
        }
        let measured: HashMap<String, u32> = names.iter().enumerate()
            .map(|(i, n)| (n.clone(), i as u32)).collect();
        let result = instrument_source(&source, &measured, None)
            .expect("instrument_source should succeed on valid Rust input");
        for name in &instrumentable {
            let name_id = measured.get(name).unwrap();
            let sync_guard = format!("piano_runtime::enter({name_id})");
            let async_guard = format!("piano_runtime::enter_async({name_id},");
            prop_assert!(
                result.source.contains(&sync_guard) || result.source.contains(&async_guard),
                "missing guard for '{}' (id {}) in:\n{}",
                name, name_id, result.source
            );
        }
    }

    /// Async functions should be wrapped with enter_async.
    #[test]
    fn async_functions_get_enter_async((names, source, instrumentable) in rust_file()) {
        if !source.contains("async fn") {
            return Ok(());
        }
        let has_instrumentable_async = instrumentable.iter().any(|name| {
            source.contains(&format!("async fn {name}"))
        });
        if !has_instrumentable_async {
            return Ok(());
        }
        if !ra_ap_syntax::SourceFile::parse(&source, ra_ap_syntax::Edition::Edition2024).errors().is_empty() {
            return Ok(());
        }
        let measured: HashMap<String, u32> = names.iter().enumerate()
            .map(|(i, n)| (n.clone(), i as u32)).collect();
        let result = instrument_source(&source, &measured, None)
            .expect("instrument_source should succeed on valid Rust input");
        prop_assert!(
            result.source.contains("enter_async"),
            "async function present but no enter_async in:\n{}",
            result.source
        );
    }

    /// Non-targeted functions should NOT get guards.
    #[test]
    fn non_targeted_functions_no_guard((names, source, _instrumentable) in rust_file()) {
        if names.len() < 2 {
            return Ok(());
        }
        if !ra_ap_syntax::SourceFile::parse(&source, ra_ap_syntax::Edition::Edition2024).errors().is_empty() {
            return Ok(());
        }
        // Target only the first function.
        let measured: HashMap<String, u32> = HashMap::from([(names[0].clone(), 0u32)]);
        let result = instrument_source(&source, &measured, None)
            .expect("instrument_source should succeed on valid Rust input");
        // Only ID 0 should appear in guards. No other IDs.
        let guard_count = result.source.matches("piano_runtime::enter(").count()
            + result.source.matches("piano_runtime::enter_async(").count();
        prop_assert!(
            guard_count <= 1,
            "expected at most 1 guard (for targeted function), got {} in:\n{}",
            guard_count, result.source
        );
    }

    /// Uninstrumentable functions (const, extern "C") should NOT get guards.
    #[test]
    fn uninstrumentable_functions_skipped((names, source, instrumentable) in rust_file()) {
        if !ra_ap_syntax::SourceFile::parse(&source, ra_ap_syntax::Edition::Edition2024).errors().is_empty() {
            return Ok(());
        }
        let instrumentable_set: std::collections::HashSet<&str> =
            instrumentable.iter().map(|s| s.as_str()).collect();
        let skipped: Vec<&str> = names.iter()
            .map(|s| s.as_str())
            .filter(|n| !instrumentable_set.contains(n))
            .collect();
        if skipped.is_empty() {
            return Ok(());
        }
        let measured: HashMap<String, u32> = names.iter().enumerate()
            .map(|(i, n)| (n.clone(), i as u32)).collect();
        let result = instrument_source(&source, &measured, None)
            .expect("instrument_source should succeed on valid Rust input");
        for name in &skipped {
            if let Some(&id) = measured.get(*name) {
                let sync_pattern = format!("piano_runtime::enter({id})");
                let async_pattern = format!("piano_runtime::enter_async({id},");
                prop_assert!(
                    !result.source.contains(&sync_pattern) && !result.source.contains(&async_pattern),
                    "uninstrumentable '{}' (id {}) was instrumented in:\n{}",
                    name, id, result.source
                );
            }
        }
    }
}
