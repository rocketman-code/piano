//! Property-based tests for the rewriter.
//!
//! Uses proptest to generate diverse Rust function signatures with randomized
//! decorators, then verifies the rewriter produces valid instrumented output.
//! Targets the "pattern matching blindness" bug class (#237, #238, #249, #270).

use std::collections::HashSet;

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
                | "match"
                | "return"
                | "break"
                | "continue"
                | "struct"
                | "enum"
                | "impl"
                | "trait"
                | "type"
                | "where"
                | "async"
                | "await"
                | "move"
                | "ref"
                | "self"
                | "super"
                | "crate"
                | "as"
                | "in"
                | "const"
                | "static"
                | "extern"
                | "unsafe"
                | "dyn"
                | "true"
                | "false"
        )
    })
}

/// A #[cfg(...)] attribute with various conditions.
fn cfg_attr() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("#[cfg(test)]".to_string()),
        Just("#[cfg(not(test))]".to_string()),
        Just("#[cfg(target_os = \"linux\")]".to_string()),
        Just("#[cfg(debug_assertions)]".to_string()),
        Just("#[cfg(feature = \"some_feature\")]".to_string()),
        Just("#[cfg(any(target_os = \"linux\", target_os = \"macos\"))]".to_string()),
        Just("#[cfg(all(unix, not(target_os = \"macos\")))]".to_string()),
        Just("#[cfg_attr(test, allow(unused))]".to_string()),
        Just("#[cfg_attr(feature = \"serde\", derive(Serialize))]".to_string()),
    ]
}

/// A non-cfg attribute.
fn other_attr() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("#[inline]".to_string()),
        Just("#[inline(always)]".to_string()),
        Just("#[inline(never)]".to_string()),
        Just("#[must_use]".to_string()),
        Just("#[allow(unused)]".to_string()),
        Just("#[allow(clippy::too_many_arguments)]".to_string()),
        Just("#[doc = \"some docs\"]".to_string()),
    ]
}

/// Zero or more attributes stacked on a function.
fn attr_stack() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof![cfg_attr(), other_attr()], 0..=3)
        .prop_map(|attrs| attrs.join("\n"))
}

/// Function visibility.
fn visibility() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("pub ".to_string()),
        Just("pub(crate) ".to_string()),
        Just("pub(super) ".to_string()),
    ]
}

/// Async modifier.
fn async_modifier() -> impl Strategy<Value = String> {
    prop_oneof![Just("".to_string()), Just("async ".to_string()),]
}

/// Function modifier that makes a function uninstrumentable.
/// Piano skips unsafe fn, const fn, and extern "C" fn.
fn skip_modifier() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => Just("".to_string()),
        1 => Just("unsafe ".to_string()),
        1 => Just("const ".to_string()),
        1 => Just("extern \"C\" ".to_string()),
    ]
}

/// Generic parameters.
fn generics() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("<T>".to_string()),
        Just("<T: Clone>".to_string()),
        Just("<T: Clone + Send>".to_string()),
        Just("<T, U>".to_string()),
    ]
}

/// Where clause (only with generics, but we keep it simple).
fn where_clause() -> impl Strategy<Value = String> {
    prop_oneof![
        9 => Just("".to_string()),
        1 => Just(" where T: std::fmt::Debug".to_string()),
    ]
}

/// Return type.
fn return_type() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just(" -> i32".to_string()),
        Just(" -> String".to_string()),
        Just(" -> bool".to_string()),
        Just(" -> Option<i32>".to_string()),
        Just(" -> Result<(), String>".to_string()),
        Just(" -> Vec<u8>".to_string()),
    ]
}

/// Parameter list.
fn params() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("x: i32".to_string()),
        Just("x: i32, y: i32".to_string()),
        Just("s: &str".to_string()),
        Just("s: String, n: usize".to_string()),
    ]
}

/// A single function definition with randomized attributes/signature.
/// Returns (name, source, instrumentable) where instrumentable is false for
/// unsafe/const/extern "C" functions.
fn function_def() -> impl Strategy<Value = (String, String, bool)> {
    (
        attr_stack(),
        visibility(),
        skip_modifier(),
        async_modifier(),
        ident(),
        generics(),
        params(),
        return_type(),
        where_clause(),
    )
        .prop_map(
            |(attrs, vis, skip_mod, async_mod, name, generics, params, ret, where_cl)| {
                let instrumentable = skip_mod.is_empty();

                // const fn and extern "C" fn cannot be async.
                let effective_async = if skip_mod.is_empty() {
                    async_mod
                } else {
                    String::new()
                };

                // Build a body that returns the right type.
                let body = match ret.as_str() {
                    "" => "let _ = 42;".to_string(),
                    " -> i32" => "42".to_string(),
                    " -> String" => "String::new()".to_string(),
                    " -> bool" => "true".to_string(),
                    " -> Option<i32>" => "Some(42)".to_string(),
                    " -> Result<(), String>" => "Ok(())".to_string(),
                    " -> Vec<u8>" => "Vec::new()".to_string(),
                    _ => "let _ = 42;".to_string(),
                };

                let attrs_block = if attrs.is_empty() {
                    String::new()
                } else {
                    format!("{attrs}\n")
                };

                let source = format!(
                    "{attrs_block}{vis}{skip_mod}{effective_async}fn {name}{generics}({params}){ret}{where_cl} {{\n    {body}\n}}\n"
                );

                (name, source, instrumentable)
            },
        )
}

/// A full Rust file with 1-4 functions (deduplicated names).
/// Returns (names, source, instrumentable_names).
fn rust_file() -> impl Strategy<Value = (Vec<String>, String, Vec<String>)> {
    prop::collection::vec(function_def(), 1..=4).prop_map(|fns| {
        let mut seen = std::collections::HashSet::new();
        let fns: Vec<_> = fns
            .into_iter()
            .filter(|(n, _, _)| seen.insert(n.clone()))
            .collect();
        let names: Vec<String> = fns.iter().map(|(n, _, _)| n.clone()).collect();
        let instrumentable_names: Vec<String> = fns
            .iter()
            .filter(|(_, _, instrumentable)| *instrumentable)
            .map(|(n, _, _)| n.clone())
            .collect();
        let source = fns
            .into_iter()
            .map(|(_, s, _)| s)
            .collect::<Vec<_>>()
            .join("\n");
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
        // Only test inputs that are valid Rust (syn can parse them).
        if syn::parse_str::<syn::File>(&source).is_err() {
            return Ok(());
        }
        let targets: HashSet<String> = names.iter().cloned().collect();
        let result = instrument_source(&source, &targets, false)
            .expect("instrument_source should succeed on valid Rust input");
        // Core invariant: output must be parseable Rust.
        let parse = syn::parse_str::<syn::File>(&result.source);
        prop_assert!(
            parse.is_ok(),
            "output failed to parse:\n{}\nerror: {:?}",
            result.source,
            parse.err()
        );
    }

    /// Every targeted instrumentable function should have a guard injected.
    #[test]
    fn targeted_functions_get_guards((names, source, instrumentable) in rust_file()) {
        if syn::parse_str::<syn::File>(&source).is_err() {
            return Ok(());
        }
        let targets: HashSet<String> = names.iter().cloned().collect();
        let result = instrument_source(&source, &targets, false)
            .expect("instrument_source should succeed on valid Rust input");
        for name in &instrumentable {
            let guard_pattern = format!("piano_runtime::enter(\"{name}\")");
            prop_assert!(
                result.source.contains(&guard_pattern),
                "missing guard for '{}' in:\n{}",
                name,
                result.source
            );
        }
    }

    /// Async functions should be wrapped with PianoFuture.
    #[test]
    fn async_functions_get_piano_future((names, source, instrumentable) in rust_file()) {
        if !source.contains("async fn") {
            return Ok(());
        }
        // Only expect PianoFuture if at least one instrumentable function is async.
        let has_instrumentable_async = instrumentable.iter().any(|name| {
            source.contains(&format!("async fn {name}"))
        });
        if !has_instrumentable_async {
            return Ok(());
        }
        if syn::parse_str::<syn::File>(&source).is_err() {
            return Ok(());
        }
        let targets: HashSet<String> = names.iter().cloned().collect();
        let result = instrument_source(&source, &targets, false)
            .expect("instrument_source should succeed on valid Rust input");
        prop_assert!(
            result.source.contains("PianoFuture"),
            "async function present but no PianoFuture in:\n{}",
            result.source
        );
    }

    /// Non-targeted functions should NOT be instrumented.
    #[test]
    fn non_targeted_functions_untouched((names, source, _instrumentable) in rust_file()) {
        if names.len() < 2 {
            return Ok(());
        }
        if syn::parse_str::<syn::File>(&source).is_err() {
            return Ok(());
        }
        // Target only the first function, check the rest are untouched.
        let targets: HashSet<String> = [names[0].clone()].into();
        let result = instrument_source(&source, &targets, false)
            .expect("instrument_source should succeed on valid Rust input");
        for name in &names[1..] {
            let guard_pattern = format!("piano_runtime::enter(\"{name}\")");
            prop_assert!(
                !result.source.contains(&guard_pattern),
                "non-targeted '{}' was instrumented in:\n{}",
                name,
                result.source
            );
        }
    }

    /// Uninstrumentable functions (unsafe, const, extern) should NOT get guards.
    #[test]
    fn uninstrumentable_functions_skipped((names, source, instrumentable) in rust_file()) {
        if syn::parse_str::<syn::File>(&source).is_err() {
            return Ok(());
        }
        let instrumentable_set: HashSet<&str> =
            instrumentable.iter().map(|s| s.as_str()).collect();
        let skipped: Vec<&str> = names
            .iter()
            .map(|s| s.as_str())
            .filter(|n| !instrumentable_set.contains(n))
            .collect();
        if skipped.is_empty() {
            return Ok(());
        }
        let targets: HashSet<String> = names.iter().cloned().collect();
        let result = instrument_source(&source, &targets, false)
            .expect("instrument_source should succeed on valid Rust input");
        for name in &skipped {
            let guard_pattern = format!("piano_runtime::enter(\"{name}\")");
            prop_assert!(
                !result.source.contains(&guard_pattern),
                "uninstrumentable '{}' was instrumented in:\n{}",
                name,
                result.source
            );
        }
    }
}
