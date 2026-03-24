//! Source rewriter: inject profiling guards into Rust source files.
//!
//! The rewriter's only job: for each measured function body, inject a
//! guard statement after the opening brace. No parameter injection.
//! No call-site modifications. No closure analysis.

pub mod allocator;
pub mod registrations;
pub mod shutdown;

use std::collections::HashMap;

use ra_ap_syntax::{AstNode, SourceFile, ast, T};
use ra_ap_syntax::ast::HasName;

use crate::source_map::{SourceMap, StringInjector};

pub struct InstrumentResult {
    pub source: String,
    pub source_map: SourceMap,
}

pub fn instrument_source(
    source: &str,
    measured: &HashMap<String, u32>,
) -> Result<InstrumentResult, String> {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let file = parse.tree();

    let mut injector = StringInjector::new();

    for node in file.syntax().descendants() {
        let Some(func) = ast::Fn::cast(node) else { continue };
        let Some(body) = func.body() else { continue };
        let Some(name) = func.name() else { continue };
        let fn_name = name.text().to_string();
        let Some(&name_id) = measured.get(&fn_name) else { continue };

        // Skip const fn and non-Rust ABI
        if func.const_token().is_some() { continue }
        if let Some(abi) = func.abi() {
            if let Some(token) = abi.string_token() {
                let abi_str = token.text();
                if abi_str != "\"Rust\"" { continue }
            }
        }

        // Find injection point: after the opening brace of the body's
        // statement list, skipping inner attributes.
        let stmt_list = body.stmt_list()
            .ok_or_else(|| format!("no stmt_list for fn {fn_name}"))?;
        let open_brace = stmt_list.syntax().children_with_tokens()
            .find(|t| t.kind() == T!['{'])
            .ok_or_else(|| format!("no opening brace for fn {fn_name}"))?;
        let mut inject_offset: usize = open_brace.text_range().end().into();

        // Skip inner attributes (#![...])
        for child in stmt_list.syntax().children() {
            if child.kind() == ra_ap_syntax::SyntaxKind::ATTR {
                let text = child.text().to_string();
                if text.starts_with("#!") {
                    inject_offset = child.text_range().end().into();
                }
            }
        }

        let is_async_fn = func.async_token().is_some();
        let is_impl_future = returns_impl_future(&func);

        if is_async_fn || is_impl_future {
            let close_brace = stmt_list.syntax().children_with_tokens()
                .filter(|t| t.kind() == T!['}'])
                .last()
                .ok_or_else(|| format!("no closing brace for fn {fn_name}"))?;
            let close_offset: usize = close_brace.text_range().start().into();

            injector.insert(
                inject_offset,
                format!("\npiano_runtime::enter_async({name_id}, async move {{"),
            );
            // async fn: .await the PianoFuture (body expects the return type, not PianoFuture)
            // impl Future: return the PianoFuture directly (it IS the return value)
            if is_async_fn {
                injector.insert(close_offset, "}).await");
            } else {
                injector.insert(close_offset, "})");
            }
        } else {
            // Sync: inject guard at body start
            injector.insert(
                inject_offset,
                format!("\nlet __piano_guard = piano_runtime::enter({name_id});"),
            );
        }
    }

    let (source, source_map) = injector.apply(source);
    Ok(InstrumentResult { source, source_map })
}

/// Check if a function's return type contains `impl Future`.
fn returns_impl_future(func: &ast::Fn) -> bool {
    let Some(ret) = func.ret_type() else { return false };
    let ret_text = ret.syntax().text().to_string();
    ret_text.contains("impl") && ret_text.contains("Future")
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Node 1: CST parse gives ast::Fn with name and body ===

    #[test]
    fn cst_finds_function() {
        let source = "fn work() {\n    let x = 1;\n}\n";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let file = parse.tree();
        let funcs: Vec<_> = file.syntax().descendants()
            .filter_map(ast::Fn::cast)
            .collect();
        assert_eq!(funcs.len(), 1);
        assert_eq!(funcs[0].name().unwrap().text(), "work");
        assert!(funcs[0].body().is_some());
    }

    // === Node 2: stmt_list + opening brace gives correct byte offset ===

    #[test]
    fn brace_offset_is_correct() {
        let source = "fn work() {\n    let x = 1;\n}\n";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let file = parse.tree();
        let func = file.syntax().descendants()
            .find_map(ast::Fn::cast).unwrap();
        let body = func.body().unwrap();
        let stmt_list = body.stmt_list().unwrap();
        let brace = stmt_list.syntax().children_with_tokens()
            .find(|t| t.kind() == T!['{'])
            .unwrap();
        let offset: usize = brace.text_range().end().into();
        // "fn work() {" is 11 chars, brace is at index 10, end is 11
        assert_eq!(offset, 11, "brace end offset");
        // Injecting at offset 11 puts text right after the '{'
        let mut s = source.to_string();
        s.insert_str(offset, "\nGUARD;");
        assert!(s.contains("{\nGUARD;"), "injection after brace. Got:\n{s}");
    }

    // === Node 3: inner attrs are skipped ===

    #[test]
    fn inner_attrs_skipped() {
        let source = "fn work() {\n    #![allow(unused)]\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        let attr_pos = result.source.find("#![allow(unused)]").unwrap();
        let guard_pos = result.source.find("piano_runtime::enter(0)").unwrap();
        assert!(
            guard_pos > attr_pos,
            "guard must come after inner attr. Got:\n{}",
            result.source
        );
    }

    // === Node 4: full injection produces correct output ===

    #[test]
    fn injects_guard_in_simple_function() {
        let source = "fn work() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn skips_unmeasured_functions() {
        let source = "fn work() {\n    1;\n}\nfn helper() {\n    2;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert_eq!(result.source.matches("piano_runtime::enter").count(), 1);
    }

    #[test]
    fn skips_const_fn() {
        let source = "const fn compute() -> u32 {\n    42\n}\n";
        let measured: HashMap<String, u32> = [("compute".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"));
    }

    #[test]
    fn skips_extern_c_fn() {
        let source = "extern \"C\" fn callback(x: i32) -> i32 {\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("callback".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"));
    }

    #[test]
    fn instruments_unsafe_fn() {
        let source = "unsafe fn danger() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("danger".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn instruments_trait_method_impl() {
        let source = "struct S;\nimpl std::fmt::Display for S {\n    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {\n        write!(f, \"S\")\n    }\n}\n";
        let measured: HashMap<String, u32> = [("fmt".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn no_parameter_injection() {
        let source = "fn work(x: i32) -> i32 {\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("fn work(x: i32) -> i32"));
        assert!(!result.source.contains("__piano_ctx"));
    }

    #[test]
    fn no_call_site_injection() {
        let source = "fn caller() {\n    work();\n}\nfn work() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> =
            [("caller".into(), 0), ("work".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("    work();"), "call site unchanged");
        assert!(!result.source.contains(".clone()"));
    }

    #[test]
    fn multiple_functions_get_different_ids() {
        let source = "fn a() {\n    1;\n}\nfn b() {\n    2;\n}\n";
        let measured: HashMap<String, u32> =
            [("a".into(), 0), ("b".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("enter(0)"));
        assert!(result.source.contains("enter(1)"));
    }

    // Grammar exhaustiveness tests are in piano-runtime/tests/rust_source_enumeration.rs
    // (pinned to the actual Rust compiler source at ~/dev/rust-lang/rust)

    #[test]
    fn instruments_inherent_impl_method() {
        let source = "struct S;\nimpl S {\n    fn method(&self) {\n        let x = 1;\n    }\n}\n";
        let measured: HashMap<String, u32> = [("method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
        assert!(result.source.contains("fn method(&self)"), "signature unchanged");
    }

    #[test]
    fn instruments_trait_default_method() {
        let source = "trait T {\n    fn default_impl(&self) {\n        let x = 1;\n    }\n}\n";
        let measured: HashMap<String, u32> = [("default_impl".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn skips_trait_abstract_method() {
        let source = "trait T {\n    fn abstract_method(&self);\n}\n";
        let measured: HashMap<String, u32> = [("abstract_method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"), "no body = no guard");
    }

    #[test]
    fn skips_foreign_function() {
        let source = "extern {\n    fn c_function(x: i32) -> i32;\n}\n";
        let measured: HashMap<String, u32> = [("c_function".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"), "foreign fn has no body");
    }

    #[test]
    fn instruments_nested_function() {
        let source = "fn outer() {\n    fn inner() {\n        let x = 1;\n    }\n    inner();\n}\n";
        let measured: HashMap<String, u32> =
            [("outer".into(), 0), ("inner".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(result.source.contains("enter(0)"), "outer gets guard");
        assert!(result.source.contains("enter(1)"), "inner gets guard");
    }

    // === Async detection ===

    #[test]
    fn async_fn_uses_enter_async() {
        let source = "async fn fetch() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(
            result.source.contains("enter_async(0"),
            "async fn must use enter_async. Got:\n{}", result.source
        );
        assert!(
            !result.source.contains("piano_runtime::enter(0)"),
            "async fn must NOT use sync enter"
        );
    }

    #[test]
    fn impl_future_uses_enter_async() {
        let source = "fn fetch() -> impl std::future::Future<Output = i32> {\n    async { 42 }\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(
            result.source.contains("enter_async(0"),
            "impl Future fn must use enter_async. Got:\n{}", result.source
        );
    }

    #[test]
    fn impl_future_short_path() {
        let source = "fn fetch() -> impl Future<Output = ()> {\n    async {}\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(
            result.source.contains("enter_async(0"),
            "impl Future (short path) must use enter_async. Got:\n{}", result.source
        );
    }

    #[test]
    fn sync_fn_does_not_use_enter_async() {
        let source = "fn work() -> i32 {\n    42\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "sync fn must use sync enter"
        );
        assert!(
            !result.source.contains("enter_async"),
            "sync fn must NOT use enter_async"
        );
    }

    #[test]
    fn async_wrapping_preserves_body() {
        let source = "async fn fetch() {\n    let x = do_work();\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured).unwrap();
        assert!(
            result.source.contains("do_work()"),
            "original body must be preserved"
        );
        assert!(
            result.source.contains("async move {"),
            "body must be wrapped in async move. Got:\n{}", result.source
        );
    }
}
