//! Source rewriter: single-pass injection of profiling infrastructure.
//!
//! One CST parse. One StringInjector. One apply(). One SourceMap.
//!
//! For non-entry files: inject guards into measured function bodies.
//! For the entry point: guards + name table + allocator wrapping + lifecycle.
//! All injection points are collected from the ORIGINAL source, then applied
//! in one pass. No multi-pass offset drift. No source map chaining.

use std::collections::HashMap;

use ra_ap_syntax::{AstNode, SourceFile, SyntaxKind, ast, T};
use ra_ap_syntax::ast::{HasAttrs, HasName};

use crate::source_map::{SourceMap, StringInjector};

pub struct InstrumentResult {
    pub source: String,
    pub source_map: SourceMap,
}

/// Parameters for entry-point-only injections (name table, allocator, lifecycle).
pub struct EntryPointParams<'a> {
    pub name_table: &'a [(u32, &'a str)],
    pub runs_dir: &'a str,
    pub cpu_time: bool,
}

/// Instrument a source file in a single pass.
///
/// When `entry_point` is None: only inject guards into measured functions.
/// When `entry_point` is Some: also inject name table, allocator wrapping,
/// and lifecycle code. All from the original source, one StringInjector.
pub fn instrument_source(
    source: &str,
    measured: &HashMap<String, u32>,
    entry_point: Option<&EntryPointParams<'_>>,
) -> Result<InstrumentResult, String> {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let file = parse.tree();

    let mut injector = StringInjector::new();

    // --- Entry point: registrations at offset 0 ---
    if let Some(ep) = entry_point {
        let reg_offset = file_level_inner_attr_end(&file);
        let mut entries = String::new();
        for (id, name) in ep.name_table {
            if !entries.is_empty() {
                entries.push_str(", ");
            }
            entries.push_str(&format!("({id}, \"{name}\")"));
        }
        injector.insert(
            reg_offset,
            format!("\nconst PIANO_NAMES: &[(u32, &str)] = &[{entries}];\n"),
        );
    }

    // --- Entry point: allocator detection and wrapping ---
    if entry_point.is_some() {
        inject_allocator(&file, source, &mut injector)?;
    }

    // --- Guards + shutdown: walk all descendants ---
    for node in file.syntax().descendants() {
        let Some(func) = ast::Fn::cast(node) else { continue };
        let Some(body) = func.body() else { continue };
        let Some(name) = func.name() else { continue };
        let fn_name = name.text().to_string();

        // Shutdown: inject lifecycle into fn main()
        if fn_name == "main" {
            if let Some(ep) = entry_point {
                let stmt_list = body.stmt_list()
                    .ok_or_else(|| "no stmt_list for fn main".to_string())?;
                let inject_offset = brace_offset_after_inner_attrs(&stmt_list)?;
                injector.insert(inject_offset, build_lifecycle_prefix(ep.runs_dir, ep.cpu_time));
            }
            continue; // main is excluded from the name table
        }

        // Guard: only for measured functions
        let Some(&name_id) = measured.get(&fn_name) else { continue };

        // Skip const fn and non-Rust ABI
        if func.const_token().is_some() { continue }
        if let Some(abi) = func.abi() {
            if let Some(token) = abi.string_token() {
                let abi_str = token.text();
                if abi_str != "\"Rust\"" { continue }
            }
        }

        let stmt_list = body.stmt_list()
            .ok_or_else(|| format!("no stmt_list for fn {fn_name}"))?;
        let inject_offset = brace_offset_after_inner_attrs(&stmt_list)?;

        let is_async_fn = func.async_token().is_some();
        let is_impl_future = returns_impl_future(&func);

        if is_async_fn || is_impl_future {
            let close_brace = stmt_list.syntax().children_with_tokens()
                .filter(|t| t.kind() == T!['}'])
                .last()
                .ok_or_else(|| format!("no closing brace for fn {fn_name}"))?;
            let close_offset: usize = close_brace.text_range().start().into();

            if is_async_fn {
                injector.insert(
                    inject_offset,
                    format!("\npiano_runtime::enter_async({name_id}, async move {{"),
                );
                injector.insert(close_offset, "}).await");
            } else {
                injector.insert(
                    inject_offset,
                    format!("\npiano_runtime::enter_async({name_id},"),
                );
                injector.insert(close_offset, ")");
            }
        } else {
            injector.insert(
                inject_offset,
                format!("\nlet __piano_guard = piano_runtime::enter({name_id});"),
            );
        }
    }

    // --- Macro expansion: replace fn-generating invocations ---
    expand_and_replace_macros(file.syntax(), measured, &mut injector);

    let (source, source_map) = injector.apply(source);
    Ok(InstrumentResult { source, source_map })
}

/// Find the byte offset after all file-level inner attributes (#![...]).
/// Returns 0 if there are none.
fn file_level_inner_attr_end(file: &SourceFile) -> usize {
    let mut offset = 0usize;
    for child in file.syntax().children() {
        if child.kind() == SyntaxKind::ATTR {
            let text = child.text().to_string();
            if text.starts_with("#!") {
                offset = child.text_range().end().into();
            }
        }
    }
    offset
}

/// Find the injection offset inside a statement list: after the opening
/// brace and any inner attributes.
fn brace_offset_after_inner_attrs(stmt_list: &ast::StmtList) -> Result<usize, String> {
    let open_brace = stmt_list.syntax().children_with_tokens()
        .find(|t| t.kind() == T!['{'])
        .ok_or_else(|| "no opening brace".to_string())?;
    let mut offset: usize = open_brace.text_range().end().into();

    for child in stmt_list.syntax().children() {
        if child.kind() == SyntaxKind::ATTR {
            let text = child.text().to_string();
            if text.starts_with("#!") {
                offset = child.text_range().end().into();
            }
        }
    }
    Ok(offset)
}

/// Check if a function's return type contains `impl Future`.
fn returns_impl_future(func: &ast::Fn) -> bool {
    let Some(ret) = func.ret_type() else { return false };
    let ret_text = ret.syntax().text().to_string();
    ret_text.contains("impl") && ret_text.contains("Future")
}

// ---------------------------------------------------------------------------
// Allocator injection (CST-based, single-pass)
// ---------------------------------------------------------------------------

/// Detect #[global_allocator] via CST and inject wrapping into the StringInjector.
///
/// Case 1 (absent): insert PianoAllocator<System> at offset 0.
/// Case 2 (present, no cfg): replace the static item with wrapped version.
/// Case 3 (present, with cfg): replace with wrapped + negated cfg fallback.
fn inject_allocator(
    file: &SourceFile,
    source: &str,
    injector: &mut StringInjector,
) -> Result<(), String> {
    // Find the static item with #[global_allocator]
    let alloc_info = find_global_allocator(file, source);

    match alloc_info {
        None => {
            // Case 1: no allocator. Inject PianoAllocator<System>.
            injector.insert(0, concat!(
                "#[global_allocator]\n",
                "static __PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n",
                "    = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
            ));
        }
        Some(info) => {
            let replacement = if let Some(ref cfg) = info.cfg_attr {
                // Case 3: cfg-gated allocator
                let neg_cfg = negate_cfg(cfg);
                format!(
                    "{cfg}\n\
                     #[global_allocator]\n\
                     static {name}: piano_runtime::PianoAllocator<{ty}>\n\
                     \x20   = piano_runtime::PianoAllocator::new({init});\n\
                     {neg_cfg}\n\
                     #[global_allocator]\n\
                     static {name}: piano_runtime::PianoAllocator<std::alloc::System>\n\
                     \x20   = piano_runtime::PianoAllocator::new(std::alloc::System);",
                    name = info.name,
                    ty = info.type_expr,
                    init = info.init_expr,
                )
            } else {
                // Case 2: no cfg, simple wrap
                format!(
                    "#[global_allocator]\n\
                     static {name}: piano_runtime::PianoAllocator<{ty}>\n\
                     \x20   = piano_runtime::PianoAllocator::new({init});",
                    name = info.name,
                    ty = info.type_expr,
                    init = info.init_expr,
                )
            };
            injector.replace(info.start, info.end, replacement);
        }
    }

    Ok(())
}

struct AllocatorInfo {
    start: usize,
    end: usize,
    name: String,
    type_expr: String,
    init_expr: String,
    cfg_attr: Option<String>,
}

/// Find a static item with #[global_allocator] using CST, extract its components.
fn find_global_allocator(file: &SourceFile, source: &str) -> Option<AllocatorInfo> {
    for node in file.syntax().descendants() {
        let Some(static_item) = ast::Static::cast(node) else { continue };

        // Check for #[global_allocator] attribute
        let has_global_alloc = static_item.attrs().any(|attr| {
            let text = attr.syntax().text().to_string();
            text.contains("global_allocator") && !text.starts_with("#!")
        });
        if !has_global_alloc {
            continue;
        }

        // Extract the full range (includes attributes)
        let start: usize = static_item.syntax().text_range().start().into();
        let end: usize = static_item.syntax().text_range().end().into();

        // Extract name, type, initializer from the source text
        let item_text = &source[start..end];

        let static_kw = item_text.find("static ")?;
        let after_static = &item_text[static_kw + 7..];
        let colon = after_static.find(':')?;
        let name = after_static[..colon].trim().to_string();

        let after_colon = &after_static[colon + 1..];
        let eq = after_colon.find('=')?;
        let type_expr = after_colon[..eq].trim().to_string();

        let after_eq = &after_colon[eq + 1..];
        let init_expr = after_eq.trim_end_matches(';').trim().to_string();

        // Check for #[cfg(...)] attribute
        let cfg_attr = static_item.attrs().find_map(|attr| {
            let text = attr.syntax().text().to_string();
            if text.starts_with("#[cfg(") || text.starts_with("#[cfg_attr(") {
                Some(text)
            } else {
                None
            }
        });

        return Some(AllocatorInfo {
            start,
            end,
            name,
            type_expr,
            init_expr,
            cfg_attr,
        });
    }
    None
}

/// Negate a #[cfg(...)] attribute to #[cfg(not(...))].
fn negate_cfg(cfg: &str) -> String {
    if let Some(inner) = cfg.strip_prefix("#[cfg(").and_then(|s| s.strip_suffix(")]")) {
        format!("#[cfg(not({inner}))]")
    } else {
        // Can't negate complex cfg_attr, fall back to not(any())
        "#[cfg(not(any()))]".to_string()
    }
}

// ---------------------------------------------------------------------------
// Lifecycle injection (shutdown)
// ---------------------------------------------------------------------------

const FILE_COLLISION_RETRIES: u32 = 4;

/// Build the lifecycle code injected at the top of main().
fn build_lifecycle_prefix(runs_dir: &str, cpu_time: bool) -> String {
    let mut s = String::new();
    s.push_str("\n    use std::io::Write as _;");
    s.push_str("\n    let __piano_sink = {");
    s.push_str(&format!(
        "\n        let __piano_dir = std::path::PathBuf::from(std::env::var(\"PIANO_RUNS_DIR\").unwrap_or_else(|_| \"{runs_dir}\".into()));"
    ));
    s.push_str("\n        match std::fs::create_dir_all(&__piano_dir) {");
    s.push_str("\n            Ok(()) => {");
    s.push_str("\n                let __piano_ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis();");
    s.push_str("\n                let __piano_pid = std::process::id();");
    s.push_str("\n                let mut __piano_file = None;");
    s.push_str("\n                let mut __piano_warned = false;");
    s.push_str(&format!(
        "\n                for __piano_suffix in 0u32..{FILE_COLLISION_RETRIES} {{"
    ));
    s.push_str("\n                    let __piano_name = if __piano_suffix == 0 {");
    s.push_str("\n                        format!(\"{}-{}.ndjson\", __piano_ts, __piano_pid)");
    s.push_str("\n                    } else {");
    s.push_str("\n                        format!(\"{}-{}-{}.ndjson\", __piano_ts, __piano_pid, __piano_suffix)");
    s.push_str("\n                    };");
    s.push_str("\n                    let __piano_path = __piano_dir.join(&__piano_name);");
    s.push_str("\n                    match std::fs::OpenOptions::new().write(true).create_new(true).open(&__piano_path) {");
    s.push_str("\n                        Ok(f) => { __piano_file = Some(f); break; }");
    s.push_str("\n                        Err(_) => {}");
    s.push_str("\n                    }");
    s.push_str("\n                }");
    s.push_str("\n                if __piano_file.is_none() && !__piano_warned {");
    s.push_str("\n                    let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output (all suffixes exhausted)\");");
    s.push_str("\n                }");
    s.push_str("\n                __piano_file.map(|f| std::sync::Arc::new(piano_runtime::file_sink::FileSink::new(f)))");
    s.push_str("\n            }");
    s.push_str("\n            Err(e) => {");
    s.push_str("\n                let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output directory {}: {}\", __piano_dir.display(), e);");
    s.push_str("\n                None");
    s.push_str("\n            }");
    s.push_str("\n        }");
    s.push_str("\n    };");
    s.push_str(&format!(
        "\n    piano_runtime::session::ProfileSession::init(__piano_sink, {cpu_time}, &PIANO_NAMES);"
    ));
    s
}

// ---------------------------------------------------------------------------
// Macro guards: token-tree walk
// ---------------------------------------------------------------------------

/// Expand fn-generating macro_rules! invocations and replace them with
/// instrumented expansions. Expression-position macros are skipped
/// (safety fence: no fn items in expansion).
fn expand_and_replace_macros(
    root: &ra_ap_syntax::SyntaxNode,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) {
    use crate::macro_expand;

    let (expansions, _defs, calls) = macro_expand::expand_fn_generating_macros(root);

    for exp in &expansions {
        let call = &calls[exp.call_idx];

        // Inject guards into the expanded text for measured functions.
        let instrumented = inject_guards_into_expansion(&exp.expanded_text, measured);

        // Replace the MACRO_CALL byte range with the instrumented expansion.
        injector.replace(call.byte_start, call.byte_end, instrumented);
    }
}

/// Inject guards into expanded macro text. Parses the expansion, finds fn
/// items, and inserts guards for functions that are in the measured map.
fn inject_guards_into_expansion(
    expanded: &str,
    measured: &HashMap<String, u32>,
) -> String {
    let parse = SourceFile::parse(expanded, ra_ap_syntax::Edition::Edition2021);

    // Collect guard insertions: (byte_offset, guard_text)
    let mut insertions: Vec<(usize, String)> = Vec::new();

    for node in parse.tree().syntax().descendants() {
        let Some(func) = ast::Fn::cast(node) else { continue };
        let Some(body) = func.body() else { continue };
        let Some(name) = func.name() else { continue };
        let fn_name = name.text().to_string();

        let Some(&name_id) = measured.get(&fn_name) else { continue };

        // Skip const fn
        if func.const_token().is_some() { continue }

        let Some(stmt_list) = body.stmt_list() else { continue };
        let Some(open_brace) = stmt_list.syntax().children_with_tokens()
            .find(|t| t.kind() == T!['{']) else { continue };

        let offset: usize = open_brace.text_range().end().into();
        insertions.push((
            offset,
            format!("\nlet __piano_guard = piano_runtime::enter({name_id});"),
        ));
    }

    // Apply in reverse order to preserve offsets.
    insertions.sort_by(|a, b| b.0.cmp(&a.0));
    let mut result = expanded.to_string();
    for (offset, text) in &insertions {
        result.insert_str(*offset, text);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(offset, 11, "brace end offset");
        let mut s = source.to_string();
        s.insert_str(offset, "\nGUARD;");
        assert!(s.contains("{\nGUARD;"), "injection after brace. Got:\n{s}");
    }

    #[test]
    fn inner_attrs_skipped() {
        let source = "fn work() {\n    #![allow(unused)]\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        let attr_pos = result.source.find("#![allow(unused)]").unwrap();
        let guard_pos = result.source.find("piano_runtime::enter(0)").unwrap();
        assert!(guard_pos > attr_pos, "guard must come after inner attr");
    }

    #[test]
    fn injects_guard_in_simple_function() {
        let source = "fn work() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn skips_unmeasured_functions() {
        let source = "fn work() {\n    1;\n}\nfn helper() {\n    2;\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert_eq!(result.source.matches("piano_runtime::enter").count(), 1);
    }

    #[test]
    fn skips_const_fn() {
        let source = "const fn compute() -> u32 {\n    42\n}\n";
        let measured: HashMap<String, u32> = [("compute".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"));
    }

    #[test]
    fn skips_extern_c_fn() {
        let source = "extern \"C\" fn callback(x: i32) -> i32 {\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("callback".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"));
    }

    #[test]
    fn instruments_unsafe_fn() {
        let source = "unsafe fn danger() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("danger".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn instruments_trait_method_impl() {
        let source = "struct S;\nimpl std::fmt::Display for S {\n    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {\n        write!(f, \"S\")\n    }\n}\n";
        let measured: HashMap<String, u32> = [("fmt".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn no_parameter_injection() {
        let source = "fn work(x: i32) -> i32 {\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("fn work(x: i32) -> i32"));
        assert!(!result.source.contains("__piano_ctx"));
    }

    #[test]
    fn no_call_site_injection() {
        let source = "fn caller() {\n    work();\n}\nfn work() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> =
            [("caller".into(), 0), ("work".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("    work();"), "call site unchanged");
        assert!(!result.source.contains(".clone()"));
    }

    #[test]
    fn multiple_functions_get_different_ids() {
        let source = "fn a() {\n    1;\n}\nfn b() {\n    2;\n}\n";
        let measured: HashMap<String, u32> =
            [("a".into(), 0), ("b".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter(0)"));
        assert!(result.source.contains("enter(1)"));
    }

    #[test]
    fn instruments_inherent_impl_method() {
        let source = "struct S;\nimpl S {\n    fn method(&self) {\n        let x = 1;\n    }\n}\n";
        let measured: HashMap<String, u32> = [("method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
        assert!(result.source.contains("fn method(&self)"), "signature unchanged");
    }

    #[test]
    fn instruments_trait_default_method() {
        let source = "trait T {\n    fn default_impl(&self) {\n        let x = 1;\n    }\n}\n";
        let measured: HashMap<String, u32> = [("default_impl".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn skips_trait_abstract_method() {
        let source = "trait T {\n    fn abstract_method(&self);\n}\n";
        let measured: HashMap<String, u32> = [("abstract_method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"), "no body = no guard");
    }

    #[test]
    fn skips_foreign_function() {
        let source = "extern {\n    fn c_function(x: i32) -> i32;\n}\n";
        let measured: HashMap<String, u32> = [("c_function".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(!result.source.contains("piano_runtime::enter"), "foreign fn has no body");
    }

    #[test]
    fn instruments_nested_function() {
        let source = "fn outer() {\n    fn inner() {\n        let x = 1;\n    }\n    inner();\n}\n";
        let measured: HashMap<String, u32> =
            [("outer".into(), 0), ("inner".into(), 1)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter(0)"), "outer gets guard");
        assert!(result.source.contains("enter(1)"), "inner gets guard");
    }

    #[test]
    fn async_fn_uses_enter_async() {
        let source = "async fn fetch() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter_async(0"), "async fn must use enter_async");
        assert!(!result.source.contains("piano_runtime::enter(0)"), "must NOT use sync enter");
    }

    #[test]
    fn impl_future_uses_enter_async() {
        let source = "fn fetch() -> impl std::future::Future<Output = i32> {\n    async { 42 }\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter_async(0"), "impl Future must use enter_async");
    }

    #[test]
    fn impl_future_short_path() {
        let source = "fn fetch() -> impl Future<Output = ()> {\n    async {}\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter_async(0"));
    }

    #[test]
    fn sync_fn_does_not_use_enter_async() {
        let source = "fn work() -> i32 {\n    42\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
        assert!(!result.source.contains("enter_async"));
    }

    #[test]
    fn async_wrapping_preserves_body() {
        let source = "async fn fetch() {\n    let x = do_work();\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("do_work()"), "original body preserved");
        assert!(result.source.contains("async move {"), "body wrapped in async move");
    }

    // === Entry point injection tests ===

    #[test]
    fn single_pass_injects_all_entry_point_concerns() {
        let source = "fn work() {\n    1;\n}\nfn main() {\n    work();\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let ep = EntryPointParams {
            name_table: &[(0, "work")],
            runs_dir: "/tmp/runs",
            cpu_time: false,
        };
        let result = instrument_source(source, &measured, Some(&ep)).unwrap();

        // Guard in work()
        assert!(result.source.contains("piano_runtime::enter(0)"), "work gets guard");
        // Name table
        assert!(result.source.contains("PIANO_NAMES"), "name table injected");
        assert!(result.source.contains("(0, \"work\")"), "name table has work");
        // Allocator (case 1: absent)
        assert!(result.source.contains("PianoAllocator<std::alloc::System>"), "allocator injected");
        // Lifecycle in main
        assert!(result.source.contains("ProfileSession::init"), "lifecycle in main");
        // Valid syntax
        let re_parse = SourceFile::parse(&result.source, ra_ap_syntax::Edition::Edition2021);
        assert!(
            re_parse.errors().is_empty(),
            "output must be valid Rust. Errors: {:?}\nSource:\n{}",
            re_parse.errors(), result.source
        );
    }

    #[test]
    fn single_pass_wraps_existing_allocator() {
        let source = "#[global_allocator]\nstatic ALLOC: MyAlloc = MyAlloc;\n\nfn work() {\n    1;\n}\nfn main() {\n    work();\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let ep = EntryPointParams {
            name_table: &[(0, "work")],
            runs_dir: "/tmp/runs",
            cpu_time: false,
        };
        let result = instrument_source(source, &measured, Some(&ep)).unwrap();

        assert!(result.source.contains("PianoAllocator<MyAlloc>"), "allocator wrapped");
        assert!(result.source.contains("PianoAllocator::new(MyAlloc)"), "init wrapped");
        assert!(result.source.contains("piano_runtime::enter(0)"), "work gets guard");
        assert!(result.source.contains("ProfileSession::init"), "lifecycle in main");
    }

    #[test]
    fn single_pass_no_source_map_chaining() {
        let source = "fn work() {\n    1;\n}\nfn main() {\n    work();\n}\n";
        let measured: HashMap<String, u32> = [("work".into(), 0)].into_iter().collect();
        let ep = EntryPointParams {
            name_table: &[(0, "work")],
            runs_dir: "/tmp/runs",
            cpu_time: false,
        };
        let result = instrument_source(source, &measured, Some(&ep)).unwrap();

        // The source map is flat (no chain). Lines far past injections
        // should map back correctly.
        let total_injected_lines = result.source.lines().count() - source.lines().count();
        let far_line = (source.lines().count() + total_injected_lines + 10) as u32;
        let remapped = result.source_map.remap_line(far_line);
        assert!(remapped > 0, "remap of far line should be positive");
    }

    // === Exhaustive enumeration: fn parent node kinds ===

    #[test]
    fn fn_parent_kinds_exhaustive() {
        use std::collections::BTreeSet;

        let source = r#"
fn free() { let _ = 1; }
mod inner { fn in_module() { let _ = 1; } }
struct S;
impl S { fn inherent_method(&self) { let _ = 1; } }
trait T { fn trait_default(&self) { let _ = 1; } fn trait_abstract(&self); }
impl T for S { fn trait_impl(&self) { let _ = 1; } fn trait_abstract(&self) { let _ = 1; } }
extern "C" { fn foreign(x: i32) -> i32; }
fn outer() { fn nested() { let _ = 1; } }
macro_rules! m { () => { fn macro_fn() { let _ = 1; } }; }
m!();
"#;

        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let file = parse.tree();

        let mut parent_kinds: BTreeSet<SyntaxKind> = BTreeSet::new();
        for node in file.syntax().descendants() {
            if ast::Fn::cast(node.clone()).is_some() {
                if let Some(parent) = node.parent() {
                    parent_kinds.insert(parent.kind());
                }
            }
        }

        let expected: BTreeSet<SyntaxKind> = [
            SyntaxKind::SOURCE_FILE,
            SyntaxKind::ITEM_LIST,
            SyntaxKind::ASSOC_ITEM_LIST,
            SyntaxKind::EXTERN_ITEM_LIST,
            SyntaxKind::STMT_LIST,
        ].into_iter().collect();

        assert_eq!(
            parent_kinds, expected,
            "ast::Fn parent SyntaxKinds have changed.\n\
             Found: {parent_kinds:?}\nExpected: {expected:?}"
        );

        let measured: HashMap<String, u32> = [
            ("free".into(), 0),
            ("in_module".into(), 1),
            ("inherent_method".into(), 2),
            ("trait_default".into(), 3),
            ("trait_impl".into(), 4),
            ("trait_abstract".into(), 5),
            ("nested".into(), 6),
            ("outer".into(), 7),
            ("macro_fn".into(), 8),
        ].into_iter().collect();

        let result = instrument_source(source, &measured, None).unwrap();

        for (name, id) in &measured {
            if name == "foreign" { continue; }
            let pattern = format!("piano_runtime::enter({id})");
            assert!(
                result.source.contains(&pattern),
                "fn {name} (id {id}) should be instrumented"
            );
        }

        assert!(
            result.source.contains("piano_runtime::enter(8)"),
            "macro_rules fn should be instrumented"
        );
    }
}
