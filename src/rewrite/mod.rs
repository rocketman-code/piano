//! Source rewriter: single-pass zero-shift injection of profiling infrastructure.
//!
//! One CST parse. One StringInjector. One apply(). Zero line shift.
//!
//! For non-entry files: inject guards into measured function bodies.
//! For the entry point: guards + name table + allocator wrapping + lifecycle.
//! All injection points are collected from the ORIGINAL source, then applied
//! in one pass. Injections go on the same line as the opening brace (no new
//! lines added). File-level items appended after user code.

use std::collections::HashMap;

use ra_ap_syntax::ast::{HasAttrs, HasModuleItem, HasName};
use ra_ap_syntax::{AstNode, SourceFile, SyntaxKind, T, ast};

use crate::source_map::StringInjector;

pub struct InstrumentResult {
    pub source: String,
}

/// Parameters for entry-point-only injections (name table, allocator, lifecycle).
pub struct EntryPointParams<'a> {
    pub name_table: &'a [(u32, &'a str)],
    pub runs_dir: &'a str,
    pub cpu_time: bool,
}

// ── Function classification pipeline ─────────────────────────────
//
// Each classification step produces a typed result.
// Transformation functions accept only the correct type.

struct Instrumentable {
    func: ast::Fn,
    stmt_list: ast::StmtList,
}

struct Selected {
    inner: Instrumentable,
    name_id: u32,
    fn_name: String,
}

struct ClassifiedAsync(Selected);
struct ClassifiedImplFuture(Selected);
struct ClassifiedSync(Selected);

fn detect_instrumentable(func: ast::Fn) -> Option<Instrumentable> {
    let classification = crate::resolve::classify_cst_fn(&func);
    if classification != crate::resolve::Classification::Instrumentable {
        return None;
    }
    let body = func.body()?;
    let stmt_list = body.stmt_list()?;
    Some(Instrumentable { func, stmt_list })
}

fn apply_filter(
    inst: Instrumentable,
    fn_name: String,
    measured: &HashMap<String, u32>,
) -> Option<Selected> {
    let &name_id = measured.get(&fn_name)?;
    Some(Selected {
        inner: inst,
        name_id,
        fn_name,
    })
}

enum FunctionKind {
    Async(ClassifiedAsync),
    ImplFuture(ClassifiedImplFuture),
    Sync(ClassifiedSync),
}

fn classify(sel: Selected) -> FunctionKind {
    if sel.inner.func.async_token().is_some() {
        FunctionKind::Async(ClassifiedAsync(sel))
    } else if returns_impl_future(&sel.inner.func) {
        FunctionKind::ImplFuture(ClassifiedImplFuture(sel))
    } else {
        FunctionKind::Sync(ClassifiedSync(sel))
    }
}

fn find_close_brace(stmt_list: &ast::StmtList, fn_name: &str) -> Result<usize, String> {
    let close_brace = stmt_list
        .syntax()
        .children_with_tokens()
        .filter(|t| t.kind() == T!['}'])
        .last()
        .ok_or_else(|| format!("no closing brace for fn {fn_name}"))?;
    Ok(close_brace.text_range().start().into())
}

fn inject_sync_guard(injector: &mut StringInjector, func: &ClassifiedSync, offset: usize) {
    injector.insert(
        offset,
        format!(
            " let __piano_guard = piano_runtime::enter({});",
            func.0.name_id
        ),
    );
}

fn inject_async_wrap(
    injector: &mut StringInjector,
    func: &ClassifiedAsync,
    offset: usize,
) -> Result<(), String> {
    let close_offset = find_close_brace(&func.0.inner.stmt_list, &func.0.fn_name)?;
    injector.insert(
        offset,
        format!(
            " piano_runtime::enter_async({}, async move {{",
            func.0.name_id
        ),
    );
    injector.insert(close_offset, "}).await");
    Ok(())
}

fn inject_impl_future_wrap(
    injector: &mut StringInjector,
    func: &ClassifiedImplFuture,
    offset: usize,
) -> Result<(), String> {
    let close_offset = find_close_brace(&func.0.inner.stmt_list, &func.0.fn_name)?;
    injector.insert(
        offset,
        format!(" piano_runtime::enter_async({},", func.0.name_id),
    );
    injector.insert(close_offset, ")");
    Ok(())
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

    // --- Entry point: registrations appended after user code ---
    if let Some(ep) = entry_point {
        let mut entries = String::new();
        for (id, name) in ep.name_table {
            if !entries.is_empty() {
                entries.push_str(", ");
            }
            entries.push_str(&format!("({id}, \"{name}\")"));
        }
        injector.insert(
            source.len(),
            format!("\nconst PIANO_NAMES: &[(u32, &str)] = &[{entries}];\n"),
        );
    }

    // --- Entry point: allocator detection and wrapping ---
    if entry_point.is_some() {
        inject_allocator(&file, source, &mut injector)?;
    }

    // --- Guards + shutdown: structured walk with scope tracking ---
    let mut scope = crate::naming::ScopeState::new();
    walk_for_guards(&file, &mut scope, measured, &mut injector, entry_point)?;

    // --- Macro expansion: replace fn-generating invocations ---
    expand_and_replace_macros(file.syntax(), measured, &mut injector);

    let source = injector.apply(source);
    Ok(InstrumentResult { source })
}

/// Find the injection offset inside a statement list: after the opening
/// brace and any inner attributes.
fn brace_offset_after_inner_attrs(stmt_list: &ast::StmtList) -> Result<usize, String> {
    let open_brace = stmt_list
        .syntax()
        .children_with_tokens()
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

// ---------------------------------------------------------------------------
// Structured walk with scope tracking (mirrors resolve.rs FnCollector)
// ---------------------------------------------------------------------------

/// Walk a source file structurally, tracking scope to produce full qualified
/// names that match resolve.rs output.
fn walk_for_guards(
    file: &SourceFile,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
    entry_point: Option<&EntryPointParams<'_>>,
) -> Result<(), String> {
    for item in file.items() {
        walk_item_for_guards(&item, scope, measured, injector, entry_point)?;
    }
    Ok(())
}

fn walk_item_for_guards(
    item: &ast::Item,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
    entry_point: Option<&EntryPointParams<'_>>,
) -> Result<(), String> {
    match item {
        ast::Item::Module(module) => {
            let mod_name = module
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_else(|| "_".to_string());
            scope.push_mod(&mod_name);
            if let Some(item_list) = module.item_list() {
                for child_item in item_list.items() {
                    walk_item_for_guards(&child_item, scope, measured, injector, entry_point)?;
                }
            }
            scope.pop();
        }
        ast::Item::Fn(func) => {
            visit_top_level_fn_for_guards(func, scope, measured, injector, entry_point)?;
        }
        ast::Item::Impl(imp) => {
            visit_impl_for_guards(imp, scope, measured, injector)?;
        }
        ast::Item::Trait(tr) => {
            visit_trait_for_guards(tr, scope, measured, injector)?;
        }
        _ => {}
    }
    Ok(())
}

fn visit_top_level_fn_for_guards(
    func: &ast::Fn,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
    entry_point: Option<&EntryPointParams<'_>>,
) -> Result<(), String> {
    let bare_name = crate::naming::qualified_name_for_fn(func);

    // Handle fn main() lifecycle injection
    if bare_name == "main" {
        if let Some(ep) = entry_point {
            let body = func
                .body()
                .ok_or_else(|| "no body for fn main".to_string())?;
            let stmt_list = body
                .stmt_list()
                .ok_or_else(|| "no stmt_list for fn main".to_string())?;
            let inject_offset = brace_offset_after_inner_attrs(&stmt_list)?;
            injector.insert(
                inject_offset,
                build_lifecycle_prefix(ep.runs_dir, ep.cpu_time),
            );
        }
    } else {
        // Instrument this function using full scoped name
        let full_name = scope.render_full(&bare_name);
        try_inject_guard(func.clone(), full_name, measured, injector)?;
    }

    // Push fn scope and walk body for nested items
    let fn_name = func
        .name()
        .map(|n| n.text().to_string())
        .unwrap_or_default();
    scope.push_fn(&fn_name);
    if let Some(body) = func.body() {
        walk_fn_body_for_guards(body, scope, measured, injector)?;
    }
    scope.pop();
    Ok(())
}

fn visit_impl_for_guards(
    imp: &ast::Impl,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    if let Some(assoc_list) = imp.assoc_item_list() {
        for assoc in assoc_list.assoc_items() {
            if let ast::AssocItem::Fn(func) = assoc {
                visit_impl_fn_for_guards(&func, scope, measured, injector)?;
            }
        }
    }
    Ok(())
}

fn visit_impl_fn_for_guards(
    func: &ast::Fn,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    let bare_qualified = crate::naming::qualified_name_for_fn(func);
    let full_name = scope.render_full(&bare_qualified);
    try_inject_guard(func.clone(), full_name, measured, injector)?;

    // Push fn scope and walk body for nested items
    let fn_name = func
        .name()
        .map(|n| n.text().to_string())
        .unwrap_or_default();
    scope.push_fn(&fn_name);
    if let Some(body) = func.body() {
        walk_fn_body_for_guards(body, scope, measured, injector)?;
    }
    scope.pop();
    Ok(())
}

fn visit_trait_for_guards(
    tr: &ast::Trait,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    if let Some(assoc_list) = tr.assoc_item_list() {
        for assoc in assoc_list.assoc_items() {
            if let ast::AssocItem::Fn(func) = assoc {
                if func.body().is_some() {
                    let bare_qualified = crate::naming::qualified_name_for_fn(&func);
                    let full_name = scope.render_full(&bare_qualified);
                    try_inject_guard(func.clone(), full_name, measured, injector)?;

                    let fn_name = func
                        .name()
                        .map(|n| n.text().to_string())
                        .unwrap_or_default();
                    scope.push_fn(&fn_name);
                    if let Some(body) = func.body() {
                        walk_fn_body_for_guards(body, scope, measured, injector)?;
                    }
                    scope.pop();
                }
            }
        }
    }
    Ok(())
}

/// Walk a function body to find nested items, tracking BLOCK_EXPR boundaries.
fn walk_fn_body_for_guards(
    body: ast::BlockExpr,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    let Some(stmt_list) = body.stmt_list() else {
        return Ok(());
    };
    for node in stmt_list.syntax().children() {
        walk_body_child_for_guards(node, scope, measured, injector)?;
    }
    Ok(())
}

fn walk_body_child_for_guards(
    node: ra_ap_syntax::SyntaxNode,
    scope: &mut crate::naming::ScopeState,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    let kind = node.kind();
    if kind == SyntaxKind::FN {
        if let Some(inner_fn) = ast::Fn::cast(node) {
            let bare_qualified = crate::naming::qualified_name_for_fn(&inner_fn);
            let full_name = scope.render_full(&bare_qualified);
            try_inject_guard(inner_fn.clone(), full_name, measured, injector)?;

            // Recurse into nested fn's body
            scope.push_fn(&bare_qualified);
            if let Some(inner_body) = inner_fn.body() {
                walk_fn_body_for_guards(inner_body, scope, measured, injector)?;
            }
            scope.pop();
        }
    } else if kind == SyntaxKind::BLOCK_EXPR {
        if let Some(inner_block) = ast::BlockExpr::cast(node) {
            scope.push_block();
            walk_fn_body_for_guards(inner_block, scope, measured, injector)?;
            scope.pop();
        }
    } else if kind == SyntaxKind::IMPL {
        if let Some(imp) = ast::Impl::cast(node) {
            visit_impl_for_guards(&imp, scope, measured, injector)?;
        }
    } else {
        for child in node.children() {
            walk_body_child_for_guards(child, scope, measured, injector)?;
        }
    }
    Ok(())
}

/// Try to inject a guard into a function. Uses the shared classification pipeline.
fn try_inject_guard(
    func: ast::Fn,
    full_name: String,
    measured: &HashMap<String, u32>,
    injector: &mut StringInjector,
) -> Result<(), String> {
    let inst = match detect_instrumentable(func) {
        Some(i) => i,
        None => return Ok(()),
    };
    let sel = match apply_filter(inst, full_name, measured) {
        Some(s) => s,
        None => return Ok(()),
    };

    let inject_offset = brace_offset_after_inner_attrs(&sel.inner.stmt_list)?;

    match classify(sel) {
        FunctionKind::Async(async_fn) => {
            inject_async_wrap(injector, &async_fn, inject_offset)?;
        }
        FunctionKind::ImplFuture(impl_fn) => {
            inject_impl_future_wrap(injector, &impl_fn, inject_offset)?;
        }
        FunctionKind::Sync(sync_fn) => {
            inject_sync_guard(injector, &sync_fn, inject_offset);
        }
    }
    Ok(())
}

/// Check if a function's return type contains `impl Future`.
fn returns_impl_future(func: &ast::Fn) -> bool {
    let Some(ret) = func.ret_type() else {
        return false;
    };
    let Some(ty) = ret.ty() else { return false };
    let ast::Type::ImplTraitType(impl_trait) = ty else {
        return false;
    };
    let Some(bounds) = impl_trait.type_bound_list() else {
        return false;
    };

    for bound in bounds.bounds() {
        let Some(bound_ty) = bound.ty() else { continue };
        let ast::Type::PathType(path_ty) = bound_ty else {
            continue;
        };
        let Some(path) = path_ty.path() else { continue };
        for segment in path.segments() {
            if let Some(name_ref) = segment.name_ref() {
                if name_ref.text() == "Future" {
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Allocator injection (CST-based, single-pass)
// ---------------------------------------------------------------------------

/// Detect #[global_allocator] via structural AST analysis and inject wrapping
/// into the StringInjector.
///
/// Case 1 (absent): insert PianoAllocator<System> at end of file.
/// Case 2 (unconditional): replace the static item with wrapped version.
/// Case 3 (cfg-gated): replace with wrapped + negated cfg fallback.
/// Case 4 (cfg_attr-applied): same wrapping strategy as cfg-gated.
fn inject_allocator(
    file: &SourceFile,
    source: &str,
    injector: &mut StringInjector,
) -> Result<(), String> {
    let classification = find_global_allocator(file, source);

    match classification {
        None => {
            // Case 1: no allocator
            injector.insert(
                source.len(),
                concat!(
                    "\n#[global_allocator]\n",
                    "static __PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n",
                    "    = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
                ),
            );
        }
        Some(AllocatorClassification::Unconditional(info)) => {
            // Case 2: direct #[global_allocator], no cfg
            let replacement = format!(
                "#[global_allocator]\n\
                 static {name}: piano_runtime::PianoAllocator<{ty}> = piano_runtime::PianoAllocator::new({init});",
                name = info.name,
                ty = info.type_expr,
                init = info.init_expr,
            );
            injector.replace(info.start, info.end, replacement);
        }
        Some(AllocatorClassification::CfgGated { info, cfg_text }) => {
            // Case 3: #[cfg(pred)] #[global_allocator]
            let neg_cfg = negate_cfg(&cfg_text);
            let original_text = &source[info.start..info.end];
            let original_newlines = original_text.chars().filter(|&c| c == '\n').count();

            let wrapped = format!(
                "{cfg_text}\n#[global_allocator]\nstatic {name}: piano_runtime::PianoAllocator<{ty}> = piano_runtime::PianoAllocator::new({init});",
                name = info.name,
                ty = info.type_expr,
                init = info.init_expr,
            );
            let wrapped_newlines = wrapped.chars().filter(|&c| c == '\n').count();
            let padded = if wrapped_newlines < original_newlines {
                format!(
                    "{wrapped}{}",
                    "\n".repeat(original_newlines - wrapped_newlines)
                )
            } else {
                wrapped
            };

            injector.replace(info.start, info.end, padded);
            injector.insert(
                source.len(),
                format!(
                    "\n{neg_cfg}\n#[global_allocator]\nstatic {name}: piano_runtime::PianoAllocator<std::alloc::System> = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
                    name = info.name,
                ),
            );
        }
        Some(AllocatorClassification::CfgAttrApplied {
            info,
            cfg_attr_text,
        }) => {
            // Case 4: #[cfg_attr(pred, global_allocator)]
            let neg_cfg = negate_cfg(&cfg_attr_text);
            let original_text = &source[info.start..info.end];
            let original_newlines = original_text.chars().filter(|&c| c == '\n').count();

            let wrapped = format!(
                "{cfg_attr_text}\nstatic {name}: piano_runtime::PianoAllocator<{ty}> = piano_runtime::PianoAllocator::new({init});",
                name = info.name,
                ty = info.type_expr,
                init = info.init_expr,
            );
            let wrapped_newlines = wrapped.chars().filter(|&c| c == '\n').count();
            let padded = if wrapped_newlines < original_newlines {
                format!(
                    "{wrapped}{}",
                    "\n".repeat(original_newlines - wrapped_newlines)
                )
            } else {
                wrapped
            };

            injector.replace(info.start, info.end, padded);
            injector.insert(
                source.len(),
                format!(
                    "\n{neg_cfg}\n#[global_allocator]\nstatic {name}: piano_runtime::PianoAllocator<std::alloc::System> = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
                    name = info.name,
                ),
            );
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
}

enum AllocatorClassification {
    Unconditional(AllocatorInfo),
    CfgGated {
        info: AllocatorInfo,
        cfg_text: String,
    },
    CfgAttrApplied {
        info: AllocatorInfo,
        cfg_attr_text: String,
    },
}

fn extract_static_info(static_item: &ast::Static, source: &str) -> Option<AllocatorInfo> {
    let start: usize = static_item.syntax().text_range().start().into();
    let end: usize = static_item.syntax().text_range().end().into();
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

    Some(AllocatorInfo {
        start,
        end,
        name,
        type_expr,
        init_expr,
    })
}

/// Find a static item with #[global_allocator] using structural AST analysis.
fn find_global_allocator(file: &SourceFile, source: &str) -> Option<AllocatorClassification> {
    for node in file.syntax().descendants() {
        let Some(static_item) = ast::Static::cast(node) else {
            continue;
        };

        let mut has_direct_global_alloc = false;
        let mut cfg_gate: Option<String> = None;
        let mut cfg_attr_global_alloc: Option<String> = None;

        for attr in static_item.attrs() {
            if attr.kind().is_inner() {
                continue;
            }
            let Some(name) = attr.simple_name() else {
                continue;
            };

            match name.as_str() {
                "global_allocator" => {
                    has_direct_global_alloc = true;
                }
                "cfg" => {
                    cfg_gate = Some(attr.syntax().text().to_string());
                }
                "cfg_attr" => {
                    let text = attr.syntax().text().to_string();
                    if text.contains("global_allocator") {
                        cfg_attr_global_alloc = Some(text);
                    }
                }
                _ => {}
            }
        }

        if has_direct_global_alloc {
            let info = extract_static_info(&static_item, source)?;
            if let Some(cfg) = cfg_gate {
                return Some(AllocatorClassification::CfgGated {
                    info,
                    cfg_text: cfg,
                });
            }
            return Some(AllocatorClassification::Unconditional(info));
        }

        if let Some(text) = cfg_attr_global_alloc {
            let info = extract_static_info(&static_item, source)?;
            return Some(AllocatorClassification::CfgAttrApplied {
                info,
                cfg_attr_text: text,
            });
        }
    }
    None
}

/// Negate a #[cfg(...)] attribute to #[cfg(not(...))].
fn negate_cfg(cfg: &str) -> String {
    if let Some(inner) = cfg
        .strip_prefix("#[cfg(")
        .and_then(|s| s.strip_suffix(")]"))
    {
        format!("#[cfg(not({inner}))]")
    } else if let Some(rest) = cfg.strip_prefix("#[cfg_attr(") {
        // Extract predicate from #[cfg_attr(PRED, ...)] using depth-aware parsing
        let mut depth = 0u32;
        let mut split_pos = None;
        for (i, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                ',' if depth == 0 => {
                    split_pos = Some(i);
                    break;
                }
                _ => {}
            }
        }
        if let Some(pos) = split_pos {
            let predicate = rest[..pos].trim();
            format!("#[cfg(not({predicate}))]")
        } else {
            "#[cfg(not(any()))]".to_string()
        }
    } else {
        "#[cfg(not(any()))]".to_string()
    }
}

// ---------------------------------------------------------------------------
// Lifecycle injection (shutdown)
// ---------------------------------------------------------------------------

const FILE_COLLISION_RETRIES: u32 = 4;

/// Build the lifecycle code injected on the same line as main()'s opening brace.
///
/// Zero-shift: all statements on one line (no newlines).
fn build_lifecycle_prefix(runs_dir: &str, cpu_time: bool) -> String {
    let mut s = String::new();
    s.push_str(" #[allow(unused_imports)] use std::io::Write as _;");
    s.push_str(" let __piano_ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis();");
    s.push_str(" let __piano_pid = std::process::id();");
    s.push_str(" let __piano_sink = {");
    s.push_str(&format!(
        " let __piano_dir = std::path::PathBuf::from(std::env::var(\"PIANO_RUNS_DIR\").unwrap_or_else(|_| \"{runs_dir}\".into()));"
    ));
    s.push_str(" match std::fs::create_dir_all(&__piano_dir) {");
    s.push_str(" Ok(()) => {");
    s.push_str(" let mut __piano_file = None;");
    s.push_str(" let mut __piano_warned = false;");
    s.push_str(&format!(
        " for __piano_suffix in 0u32..{FILE_COLLISION_RETRIES} {{"
    ));
    s.push_str(" let __piano_name = if __piano_suffix == 0 {");
    s.push_str(" format!(\"{}-{}.ndjson\", __piano_ts, __piano_pid)");
    s.push_str(" } else {");
    s.push_str(" format!(\"{}-{}-{}.ndjson\", __piano_ts, __piano_pid, __piano_suffix)");
    s.push_str(" };");
    s.push_str(" let __piano_path = __piano_dir.join(&__piano_name);");
    s.push_str(
        " match std::fs::OpenOptions::new().write(true).create_new(true).open(&__piano_path) {",
    );
    s.push_str(" Ok(f) => { __piano_file = Some(f); break; }");
    s.push_str(" Err(_) => {}");
    s.push_str(" }");
    s.push_str(" }");
    s.push_str(" if __piano_file.is_none() && !__piano_warned {");
    s.push_str(" let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output (all suffixes exhausted)\");");
    s.push_str(" }");
    s.push_str(
        " __piano_file.map(|f| std::sync::Arc::new(piano_runtime::file_sink::FileSink::new(f)))",
    );
    s.push_str(" }");
    s.push_str(" Err(e) => {");
    s.push_str(" let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output directory {}: {}\", __piano_dir.display(), e);");
    s.push_str(" None");
    s.push_str(" }");
    s.push_str(" }");
    s.push_str(" };");
    s.push_str(" let __piano_run_id = format!(\"{}-{}\", __piano_ts, __piano_pid);");
    s.push_str(&format!(
        " piano_runtime::session::ProfileSession::init(__piano_sink, {cpu_time}, &PIANO_NAMES, &__piano_run_id, __piano_ts);"
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

        let impl_prefix = crate::naming::impl_prefix_at(root, call.byte_start);
        let instrumented =
            inject_guards_into_expansion(&exp.expanded_text, measured, impl_prefix.as_deref());

        injector.replace(call.byte_start, call.byte_end, instrumented);
    }
}

/// Inject guards into expanded macro text. Uses the shared classification
/// pipeline (detect_instrumentable -> apply_filter -> classify) to ensure
/// consistent behavior with the main instrument_source loop.
fn inject_guards_into_expansion(
    expanded: &str,
    measured: &HashMap<String, u32>,
    impl_prefix: Option<&str>,
) -> String {
    let parse = SourceFile::parse(expanded, ra_ap_syntax::Edition::Edition2021);
    let mut insertions: Vec<(usize, String)> = Vec::new();

    for node in parse.tree().syntax().descendants() {
        let Some(func) = ast::Fn::cast(node) else {
            continue;
        };

        let bare_name = crate::naming::qualified_name_for_fn(&func);
        let fn_name = match impl_prefix {
            Some(prefix) => format!("{prefix}::{bare_name}"),
            None => bare_name,
        };

        let inst = match detect_instrumentable(func) {
            Some(i) => i,
            None => continue,
        };
        let sel = match apply_filter(inst, fn_name, measured) {
            Some(s) => s,
            None => continue,
        };

        let Some(open_brace) = sel
            .inner
            .stmt_list
            .syntax()
            .children_with_tokens()
            .find(|t| t.kind() == T!['{'])
        else {
            continue;
        };
        let offset: usize = open_brace.text_range().end().into();

        match classify(sel) {
            FunctionKind::Async(async_fn) => {
                let Ok(close_offset) =
                    find_close_brace(&async_fn.0.inner.stmt_list, &async_fn.0.fn_name)
                else {
                    continue;
                };
                insertions.push((
                    offset,
                    format!(
                        " piano_runtime::enter_async({}, async move {{",
                        async_fn.0.name_id
                    ),
                ));
                insertions.push((close_offset, "}).await".to_string()));
            }
            FunctionKind::ImplFuture(impl_fn) => {
                let Ok(close_offset) =
                    find_close_brace(&impl_fn.0.inner.stmt_list, &impl_fn.0.fn_name)
                else {
                    continue;
                };
                insertions.push((
                    offset,
                    format!(" piano_runtime::enter_async({},", impl_fn.0.name_id),
                ));
                insertions.push((close_offset, ")".to_string()));
            }
            FunctionKind::Sync(sync_fn) => {
                insertions.push((
                    offset,
                    format!(
                        " let __piano_guard = piano_runtime::enter({});",
                        sync_fn.0.name_id
                    ),
                ));
            }
        }
    }

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
    use ra_ap_syntax::ast::HasName;

    #[test]
    fn cst_finds_function() {
        let source = "fn work() {\n    let x = 1;\n}\n";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let file = parse.tree();
        let funcs: Vec<_> = file
            .syntax()
            .descendants()
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
        let func = file.syntax().descendants().find_map(ast::Fn::cast).unwrap();
        let body = func.body().unwrap();
        let stmt_list = body.stmt_list().unwrap();
        let brace = stmt_list
            .syntax()
            .children_with_tokens()
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
    fn skips_bare_extern_fn() {
        let source = "extern fn callback(x: i32) -> i32 {\n    x + 1\n}\n";
        let measured: HashMap<String, u32> = [("callback".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            !result.source.contains("piano_runtime::enter"),
            "bare extern fn has C ABI and must not be instrumented. Got:\n{}",
            result.source,
        );
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
        let measured: HashMap<String, u32> =
            [("<S as Display>::fmt".into(), 0)].into_iter().collect();
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
        let measured: HashMap<String, u32> = [("caller".into(), 0), ("work".into(), 1)]
            .into_iter()
            .collect();
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
        let measured: HashMap<String, u32> = [("S::method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
        assert!(
            result.source.contains("fn method(&self)"),
            "signature unchanged"
        );
    }

    #[test]
    fn instruments_trait_default_method() {
        let source = "trait T {\n    fn default_impl(&self) {\n        let x = 1;\n    }\n}\n";
        let measured: HashMap<String, u32> = [("T::default_impl".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("piano_runtime::enter(0)"));
    }

    #[test]
    fn skips_trait_abstract_method() {
        let source = "trait T {\n    fn abstract_method(&self);\n}\n";
        let measured: HashMap<String, u32> = [("abstract_method".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            !result.source.contains("piano_runtime::enter"),
            "no body = no guard"
        );
    }

    #[test]
    fn skips_foreign_function() {
        let source = "extern {\n    fn c_function(x: i32) -> i32;\n}\n";
        let measured: HashMap<String, u32> = [("c_function".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            !result.source.contains("piano_runtime::enter"),
            "foreign fn has no body"
        );
    }

    #[test]
    fn instruments_nested_function() {
        let source = "fn outer() {\n    fn inner() {\n        let x = 1;\n    }\n    inner();\n}\n";
        let measured: HashMap<String, u32> = [("outer".into(), 0), ("outer::inner".into(), 1)]
            .into_iter()
            .collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(result.source.contains("enter(0)"), "outer gets guard");
        assert!(result.source.contains("enter(1)"), "inner gets guard");
    }

    #[test]
    fn async_fn_uses_enter_async() {
        let source = "async fn fetch() {\n    let x = 1;\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            result.source.contains("enter_async(0"),
            "async fn must use enter_async"
        );
        assert!(
            !result.source.contains("piano_runtime::enter(0)"),
            "must NOT use sync enter"
        );
    }

    #[test]
    fn impl_future_uses_enter_async() {
        let source =
            "fn fetch() -> impl std::future::Future<Output = i32> {\n    async { 42 }\n}\n";
        let measured: HashMap<String, u32> = [("fetch".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            result.source.contains("enter_async(0"),
            "impl Future must use enter_async"
        );
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
        assert!(
            result.source.contains("do_work()"),
            "original body preserved"
        );
        assert!(
            result.source.contains("async move {"),
            "body wrapped in async move"
        );
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
        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "work gets guard"
        );
        // Name table
        assert!(result.source.contains("PIANO_NAMES"), "name table injected");
        assert!(
            result.source.contains("(0, \"work\")"),
            "name table has work"
        );
        // Allocator (case 1: absent)
        assert!(
            result.source.contains("PianoAllocator<std::alloc::System>"),
            "allocator injected"
        );
        // Lifecycle in main
        assert!(
            result.source.contains("ProfileSession::init"),
            "lifecycle in main"
        );
        // Valid syntax
        let re_parse = SourceFile::parse(&result.source, ra_ap_syntax::Edition::Edition2021);
        assert!(
            re_parse.errors().is_empty(),
            "output must be valid Rust. Errors: {:?}\nSource:\n{}",
            re_parse.errors(),
            result.source
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

        assert!(
            result.source.contains("PianoAllocator<MyAlloc>"),
            "allocator wrapped"
        );
        assert!(
            result.source.contains("PianoAllocator::new(MyAlloc)"),
            "init wrapped"
        );
        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "work gets guard"
        );
        assert!(
            result.source.contains("ProfileSession::init"),
            "lifecycle in main"
        );
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
        ]
        .into_iter()
        .collect();

        assert_eq!(
            parent_kinds, expected,
            "ast::Fn parent SyntaxKinds have changed.\n\
             Found: {parent_kinds:?}\nExpected: {expected:?}"
        );

        let measured: HashMap<String, u32> = [
            ("free".into(), 0),
            ("inner::in_module".into(), 1),
            ("S::inherent_method".into(), 2),
            ("T::trait_default".into(), 3),
            ("<S as T>::trait_impl".into(), 4),
            ("<S as T>::trait_abstract".into(), 5),
            ("outer::nested".into(), 6),
            ("outer".into(), 7),
            ("macro_fn".into(), 8),
        ]
        .into_iter()
        .collect();

        let result = instrument_source(source, &measured, None).unwrap();

        for (name, id) in &measured {
            if name == "foreign" {
                continue;
            }
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

    #[test]
    fn macro_metavar_fn_gets_guard() {
        let source = r#"
macro_rules! make_fn { ($name:ident) => { fn $name() { let _ = 1; } }; }
make_fn!(dynamic_fn);
fn main() {}
"#;
        let measured: HashMap<String, u32> = [("dynamic_fn".into(), 0)].into_iter().collect();
        let result =
            instrument_source(source, &measured, None).expect("instrument_source should succeed");

        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "metavar-expanded fn should get a guard. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn macro_unmeasured_fn_not_guarded() {
        let source = r#"
macro_rules! make_fn { ($name:ident) => { fn $name() { let _ = 1; } }; }
make_fn!(unlisted_fn);
fn main() {}
"#;
        // unlisted_fn is NOT in the measured map.
        let measured: HashMap<String, u32> = HashMap::new();
        let result =
            instrument_source(source, &measured, None).expect("instrument_source should succeed");

        assert!(
            !result.source.contains("piano_runtime::enter"),
            "unmeasured macro fn should not get a guard. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn macro_const_fn_not_guarded() {
        let source = r#"
macro_rules! make_const { () => { const fn fixed() -> u32 { 42 } }; }
make_const!();
fn main() {}
"#;
        let measured: HashMap<String, u32> = [("fixed".into(), 0)].into_iter().collect();
        let result =
            instrument_source(source, &measured, None).expect("instrument_source should succeed");

        assert!(
            !result.source.contains("piano_runtime::enter"),
            "const fn from macro should be skipped. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn macro_fn_in_impl_block_gets_guard() {
        let source = r#"
struct S;
macro_rules! make_method { () => { fn method(&self) { let _ = 1; } }; }
impl S {
    make_method!();
}
fn main() {}
"#;
        let measured: HashMap<String, u32> = [("S::method".into(), 0)].into_iter().collect();
        let result =
            instrument_source(source, &measured, None).expect("instrument_source should succeed");

        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "macro-expanded fn inside impl should be instrumented with qualified name S::method. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn macro_mixed_measured_unmeasured() {
        let source = r#"
macro_rules! pair { () => { fn tracked() { let _ = 1; } fn untracked() { let _ = 2; } }; }
pair!();
fn main() {}
"#;
        // Only "tracked" is in the measured map.
        let measured: HashMap<String, u32> = [("tracked".into(), 0)].into_iter().collect();
        let result =
            instrument_source(source, &measured, None).expect("instrument_source should succeed");

        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "measured fn should get a guard. Got:\n{}",
            result.source
        );
        assert_eq!(
            result.source.matches("piano_runtime::enter").count(),
            1,
            "only the measured fn should get a guard, not both. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn impl_future_standard_path() {
        let source = "fn f() -> impl std::future::Future<Output = i32> { async { 42 } }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(returns_impl_future(&func));
    }

    #[test]
    fn impl_future_short_name() {
        let source = "fn f() -> impl Future<Output = ()> { async {} }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(returns_impl_future(&func));
    }

    #[test]
    fn impl_future_with_clone_bound() {
        let source = "fn f() -> impl Clone + Future<Output = i32> { async { 42 } }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(returns_impl_future(&func));
    }

    #[test]
    fn impl_iterator_of_futures_is_not_impl_future() {
        let source = "fn f() -> impl Iterator<Item = impl Future<Output = i32>> { todo!() }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(
            !returns_impl_future(&func),
            "nested impl Future in generic param is not top-level impl Future"
        );
    }

    #[test]
    fn box_dyn_future_is_not_impl_future() {
        let source = "fn f() -> Box<dyn Future<Output = i32>> { todo!() }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(!returns_impl_future(&func));
    }

    #[test]
    fn no_return_type_is_not_impl_future() {
        let source = "fn f() { }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(!returns_impl_future(&func));
    }

    #[test]
    fn plain_return_type_is_not_impl_future() {
        let source = "fn f() -> i32 { 42 }";
        let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
        let func = parse
            .tree()
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .unwrap();
        assert!(!returns_impl_future(&func));
    }

    #[test]
    fn impl_iterator_of_futures_gets_sync_guard() {
        let source = r#"fn make_futures() -> impl Iterator<Item = impl std::future::Future<Output = i32>> {
    vec![async { 42 }].into_iter()
}
"#;
        let measured: HashMap<String, u32> = [("make_futures".into(), 0)].into_iter().collect();
        let result = instrument_source(source, &measured, None).unwrap();
        assert!(
            result.source.contains("piano_runtime::enter(0)"),
            "impl Iterator<Item = impl Future> should get sync guard, not PianoFuture wrap. Got:\n{}",
            result.source,
        );
        assert!(
            !result.source.contains("enter_async"),
            "must NOT use enter_async for impl Iterator. Got:\n{}",
            result.source,
        );
    }
}
