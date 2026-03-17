pub(crate) mod allocator;
pub(crate) mod macro_rules;
pub(crate) mod shutdown;

use std::collections::{HashMap, HashSet};

use ra_ap_syntax::ast::HasArgList;
use ra_ap_syntax::ast::HasAttrs;
use ra_ap_syntax::ast::HasGenericArgs;
use ra_ap_syntax::ast::HasModuleItem;
use ra_ap_syntax::ast::HasName;
use ra_ap_syntax::{AstNode, AstToken, Edition, SyntaxKind, ast};
use quote::ToTokens;
use syn::visit_mut::VisitMut;

use crate::resolve::{Classification, classify};
use crate::source_map::{SourceMap, StringInjector};

pub use allocator::{AllocatorKind, detect_allocator_kind, inject_global_allocator};
pub use shutdown::inject_shutdown;

pub use macro_rules::discover_macro_fn_names;
use macro_rules::MacroInstrumenter;

/// Result of instrumenting a source file.
pub struct InstrumentResult {
    pub source: String,
    pub source_map: SourceMap,
    /// Functions that contain concurrency patterns, with the pattern name.
    /// e.g. [("concurrent_discover", "rayon::scope"), ("process_all", "par_iter")]
    pub concurrency: Vec<(String, String)>,
    /// Literal function names found in macro_rules! bodies.
    /// Metavar names are excluded -- they resolve at macro expansion time.
    pub macro_fn_names: Vec<String>,
}

/// Rewrite `source` so that every function whose qualified name is in
/// `measured` gets an RAII timing guard injected. All instrumentable functions
/// (not just measured ones) receive a `__piano_ctx` parameter for context
/// propagation.
///
/// Top-level functions match by bare name (e.g. "walk"). Impl methods match by
/// "Type::method" (e.g. "Walker::walk"). Trait default methods match by "Trait::method" (e.g. "Drawable::draw").
///
pub fn instrument_source(
    source: &str,
    measured: &HashMap<String, u32>,
    all_instrumentable: &HashSet<String>,
    instrument_macros: bool,
    module_prefix: &str,
    macro_name_ids: &HashMap<String, u32>,
) -> Result<InstrumentResult, String> {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();

    // Collect injection points via CST walk.
    let mut collector = InjectionCollector {
        source,
        measured: measured.clone(),
        all_instrumentable: all_instrumentable.clone(),
        injector: StringInjector::new(),
        current_impl: None,
        current_trait: None,
        concurrency: Vec::new(),
        module_prefix: module_prefix.to_string(),
        mod_path: Vec::new(),
    };
    collector.walk_file(&file);

    let (rewritten, source_map) = collector.injector.apply(source);

    // Macro instrumentation uses token-stream mutation (separate from the
    // string-injection path). Apply it on the rewritten source if needed.
    // Token-stream rendering invalidates our source_map,
    // so we reset it to empty when this path is taken.
    let (final_source, final_map, macro_fn_names) = if instrument_macros {
        let mut rewritten_file: syn::File =
            syn::parse_str(&rewritten).map_err(|e| e.to_string())?;
        let mut macro_visitor = MacroInstrumenter {
            module_prefix: module_prefix.to_string(),
            collected_names: Vec::new(),
            name_ids: macro_name_ids.clone(),
        };
        macro_visitor.visit_file_mut(&mut rewritten_file);
        (
            rewritten_file.to_token_stream().to_string(),
            SourceMap::default(),
            macro_visitor.collected_names,
        )
    } else {
        (rewritten, source_map, Vec::new())
    };

    Ok(InstrumentResult {
        source: final_source,
        source_map: final_map,
        concurrency: collector.concurrency,
        macro_fn_names,
    })
}

/// Method names that indicate parallel iterator chains.
const PARALLEL_ITER_METHODS: &[&str] = &[
    "par_iter",
    "par_iter_mut",
    "into_par_iter",
    "par_bridge",
    "par_chunks",
    "par_chunks_mut",
    "par_windows",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FutureReturnKind {
    ImplFuture,
    PinBoxDynFuture,
    KnownAlias,
}

/// Extract the ABI string from a CST `ast::Fn`, stripping surrounding quotes.
///
/// Returns `None` when there is no `extern` keyword (default Rust ABI).
/// Returns `Some("")` for bare `extern fn` (no ABI string, defaults to C).
/// Returns `Some("C")`, `Some("Rust")`, etc. for explicit ABI strings.
fn extract_cst_abi(func: &ast::Fn) -> Option<String> {
    let abi = func.abi()?;
    match abi.abi_string() {
        Some(abi_str) => {
            // abi_string().text() returns the token including quotes, e.g. "\"Rust\"".
            let raw = abi_str.text();
            let unquoted = raw
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(raw);
            Some(unquoted.to_string())
        }
        None => {
            // bare `extern fn` (no ABI string) defaults to "C"
            Some(String::new())
        }
    }
}

/// Check if an `ast::Fn` return type indicates `-> impl Future<...>`.
fn returns_future_cst(func: &ast::Fn) -> Option<FutureReturnKind> {
    let ret = func.ret_type()?;
    let ty = ret.ty()?;
    returns_future_ty_cst(&ty)
}

fn returns_future_ty_cst(ty: &ast::Type) -> Option<FutureReturnKind> {
    match ty {
        ast::Type::ImplTraitType(impl_trait) => {
            let bounds = impl_trait.type_bound_list()?;
            if has_future_bound_cst(&bounds) {
                Some(FutureReturnKind::ImplFuture)
            } else {
                None
            }
        }
        ast::Type::PathType(path_type) => {
            let path = path_type.path()?;
            let last_seg = last_path_segment(&path)?;
            let name = last_seg.name_ref()?.text().to_string();
            match name.as_str() {
                "Pin" => check_pin_box_dyn_future_cst(&last_seg),
                "BoxFuture" | "LocalBoxFuture" => Some(FutureReturnKind::KnownAlias),
                _ => None,
            }
        }
        ast::Type::ParenType(paren) => {
            let inner = paren.ty()?;
            returns_future_ty_cst(&inner)
        }
        _ => None,
    }
}

fn has_future_bound_cst(bounds: &ast::TypeBoundList) -> bool {
    bounds.bounds().any(|bound| {
        if let Some(ast::Type::PathType(path_type)) = bound.ty() {
            if let Some(name_ref) = path_type
                .path()
                .and_then(|p| last_path_segment(&p))
                .and_then(|seg| seg.name_ref())
            {
                return name_ref.text() == "Future";
            }
        }
        false
    })
}

fn check_pin_box_dyn_future_cst(pin_seg: &ast::PathSegment) -> Option<FutureReturnKind> {
    let args = pin_seg.generic_arg_list()?;
    let first_type_arg = args.generic_args().find_map(|arg| {
        if let ast::GenericArg::TypeArg(ta) = arg {
            ta.ty()
        } else {
            None
        }
    })?;

    if let ast::Type::PathType(path_type) = first_type_arg {
        let path = path_type.path()?;
        let last = last_path_segment(&path)?;
        if last.name_ref()?.text() == "Box" {
            let box_args = last.generic_arg_list()?;
            let box_inner = box_args.generic_args().find_map(|arg| {
                if let ast::GenericArg::TypeArg(ta) = arg {
                    ta.ty()
                } else {
                    None
                }
            })?;
            if let ast::Type::DynTraitType(dyn_type) = box_inner {
                let bounds = dyn_type.type_bound_list()?;
                if has_future_bound_cst(&bounds) {
                    return Some(FutureReturnKind::PinBoxDynFuture);
                }
            }
        }
    }
    None
}

/// Get the last segment of a ra_ap_syntax path.
///
/// ra_ap_syntax paths are nested: `a::b::c` is Path { qualifier: Path { qualifier: Path { segment: a }, segment: b }, segment: c }.
/// The `.segment()` method returns the LAST segment directly.
fn last_path_segment(path: &ast::Path) -> Option<ast::PathSegment> {
    path.segment()
}

/// Extract the type name from a CST type node.
///
/// Uses the full syntax text (including generics, references, etc.) to match
/// what `naming::render_type` produces from syn types. This ensures
/// InjectionCollector and FnCollector agree on qualified names.
fn type_ident_cst(ty: &ast::Type) -> String {
    ty.syntax().text().to_string()
}

/// Extract the last segment name from a trait type in a CST.
///
/// For `fmt::Display`, returns `"Display"`. For `MyTrait`, returns `"MyTrait"`.
/// Matches what `naming::render_impl_name` does: uses the last segment of the
/// trait path only.
fn trait_last_segment_cst(ty: &ast::Type) -> String {
    if let ast::Type::PathType(path_type) = ty {
        if let Some(path) = path_type.path() {
            if let Some(seg) = last_path_segment(&path) {
                if let Some(name_ref) = seg.name_ref() {
                    return name_ref.text().to_string();
                }
            }
        }
    }
    ty.syntax().text().to_string()
}

/// Byte offset of the start of a CST node.
fn start_byte(node: &impl AstNode) -> usize {
    node.syntax().text_range().start().into()
}

/// Byte offset of the end of a CST node.
fn end_byte(node: &impl AstNode) -> usize {
    node.syntax().text_range().end().into()
}

/// Byte offset of a syntax token's start.
fn token_start(token: &ra_ap_syntax::SyntaxToken) -> usize {
    token.text_range().start().into()
}

/// Skip past inner attributes and inner doc comments in a CST block body.
///
/// Given a `StmtList`, returns the byte offset just after all inner attributes
/// (first-class CST nodes), ready for guard injection. Falls back to just after
/// the opening brace if no inner attrs are present.
fn after_inner_attrs_in_block(source: &str, stmt_list: &ast::StmtList) -> usize {
    let l_curly = stmt_list.l_curly_token();
    let base = match &l_curly {
        Some(t) => token_start(t) + t.text().len(),
        None => start_byte(stmt_list),
    };

    // Walk the CST children of the StmtList. Inner attributes appear as Attr
    // nodes with `#!` syntax. Inner doc comments appear as COMMENT tokens with
    // `//!` or `/*!` prefix. We skip past them all.
    let mut pos = base;
    for child in stmt_list.syntax().children_with_tokens() {
        match &child {
            ra_ap_syntax::NodeOrToken::Token(token) => {
                // Skip whitespace and inner doc comments
                if token.kind() == SyntaxKind::WHITESPACE {
                    let end: usize = token.text_range().end().into();
                    if end > pos {
                        pos = end;
                    }
                    continue;
                }
                if token.kind() == SyntaxKind::COMMENT {
                    let text = token.text();
                    if text.starts_with("//!") || text.starts_with("/*!") {
                        let end: usize = token.text_range().end().into();
                        // Also skip any newline after a //! comment
                        if text.starts_with("//!") {
                            let after = end;
                            if after < source.len() && source.as_bytes()[after] == b'\n' {
                                pos = after + 1;
                            } else {
                                pos = end;
                            }
                        } else {
                            pos = end;
                        }
                        continue;
                    }
                    // Regular comments between inner attrs -- skip them
                    let end: usize = token.text_range().end().into();
                    if end > pos {
                        pos = end;
                    }
                    continue;
                }
                // Opening brace -- skip it
                if token.kind() == SyntaxKind::L_CURLY {
                    continue;
                }
                // Anything else (non-whitespace, non-comment) -- stop
                break;
            }
            ra_ap_syntax::NodeOrToken::Node(node) => {
                // Inner attribute: #![...]
                if let Some(attr) = ast::Attr::cast(node.clone()) {
                    if attr.excl_token().is_some() {
                        let end: usize = node.text_range().end().into();
                        if end > pos {
                            pos = end;
                        }
                        continue;
                    }
                }
                // Anything else (statement, expression) -- stop
                break;
            }
        }
    }
    pos
}

/// Read-only CST walker that collects text injection points.
struct InjectionCollector<'s> {
    source: &'s str,
    measured: HashMap<String, u32>,
    all_instrumentable: HashSet<String>,
    injector: StringInjector,
    current_impl: Option<String>,
    current_trait: Option<String>,
    /// Collected concurrency info: (function_name, pattern_name).
    concurrency: Vec<(String, String)>,
    module_prefix: String,
    /// Inline module nesting path (e.g. ["inner", "sub"] for `mod inner { mod sub { ... } }`).
    /// Used to build qualified names that match what FnCollector produces with ScopeState.
    mod_path: Vec<String>,
}

impl<'s> InjectionCollector<'s> {
    fn qualify_name(&self, name: &str) -> String {
        crate::resolve::qualify(&self.module_prefix, name)
    }

    /// Walk a parsed source file and collect all injection points.
    fn walk_file(&mut self, file: &ast::SourceFile) {
        for item in file.items() {
            self.walk_item(&item);
        }
    }

    /// Walk a single top-level item.
    fn walk_item(&mut self, item: &ast::Item) {
        match item {
            ast::Item::Fn(func) => {
                self.visit_fn(func, FnContext::TopLevel);
            }
            ast::Item::Impl(imp) => {
                self.visit_impl(imp);
            }
            ast::Item::Trait(tr) => {
                self.visit_trait(tr);
            }
            ast::Item::Module(module) => {
                self.visit_module(module);
            }
            _ => {}
        }
    }

    /// Build a qualified name with the current inline module path prepended.
    /// Matches what FnCollector's ScopeState.render_minimal produces.
    fn mod_qualify(&self, name: &str) -> String {
        if self.mod_path.is_empty() {
            name.to_string()
        } else {
            format!("{}::{name}", self.mod_path.join("::"))
        }
    }

    fn visit_module(&mut self, module: &ast::Module) {
        let mod_name = module
            .name()
            .map(|n| n.text().to_string())
            .unwrap_or_else(|| "_".to_string());
        self.mod_path.push(mod_name);
        if let Some(item_list) = module.item_list() {
            for item in item_list.items() {
                self.walk_item(&item);
            }
        }
        self.mod_path.pop();
    }

    fn visit_impl(&mut self, imp: &ast::Impl) {
        let self_ty = imp.self_ty().map(|ty| type_ident_cst(&ty)).unwrap_or_else(|| "_".to_string());
        let prev = self.current_impl.take();

        // For trait impls (impl Trait for Type), include trait name for disambiguation.
        // Uses last segment only (e.g. "Display" from "fmt::Display") to match
        // what naming::render_impl_name does with syn types.
        let impl_name = if let Some(trait_ty) = imp.trait_() {
            let trait_name = trait_last_segment_cst(&trait_ty);
            format!("<{self_ty} as {trait_name}>")
        } else {
            self_ty
        };
        self.current_impl = Some(impl_name);

        if let Some(assoc_list) = imp.assoc_item_list() {
            for assoc in assoc_list.assoc_items() {
                if let ast::AssocItem::Fn(func) = assoc {
                    self.visit_fn(&func, FnContext::Impl);
                }
            }
        }
        self.current_impl = prev;
    }

    fn visit_trait(&mut self, tr: &ast::Trait) {
        let trait_name = tr.name().map(|n| n.text().to_string()).unwrap_or_else(|| "_".to_string());
        let prev = self.current_trait.take();
        self.current_trait = Some(trait_name);

        if let Some(assoc_list) = tr.assoc_item_list() {
            for assoc in assoc_list.assoc_items() {
                if let ast::AssocItem::Fn(func) = assoc {
                    // Only instrument trait methods with default bodies.
                    if func.body().is_some() {
                        self.visit_fn(&func, FnContext::Trait);
                    }
                }
            }
        }
        self.current_trait = prev;
    }

    fn visit_fn(&mut self, func: &ast::Fn, ctx: FnContext) {
        let cst_abi = extract_cst_abi(func);
        if !matches!(
            classify(func.const_token().is_some(), cst_abi.as_deref()),
            Classification::Instrumentable
        ) {
            return;
        }

        let name = func.name().map(|n| n.text().to_string()).unwrap_or_default();

        // Compute qualified name based on context.
        // Prepend inline module path to match what FnCollector produces via
        // ScopeState (which tracks mod nesting in the minimal name).
        let fn_qualified = match ctx {
            FnContext::TopLevel => name.clone(),
            FnContext::Impl => {
                if let Some(ref impl_ty) = self.current_impl {
                    format!("{impl_ty}::{name}")
                } else {
                    name.clone()
                }
            }
            FnContext::Trait => {
                if let Some(ref trait_name) = self.current_trait {
                    format!("{trait_name}::{name}")
                } else {
                    name.clone()
                }
            }
        };
        let qualified = self.mod_qualify(&fn_qualified);

        let is_main = matches!(ctx, FnContext::TopLevel) && name == "main";

        // Inject ctx parameter for all instrumentable functions EXCEPT main.
        if !is_main {
            self.inject_ctx_param(func);
        }

        // If measured, also inject guard.
        if let Some(&name_id) = self.measured.get(&qualified) {
            self.collect_guard(func, &qualified, name_id);
        }

        // Walk descendants for call-site injections and closure pre-clones.
        if let Some(body) = func.body() {
            self.walk_expr_descendants(&body);
        }
    }

    /// Walk all descendant expressions for call-site ctx.clone() injection,
    /// move closure pre-cloning, and inner fn instrumentation.
    fn walk_expr_descendants(&mut self, body: &ast::BlockExpr) {
        for node in body.syntax().descendants() {
            match node.kind() {
                SyntaxKind::CALL_EXPR => {
                    if let Some(call) = ast::CallExpr::cast(node) {
                        self.visit_call_expr(&call);
                    }
                }
                SyntaxKind::METHOD_CALL_EXPR => {
                    if let Some(mc) = ast::MethodCallExpr::cast(node) {
                        self.visit_method_call_expr(&mc);
                    }
                }
                SyntaxKind::CLOSURE_EXPR => {
                    if let Some(closure) = ast::ClosureExpr::cast(node) {
                        self.visit_closure_expr(&closure);
                    }
                }
                SyntaxKind::FN => {
                    // Inner named functions get their own ctx parameter
                    // and guard -- they are independent compiled functions.
                    // Call-site injections inside their bodies are already
                    // handled by this descendants() traversal, so we only
                    // inject the ctx param and guard here (no recursive walk).
                    if let Some(inner_fn) = ast::Fn::cast(node) {
                        self.visit_inner_fn(&inner_fn);
                    }
                }
                SyntaxKind::MACRO_CALL => {
                    // Macro invocations contain TOKEN_TREE nodes where the CST
                    // parser does not produce CALL_EXPR. Scan token trees for
                    // IDENT + paren-TOKEN_TREE patterns to inject ctx.clone()
                    // at call sites inside macros.
                    self.visit_macro_call_token_trees(&node);
                }
                _ => {}
            }
        }
    }

    /// Instrument an inner fn item. Injects ctx parameter and guard without
    /// recursively walking the body (the caller's descendants() already covers
    /// call-site injections inside the inner fn).
    fn visit_inner_fn(&mut self, func: &ast::Fn) {
        let cst_abi = extract_cst_abi(func);
        if !matches!(
            classify(func.const_token().is_some(), cst_abi.as_deref()),
            Classification::Instrumentable
        ) {
            return;
        }

        let name = func.name().map(|n| n.text().to_string()).unwrap_or_default();
        let qualified = self.mod_qualify(&name);

        self.inject_ctx_param(func);

        if let Some(&name_id) = self.measured.get(&qualified) {
            self.collect_guard(func, &qualified, name_id);
        }
    }

    /// Inject `__piano_ctx.clone()` as the last argument at a call site.
    fn inject_ctx_clone_at_call_cst(
        &mut self,
        arg_list: &ast::ArgList,
        has_args: bool,
    ) {
        let close_paren = arg_list.r_paren_token();
        let close_pos = match close_paren {
            Some(ref t) => token_start(t),
            None => return,
        };
        if !has_args {
            self.injector.insert(close_pos, "__piano_ctx.clone()");
        } else {
            self.injector.insert(close_pos, ", __piano_ctx.clone()");
        }
    }

    /// Inject the `__piano_ctx` parameter at the closing paren of the signature.
    fn inject_ctx_param(&mut self, func: &ast::Fn) {
        let param_list = match func.param_list() {
            Some(pl) => pl,
            None => return,
        };
        let close_paren = match param_list.r_paren_token() {
            Some(t) => t,
            None => return,
        };
        let close_pos = token_start(&close_paren);
        let has_params = param_list.params().next().is_some()
            || param_list.self_param().is_some();

        if !has_params {
            self.injector
                .insert(close_pos, "__piano_ctx: piano_runtime::ctx::Ctx");
        } else {
            self.injector
                .insert(close_pos, ", __piano_ctx: piano_runtime::ctx::Ctx");
        }
    }

    fn collect_guard(&mut self, func: &ast::Fn, name: &str, name_id: u32) {
        let qualified_name = self.qualify_name(name);
        let is_async = func.async_token().is_some();

        let body = match func.body() {
            Some(b) => b,
            None => return,
        };
        let stmt_list = match body.stmt_list() {
            Some(sl) => sl,
            None => return,
        };

        // For non-async functions returning futures, wrap only the trailing expression.
        if !is_async {
            if let Some(kind) = returns_future_cst(func) {
                self.collect_future_return_guard(func, &stmt_list, kind, name_id);
                return;
            }
        }

        let open_byte = after_inner_attrs_in_block(self.source, &stmt_list);

        // Check for concurrency patterns (detection only, no fork/adopt).
        if let Some(pattern) = find_concurrency_pattern_cst(&body) {
            self.concurrency.push((qualified_name, pattern));
        }

        if is_async {
            // Async function: wrap body in PianoFuture.
            let close_curly = stmt_list.r_curly_token();
            let close_byte = match close_curly {
                Some(ref t) => token_start(t),
                None => return,
            };

            self.injector.insert(
                open_byte,
                format!(
                    "\nlet (__piano_state, __piano_ctx) = __piano_ctx.enter_async({name_id});\
                     \npiano_runtime::PianoFuture::new(__piano_state, async move {{"
                ),
            );
            self.injector.insert(close_byte, "\n}).await\n");
        } else {
            // Sync function: just inject guard.
            self.injector.insert(
                open_byte,
                format!("\n    let (__piano_guard, __piano_ctx) = __piano_ctx.enter({name_id});"),
            );
        }
    }

    /// Collect injections for a future-returning (non-async) function.
    fn collect_future_return_guard(
        &mut self,
        func: &ast::Fn,
        stmt_list: &ast::StmtList,
        kind: FutureReturnKind,
        name_id: u32,
    ) {
        // Find trailing expression.
        let trailing_expr = stmt_list.tail_expr();
        let Some(trailing) = trailing_expr else {
            // No trailing expression -- fall back to sync guard.
            let open_byte = after_inner_attrs_in_block(self.source, stmt_list);
            self.injector.insert(
                open_byte,
                format!("\n    let (__piano_guard, __piano_ctx) = __piano_ctx.enter({name_id});"),
            );
            return;
        };

        let trailing_start: usize = trailing.syntax().text_range().start().into();
        let trailing_end: usize = trailing.syntax().text_range().end().into();

        match kind {
            FutureReturnKind::ImplFuture => {
                self.injector.insert(
                    trailing_start,
                    format!(
                        "let (__piano_state, __piano_ctx) = __piano_ctx.enter_async({name_id});\
                         \npiano_runtime::PianoFuture::new(__piano_state, async move {{\
                         \n        ("
                    ),
                );
                self.injector.insert(trailing_end, ").await\n    })");
            }
            FutureReturnKind::PinBoxDynFuture | FutureReturnKind::KnownAlias => {
                // Get the return type text from the CST.
                let return_type = func.ret_type()
                    .and_then(|rt| rt.ty())
                    .map(|ty| ty.syntax().text().to_string())
                    .unwrap_or_default();

                // For boxed-future returns, wrap early `return` expressions too.
                self.collect_return_wrappers_cst(stmt_list, name_id, &return_type);

                self.injector.insert(
                    trailing_start,
                    format!(
                        "let (__piano_state, __piano_ctx) = __piano_ctx.enter_async({name_id});\
                         \nBox::pin(piano_runtime::PianoFuture::new(__piano_state, async move {{\
                         \n        let __piano_inner: {return_type} = "
                    ),
                );
                self.injector
                    .insert(trailing_end, ";\n        __piano_inner.await\n    }))");
            }
        }
    }

    /// Walk the block's preceding statements for `return <expr>` and wrap them.
    fn collect_return_wrappers_cst(
        &mut self,
        stmt_list: &ast::StmtList,
        name_id: u32,
        return_type: &str,
    ) {
        // Walk all statements (the tail_expr is separate in the CST).
        for stmt in stmt_list.statements() {
            self.find_returns_in_node(stmt.syntax(), name_id, return_type);
        }
    }

    /// Recursively find return expressions in a syntax node, stopping at
    /// closure and async block boundaries.
    fn find_returns_in_node(
        &mut self,
        node: &ra_ap_syntax::SyntaxNode,
        name_id: u32,
        return_type: &str,
    ) {
        for child in node.children() {
            // Stop at boundaries: closures and async blocks.
            if child.kind() == SyntaxKind::CLOSURE_EXPR
                || child.kind() == SyntaxKind::BLOCK_EXPR
                    && ast::BlockExpr::cast(child.clone())
                        .is_some_and(|b| b.async_token().is_some())
            {
                continue;
            }

            if child.kind() == SyntaxKind::RETURN_EXPR {
                if let Some(ret) = ast::ReturnExpr::cast(child.clone()) {
                    if let Some(inner) = ret.expr() {
                        let inner_start: usize = inner.syntax().text_range().start().into();
                        let inner_end: usize = inner.syntax().text_range().end().into();
                        self.injector.insert(
                            inner_start,
                            format!(
                                "{{ let (__piano_state, _) = __piano_ctx.enter_async({name_id}); \
                                 Box::pin(piano_runtime::PianoFuture::new(__piano_state, async move {{\
                                 \n            let __piano_inner: {return_type} = "
                            ),
                        );
                        self.injector
                            .insert(inner_end, ";\n            __piano_inner.await\n        })) }");
                    }
                    // Don't recurse into the return's inner expression.
                    continue;
                }
            }

            // Recurse into child nodes.
            self.find_returns_in_node(&child, name_id, return_type);
        }
    }

    fn visit_call_expr(&mut self, call: &ast::CallExpr) {
        // Direct / qualified path calls: foo(a, b) or module::foo(a, b)
        if let Some(ast::Expr::PathExpr(path_expr)) = call.expr() {
            if let Some(name_ref) = path_expr
                .path()
                .and_then(|p| last_path_segment(&p))
                .and_then(|seg| seg.name_ref())
            {
                let name = name_ref.text().to_string();
                if self.all_instrumentable.contains(&name) {
                    if let Some(arg_list) = call.arg_list() {
                        let has_args = arg_list.args().next().is_some();
                        self.inject_ctx_clone_at_call_cst(&arg_list, has_args);
                    }
                }
            }
        }
    }

    fn visit_method_call_expr(&mut self, mc: &ast::MethodCallExpr) {
        let method_name = match mc.name_ref() {
            Some(nr) => nr.text().to_string(),
            None => return,
        };
        // Only match self-method calls where we know the type.
        if let Some(receiver) = mc.receiver() {
            if is_self_receiver_cst(&receiver) {
                if let Some(ref impl_type) = self.current_impl {
                    let qualified = format!("{impl_type}::{method_name}");
                    if self.all_instrumentable.contains(&qualified) {
                        if let Some(arg_list) = mc.arg_list() {
                            let has_args = arg_list.args().next().is_some();
                            self.inject_ctx_clone_at_call_cst(&arg_list, has_args);
                        }
                    }
                }
            }
        }
    }

    fn visit_closure_expr(&mut self, closure: &ast::ClosureExpr) {
        // Move closures that contain calls to instrumented functions need
        // `let __piano_ctx = __piano_ctx.clone();` before the closure expression
        // so the clone is captured instead of the original (can't move the same
        // value twice).
        if closure.move_token().is_some()
            && contains_instrumentable_call_cst(closure.syntax(), &self.all_instrumentable)
        {
            // Find the enclosing statement so the `let` is injected at a
            // position where statements are valid. Without this, closures
            // used as function/method arguments would get the `let` injected
            // inside the argument list, producing invalid syntax.
            let insert_byte = enclosing_stmt_start(closure.syntax())
                .unwrap_or_else(|| closure.syntax().text_range().start().into());
            self.injector.insert(
                insert_byte,
                "let __piano_ctx = __piano_ctx.clone();\n",
            );
        }
    }

    /// Scan TOKEN_TREE children of a MACRO_CALL for call-site patterns
    /// (IDENT followed by paren-TOKEN_TREE) and inject ctx.clone().
    fn visit_macro_call_token_trees(&mut self, macro_call: &ra_ap_syntax::SyntaxNode) {
        // Recursively visit all TOKEN_TREE descendants of the macro call.
        for tt_node in macro_call.descendants() {
            if tt_node.kind() != SyntaxKind::TOKEN_TREE {
                continue;
            }
            self.scan_token_tree_for_calls(&tt_node);
        }
    }

    /// Scan a single TOKEN_TREE node for IDENT + paren-TOKEN_TREE patterns.
    ///
    /// The CST for `child_a(x, y)` inside a macro TOKEN_TREE looks like:
    ///   TOKEN IDENT "child_a"
    ///   TOKEN_TREE
    ///     TOKEN L_PAREN "("
    ///     TOKEN IDENT "x"
    ///     TOKEN COMMA ","
    ///     TOKEN IDENT "y"
    ///     TOKEN R_PAREN ")"
    ///
    /// For path-qualified `module::child_a()`:
    ///   TOKEN IDENT "module"
    ///   TOKEN COLON ":"
    ///   TOKEN COLON ":"
    ///   TOKEN IDENT "child_a"
    ///   TOKEN_TREE
    ///     TOKEN L_PAREN "("
    ///     TOKEN R_PAREN ")"
    fn scan_token_tree_for_calls(&mut self, tt: &ra_ap_syntax::SyntaxNode) {
        use ra_ap_syntax::NodeOrToken;

        let children: Vec<_> = tt.children_with_tokens().collect();

        let mut i = 0;
        while i < children.len() {
            // Look for IDENT token
            let ident_text = match &children[i] {
                NodeOrToken::Token(tok) if tok.kind() == SyntaxKind::IDENT => {
                    tok.text().to_string()
                }
                _ => {
                    i += 1;
                    continue;
                }
            };

            // Walk forward past any `::IDENT` path segments to find the last
            // identifier before the paren TOKEN_TREE. This handles
            // `module::child_a()` -- we want "child_a" as the function name.
            let mut fn_name = ident_text;
            let mut j = i + 1;
            while j + 2 < children.len() {
                let is_colon1 = matches!(&children[j], NodeOrToken::Token(t) if t.kind() == SyntaxKind::COLON);
                let is_colon2 = matches!(&children[j + 1], NodeOrToken::Token(t) if t.kind() == SyntaxKind::COLON);
                let next_ident = match &children[j + 2] {
                    NodeOrToken::Token(t) if t.kind() == SyntaxKind::IDENT => {
                        Some(t.text().to_string())
                    }
                    _ => None,
                };
                if is_colon1 && is_colon2 {
                    if let Some(name) = next_ident {
                        fn_name = name;
                        j += 3;
                        continue;
                    }
                }
                break;
            }

            // Check if the next child after the IDENT (or path) is a
            // paren-TOKEN_TREE (the call's argument list).
            if j < children.len() {
                if let NodeOrToken::Node(arg_tt) = &children[j] {
                    if arg_tt.kind() == SyntaxKind::TOKEN_TREE {
                        // Verify it starts with L_PAREN (could be braces or brackets).
                        let starts_with_paren = arg_tt.children_with_tokens().next().is_some_and(
                            |first| matches!(first, NodeOrToken::Token(t) if t.kind() == SyntaxKind::L_PAREN),
                        );
                        if starts_with_paren && self.all_instrumentable.contains(&fn_name) {
                            self.inject_ctx_clone_in_token_tree(arg_tt);
                        }
                    }
                }
            }

            i = j.max(i + 1);
        }
    }

    /// Inject `__piano_ctx.clone()` before the closing R_PAREN of a
    /// paren-TOKEN_TREE that represents a call's argument list.
    fn inject_ctx_clone_in_token_tree(&mut self, arg_tt: &ra_ap_syntax::SyntaxNode) {
        use ra_ap_syntax::NodeOrToken;

        // Find the R_PAREN token. In a well-formed paren TOKEN_TREE it is
        // the last child, but we search for the last R_PAREN to be safe.
        let mut close_pos: Option<usize> = None;
        for child in arg_tt.children_with_tokens() {
            if let NodeOrToken::Token(t) = child {
                if t.kind() == SyntaxKind::R_PAREN {
                    close_pos = Some(t.text_range().start().into());
                }
            }
        }
        let close_pos = match close_pos {
            Some(pos) => pos,
            None => return,
        };

        // Check if there are any arguments (anything between L_PAREN and R_PAREN
        // that isn't whitespace).
        let has_args = arg_tt.children_with_tokens().any(|child| {
            match child {
                NodeOrToken::Token(t) => {
                    t.kind() != SyntaxKind::L_PAREN
                        && t.kind() != SyntaxKind::R_PAREN
                        && t.kind() != SyntaxKind::WHITESPACE
                }
                NodeOrToken::Node(_) => true,
            }
        });

        if has_args {
            self.injector.insert(close_pos, ", __piano_ctx.clone()");
        } else {
            self.injector.insert(close_pos, "__piano_ctx.clone()");
        }
    }
}

#[derive(Clone, Copy)]
enum FnContext {
    TopLevel,
    Impl,
    Trait,
}

/// Check whether an expression is `self` (or `Self`) using the CST.
fn is_self_receiver_cst(expr: &ast::Expr) -> bool {
    if let ast::Expr::PathExpr(path_expr) = expr {
        if let Some(path) = path_expr.path() {
            if path.qualifier().is_none() {
                if let Some(seg) = path.segment() {
                    if let Some(name_ref) = seg.name_ref() {
                        let text = name_ref.text();
                        return text == "self" || text == "Self";
                    }
                }
            }
        }
    }
    false
}

/// Walk up from `node` to find the nearest enclosing statement (EXPR_STMT or
/// LET_STMT) and return its start byte offset. Returns None if no statement
/// ancestor is found (e.g. the node is already at the top level).
///
/// This is used by move-closure pre-clone injection: the `let __piano_ctx = ...`
/// must be placed at a position where statements are valid (before the enclosing
/// statement), not immediately before the closure which may be inside an
/// argument list.
fn enclosing_stmt_start(node: &ra_ap_syntax::SyntaxNode) -> Option<usize> {
    let mut current = node.parent();
    while let Some(parent) = current {
        if parent.kind() == SyntaxKind::EXPR_STMT || parent.kind() == SyntaxKind::LET_STMT {
            return Some(parent.text_range().start().into());
        }
        current = parent.parent();
    }
    None
}

/// Check whether a CST subtree contains any call to a function in the given set.
fn contains_instrumentable_call_cst(
    node: &ra_ap_syntax::SyntaxNode,
    names: &HashSet<String>,
) -> bool {
    for descendant in node.descendants() {
        if descendant.kind() != SyntaxKind::CALL_EXPR {
            continue;
        }
        let call = match ast::CallExpr::cast(descendant) {
            Some(c) => c,
            None => continue,
        };
        if let Some(ast::Expr::PathExpr(path_expr)) = call.expr() {
            let matched = path_expr
                .path()
                .and_then(|p| last_path_segment(&p))
                .and_then(|seg| seg.name_ref())
                .is_some_and(|nr| names.contains(nr.text().as_str()));
            if matched {
                return true;
            }
        }
    }
    false
}

/// Concurrency trigger methods (scope, scope_fifo, join, spawn).
const CONCURRENCY_TRIGGER_METHODS: &[&str] = &["scope", "scope_fifo", "join", "spawn"];

/// Find the first concurrency pattern in a block expression using the CST.
///
/// Walks the CST subtree looking for method calls or function calls matching
/// known concurrency patterns. Stops recursion at async blocks.
fn find_concurrency_pattern_cst(block: &ast::BlockExpr) -> Option<String> {
    find_concurrency_in_node(block.syntax())
}

fn find_concurrency_in_node(node: &ra_ap_syntax::SyntaxNode) -> Option<String> {
    for child in node.children() {
        // Don't recurse into async blocks.
        if child.kind() == SyntaxKind::BLOCK_EXPR {
            if let Some(be) = ast::BlockExpr::cast(child.clone()) {
                if be.async_token().is_some() {
                    continue;
                }
            }
        }

        match child.kind() {
            SyntaxKind::METHOD_CALL_EXPR => {
                if let Some(mc) = ast::MethodCallExpr::cast(child.clone()) {
                    if let Some(name_ref) = mc.name_ref() {
                        let method = name_ref.text().to_string();
                        if PARALLEL_ITER_METHODS.contains(&method.as_str()) {
                            return Some(method);
                        }
                        if CONCURRENCY_TRIGGER_METHODS.contains(&method.as_str()) {
                            return Some(method);
                        }
                    }
                }
            }
            SyntaxKind::CALL_EXPR => {
                if let Some(call) = ast::CallExpr::cast(child.clone()) {
                    if let Some(ast::Expr::PathExpr(path_expr)) = call.expr() {
                        if let Some(path) = path_expr.path() {
                            if let Some(name_ref) = last_path_segment(&path)
                                .and_then(|seg| seg.name_ref())
                            {
                                let name = name_ref.text().to_string();
                                if CONCURRENCY_TRIGGER_METHODS.contains(&name.as_str()) {
                                    // Build the full path, e.g. "rayon::scope"
                                    let full_path: String = path.segments()
                                        .filter_map(|s| s.name_ref().map(|nr| nr.text().to_string()))
                                        .collect::<Vec<_>>()
                                        .join("::");
                                    return Some(full_path);
                                }
                            }
                        }
                    }
                }
            }
            SyntaxKind::CLOSURE_EXPR => {
                // Recurse into closure body (concurrency can be inside a closure arg).
                if let Some(closure) = ast::ClosureExpr::cast(child.clone()) {
                    if let Some(body) = closure.body() {
                        if let Some(found) = find_concurrency_in_node(body.syntax()) {
                            return Some(found);
                        }
                    }
                }
                continue;
            }
            _ => {}
        }

        // Default recursion.
        if let Some(found) = find_concurrency_in_node(&child) {
            return Some(found);
        }
    }
    None
}

/// Inject `piano_runtime::register(name)` calls at the top of `fn main`.
///
/// This ensures every instrumented function appears in the output, even if it
/// was never called during the run.
pub fn inject_registrations(
    source: &str,
    names: &[(u32, &str)],
) -> Result<(String, SourceMap), String> {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();
    let mut injector = StringInjector::new();

    // Find the position after all file-level inner attributes.
    let insert_pos = file.attrs()
        .filter(|attr| attr.excl_token().is_some())
        .last()
        .map(|attr| end_byte(&attr))
        .unwrap_or(0);

    let mut text = String::from("\nconst PIANO_NAMES: &[(u32, &str)] = &[");
    for (id, name) in names {
        text.push_str(&format!("({id}, {name:?}), "));
    }
    text.push_str("];\n");

    injector.insert(insert_pos, text);
    Ok(injector.apply(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: calls instrument_source with all_instrumentable derived from
    /// measured keys (sufficient for tests that don't exercise call-site injection).
    fn instrument(
        source: &str,
        measured: &HashMap<String, u32>,
        instrument_macros: bool,
        module_prefix: &str,
    ) -> Result<InstrumentResult, String> {
        let all_instrumentable: HashSet<String> = measured.keys().cloned().collect();
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        instrument_source(source, measured, &all_instrumentable, instrument_macros, module_prefix, &macro_name_ids)
    }

    #[test]
    fn instruments_top_level_function() {
        let source = r#"
fn walk() {
    do_stuff();
}

fn other() {
    do_other();
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("walk".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;

        assert!(
            result.contains("__piano_ctx.enter("),
            "walk should be instrumented. Got:\n{result}"
        );
        // Both walk and other should get ctx parameter (both are instrumentable).
        assert!(
            result.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "instrumentable functions should get ctx parameter. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_impl_method() {
        let source = r#"
struct Walker;

impl Walker {
    fn walk(&self) {
        self.step();
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Walker::walk".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;

        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "Walker::walk should be instrumented. Got:\n{result}",
        );
        assert!(
            result.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "should get ctx parameter. Got:\n{result}",
        );
    }

    #[test]
    fn preserves_function_signature_and_body() {
        let source = r#"
fn compute(x: i32, y: i32) -> i32 {
    x + y
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("compute".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;

        assert!(result.contains("fn compute(x: i32, y: i32"), "signature preserved");
        assert!(result.contains("x + y"), "body preserved");
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "guard injected. Got:\n{result}"
        );
        assert!(
            result.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "ctx param injected. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_functions_instrumented() {
        let source = r#"
fn a() {}
fn b() {}
fn c() {}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("a".to_string(), 0u32), ("c".to_string(), 1u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;

        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "a should be instrumented. Got:\n{result}"
        );
        assert!(
            result.contains("__piano_ctx.enter(1)"),
            "c should be instrumented. Got:\n{result}"
        );
        // All three should get ctx parameter.
        let ctx_count = result.matches("__piano_ctx: piano_runtime::ctx::Ctx").count();
        assert_eq!(
            ctx_count, 3,
            "all 3 instrumentable functions should get ctx param. Got {ctx_count} in:\n{result}"
        );
    }

    #[test]
    fn injects_name_table_at_file_level() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let names = vec![(0, "walk"), (1, "parse")];
        let (result, _map) = inject_registrations(source, &names).unwrap();
        assert!(
            result.contains("const PIANO_NAMES: &[(u32, &str)]"),
            "should have name table const. Got:\n{result}"
        );
        assert!(
            result.contains("(0, \"walk\")"),
            "should contain walk entry. Got:\n{result}"
        );
        assert!(
            result.contains("(1, \"parse\")"),
            "should contain parse entry. Got:\n{result}"
        );
        assert!(
            !result.contains("piano_runtime::register"),
            "should NOT have register calls. Got:\n{result}"
        );
    }

    #[test]
    fn concurrency_detected_but_same_guard_as_non_concurrency() {
        let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("process_all".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should have guard. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt. Got:\n{}",
            result.source
        );
        // Concurrency is detected -- par_iter spawns parallel work.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "par_iter");
    }

    #[test]
    fn no_fork_for_thread_spawn() {
        let source = r#"
fn do_work() {
    std::thread::spawn(|| {
        heavy_computation();
    });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("do_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for std::thread::spawn. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt for std::thread::spawn. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        // Detached spawns report concurrency -- parallel work detected.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "std::thread::spawn");
    }

    #[test]
    fn concurrency_warning_for_thread_spawn() {
        let source = r#"
fn do_work() {
    std::thread::spawn(|| { work(); });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("do_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert_eq!(
            result.concurrency.len(),
            1,
            "thread::spawn should report concurrency. Got: {:?}",
            result.concurrency
        );
        assert_eq!(result.concurrency[0].0, "do_work");
        assert_eq!(result.concurrency[0].1, "std::thread::spawn");
    }

    #[test]
    fn concurrency_warning_for_tokio_spawn() {
        let source = r#"
fn do_work() {
    tokio::spawn(async { work().await; });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("do_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert_eq!(
            result.concurrency.len(),
            1,
            "tokio::spawn should report concurrency. Got: {:?}",
            result.concurrency
        );
        assert_eq!(result.concurrency[0].0, "do_work");
        assert_eq!(result.concurrency[0].1, "tokio::spawn");
    }

    #[test]
    fn no_fork_for_short_path_thread_spawn() {
        // `use std::thread::spawn; spawn(|| ...)` -- bare name, no path prefix
        let source = r#"
fn do_work() {
    spawn(|| {
        heavy_computation();
    });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("do_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for bare spawn(). Got:\n{}",
            result.source
        );
        // Bare spawn() still triggers concurrency detection.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "spawn");
    }

    #[test]
    fn no_fork_for_rayon_spawn_free_function() {
        let source = r#"
fn do_work() {
    rayon::spawn(|| {
        heavy_computation();
    });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("do_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for rayon::spawn free function. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt for rayon::spawn free function. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        // rayon::spawn triggers concurrency detection.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "rayon::spawn");
    }

    #[test]
    fn nested_scope_inside_detached_spawn() {
        let source = r#"
fn work() {
    std::thread::spawn(|| {
        rayon::scope(|s| {
            s.spawn(|_| { inner(); });
        });
    });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork when scope is nested inside detached spawn. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt when scope is nested inside detached spawn. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        // The outer spawn triggers concurrency detection.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "std::thread::spawn");
    }

    #[test]
    fn par_iter_inside_async_block_not_detected() {
        let source = r#"
fn outer() {
    async {
        items.par_iter().for_each(|x| process(x));
    };
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("outer".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for par_iter inside async block. Got:\n{}",
            result.source
        );
        assert!(
            result.concurrency.is_empty(),
            "par_iter inside async block should not report concurrency. Got: {:?}",
            result.concurrency
        );
    }

    #[test]
    fn concurrency_rayon_scope_detected_no_fork_adopt() {
        let source = r#"
fn parallel_work() {
    rayon::scope(|s| {
        s.spawn(|_| { work_a(); });
        s.spawn(|_| { work_b(); });
    });
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("parallel_work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        // Concurrency detected.
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].1, "rayon::scope");
        // No fork/adopt.
        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt. Got:\n{}",
            result.source
        );
        // Has guard.
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should have guard. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn reports_concurrency_for_par_iter() {
        let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("process_all".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].0, "process_all");
        assert_eq!(result.concurrency[0].1, "par_iter");
    }

    #[test]
    fn reports_concurrency_for_rayon_scope() {
        let source = r#"
fn concurrent_discover() {
    rayon::scope(|s| {
        s.spawn(|_| { work(); });
    });
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("concurrent_discover".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert_eq!(result.concurrency.len(), 1);
        assert_eq!(result.concurrency[0].0, "concurrent_discover");
        assert_eq!(result.concurrency[0].1, "rayon::scope");
    }

    #[test]
    fn no_concurrency_for_regular_function() {
        let source = r#"
fn simple() {
    do_stuff();
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("simple".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(result.concurrency.is_empty());
    }

    #[test]
    fn instruments_async_fn() {
        let source = r#"
async fn fetch_data(id: u32) -> String {
    format!("data-{id}")
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("fetch_data".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter_async(0)"),
            "async function SHOULD be instrumented with enter_async. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "async fn should be wrapped in PianoFuture with __piano_state. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "should get ctx parameter. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn does_not_skip_sync_fn() {
        let source = r#"
fn compute(x: u64) -> u64 {
    x * 2
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("compute".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "sync function should be instrumented. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn instruments_async_impl_method() {
        let source = r#"
struct Client;

impl Client {
    async fn fetch(&self) -> String {
        "data".to_string()
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Client::fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter_async(0)"),
            "async impl method SHOULD be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "async impl method should be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn instruments_async_trait_default_method() {
        let source = r#"
trait Service {
    async fn handle(&self) -> String {
        "ok".to_string()
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Service::handle".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter_async(0)"),
            "async trait default method SHOULD be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "async trait method should be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn skips_uninstrumentable_functions() {
        let source = r#"
trait Processor {
    fn process(&self);
    fn default_method(&self) {
        do_work();
    }
    unsafe fn unsafe_default(&self) {
        dangerous_work();
    }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("Processor::default_method".to_string(), 0u32),
            ("Processor::unsafe_default".to_string(), 1u32),
        ]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "safe trait default method should be instrumented. Got:\n{}",
            result.source
        );
        // unsafe fn IS instrumentable -- guard is pure safe code, gets ctx param and guard.
        assert!(
            result.source.contains("__piano_ctx.enter(1)"),
            "unsafe trait default method should be instrumented. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn wraps_async_fn_body_in_piano_future() {
        let source = r#"
async fn handler(x: i32) -> String {
    let result = fetch(x).await;
    format!("{result}")
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("handler".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"));
        assert!(result.source.contains(".await"));
        assert!(result.source.contains("__piano_ctx.enter_async("));
        // Should NOT have old-style enter or fork/adopt.
        assert!(!result.source.contains("piano_runtime::enter("));
        assert!(!result.source.contains("piano_runtime::fork()"));
        assert!(!result.source.contains("piano_runtime::adopt"));
    }

    #[test]
    fn does_not_wrap_sync_fn_in_piano_future() {
        let source = r#"
fn compute(x: u64) -> u64 {
    x * 2
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("compute".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "sync fn should be instrumented"
        );
        assert!(
            !result.source.contains("PianoFuture"),
            "sync fn should NOT be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn non_measured_fn_gets_ctx_but_no_guard() {
        let source = r#"
fn not_measured() {
    fetch().await;
}
"#;
        // not_measured is NOT in the measured map.
        let measured: HashMap<String, u32> = HashMap::from([("other".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "non-measured fn should still get ctx parameter. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("__piano_ctx.enter("),
            "non-measured fn should NOT get guard. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("PianoFuture"),
            "non-measured fn should NOT get PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn excluded_fn_gets_no_ctx_no_guard() {
        let source = r#"
const fn lookup(x: u32) -> u32 {
    x + 1
}
extern "C" fn ffi_handler(x: i32) -> i32 {
    x * 2
}
fn normal() {
    do_stuff();
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("lookup".to_string(), 0u32),
            ("ffi_handler".to_string(), 1u32),
            ("normal".to_string(), 2u32),
        ]);
        let result = instrument(source, &measured, false, "").unwrap();
        // const fn: no ctx, no guard.
        assert!(
            !result.source.contains("fn lookup(x: u32, __piano_ctx"),
            "const fn should NOT get ctx parameter. Got:\n{}",
            result.source
        );
        // extern "C" fn: no ctx, no guard.
        assert!(
            !result.source.contains("fn ffi_handler(x: i32, __piano_ctx"),
            "extern C fn should NOT get ctx parameter. Got:\n{}",
            result.source
        );
        // normal fn: yes ctx, yes guard.
        assert!(
            result.source.contains("__piano_ctx.enter(2)"),
            "normal fn should be instrumented. Got:\n{}",
            result.source
        );
    }

    /// Parse a function declaration string and extract the ast::Fn node.
    fn parse_fn(code: &str) -> ast::Fn {
        let parse = ast::SourceFile::parse(code, Edition::Edition2024);
        let file = parse.tree();
        file.items()
            .find_map(|item| {
                if let ast::Item::Fn(f) = item { Some(f) } else { None }
            })
            .expect("expected a function in the source")
    }

    #[test]
    fn returns_future_detects_impl_future() {
        let func = parse_fn("fn foo() -> impl Future<Output = i32> {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_impl_future_with_send() {
        let func = parse_fn("fn foo() -> impl Future<Output = i32> + Send {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_impl_future_with_lifetime() {
        let func = parse_fn("fn foo(s: &str) -> impl Future<Output = usize> + '_ {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_pin_box_dyn_future() {
        let func = parse_fn("fn foo() -> Pin<Box<dyn Future<Output = i32>>> {}");
        assert_eq!(
            returns_future_cst(&func),
            Some(FutureReturnKind::PinBoxDynFuture)
        );
    }

    #[test]
    fn returns_future_detects_pin_box_dyn_future_send() {
        let func = parse_fn("fn foo() -> Pin<Box<dyn Future<Output = i32> + Send>> {}");
        assert_eq!(
            returns_future_cst(&func),
            Some(FutureReturnKind::PinBoxDynFuture)
        );
    }

    #[test]
    fn returns_future_detects_box_future_alias() {
        let func = parse_fn("fn foo() -> BoxFuture<'static, i32> {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::KnownAlias));
    }

    #[test]
    fn returns_future_detects_local_box_future_alias() {
        let func = parse_fn("fn foo() -> LocalBoxFuture<'_, i32> {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::KnownAlias));
    }

    #[test]
    fn returns_future_rejects_plain_type() {
        let func = parse_fn("fn foo() -> i32 {}");
        assert_eq!(returns_future_cst(&func), None);
    }

    #[test]
    fn returns_future_rejects_result_of_future() {
        let func = parse_fn("fn foo() -> Result<impl Future<Output = i32>, Error> {}");
        assert_eq!(returns_future_cst(&func), None);
    }

    #[test]
    fn returns_future_rejects_no_return_type() {
        let func = parse_fn("fn foo() {}");
        assert_eq!(returns_future_cst(&func), None);
    }

    #[test]
    fn returns_future_rejects_string_return() {
        let func = parse_fn("fn foo() -> String {}");
        assert_eq!(returns_future_cst(&func), None);
    }

    #[test]
    fn returns_future_detects_fully_qualified_future() {
        let func = parse_fn("fn foo() -> impl std::future::Future<Output = i32> {}");
        assert_eq!(returns_future_cst(&func), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn instruments_impl_future_fn() {
        let source = r#"
use std::future::Future;

fn fetch() -> impl Future<Output = String> {
    async { "data".to_string() }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "impl Future fn should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("__piano_ctx.enter_async(0)"),
            "guard should use enter_async. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains(".await"),
            "trailing expression should be awaited inside async block. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "should get ctx parameter. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_preserves_setup_stmts() {
        let source = r#"
use std::future::Future;

fn fetch(x: i32) -> impl Future<Output = i32> {
    let doubled = x * 2;
    async move { doubled }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let setup_pos = result.source.find("let doubled").unwrap();
        let piano_pos = result.source.find("PianoFuture::new").unwrap();
        assert!(
            setup_pos < piano_pos,
            "setup stmt should precede PianoFuture wrapping. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_no_sync_guard() {
        let source = r#"
use std::future::Future;

fn fetch() -> impl Future<Output = String> {
    async { "data".to_string() }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let count = result.source.matches("__piano_ctx.enter_async(").count();
        assert_eq!(
            count, 1,
            "should have exactly one enter_async() call (inside PianoFuture). Got {} in:\n{}",
            count, result.source,
        );
        assert!(
            !result.source.contains("__piano_ctx.enter(0)"),
            "should NOT have sync guard for impl Future fn. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_delegation() {
        let source = r#"
use std::future::Future;

async fn helper() -> i32 { 42 }

fn delegator() -> impl Future<Output = i32> {
    helper()
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("delegator".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "delegation should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_with_send_bound() {
        let source = r#"
use std::future::Future;

fn fetch() -> impl Future<Output = String> + Send {
    async { "data".to_string() }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "impl Future + Send should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn instruments_pin_box_dyn_future_fn() {
        let source = r#"
use std::future::Future;
use std::pin::Pin;

fn fetch() -> Pin<Box<dyn Future<Output = String> + Send>> {
    Box::pin(async { "data".to_string() })
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "Pin<Box<dyn Future>> fn should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("Box::pin"),
            "should re-box the PianoFuture for Pin<Box<dyn Future>> return. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("__piano_inner"),
            "should use type-annotated binding for coercion. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn instruments_box_future_alias() {
        let source = r#"
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
use std::future::Future;
use std::pin::Pin;

fn fetch() -> BoxFuture<'static, String> {
    Box::pin(async { "data".to_string() })
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "BoxFuture alias fn should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_impl_method() {
        let source = r#"
use std::future::Future;

struct Client;

impl Client {
    fn fetch(&self) -> impl Future<Output = String> + '_ {
        async move { "data".to_string() }
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Client::fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "impl method returning impl Future should be wrapped. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("__piano_ctx.enter_async(0)"),
            "guard should use enter_async with name_id. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn impl_future_trait_default_method() {
        let source = r#"
use std::future::Future;

trait Service {
    fn call(&self) -> impl Future<Output = String> {
        async { "ok".to_string() }
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Service::call".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new(__piano_state, async move"),
            "trait default method returning impl Future should be wrapped. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn sync_fn_unchanged_after_refactor() {
        let source = r#"
fn compute(x: u64) -> u64 {
    x * 2
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("compute".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "sync fn should still get sync guard. Got:\n{}",
            result.source,
        );
        assert!(
            !result.source.contains("PianoFuture"),
            "sync fn should NOT get PianoFuture. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn pin_box_dyn_future_early_return_is_wrapped() {
        let source = r#"
use std::future::Future;
use std::pin::Pin;

fn fetch(flag: bool) -> Pin<Box<dyn Future<Output = String> + Send>> {
    if flag {
        return Box::pin(async { "early".to_string() });
    }
    Box::pin(async { "normal".to_string() })
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let piano_count = result
            .source
            .matches("piano_runtime::PianoFuture::new")
            .count();
        assert_eq!(
            piano_count, 2,
            "both early return and trailing expr should be wrapped in PianoFuture. Got {} in:\n{}",
            piano_count, result.source,
        );
    }

    #[test]
    fn return_inside_closure_not_wrapped() {
        let source = r#"
use std::future::Future;

fn fetch(items: Vec<i32>) -> impl Future<Output = Vec<i32>> {
    let filtered: Vec<i32> = items.into_iter().filter(|x| {
        if *x < 0 { return false; }
        true
    }).collect();
    async move { filtered }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let piano_count = result
            .source
            .matches("piano_runtime::PianoFuture::new")
            .count();
        assert_eq!(
            piano_count, 1,
            "closure return should not be wrapped. Got {} in:\n{}",
            piano_count, result.source,
        );
    }

    #[test]
    fn return_inside_async_block_not_wrapped() {
        let source = r#"
use std::future::Future;
use std::pin::Pin;

fn fetch() -> Pin<Box<dyn Future<Output = i32> + Send>> {
    Box::pin(async {
        if true { return 42; }
        0
    })
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let piano_count = result
            .source
            .matches("piano_runtime::PianoFuture::new")
            .count();
        assert_eq!(
            piano_count, 1,
            "return inside async block should not be wrapped. Got {} in:\n{}",
            piano_count, result.source,
        );
    }

    #[test]
    fn impl_future_early_return_not_wrapped() {
        // Known limitation: for `-> impl Future`, early returns are not wrapped
        // in PianoFuture. Only the trailing expression is instrumented. This is
        // because wrapping would change the concrete return type, potentially
        // causing compilation errors when different return paths produce
        // different opaque types.
        let source = r#"
use std::future::Future;

fn fetch(flag: bool) -> impl Future<Output = String> {
    if flag {
        return async { "early".to_string() };
    }
    async { "normal".to_string() }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let piano_count = result
            .source
            .matches("piano_runtime::PianoFuture::new")
            .count();
        assert_eq!(
            piano_count, 1,
            "only trailing expr should be wrapped (early return is a known gap). Got {} in:\n{}",
            piano_count, result.source,
        );
    }

    #[test]
    fn impl_future_no_trailing_expr_falls_back_to_sync_guard() {
        let source = r#"
use std::future::Future;

fn foo() -> impl Future<Output = ()> {
    return async {};
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("foo".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "should fall back to sync guard when no trailing expression. Got:\n{}",
            result.source,
        );
        assert!(
            !result.source.contains("PianoFuture"),
            "should NOT wrap in PianoFuture when no trailing expression. Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn instrument_preserves_original_line_numbers() {
        let source =
            "fn target() {\n    let x = 1;\n    let y = 2;\n}\n\nfn other() {\n    let z = 3;\n}\n";
        let measured: HashMap<String, u32> = HashMap::from([("target".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let result_lines: Vec<&str> = result.source.lines().collect();
        // "fn other()" is on original line 6. With injections for target(),
        // it should be shifted. The ctx param injection is inline (no new line),
        // and the guard adds 1 line.
        let other_pos = result_lines
            .iter()
            .position(|l| l.contains("fn other("))
            .unwrap();
        assert_eq!(
            other_pos, 6,
            "fn other() should be at line index 6 (orig 5 + 1 injection). Got:\n{}",
            result.source,
        );
        // The source_map should remap correctly.
        let map = &result.source_map;
        assert_eq!(map.remap_line(7), Some(6));
    }

    #[test]
    fn module_prefix_qualifies_concurrency_info() {
        let source = r#"
fn process_all(data: &[i32]) {
    data.par_iter().for_each(|x| { let _ = x; });
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("process_all".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "worker").unwrap();
        assert_eq!(
            result.concurrency.first().map(|(name, _)| name.as_str()),
            Some("worker::process_all"),
            "concurrency info should use qualified name",
        );
    }

    #[test]
    fn trait_impl_methods_disambiguated() {
        let source = r#"
use std::fmt;

struct Point { x: i32, y: i32 }

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.x, self.y)
    }
}

impl fmt::Debug for Point {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Point({}, {})", self.x, self.y)
    }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("<Point as Display>::fmt".to_string(), 0u32),
            ("<Point as Debug>::fmt".to_string(), 1u32),
        ]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "Display::fmt should be instrumented. Got:\n{result}"
        );
        assert!(
            result.contains("__piano_ctx.enter(1)"),
            "Debug::fmt should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn inherent_impl_methods_unchanged() {
        let source = r#"
struct Walker;

impl Walker {
    fn walk(&self) {
        // inherent method
    }
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("Walker::walk".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "inherent methods should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn guard_injection_preserves_inner_attrs_in_fn_body() {
        let source = r#"
fn work() {
    #![allow(unused_variables)]
    let x = 42;
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;

        // Must re-parse successfully.
        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));

        // Inner attr must precede the guard.
        let attr_pos = result.find("#![allow(unused_variables)]").unwrap();
        let guard_pos = result.find("__piano_ctx.enter(").unwrap();
        assert!(
            attr_pos < guard_pos,
            "inner attr must precede guard. Got:\n{result}"
        );
    }

    #[test]
    fn registrations_insert_after_file_level_inner_attrs() {
        let source = r#"#![allow(unused)]

fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_registrations(source, &[(0, "work")]).unwrap();

        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));

        let attr_pos = result.find("#![allow(unused)]").unwrap();
        let names_pos = result.find("PIANO_NAMES").unwrap();
        assert!(
            names_pos > attr_pos,
            "name table must come after file-level inner attrs. Got:\n{result}"
        );
    }

    #[test]
    fn file_level_inner_attrs_survive_full_pipeline() {
        let source = r#"#![cfg_attr(test, feature(test))]
#![allow(dead_code)]

fn main() {
    let _ = work();
}

fn work() -> u64 {
    42
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        let mut map = result.source_map;
        let mut current = result.source;

        let (s, m) = inject_registrations(&current, &[(0, "work")]).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_global_allocator(&current, AllocatorKind::Absent).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_shutdown(&current, "/tmp/piano/runs", false, false).unwrap();
        map.merge(m);

        // The final source must parse.
        syn::parse_str::<syn::File>(&s)
            .unwrap_or_else(|e| panic!("full pipeline output should parse: {e}\n\n{s}"));

        // Inner attrs must be at the top.
        assert!(
            s.starts_with("#![cfg_attr"),
            "inner attrs must stay at top. Got:\n{s}"
        );
    }

    #[test]
    fn detects_concurrency_in_try_expr() {
        // par_iter inside a ? expression -- previously missed by wildcard
        let source = r#"
fn work(items: &[Item]) -> Result<()> {
    items.par_iter().map(|x| process(x)).collect::<Result<Vec<_>>>()?;
    Ok(())
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect par_iter inside Try expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn detects_concurrency_in_reference_expr() {
        // scope inside a & reference -- previously missed
        let source = r#"
fn work() {
    let r = &rayon::scope(|s| {
        s.spawn(|_| { do_work(); });
    });
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect scope inside Reference expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn detects_concurrency_in_tuple_expr() {
        let source = r#"
fn work(items: &[Item]) {
    let _ = (items.par_iter().for_each(|x| process(x)), 42);
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect par_iter inside Tuple expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn detects_concurrency_in_if_expr() {
        let source = r#"
fn work(items: &[Item], flag: bool) {
    if flag {
        items.par_iter().for_each(|x| process(x));
    }
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect par_iter inside If expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn detects_concurrency_in_assign_expr() {
        let source = r#"
fn work(items: &[Item]) {
    let mut result = Vec::new();
    result = items.par_iter().map(|x| process(x)).collect();
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("work".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect par_iter inside Assign expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn wraps_return_inside_assign_in_boxed_future() {
        let source = r#"
fn fetch(flag: bool) -> Pin<Box<dyn Future<Output = i32>>> {
    let mut x = 0;
    x = if flag { return Box::pin(async { 0 }); } else { 1 };
    Box::pin(async { x })
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("fetch".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "")
            .unwrap()
            .source;
        // The return value should be wrapped with PianoFuture.
        // Count PianoFuture occurrences: one for the trailing expr, one for the return.
        let future_count = result.matches("PianoFuture::new").count();
        assert!(
            future_count >= 2,
            "should wrap return inside Assign with PianoFuture (expected >= 2, got {future_count}). Got:\n{result}"
        );
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn pass_through_fn_gets_ctx_but_not_guard() {
        // A function that is instrumentable but NOT in the measured map.
        // It should get __piano_ctx param but no guard.
        let source = r#"
fn helper() {
    do_stuff();
}

fn measured_fn() {
    do_other();
}
"#;
        let measured: HashMap<String, u32> =
            HashMap::from([("measured_fn".to_string(), 0u32)]);
        let result = instrument(source, &measured, false, "").unwrap();

        // Both should get ctx.
        let ctx_count = result
            .source
            .matches("__piano_ctx: piano_runtime::ctx::Ctx")
            .count();
        assert_eq!(
            ctx_count, 2,
            "both functions should get ctx parameter. Got {ctx_count} in:\n{}",
            result.source
        );

        // Only measured_fn should get a guard.
        assert!(
            result.source.contains("__piano_ctx.enter(0)"),
            "measured_fn should have guard. Got:\n{}",
            result.source
        );

        // helper should NOT have any enter call.
        // Check the helper function body doesn't contain enter.
        let helper_start = result.source.find("fn helper(").unwrap();
        let measured_start = result.source.find("fn measured_fn(").unwrap();
        let helper_body = &result.source[helper_start..measured_start];
        assert!(
            !helper_body.contains("__piano_ctx.enter("),
            "helper (pass-through) should NOT have guard. Got:\n{}",
            helper_body
        );
    }

    #[test]
    fn concurrency_fn_gets_same_guard_as_non_concurrency() {
        // Concurrency and non-concurrency functions get identical guards --
        // the context is threaded explicitly, so no special treatment is needed.
        let source_conc = r#"
fn concurrent_fn() {
    items.par_iter().for_each(|x| process(x));
}
"#;
        let source_simple = r#"
fn simple_fn() {
    do_stuff();
}
"#;
        let measured_conc: HashMap<String, u32> =
            HashMap::from([("concurrent_fn".to_string(), 0u32)]);
        let measured_simple: HashMap<String, u32> =
            HashMap::from([("simple_fn".to_string(), 0u32)]);

        let result_conc = instrument(source_conc, &measured_conc, false, "").unwrap();
        let result_simple =
            instrument(source_simple, &measured_simple, false, "").unwrap();

        // Both should have the same guard pattern.
        assert!(result_conc.source.contains("__piano_ctx.enter(0)"));
        assert!(result_simple.source.contains("__piano_ctx.enter(0)"));
        // Neither should have fork/adopt.
        assert!(!result_conc.source.contains("piano_runtime::fork()"));
        assert!(!result_conc.source.contains("piano_runtime::adopt"));
        // Concurrency IS still detected.
        assert!(!result_conc.concurrency.is_empty());
        assert!(result_simple.concurrency.is_empty());
    }

    #[test]
    fn call_site_gets_ctx_clone() {
        let source = r#"
fn caller() {
    callee(1, 2);
}

fn callee(a: i32, b: i32) {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("callee(1, 2, __piano_ctx.clone())"),
            "call to known function should get ctx.clone() appended. Got:\n{result}"
        );
    }

    #[test]
    fn call_site_unknown_function_no_ctx() {
        let source = r#"
fn caller() {
    unknown(1, 2);
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        // "unknown" is NOT in all_instrumentable.
        let all_instrumentable: HashSet<String> = ["caller".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            !result.contains("__piano_ctx.clone()"),
            "call to unknown function should NOT get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn self_method_call_gets_ctx_clone() {
        let source = r#"
struct Foo;

impl Foo {
    fn outer(&self) {
        self.inner();
    }

    fn inner(&self) {}
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("Foo::outer".to_string(), 0u32),
            ("Foo::inner".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["Foo::outer".to_string(), "Foo::inner".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("self.inner(__piano_ctx.clone())"),
            "self.method() call to known type::method should get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn qualified_call_gets_ctx_clone() {
        let source = r#"
fn caller() {
    module::helper();
}

fn helper() {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("helper".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "helper".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("module::helper(__piano_ctx.clone())"),
            "qualified path call where last segment is instrumentable should get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn call_inside_closure_gets_ctx_clone() {
        let source = r#"
fn caller() {
    let f = || {
        callee(1);
    };
}

fn callee(x: i32) {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("callee(1, __piano_ctx.clone())"),
            "call inside closure should also get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn main_does_not_get_ctx_param() {
        let source = r#"
fn main() {
    helper();
}

fn helper() {
    do_work();
}
"#;
        // main is NOT in measured -- it is the lifecycle boundary (creates root Ctx),
        // not a profiled function, so it is excluded from the name table.
        // helper is measured and instrumentable.
        let measured: HashMap<String, u32> = HashMap::from([("helper".to_string(), 0u32)]);
        // Both main and helper are instrumentable (main for call-site injection).
        let all_instrumentable: HashSet<String> =
            ["main".to_string(), "helper".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // main must NOT receive __piano_ctx parameter -- it creates the root Ctx via inject_shutdown.
        assert!(
            result.contains("fn main()"),
            "main should keep its original empty parameter list. Got:\n{result}"
        );
        // helper SHOULD receive __piano_ctx parameter.
        assert!(
            result.contains("fn helper(__piano_ctx: piano_runtime::ctx::Ctx)"),
            "helper should get ctx parameter. Got:\n{result}"
        );
        // Call site inside main should still get __piano_ctx.clone().
        assert!(
            result.contains("helper(__piano_ctx.clone())"),
            "call to helper inside main should get ctx.clone(). Got:\n{result}"
        );
    }

    // --- Move closure pre-clone tests (ownership: can't move ctx twice) ---

    #[test]
    fn move_closure_with_instrumented_call_gets_pre_clone() {
        let source = r#"
fn caller() {
    let f = move || {
        callee();
    };
}

fn callee() {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("let __piano_ctx = __piano_ctx.clone();\nlet f = move ||"),
            "move closure with instrumented call should get pre-clone before enclosing statement. Got:\n{result}"
        );
        assert!(
            result.contains("callee(__piano_ctx.clone())"),
            "call inside move closure should still get ctx.clone() arg. Got:\n{result}"
        );
    }

    #[test]
    fn non_move_closure_no_pre_clone() {
        let source = r#"
fn caller() {
    let f = || {
        callee();
    };
}

fn callee() {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // Non-move closures borrow __piano_ctx (Ctx is Sync), no pre-clone needed.
        assert!(
            !result.contains("let __piano_ctx = __piano_ctx.clone();\n"),
            "non-move closure should NOT get pre-clone. Got:\n{result}"
        );
        assert!(
            result.contains("callee(__piano_ctx.clone())"),
            "call inside non-move closure should still get ctx.clone() arg. Got:\n{result}"
        );
    }

    #[test]
    fn move_closure_without_instrumented_call_no_pre_clone() {
        let source = r#"
fn caller() {
    let f = move || {
        println!("hi");
    };
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> = ["caller".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            !result.contains("let __piano_ctx = __piano_ctx.clone();\n"),
            "move closure without instrumented calls should NOT get pre-clone. Got:\n{result}"
        );
    }

    #[test]
    fn move_closure_in_for_loop() {
        let source = r#"
fn caller() {
    for x in items {
        let f = move || {
            callee();
        };
    }
}

fn callee() {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("let __piano_ctx = __piano_ctx.clone();\nlet f = move ||"),
            "move closure in for-loop should get pre-clone before enclosing statement. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_move_closures() {
        let source = r#"
fn caller() {
    let f1 = move || {
        callee();
    };
    let f2 = move || {
        callee();
    };
}

fn callee() {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        let pre_clone_count = result.matches("let __piano_ctx = __piano_ctx.clone();\n").count();
        assert_eq!(
            pre_clone_count, 2,
            "each move closure should get its own pre-clone. Got {pre_clone_count} in:\n{result}"
        );
    }

    #[test]
    fn arbitrary_obj_method_not_matched() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    let obj = SomeStruct {};
    obj.process();
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "process".to_string()].into();
        let macro_name_ids: HashMap<String, u32> =
            HashMap::from([("caller".to_string(), 0u32), ("process".to_string(), 1u32)]);
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &macro_name_ids)
            .unwrap()
            .source;

        assert!(
            result.contains("obj.process()"),
            "arbitrary obj.method() should NOT be transformed. Got:\n{result}"
        );
        assert!(
            !result.contains("obj.process(__piano_ctx.clone())"),
            "arbitrary obj.method() should NOT get ctx.clone() injected. Got:\n{result}"
        );
    }

    // --- Inner fn instrumentation, closure exclusion ---

    #[test]
    fn inner_fn_is_instrumented() {
        let source = r#"
fn outer(__piano_ctx: piano_runtime::ctx::Ctx) {
    fn inner(x: i32) -> i32 {
        x + 1
    }
    inner(5);
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("outer".to_string(), 0u32),
            ("inner".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["outer".to_string(), "inner".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // Inner named fn receives its own ctx parameter (independent compiled function).
        assert!(
            result.contains("fn inner(x: i32, __piano_ctx: piano_runtime::ctx::Ctx)"),
            "inner fn should receive __piano_ctx parameter. Got:\n{result}"
        );
        // Inner named fn receives its own guard.
        assert!(
            result.contains("__piano_ctx.enter(1)"),
            "inner fn should get guard injection with its name_id. Got:\n{result}"
        );
    }

    #[test]
    fn closure_is_not_instrumented() {
        let source = r#"
fn outer(__piano_ctx: piano_runtime::ctx::Ctx) {
    let f = |x: i32| x + 1;
    f(5);
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("outer".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["outer".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // Closures do NOT get ctx parameter injection (instrumenting closures
        // changes their capture set, which can break thread-safety traits).
        assert!(
            !result.contains("|x: i32, __piano_ctx"),
            "closure should NOT receive __piano_ctx parameter. Got:\n{result}"
        );
        // Closures do NOT get guard injection.
        let guard_count = result.matches("__piano_ctx.enter(").count();
        assert_eq!(
            guard_count, 1,
            "only outer should have a guard, not the closure. Got {guard_count} in:\n{result}"
        );
    }

    // ---- Macro call-site injection tests ----

    #[test]
    fn macro_call_site_basic() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(child_a());
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "child_a".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("child_a(__piano_ctx.clone())"),
            "call inside macro should get ctx.clone() injected. Got:\n{result}"
        );
    }

    #[test]
    fn macro_call_site_with_args() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(child_a(x, y));
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "child_a".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("child_a(x, y, __piano_ctx.clone())"),
            "call inside macro with args should get ctx.clone() appended. Got:\n{result}"
        );
    }

    #[test]
    fn macro_call_site_multiple_calls() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(child_a(), child_b());
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "child_a".to_string(), "child_b".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("child_a(__piano_ctx.clone())"),
            "first call inside macro should get ctx.clone(). Got:\n{result}"
        );
        assert!(
            result.contains("child_b(__piano_ctx.clone())"),
            "second call inside macro should get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn macro_call_site_path_qualified() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(module::child_a());
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "child_a".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("module::child_a(__piano_ctx.clone())"),
            "path-qualified call inside macro should get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn macro_call_site_non_instrumentable_unchanged() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(not_instrumented());
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            !result.contains("__piano_ctx.clone()"),
            "non-instrumentable call inside macro should NOT get ctx.clone(). Got:\n{result}"
        );
    }

    #[test]
    fn macro_call_site_nested_parens() {
        let source = r#"
fn caller(__piano_ctx: piano_runtime::ctx::Ctx) {
    some_macro!(child_a(f(x)));
}
"#;
        let measured: HashMap<String, u32> = HashMap::from([("caller".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "child_a".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        assert!(
            result.contains("child_a(f(x), __piano_ctx.clone())"),
            "outer instrumentable call should get ctx.clone(), inner non-instrumentable should not. Got:\n{result}"
        );
        // Ensure f(x) did NOT get injected (f is not instrumentable)
        assert!(
            !result.contains("f(x, __piano_ctx.clone())"),
            "inner non-instrumentable call should NOT get ctx.clone(). Got:\n{result}"
        );
    }

    // --- Pass-through vs measured tests ---
    //
    // When selectors narrow the measured set, non-measured functions become
    // pass-through: they receive ctx parameter (for context chain propagation)
    // but NO guard (zero profiling overhead).

    /// A non-measured function gets ctx parameter but no guard (zero profiling overhead).
    #[test]
    fn pass_through_gets_ctx_but_no_guard() {
        let source = r#"
fn measured() {
    do_stuff();
}

fn pass_through() {
    measured();
}
"#;
        // Only "measured" is in the measured map; "pass_through" is instrumentable but not measured.
        let measured_map: HashMap<String, u32> = HashMap::from([("measured".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            HashSet::from(["measured".to_string(), "pass_through".to_string()]);
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        // measured() should have both ctx param and guard.
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "measured function should get guard. Got:\n{result}"
        );

        // pass_through() should have ctx param but no guard.
        // Count ctx parameters -- both functions should have one.
        let ctx_param_count = result.matches("__piano_ctx: piano_runtime::ctx::Ctx").count();
        assert_eq!(
            ctx_param_count, 2,
            "both functions should get ctx param (measured + pass-through). Got {ctx_param_count} in:\n{result}"
        );

        // Only one guard total (for the measured function).
        let guard_count = result.matches("__piano_ctx.enter(").count();
        assert_eq!(
            guard_count, 1,
            "only measured function should get guard. Got {guard_count} in:\n{result}"
        );
    }

    /// Pass-through functions forward ctx.clone() at call sites of measured functions.
    #[test]
    fn pass_through_forwards_ctx_clone_to_measured_calls() {
        let source = r#"
fn measured() {
    do_stuff();
}

fn pass_through() {
    measured();
}
"#;
        let measured_map: HashMap<String, u32> = HashMap::from([("measured".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            HashSet::from(["measured".to_string(), "pass_through".to_string()]);
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        // The call to measured() inside pass_through should get ctx.clone() injected.
        assert!(
            result.contains("measured(__piano_ctx.clone())"),
            "call to measured fn should get ctx.clone() forwarded. Got:\n{result}"
        );
    }

    /// The runtime has no knowledge of pass-through vs measured -- the distinction
    /// is entirely in the rewriter. Pass-through functions produce no runtime calls.
    #[test]
    fn pass_through_produces_no_runtime_calls() {
        let source = r#"
fn measured() {
    do_stuff();
}

fn pass_through_only() {
    helper();
}
"#;
        // pass_through_only is instrumentable but not measured, and calls no measured functions.
        let measured_map: HashMap<String, u32> = HashMap::from([("measured".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            HashSet::from(["measured".to_string(), "pass_through_only".to_string()]);
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        // Split at the start of pass_through_only's body to isolate it.
        let pt_start = result.find("fn pass_through_only").expect("function must exist");
        let pt_body = &result[pt_start..];
        let pt_end = pt_body.find("\n\nfn ").unwrap_or(pt_body.len());
        let pt_section = &pt_body[..pt_end];

        // pass_through_only should have ctx param but no enter() call.
        assert!(
            pt_section.contains("__piano_ctx: piano_runtime::ctx::Ctx"),
            "pass-through should have ctx param. Got:\n{pt_section}"
        );
        assert!(
            !pt_section.contains("__piano_ctx.enter("),
            "pass-through should NOT call enter(). Got:\n{pt_section}"
        );
        assert!(
            !pt_section.contains("enter_async("),
            "pass-through should NOT call enter_async(). Got:\n{pt_section}"
        );
    }

    /// Impl method as pass-through gets ctx but no guard (zero profiling overhead).
    #[test]
    fn pass_through_impl_method_gets_ctx_no_guard() {
        let source = r#"
struct Server;

impl Server {
    fn handle(&self) {
        self.process();
    }

    fn process(&self) {
        do_work();
    }
}
"#;
        // Only Server::process is measured; Server::handle is pass-through.
        let measured_map: HashMap<String, u32> =
            HashMap::from([("Server::process".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            HashSet::from(["Server::handle".to_string(), "Server::process".to_string()]);
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        // Both methods should get ctx param.
        let ctx_count = result.matches("__piano_ctx: piano_runtime::ctx::Ctx").count();
        assert_eq!(
            ctx_count, 2,
            "both methods should get ctx param. Got {ctx_count} in:\n{result}"
        );

        // Only process() should get guard.
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "measured method should get guard. Got:\n{result}"
        );
        let guard_count = result.matches("__piano_ctx.enter(").count();
        assert_eq!(
            guard_count, 1,
            "only measured method should get guard. Got {guard_count} in:\n{result}"
        );
    }

    /// When all selectors are empty (default), every function is measured.
    #[test]
    fn default_measures_everything() {
        let source = r#"
fn alpha() {
    do_a();
}

fn beta() {
    do_b();
}

fn gamma() {
    do_c();
}
"#;
        // All functions are both measured and instrumentable (default mode).
        let measured_map: HashMap<String, u32> = HashMap::from([
            ("alpha".to_string(), 0u32),
            ("beta".to_string(), 1u32),
            ("gamma".to_string(), 2u32),
        ]);
        let all_instrumentable: HashSet<String> = measured_map.keys().cloned().collect();
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        let guard_count = result.matches("__piano_ctx.enter(").count();
        assert_eq!(
            guard_count, 3,
            "all functions should be measured. Got {guard_count} in:\n{result}"
        );
        let ctx_count = result.matches("__piano_ctx: piano_runtime::ctx::Ctx").count();
        assert_eq!(
            ctx_count, 3,
            "all functions should get ctx param. Got {ctx_count} in:\n{result}"
        );
    }

    /// --skip semantics at the rewriter level: a function in all_instrumentable
    /// but not in measured behaves as pass-through (ctx param, no guard).
    #[test]
    fn skip_removes_from_measured_set() {
        let source = r#"
fn kept() {
    do_stuff();
}

fn skipped() {
    do_other();
}
"#;
        // Both are instrumentable, but "skipped" is excluded from measured (--skip effect).
        let measured_map: HashMap<String, u32> = HashMap::from([("kept".to_string(), 0u32)]);
        let all_instrumentable: HashSet<String> =
            HashSet::from(["kept".to_string(), "skipped".to_string()]);
        let macro_name_ids: HashMap<String, u32> = HashMap::new();
        let result = instrument_source(
            source,
            &measured_map,
            &all_instrumentable,
            false,
            "",
            &macro_name_ids,
        )
        .unwrap()
        .source;

        // kept() should have guard.
        assert!(
            result.contains("__piano_ctx.enter(0)"),
            "kept function should have guard. Got:\n{result}"
        );

        // skipped() should have ctx param but no guard.
        let guard_count = result.matches("__piano_ctx.enter(").count();
        assert_eq!(
            guard_count, 1,
            "only kept function should get guard (skipped is pass-through). Got {guard_count} in:\n{result}"
        );

        // Both should have ctx param.
        let ctx_count = result.matches("__piano_ctx: piano_runtime::ctx::Ctx").count();
        assert_eq!(
            ctx_count, 2,
            "both functions should get ctx param. Got {ctx_count} in:\n{result}"
        );
    }

    /// Move closure passed as method argument gets pre-clone at statement level,
    /// not inside the argument list (Rust ownership: can't move ctx twice).
    #[test]
    fn move_closure_as_method_arg_pre_clone_at_stmt() {
        let source = r#"
fn main() {
    rayon::scope(|s| {
        for i in 0..4 {
            s.spawn(move |_| {
                worker(i);
            });
        }
    });
}

fn worker(id: usize) {}
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("main".to_string(), 0u32),
            ("worker".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["main".to_string(), "worker".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // Pre-clone must be a separate statement BEFORE s.spawn(), not inside
        // the argument list. The closure itself should still get ctx.clone()
        // injected at the worker() call site.
        assert!(
            result.contains("let __piano_ctx = __piano_ctx.clone();\ns.spawn(move |_|"),
            "pre-clone should be before the s.spawn() statement, not inside arg list. Got:\n{result}"
        );
        assert!(
            result.contains("worker(i, __piano_ctx.clone())"),
            "call to worker inside closure should get ctx.clone(). Got:\n{result}"
        );
        // s.spawn() must NOT get ctx.clone() injected (arbitrary object method).
        assert!(
            !result.contains("s.spawn(move |_| {\n                worker(i, __piano_ctx.clone());\n            }, __piano_ctx.clone())"),
            "s.spawn() should NOT get ctx.clone() as second arg. Got:\n{result}"
        );
    }

    /// Move closure as argument to function call (not method call)
    /// also gets pre-clone at statement level.
    #[test]
    fn move_closure_as_fn_arg_pre_clone_at_stmt() {
        let source = r#"
fn caller() {
    run_task(move || {
        callee();
    });
}

fn callee() {}
fn run_task(f: impl FnOnce()) { f(); }
"#;
        let measured: HashMap<String, u32> = HashMap::from([
            ("caller".to_string(), 0u32),
            ("callee".to_string(), 1u32),
        ]);
        let all_instrumentable: HashSet<String> =
            ["caller".to_string(), "callee".to_string(), "run_task".to_string()].into();
        let result = instrument_source(source, &measured, &all_instrumentable, false, "", &HashMap::new())
            .unwrap()
            .source;

        // Pre-clone at statement level, not inside run_task()'s argument list.
        assert!(
            result.contains("let __piano_ctx = __piano_ctx.clone();\nrun_task(move ||"),
            "pre-clone should be before run_task() statement. Got:\n{result}"
        );
    }
}
