use std::collections::HashSet;

use quote::quote;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;

use crate::resolve::is_instrumentable;
use crate::source_map::{SourceMap, StringInjector};

/// Result of instrumenting a source file.
pub struct InstrumentResult {
    pub source: String,
    pub source_map: SourceMap,
    /// Functions that contain concurrency patterns, with the pattern name.
    /// e.g. [("concurrent_discover", "rayon::scope"), ("process_all", "par_iter")]
    pub concurrency: Vec<(String, String)>,
}

/// Rewrite `source` so that every function whose name (or qualified name) is in
/// `targets` gets an RAII timing guard injected as its first statement.
///
/// Top-level functions match by bare name (e.g. "walk"). Impl methods match by
/// "Type::method" (e.g. "Walker::walk"). Trait default methods match by "Trait::method" (e.g. "Drawable::draw").
///
pub fn instrument_source(
    source: &str,
    targets: &HashSet<String>,
    instrument_macros: bool,
) -> Result<InstrumentResult, syn::Error> {
    let file: syn::File = syn::parse_str(source)?;

    // Collect injection points via read-only visitor.
    let mut collector = InjectionCollector {
        source,
        targets: targets.clone(),
        injector: StringInjector::new(),
        current_impl: None,
        current_trait: None,
        concurrency: Vec::new(),
    };
    collector.visit_file(&file);

    let (rewritten, source_map) = collector.injector.apply(source);

    // Macro instrumentation uses token-stream mutation (separate from the
    // string-injection path). Apply it on the rewritten source if needed.
    // prettyplease reformats the entire file, invalidating our source_map,
    // so we reset it to empty when this path is taken.
    let (final_source, final_map) = if instrument_macros {
        let mut rewritten_file: syn::File = syn::parse_str(&rewritten)?;
        let mut macro_visitor = MacroInstrumenter;
        macro_visitor.visit_file_mut(&mut rewritten_file);
        (prettyplease::unparse(&rewritten_file), SourceMap::default())
    } else {
        (rewritten, source_map)
    };

    Ok(InstrumentResult {
        source: final_source,
        source_map: final_map,
        concurrency: collector.concurrency,
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

/// Function/method names where closures should receive adopt injection
/// (i.e., the closure body starts with `adopt(ctx)` so its time is attributed
/// to the parent span).
const ADOPT_INJECTION_TARGETS: &[&str] = &["spawn", "scope", "scope_fifo", "join"];
/// Scope-like functions whose closures coordinate workers (don't adopt, recurse body).
const SCOPE_FUNCTIONS: &[&str] = &["scope", "scope_fifo"];

/// Functions that trigger fork injection at the call site (i.e., the parent
/// function gets `let ctx = fork()` so child closures can adopt the span).
///
/// "spawn" is intentionally excluded: all free-function spawns
/// (std::thread::spawn, rayon::spawn, tokio::spawn) require F: Send + 'static.
/// Moving the owned SpanContext into the closure compiles, but CPU time
/// attribution breaks: SpanContext::drop calls apply_children() which accesses
/// the STACK thread-local. When SpanContext drops on the child thread, the
/// child's stack is empty (AdoptGuard already popped the synthetic parent),
/// so the child's CPU contribution is silently lost.
///
/// Scoped s.spawn() method calls inside scope closure bodies are handled
/// separately by recurse_closure_body_for_spawns -- they work because the
/// scope guarantees the closure completes before the parent returns, so
/// SpanContext stays on the parent thread.
const FORK_INJECTION_TRIGGERS: &[&str] = &["scope", "scope_fifo", "join"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FutureReturnKind {
    ImplFuture,
    PinBoxDynFuture,
    KnownAlias,
}

fn returns_future(sig: &syn::Signature) -> Option<FutureReturnKind> {
    let syn::ReturnType::Type(_, ty) = &sig.output else {
        return None;
    };
    returns_future_ty(ty)
}

fn returns_future_ty(ty: &syn::Type) -> Option<FutureReturnKind> {
    match ty {
        syn::Type::ImplTrait(impl_trait) => {
            if has_future_bound(&impl_trait.bounds) {
                Some(FutureReturnKind::ImplFuture)
            } else {
                None
            }
        }
        syn::Type::Path(type_path) => {
            let last = type_path.path.segments.last()?;
            let ident = last.ident.to_string();
            match ident.as_str() {
                "Pin" => check_pin_box_dyn_future(last),
                "BoxFuture" | "LocalBoxFuture" => Some(FutureReturnKind::KnownAlias),
                _ => None,
            }
        }
        syn::Type::Paren(paren) => returns_future_ty(&paren.elem),
        _ => None,
    }
}

fn has_future_bound(
    bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::token::Plus>,
) -> bool {
    bounds.iter().any(|bound| {
        if let syn::TypeParamBound::Trait(trait_bound) = bound {
            is_future_path(&trait_bound.path)
        } else {
            false
        }
    })
}

fn is_future_path(path: &syn::Path) -> bool {
    path.segments
        .last()
        .is_some_and(|seg| seg.ident == "Future")
}

fn check_pin_box_dyn_future(pin_seg: &syn::PathSegment) -> Option<FutureReturnKind> {
    let inner_ty = first_type_arg(pin_seg)?;
    if let syn::Type::Path(type_path) = inner_ty {
        let last = type_path.path.segments.last()?;
        if last.ident == "Box" {
            let box_inner = first_type_arg(last)?;
            if let syn::Type::TraitObject(trait_obj) = box_inner {
                if has_future_bound(&trait_obj.bounds) {
                    return Some(FutureReturnKind::PinBoxDynFuture);
                }
            }
        }
    }
    None
}

fn first_type_arg(seg: &syn::PathSegment) -> Option<&syn::Type> {
    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
        args.args.iter().find_map(|arg| {
            if let syn::GenericArgument::Type(ty) = arg {
                Some(ty)
            } else {
                None
            }
        })
    } else {
        None
    }
}

/// Convert a proc_macro2 LineColumn to a byte offset in the source string.
/// Handles both `\n` and `\r\n` line endings correctly by splitting on `\n`
/// so that any trailing `\r` stays in the line's byte length.
fn line_col_to_byte(source: &str, lc: proc_macro2::LineColumn) -> usize {
    debug_assert!(lc.line >= 1, "proc_macro2 lines are 1-indexed");
    let mut byte = 0;
    for (i, line) in source.split('\n').enumerate() {
        if i + 1 == lc.line {
            return byte + lc.column;
        }
        byte += line.len() + 1; // +1 for the \n we split on
    }
    byte + lc.column
}

/// Read-only AST visitor that collects text injection points.
struct InjectionCollector<'s> {
    source: &'s str,
    targets: HashSet<String>,
    injector: StringInjector,
    current_impl: Option<String>,
    current_trait: Option<String>,
    /// Collected concurrency info: (function_name, pattern_name).
    concurrency: Vec<(String, String)>,
}

impl<'s> InjectionCollector<'s> {
    fn collect_guard(&mut self, block: &syn::Block, name: &str, sig: &syn::Signature) {
        if !self.targets.contains(name) {
            return;
        }
        let is_async = sig.asyncness.is_some();

        // For non-async functions returning futures, wrap only the trailing expression.
        if !is_async {
            if let Some(kind) = returns_future(sig) {
                self.collect_future_return_guard(block, name, sig, kind);
                return;
            }
        }

        let open_pos = block.brace_token.span.open().start();
        let open_byte = line_col_to_byte(self.source, open_pos) + 1; // after '{'

        // Check for concurrency patterns.
        if let Some(pattern) = find_concurrency_pattern(block) {
            self.concurrency.push((name.to_string(), pattern));

            if is_async {
                // Async + concurrency: wrap entire body in PianoFuture with
                // guard + fork inside.
                let close_pos = block.brace_token.span.close().start();
                let close_byte = line_col_to_byte(self.source, close_pos);

                self.injector.insert(
                    open_byte,
                    format!(
                        "\npiano_runtime::PianoFuture::new(async move {{\
                         \n    let __piano_guard = piano_runtime::enter({name:?});\
                         \n    let __piano_ctx_owned = piano_runtime::fork();\
                         \n    let __piano_ctx = __piano_ctx_owned.as_ref();"
                    ),
                );

                // Collect adopt injections for closures inside the block.
                collect_adopt_in_block_stmts(
                    &block.stmts,
                    false,
                    false,
                    self.source,
                    &mut self.injector,
                );

                self.injector.insert(close_byte, "\n}).await\n");
            } else {
                // Sync + concurrency: guard + fork at top.
                self.injector.insert(
                    open_byte,
                    format!(
                        "\n    let __piano_guard = piano_runtime::enter({name:?});\
                         \n    let __piano_ctx_owned = piano_runtime::fork();\
                         \n    let __piano_ctx = __piano_ctx_owned.as_ref();"
                    ),
                );

                // Collect adopt injections for closures inside the block.
                collect_adopt_in_block_stmts(
                    &block.stmts,
                    false,
                    false,
                    self.source,
                    &mut self.injector,
                );
            }
            return;
        }

        // Simple case: no concurrency.
        if is_async {
            // Async function: wrap body in PianoFuture.
            let close_pos = block.brace_token.span.close().start();
            let close_byte = line_col_to_byte(self.source, close_pos);

            self.injector.insert(
                open_byte,
                format!(
                    "\npiano_runtime::PianoFuture::new(async move {{\
                     \n    let __piano_guard = piano_runtime::enter({name:?});"
                ),
            );
            self.injector.insert(close_byte, "\n}).await\n");
        } else {
            // Sync function: just inject guard.
            self.injector.insert(
                open_byte,
                format!("\n    let __piano_guard = piano_runtime::enter({name:?});"),
            );
        }
    }

    /// Collect injections for a future-returning (non-async) function.
    fn collect_future_return_guard(
        &mut self,
        block: &syn::Block,
        name: &str,
        sig: &syn::Signature,
        kind: FutureReturnKind,
    ) {
        // Find trailing expression.
        let Some(syn::Stmt::Expr(trailing_expr, None)) = block.stmts.last() else {
            // No trailing expression -- fall back to sync guard.
            let open_pos = block.brace_token.span.open().start();
            let open_byte = line_col_to_byte(self.source, open_pos) + 1;
            self.injector.insert(
                open_byte,
                format!("\n    let __piano_guard = piano_runtime::enter({name:?});"),
            );
            return;
        };

        let trailing_span = trailing_expr.span();
        let trailing_start = line_col_to_byte(self.source, trailing_span.start());
        let trailing_end = line_col_to_byte(self.source, trailing_span.end());

        match kind {
            FutureReturnKind::ImplFuture => {
                self.injector.insert(
                    trailing_start,
                    format!(
                        "piano_runtime::PianoFuture::new(async move {{\
                         \n        let __piano_guard = piano_runtime::enter({name:?});\
                         \n        ("
                    ),
                );
                self.injector.insert(trailing_end, ").await\n    })");
            }
            FutureReturnKind::PinBoxDynFuture | FutureReturnKind::KnownAlias => {
                let return_type = match &sig.output {
                    syn::ReturnType::Type(_, ty) => quote!(#ty).to_string(),
                    _ => unreachable!("returns_future already checked this"),
                };

                // For boxed-future returns, wrap early `return` expressions too.
                self.collect_return_wrappers(block, name, &return_type);

                self.injector.insert(
                    trailing_start,
                    format!(
                        "Box::pin(piano_runtime::PianoFuture::new(async move {{\
                         \n        let __piano_guard = piano_runtime::enter({name:?});\
                         \n        let __piano_inner: {return_type} = "
                    ),
                );
                self.injector
                    .insert(trailing_end, ";\n        __piano_inner.await\n    }))");
            }
        }
    }

    /// Walk the block's preceding statements for `return <expr>` and wrap them.
    fn collect_return_wrappers(&mut self, block: &syn::Block, name: &str, return_type: &str) {
        // Walk all stmts except the last (trailing expr) for return expressions.
        let stmts = if block.stmts.len() > 1 {
            &block.stmts[..block.stmts.len() - 1]
        } else {
            return;
        };
        for stmt in stmts {
            collect_return_wrappers_in_stmt(
                stmt,
                name,
                return_type,
                self.source,
                &mut self.injector,
            );
        }
    }
}

/// Walk a statement tree for `return <expr>` expressions (for future-returning functions).
/// Inject wrapping text around the return expression value.
fn collect_return_wrappers_in_stmt(
    stmt: &syn::Stmt,
    name: &str,
    return_type: &str,
    source: &str,
    injector: &mut StringInjector,
) {
    match stmt {
        syn::Stmt::Expr(expr, _) => {
            collect_return_wrappers_in_expr(expr, name, return_type, source, injector);
        }
        syn::Stmt::Local(local) => {
            if let Some(init) = &local.init {
                collect_return_wrappers_in_expr(&init.expr, name, return_type, source, injector);
            }
        }
        _ => {}
    }
}

fn collect_return_wrappers_in_expr(
    expr: &syn::Expr,
    name: &str,
    return_type: &str,
    source: &str,
    injector: &mut StringInjector,
) {
    match expr {
        // Boundaries: return inside closures/async blocks is not our function's return.
        syn::Expr::Closure(_) | syn::Expr::Async(_) => {}
        syn::Expr::Return(ret) => {
            if let Some(inner) = &ret.expr {
                let inner_start = line_col_to_byte(source, inner.span().start());
                let inner_end = line_col_to_byte(source, inner.span().end());
                injector.insert(
                    inner_start,
                    format!(
                        "Box::pin(piano_runtime::PianoFuture::new(async move {{\
                         \n            let __piano_guard = piano_runtime::enter({name:?});\
                         \n            let __piano_inner: {return_type} = "
                    ),
                );
                injector.insert(inner_end, ";\n            __piano_inner.await\n        }))");
            }
        }
        // Recurse into sub-expressions.
        syn::Expr::Block(b) => {
            for stmt in &b.block.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
        }
        syn::Expr::If(i) => {
            for stmt in &i.then_branch.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
            if let Some((_, else_expr)) = &i.else_branch {
                collect_return_wrappers_in_expr(else_expr, name, return_type, source, injector);
            }
        }
        syn::Expr::Match(m) => {
            for arm in &m.arms {
                collect_return_wrappers_in_expr(&arm.body, name, return_type, source, injector);
            }
        }
        syn::Expr::ForLoop(f) => {
            for stmt in &f.body.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
        }
        syn::Expr::While(w) => {
            for stmt in &w.body.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
        }
        syn::Expr::Loop(l) => {
            for stmt in &l.body.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
        }
        syn::Expr::Unsafe(u) => {
            for stmt in &u.block.stmts {
                collect_return_wrappers_in_stmt(stmt, name, return_type, source, injector);
            }
        }
        _ => {}
    }
}

/// Find the first concurrency pattern in a block and return its name.
///
/// For method calls matching PARALLEL_ITER_METHODS: returns the method name (e.g. "par_iter").
/// For method calls matching FORK_INJECTION_TRIGGERS: returns the method name (e.g. "scope").
/// For function calls matching FORK_INJECTION_TRIGGERS: returns the full path (e.g. "rayon::scope").
/// Stops recursion at detached spawn calls (method or function) since their
/// 'static boundary prevents fork/adopt from working.
fn find_concurrency_pattern(block: &syn::Block) -> Option<String> {
    block.stmts.iter().find_map(find_pattern_in_stmt)
}

fn find_pattern_in_stmt(stmt: &syn::Stmt) -> Option<String> {
    match stmt {
        syn::Stmt::Expr(e, _) => find_pattern_in_expr(e),
        syn::Stmt::Local(local) => local
            .init
            .as_ref()
            .and_then(|init| find_pattern_in_expr(&init.expr)),
        _ => None,
    }
}

/// Extract the last path segment name from a function call expression.
/// e.g. `rayon::scope(...)` -> Some("scope"), `foo(...)` -> Some("foo").
fn call_func_name(call: &syn::ExprCall) -> Option<String> {
    if let syn::Expr::Path(path) = &*call.func {
        path.path.segments.last().map(|s| s.ident.to_string())
    } else {
        None
    }
}

fn find_pattern_in_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            if PARALLEL_ITER_METHODS.contains(&method.as_str()) {
                return Some(method);
            }
            if FORK_INJECTION_TRIGGERS.contains(&method.as_str()) {
                return Some(method);
            }
            // Don't recurse into detached .spawn() args -- anything inside
            // inherits the 'static boundary, so fork/adopt can't help.
            if method == "spawn" {
                return None;
            }
            if let Some(p) = find_pattern_in_expr(&mc.receiver) {
                return Some(p);
            }
            mc.args.iter().find_map(find_pattern_in_expr)
        }
        syn::Expr::Call(call) => {
            let name = call_func_name(call);
            if let Some(ref n) = name
                && FORK_INJECTION_TRIGGERS.contains(&n.as_str())
            {
                // Build the full path, e.g. "rayon::scope"
                if let syn::Expr::Path(path) = &*call.func {
                    let full_path: String = path
                        .path
                        .segments
                        .iter()
                        .map(|s| s.ident.to_string())
                        .collect::<Vec<_>>()
                        .join("::");
                    return Some(full_path);
                }
            }
            // Don't recurse into detached spawn args -- anything inside
            // inherits the 'static boundary, so fork/adopt can't help.
            if name.as_deref() == Some("spawn") {
                return None;
            }
            call.args.iter().find_map(find_pattern_in_expr)
        }
        syn::Expr::Block(b) => b.block.stmts.iter().find_map(find_pattern_in_stmt),
        syn::Expr::Closure(c) => find_pattern_in_expr(&c.body),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// String-injection adopt collection (read-only AST walk)
// ---------------------------------------------------------------------------

// Hard-coded 8-space indent — may not match user's style, but the
// instrumented source is transient (never shown to users).
const ADOPT_STMT_TEXT: &str =
    "\n        let __piano_adopt = __piano_ctx.map(|c| piano_runtime::adopt(c));";

/// Walk statements in a block and collect adopt injections at closure brace positions.
fn collect_adopt_in_block_stmts(
    stmts: &[syn::Stmt],
    in_parallel_chain: bool,
    in_scope_body: bool,
    source: &str,
    injector: &mut StringInjector,
) {
    for stmt in stmts {
        match stmt {
            syn::Stmt::Expr(expr, _) => {
                collect_adopt_in_expr(expr, in_parallel_chain, in_scope_body, source, injector);
            }
            syn::Stmt::Local(local) => {
                if let Some(init) = &local.init {
                    collect_adopt_in_expr(
                        &init.expr,
                        in_parallel_chain,
                        in_scope_body,
                        source,
                        injector,
                    );
                }
            }
            _ => {}
        }
    }
}

/// Read-only version of inject_adopt_in_concurrency_closures.
/// Collects byte offsets for adopt injection instead of mutating the AST.
fn collect_adopt_in_expr(
    expr: &syn::Expr,
    in_parallel_chain: bool,
    in_scope_body: bool,
    source: &str,
    injector: &mut StringInjector,
) {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            let is_par = PARALLEL_ITER_METHODS.contains(&method.as_str());
            let is_spawn = ADOPT_INJECTION_TARGETS.contains(&method.as_str());
            let is_scope = SCOPE_FUNCTIONS.contains(&method.as_str());
            let is_detached = method == "spawn" && !in_scope_body;

            // Recurse into receiver first
            collect_adopt_in_expr(
                &mc.receiver,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );

            let chain_active =
                in_parallel_chain || is_par || receiver_has_parallel_method(&mc.receiver);

            if is_detached {
                // Detached spawn -- don't inject adopt.
            } else if is_scope {
                // Scope closures are coordinators -- recurse for nested spawns.
                for arg in &mc.args {
                    if let syn::Expr::Closure(closure) = arg {
                        collect_adopt_in_scope_closure(closure, source, injector);
                    } else {
                        collect_adopt_in_expr(arg, false, in_scope_body, source, injector);
                    }
                }
            } else if (chain_active && !is_par) || is_spawn {
                // Worker closures: inject adopt, then recurse body.
                for arg in &mc.args {
                    if let syn::Expr::Closure(closure) = arg {
                        collect_adopt_at_closure_start(closure, source, injector);
                        if let syn::Expr::Block(block) = &*closure.body {
                            collect_adopt_in_block_stmts(
                                &block.block.stmts,
                                false,
                                in_scope_body,
                                source,
                                injector,
                            );
                        }
                    } else {
                        collect_adopt_in_expr(arg, false, in_scope_body, source, injector);
                    }
                }
            } else {
                for arg in &mc.args {
                    collect_adopt_in_expr(arg, chain_active, in_scope_body, source, injector);
                }
            }
        }
        syn::Expr::Call(call) => {
            let func_name = call_func_name(call);
            let is_spawn = func_name
                .as_ref()
                .is_some_and(|n| ADOPT_INJECTION_TARGETS.contains(&n.as_str()));
            let is_scope = func_name
                .as_ref()
                .is_some_and(|n| SCOPE_FUNCTIONS.contains(&n.as_str()));
            let is_detached = func_name.as_deref() == Some("spawn");

            if is_scope {
                for arg in &call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        collect_adopt_in_scope_closure(closure, source, injector);
                    } else {
                        collect_adopt_in_expr(arg, false, in_scope_body, source, injector);
                    }
                }
            } else if is_spawn && !is_detached {
                for arg in &call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        collect_adopt_at_closure_start(closure, source, injector);
                        if let syn::Expr::Block(block) = &*closure.body {
                            collect_adopt_in_block_stmts(
                                &block.block.stmts,
                                false,
                                in_scope_body,
                                source,
                                injector,
                            );
                        }
                    } else {
                        collect_adopt_in_expr(arg, false, in_scope_body, source, injector);
                    }
                }
            } else if is_detached {
                // Detached spawn -- don't recurse.
            } else {
                for arg in &call.args {
                    collect_adopt_in_expr(arg, false, in_scope_body, source, injector);
                }
            }
        }
        syn::Expr::Block(b) => {
            collect_adopt_in_block_stmts(
                &b.block.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
        }
        syn::Expr::ForLoop(f) => {
            collect_adopt_in_block_stmts(
                &f.body.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
        }
        syn::Expr::While(w) => {
            collect_adopt_in_block_stmts(
                &w.body.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
        }
        syn::Expr::Loop(l) => {
            collect_adopt_in_block_stmts(
                &l.body.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
        }
        syn::Expr::If(i) => {
            collect_adopt_in_block_stmts(
                &i.then_branch.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
            if let Some((_, else_branch)) = &i.else_branch {
                collect_adopt_in_expr(
                    else_branch,
                    in_parallel_chain,
                    in_scope_body,
                    source,
                    injector,
                );
            }
        }
        syn::Expr::Match(m) => {
            collect_adopt_in_expr(&m.expr, in_parallel_chain, in_scope_body, source, injector);
            for arm in &m.arms {
                if let Some((_, guard)) = &arm.guard {
                    collect_adopt_in_expr(
                        guard,
                        in_parallel_chain,
                        in_scope_body,
                        source,
                        injector,
                    );
                }
                collect_adopt_in_expr(
                    &arm.body,
                    in_parallel_chain,
                    in_scope_body,
                    source,
                    injector,
                );
            }
        }
        syn::Expr::Unsafe(u) => {
            collect_adopt_in_block_stmts(
                &u.block.stmts,
                in_parallel_chain,
                in_scope_body,
                source,
                injector,
            );
        }
        _ => {}
    }
}

/// Inject adopt at the start of a closure body.
fn collect_adopt_at_closure_start(
    closure: &syn::ExprClosure,
    source: &str,
    injector: &mut StringInjector,
) {
    match &*closure.body {
        syn::Expr::Block(block) => {
            let open_pos = block.block.brace_token.span.open().start();
            let open_byte = line_col_to_byte(source, open_pos) + 1;
            injector.insert(open_byte, ADOPT_STMT_TEXT);
        }
        other => {
            // Closure without braces (e.g. `|x| x + 1`).
            // Inject adopt before the body expression by wrapping in a block.
            let body_start = line_col_to_byte(source, other.span().start());
            let body_end = line_col_to_byte(source, other.span().end());
            injector.insert(body_start, format!("{{ {ADOPT_STMT_TEXT}\n        "));
            injector.insert(body_end, "\n    }");
        }
    }
}

/// Recurse into a scope closure body for nested spawn calls.
fn collect_adopt_in_scope_closure(
    closure: &syn::ExprClosure,
    source: &str,
    injector: &mut StringInjector,
) {
    if let syn::Expr::Block(block) = &*closure.body {
        collect_adopt_in_block_stmts(&block.block.stmts, false, true, source, injector);
    }
}

// The old AST-mutation adopt injection functions (inject_adopt_in_concurrency_closures,
// inject_adopt_at_closure_start, recurse_closure_body_for_spawns, inject_adopt_in_stmts)
// have been replaced by the string-injection equivalents above (collect_adopt_in_expr,
// collect_adopt_at_closure_start, collect_adopt_in_scope_closure, collect_adopt_in_block_stmts).

fn receiver_has_parallel_method(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            PARALLEL_ITER_METHODS.contains(&method.as_str())
                || receiver_has_parallel_method(&mc.receiver)
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// macro_rules! token-stream scanning
// ---------------------------------------------------------------------------

/// Result of matching a fn pattern in a flat token vector.
struct FnMatch {
    /// The tokens that form the function name (either a single Ident for
    /// literal names, or [Punct('$'), Ident] for metavar names).
    name_tokens: Vec<proc_macro2::TokenTree>,
    /// Index of the brace Group containing the function body.
    body_index: usize,
}

/// Check whether an `ItemMacro` is a `macro_rules!` definition.
fn is_macro_rules(item: &syn::ItemMacro) -> bool {
    item.mac.path.is_ident("macro_rules")
}

/// Top-level entry: scan a macro_rules! token stream for rule arms and
/// instrument any fn items found in each arm's template body.
fn instrument_macro_tokens(tokens: &mut proc_macro2::TokenStream) {
    let mut tts: Vec<proc_macro2::TokenTree> = tokens.clone().into_iter().collect();
    let len = tts.len();
    let mut i = 0;
    let mut modified = false;

    while i < len {
        // Look for `=> { ... }` — the pattern separator followed by the
        // template group. The `=>` is two Punct tokens: `=` then `>`.
        if is_fat_arrow(&tts, i) {
            let template_idx = i + 2;
            if template_idx < len {
                if let proc_macro2::TokenTree::Group(ref group) = tts[template_idx] {
                    // Templates can use brace, paren, or bracket delimiters.
                    let mut inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    inject_fn_guards_in_tokens(&mut inner);
                    let new_stream: proc_macro2::TokenStream = inner.into_iter().collect();
                    let mut new_group = proc_macro2::Group::new(group.delimiter(), new_stream);
                    new_group.set_span(group.span());
                    tts[template_idx] = proc_macro2::TokenTree::Group(new_group);
                    modified = true;
                }
            }
            i += 3;
        } else {
            i += 1;
        }
    }

    if modified {
        *tokens = tts.into_iter().collect();
    }
}

/// Check if tokens[i..i+2] form a fat arrow `=>`.
fn is_fat_arrow(tokens: &[proc_macro2::TokenTree], i: usize) -> bool {
    if i + 1 >= tokens.len() {
        return false;
    }
    matches!(
        (&tokens[i], &tokens[i + 1]),
        (
            proc_macro2::TokenTree::Punct(eq),
            proc_macro2::TokenTree::Punct(gt),
        ) if eq.as_char() == '=' && eq.spacing() == proc_macro2::Spacing::Joint && gt.as_char() == '>'
    )
}

/// Scan a flat token vector for fn patterns and inject guards into each
/// matched function body. Recurses into brace groups to handle fn items
/// inside impl blocks within macro templates.
fn inject_fn_guards_in_tokens(tokens: &mut [proc_macro2::TokenTree]) {
    let mut i = 0;
    while i < tokens.len() {
        // Skip non-instrumentable fn definitions (const fn, unsafe fn, extern fn)
        // so we don't accidentally match the inner `fn` keyword.
        if let Some(skip_to) = skip_non_instrumentable_fn(tokens, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_pattern(tokens, i) {
            let body_idx = fm.body_index;
            // Build the guard token stream for this function name.
            let guard = make_guard_tokens(&fm.name_tokens);
            let guard_tts: Vec<proc_macro2::TokenTree> = guard.into_iter().collect();

            // Inject guard tokens at the start of the body brace group.
            if let proc_macro2::TokenTree::Group(ref group) = tokens[body_idx] {
                let mut body_tts: Vec<proc_macro2::TokenTree> =
                    group.stream().into_iter().collect();
                for (j, tt) in guard_tts.into_iter().enumerate() {
                    body_tts.insert(j, tt);
                }
                let new_stream: proc_macro2::TokenStream = body_tts.into_iter().collect();
                let mut new_group =
                    proc_macro2::Group::new(proc_macro2::Delimiter::Brace, new_stream);
                new_group.set_span(group.span());
                tokens[body_idx] = proc_macro2::TokenTree::Group(new_group);
            }
            // Advance past the body group.
            i = body_idx + 1;
        } else {
            // Recurse into brace groups (handles fn inside impl blocks, etc.).
            if let proc_macro2::TokenTree::Group(ref group) = tokens[i] {
                if group.delimiter() == proc_macro2::Delimiter::Brace {
                    let mut inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    inject_fn_guards_in_tokens(&mut inner);
                    let new_stream: proc_macro2::TokenStream = inner.into_iter().collect();
                    let mut new_group =
                        proc_macro2::Group::new(proc_macro2::Delimiter::Brace, new_stream);
                    new_group.set_span(group.span());
                    tokens[i] = proc_macro2::TokenTree::Group(new_group);
                }
            }
            i += 1;
        }
    }
}

/// If the token at position `i` starts a non-instrumentable fn definition
/// (const fn, unsafe fn, extern fn with non-Rust ABI), return the index just
/// past the body brace group so the caller can skip the entire definition.
/// Returns None if this is not a non-instrumentable fn.
///
/// Unlike match_fn_pattern, this requires `fn` to immediately follow the
/// modifier(s). A `const SIZE: usize = 42;` is NOT a const fn — we must not
/// greedily scan forward and swallow the next real fn.
fn skip_non_instrumentable_fn(tokens: &[proc_macro2::TokenTree], i: usize) -> Option<usize> {
    let len = tokens.len();
    let mut pos = i;

    // Optional: pub [(...)]
    if is_ident(tokens, pos, "pub") {
        pos += 1;
        if pos < len {
            if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
                if g.delimiter() == proc_macro2::Delimiter::Parenthesis {
                    pos += 1;
                }
            }
        }
    }

    // Must see one of: const, unsafe, extern (non-Rust ABI) — then `fn` must follow immediately.
    if is_ident(tokens, pos, "const") {
        pos += 1;
    } else if is_ident(tokens, pos, "unsafe") {
        pos += 1;
        // unsafe extern "ABI" fn
        if is_ident(tokens, pos, "extern") {
            pos += 1;
            if pos < len {
                if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                    pos += 1; // skip ABI string like "C"
                }
            }
        }
    } else if is_non_rust_extern(tokens, pos) {
        pos += 1;
        // extern "ABI" fn
        if pos < len {
            if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                pos += 1; // skip ABI string like "C"
            }
        }
    } else {
        return None;
    }

    // `fn` must be the very next token — no greedy scanning.
    if !is_ident(tokens, pos, "fn") {
        return None;
    }

    // Skip past fn, name, params, return type to find the body brace group.
    pos += 1;
    while pos < len {
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Brace {
                return Some(pos + 1);
            }
        }
        pos += 1;
    }

    None
}

/// Try to match a fn pattern starting at position `start`:
///   [pub [( ... )]]? [extern "Rust"]? [async]? fn NAME ( ... ) [-> ...]? { ... }
///
/// Rejects const fn, unsafe fn, and extern fn with non-Rust ABI (matching is_instrumentable).
/// NAME can be an Ident (literal name) or $metavar (Punct('$') + Ident).
fn match_fn_pattern(tokens: &[proc_macro2::TokenTree], start: usize) -> Option<FnMatch> {
    let len = tokens.len();
    let mut pos = start;

    // Check for non-instrumentable prefixes: const, unsafe, extern (non-Rust ABI).
    // If we see any of these before `fn`, skip this position.
    if is_ident(tokens, pos, "const")
        || is_ident(tokens, pos, "unsafe")
        || is_non_rust_extern(tokens, pos)
    {
        return None;
    }

    // Optional: `pub` with optional visibility group like `pub(crate)`.
    if is_ident(tokens, pos, "pub") {
        pos += 1;
        if pos >= len {
            return None;
        }
        // Skip optional visibility restriction group: `(crate)`, `(super)`, etc.
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Parenthesis {
                pos += 1;
                if pos >= len {
                    return None;
                }
            }
        }

        // After pub, check for non-instrumentable prefixes.
        if is_ident(tokens, pos, "const")
            || is_ident(tokens, pos, "unsafe")
            || is_non_rust_extern(tokens, pos)
        {
            return None;
        }
    }

    // Optional: `extern "Rust"` (Rust ABI is instrumentable).
    if is_ident(tokens, pos, "extern") {
        pos += 1; // skip `extern`
        if pos < len {
            if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                pos += 1; // skip `"Rust"`
            }
        }
        if pos >= len {
            return None;
        }
    }

    // Optional: `async`.
    if is_ident(tokens, pos, "async") {
        pos += 1;
        if pos >= len {
            return None;
        }
    }

    // Required: `fn`.
    if !is_ident(tokens, pos, "fn") {
        return None;
    }
    pos += 1;
    if pos >= len {
        return None;
    }

    // Required: NAME — either a plain Ident or $metavar (Punct('$') + Ident).
    let name_tokens: Vec<proc_macro2::TokenTree>;
    if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
        if p.as_char() == '$' {
            // Metavar: $name
            if pos + 1 >= len {
                return None;
            }
            if let proc_macro2::TokenTree::Ident(_) = &tokens[pos + 1] {
                name_tokens = vec![tokens[pos].clone(), tokens[pos + 1].clone()];
                pos += 2;
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else if let proc_macro2::TokenTree::Ident(_) = &tokens[pos] {
        name_tokens = vec![tokens[pos].clone()];
        pos += 1;
    } else {
        return None;
    }

    if pos >= len {
        return None;
    }

    // Optional: generic parameters `<...>`. In macro token streams, angle
    // brackets are individual Punct tokens, so we track nesting depth to skip
    // past the entire generic parameter list.
    if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
        if p.as_char() == '<' {
            let mut depth = 1u32;
            pos += 1;
            while pos < len && depth > 0 {
                if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
                    match p.as_char() {
                        '<' => depth += 1,
                        '>' => {
                            // Don't decrement for `->` (return type arrow).
                            // In proc_macro2, `->` is two separate Punct tokens:
                            // `-` then `>`. Check the previous token.
                            let is_arrow = pos > 0
                                && matches!(
                                    &tokens[pos - 1],
                                    proc_macro2::TokenTree::Punct(prev)
                                        if prev.as_char() == '-' && prev.spacing() == proc_macro2::Spacing::Joint
                                );
                            if !is_arrow {
                                depth -= 1;
                            }
                        }
                        _ => {}
                    }
                }
                pos += 1;
            }
            if depth > 0 || pos >= len {
                return None;
            }
        }
    }

    // Required: parameter list (parenthesized group).
    if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
        if g.delimiter() != proc_macro2::Delimiter::Parenthesis {
            return None;
        }
    } else {
        return None;
    }
    pos += 1;

    // Optional: return type `-> ...` — skip tokens until we find a brace group.
    while pos < len {
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Brace {
                break;
            }
        }
        pos += 1;
    }

    if pos >= len {
        return None;
    }

    // Required: body (brace group).
    if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
        if g.delimiter() == proc_macro2::Delimiter::Brace {
            return Some(FnMatch {
                name_tokens,
                body_index: pos,
            });
        }
    }

    None
}

/// Check if tokens[i] is an Ident matching the given name.
fn is_ident(tokens: &[proc_macro2::TokenTree], i: usize, name: &str) -> bool {
    if i >= tokens.len() {
        return false;
    }
    matches!(&tokens[i], proc_macro2::TokenTree::Ident(ident) if *ident == name)
}

/// Check if `extern` at position `i` has a non-Rust ABI (i.e. should be skipped).
/// Returns true for `extern fn`, `extern "C" fn`, etc.
/// Returns false for `extern "Rust" fn` (instrumentable, matching AST-level behavior).
fn is_non_rust_extern(tokens: &[proc_macro2::TokenTree], i: usize) -> bool {
    if !is_ident(tokens, i, "extern") {
        return false;
    }
    // Peek at next token: if it's a "Rust" string literal, this is instrumentable.
    if i + 1 < tokens.len() {
        if let proc_macro2::TokenTree::Literal(lit) = &tokens[i + 1] {
            let s = lit.to_string();
            if s == "\"Rust\"" {
                return false;
            }
        }
    }
    true
}

/// Build the guard statement tokens for a function name.
///
/// For a literal name like `initialize`:
///   `let __piano_guard = piano_runtime::enter("initialize");`
///
/// For a metavar name like `$name`:
///   `let __piano_guard = piano_runtime::enter(stringify!($name));`
fn make_guard_tokens(name_tokens: &[proc_macro2::TokenTree]) -> proc_macro2::TokenStream {
    let is_metavar = name_tokens.len() == 2
        && matches!(&name_tokens[0], proc_macro2::TokenTree::Punct(p) if p.as_char() == '$');

    if is_metavar {
        // For metavar names, we need to build:
        //   let __piano_guard = piano_runtime::enter(stringify!($name));
        // We construct the tokens manually because `quote!` doesn't emit `$`.
        let dollar = proc_macro2::Punct::new('$', proc_macro2::Spacing::Alone);
        let metavar_ident = name_tokens[1].clone();

        // Build: stringify!($name)
        let stringify_inner: proc_macro2::TokenStream =
            vec![proc_macro2::TokenTree::Punct(dollar), metavar_ident]
                .into_iter()
                .collect();
        // Build the full statement using manual token construction
        // (quote! cannot emit raw $ tokens for macro metavariables).
        let stringify_call: proc_macro2::TokenStream = {
            let span = proc_macro2::Span::call_site();
            vec![
                proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("stringify", span)),
                proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    '!',
                    proc_macro2::Spacing::Alone,
                )),
                proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                    proc_macro2::Delimiter::Parenthesis,
                    stringify_inner,
                )),
            ]
            .into_iter()
            .collect()
        };

        // Build: let __piano_guard = piano_runtime::enter(stringify!($name));
        let enter_arg =
            proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, stringify_call);

        let span = proc_macro2::Span::call_site();
        vec![
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("let", span)),
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("__piano_guard", span)),
            proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                '=',
                proc_macro2::Spacing::Alone,
            )),
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("piano_runtime", span)),
            proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                ':',
                proc_macro2::Spacing::Joint,
            )),
            proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                ':',
                proc_macro2::Spacing::Alone,
            )),
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("enter", span)),
            proc_macro2::TokenTree::Group(enter_arg),
            proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                ';',
                proc_macro2::Spacing::Alone,
            )),
        ]
        .into_iter()
        .collect()
    } else {
        // Literal name — use quote! for cleaner generation.
        let name_str = match &name_tokens[0] {
            proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
            _ => unreachable!(
                "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
            ),
        };
        quote! {
            let __piano_guard = piano_runtime::enter(#name_str);
        }
    }
}

impl<'s> Visit<'s> for InjectionCollector<'s> {
    fn visit_item_fn(&mut self, node: &'s syn::ItemFn) {
        if is_instrumentable(&node.sig) {
            let name = node.sig.ident.to_string();
            self.collect_guard(&node.block, &name, &node.sig);
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_impl(&mut self, node: &'s syn::ItemImpl) {
        let type_name = type_ident(&node.self_ty);
        let prev = self.current_impl.take();
        self.current_impl = Some(type_name);
        syn::visit::visit_item_impl(self, node);
        self.current_impl = prev;
    }

    fn visit_impl_item_fn(&mut self, node: &'s syn::ImplItemFn) {
        if is_instrumentable(&node.sig) {
            let method = node.sig.ident.to_string();
            let qualified = match &self.current_impl {
                Some(ty) => format!("{ty}::{method}"),
                None => method,
            };
            self.collect_guard(&node.block, &qualified, &node.sig);
        }
        syn::visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_trait(&mut self, node: &'s syn::ItemTrait) {
        let trait_name = node.ident.to_string();
        let prev = self.current_trait.take();
        self.current_trait = Some(trait_name);
        syn::visit::visit_item_trait(self, node);
        self.current_trait = prev;
    }

    fn visit_trait_item_fn(&mut self, node: &'s syn::TraitItemFn) {
        if let Some(block) = &node.default {
            if is_instrumentable(&node.sig) {
                let method = node.sig.ident.to_string();
                let qualified = match &self.current_trait {
                    Some(trait_name) => format!("{trait_name}::{method}"),
                    None => method,
                };
                self.collect_guard(block, &qualified, &node.sig);
            }
        }
        syn::visit::visit_trait_item_fn(self, node);
    }
}

/// Separate VisitMut that only handles macro_rules! instrumentation.
/// Kept as VisitMut because macro instrumentation modifies token streams directly.
struct MacroInstrumenter;

impl VisitMut for MacroInstrumenter {
    fn visit_item_macro_mut(&mut self, node: &mut syn::ItemMacro) {
        if is_macro_rules(node) {
            instrument_macro_tokens(&mut node.mac.tokens);
        }
        syn::visit_mut::visit_item_macro_mut(self, node);
    }
}

/// Inject `piano_runtime::register(name)` calls at the top of `fn main`.
///
/// This ensures every instrumented function appears in the output, even if it
/// was never called during the run.
pub fn inject_registrations(
    source: &str,
    names: &[String],
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut injector = StringInjector::new();
    for item in &file.items {
        if let syn::Item::Fn(func) = item {
            if func.sig.ident == "main" {
                let open = func.block.brace_token.span.open().start();
                let byte_offset = line_col_to_byte(source, open) + 1;
                let mut text = String::new();
                for name in names {
                    text.push_str(&format!("\n    piano_runtime::register(\"{name}\");"));
                }
                injector.insert(byte_offset, text);
                break;
            }
        }
    }
    Ok(injector.apply(source))
}

/// Classification of the user's `#[global_allocator]` declaration.
pub enum AllocatorKind {
    /// No `#[global_allocator]` found in the source.
    Absent,
    /// `#[global_allocator]` without any `#[cfg(...)]` gate.
    Unconditional,
    /// One or more `#[global_allocator]` statics, each behind a `#[cfg(...)]`.
    /// The `Vec` contains the cfg predicate `Meta` from each `#[cfg(pred)]`.
    CfgGated(Vec<syn::Meta>),
}

impl std::fmt::Debug for AllocatorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocatorKind::Absent => write!(f, "Absent"),
            AllocatorKind::Unconditional => write!(f, "Unconditional"),
            AllocatorKind::CfgGated(preds) => {
                write!(f, "CfgGated({} predicates)", preds.len())
            }
        }
    }
}

/// Check whether a static item has a `#[global_allocator]` attribute,
/// either directly or inside a `#[cfg_attr(condition, global_allocator)]`.
fn has_global_allocator_attr(static_item: &syn::ItemStatic) -> bool {
    static_item.attrs.iter().any(|a| {
        if a.path().is_ident("global_allocator") {
            return true;
        }
        if a.path().is_ident("cfg_attr") {
            return cfg_attr_contains_global_allocator(a);
        }
        false
    })
}

/// Check whether a `#[cfg_attr(...)]` attribute contains `global_allocator`
/// among its conditional attributes.
///
/// `cfg_attr` has the form `cfg_attr(condition, attr1, attr2, ...)`.
/// We parse the token stream and check if any attr after the condition
/// is the path `global_allocator`.
fn cfg_attr_contains_global_allocator(attr: &syn::Attribute) -> bool {
    let Ok(nested) = attr.parse_args_with(
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
    ) else {
        return false;
    };
    // First element is the condition; remaining elements are the conditional attributes.
    nested
        .iter()
        .skip(1)
        .any(|meta| meta.path().is_ident("global_allocator"))
}

/// Extract the condition `Meta` from a `#[cfg_attr(condition, ...)]` that
/// contains `global_allocator`. Returns `None` if the attribute is not a
/// matching `cfg_attr`.
fn cfg_attr_global_allocator_condition(attr: &syn::Attribute) -> Option<syn::Meta> {
    if !attr.path().is_ident("cfg_attr") {
        return None;
    }
    let nested = attr
        .parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
        .ok()?;
    let has_global_alloc = nested
        .iter()
        .skip(1)
        .any(|meta| meta.path().is_ident("global_allocator"));
    if has_global_alloc {
        nested.into_iter().next()
    } else {
        None
    }
}

/// Extract byte range for a span in the source string.
fn span_byte_range(source: &str, span: proc_macro2::Span) -> (usize, usize) {
    let start = line_col_to_byte(source, span.start());
    let end = line_col_to_byte(source, span.end());
    (start, end)
}

/// Wrap a `#[global_allocator]` static's type and initializer with `PianoAllocator`
/// using string replacement. Returns a list of (start, end, replacement) edits.
fn wrap_allocator_edits(
    source: &str,
    static_item: &syn::ItemStatic,
) -> Vec<(usize, usize, String)> {
    let mut edits = Vec::new();

    // Wrap the type: T -> piano_runtime::PianoAllocator<T>
    let (ty_start, ty_end) = span_byte_range(source, static_item.ty.span());
    let orig_ty = &source[ty_start..ty_end];
    edits.push((
        ty_start,
        ty_end,
        format!("piano_runtime::PianoAllocator<{orig_ty}>"),
    ));

    // Wrap the expr: E -> piano_runtime::PianoAllocator::new(E)
    let (expr_start, expr_end) = span_byte_range(source, static_item.expr.span());
    let orig_expr = &source[expr_start..expr_end];
    edits.push((
        expr_start,
        expr_end,
        format!("piano_runtime::PianoAllocator::new({orig_expr})"),
    ));

    edits
}

/// Walk the parsed file and classify any `#[global_allocator]` statics.
pub fn detect_allocator_kind(source: &str) -> Result<AllocatorKind, syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut cfg_predicates: Vec<syn::Meta> = Vec::new();

    for item in &file.items {
        if let syn::Item::Static(static_item) = item {
            if !has_global_allocator_attr(static_item) {
                continue;
            }

            let mut item_cfgs: Vec<syn::Meta> = Vec::new();
            for a in &static_item.attrs {
                if a.path().is_ident("cfg") {
                    item_cfgs.push(a.parse_args::<syn::Meta>()?);
                } else if let Some(condition) = cfg_attr_global_allocator_condition(a) {
                    item_cfgs.push(condition);
                }
            }

            if item_cfgs.is_empty() {
                return Ok(AllocatorKind::Unconditional);
            }

            // Multiple #[cfg] on the same item is semantically #[cfg(all(...))].
            let combined: syn::Meta = if item_cfgs.len() == 1 {
                item_cfgs.remove(0)
            } else {
                syn::parse_quote! { all(#(#item_cfgs),*) }
            };
            cfg_predicates.push(combined);
        }
    }

    if cfg_predicates.is_empty() {
        Ok(AllocatorKind::Absent)
    } else {
        Ok(AllocatorKind::CfgGated(cfg_predicates))
    }
}

/// Inject a `#[global_allocator]` static using `PianoAllocator`.
///
/// Behavior depends on `AllocatorKind`:
/// - `Absent`: inject `PianoAllocator<System>` unconditionally.
/// - `Unconditional`: wrap the existing allocator with `PianoAllocator`.
/// - `CfgGated`: wrap each cfg-gated allocator AND inject a cfg-negated
///   `PianoAllocator<System>` fallback for platforms where none match.
pub fn inject_global_allocator(
    source: &str,
    kind: AllocatorKind,
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;

    match kind {
        AllocatorKind::Absent => {
            let text = "\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n";
            let mut injector = StringInjector::new();
            injector.insert(0, text);
            Ok(injector.apply(source))
        }
        AllocatorKind::Unconditional => {
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in &file.items {
                if let syn::Item::Static(static_item) = item {
                    if has_global_allocator_attr(static_item) {
                        edits = wrap_allocator_edits(source, static_item);
                        break;
                    }
                }
            }
            Ok((apply_replacements(source, &edits), SourceMap::default()))
        }
        AllocatorKind::CfgGated(ref predicates) => {
            // Collect type/expr edits for each cfg-gated allocator.
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in &file.items {
                if let syn::Item::Static(static_item) = item {
                    if has_global_allocator_attr(static_item) {
                        edits.extend(wrap_allocator_edits(source, static_item));
                    }
                }
            }

            // Build the cfg-negated fallback text.
            // Format predicates compactly (quote! adds unwanted spaces).
            let pred_strs: Vec<String> =
                predicates.iter().map(|p| quote!(#p).to_string()).collect();
            let negated_str = if pred_strs.len() == 1 {
                format!("not({})", pred_strs[0])
            } else {
                format!("not(any({}))", pred_strs.join(", "))
            };
            let fallback = format!(
                "\n#[cfg({negated_str})]\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n"
            );

            // Apply type/expr replacements first, then prepend fallback.
            let replaced = apply_replacements(source, &edits);
            // Track the prepended fallback lines in the SourceMap so error
            // remapping knows original lines shifted down.
            let fallback_newlines = fallback.bytes().filter(|&b| b == b'\n').count() as u32;
            let mut map = SourceMap::new();
            if fallback_newlines > 0 {
                map.record(1, fallback_newlines);
            }
            let result = format!("{fallback}{replaced}");
            Ok((result, map))
        }
    }
}

/// Apply a set of non-overlapping (start, end, replacement) edits to source.
/// Edits must not overlap. They are applied in offset order.
fn apply_replacements(source: &str, edits: &[(usize, usize, String)]) -> String {
    let mut sorted: Vec<&(usize, usize, String)> = edits.iter().collect();
    sorted.sort_by_key(|(start, _, _)| *start);

    let mut result = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for (start, end, replacement) in &sorted {
        debug_assert!(
            cursor <= *start,
            "overlapping edits: cursor {cursor} > start {start}"
        );
        result.push_str(&source[cursor..*start]);
        result.push_str(replacement);
        cursor = *end;
    }
    result.push_str(&source[cursor..]);
    result
}

/// Wrap `fn main`'s body in `catch_unwind` and inject `piano_runtime::shutdown()`.
///
/// When `runs_dir` is `Some`, emits `shutdown_to(dir)` to write run data to
/// the given project-local directory. When `None`, falls back to `shutdown()`
/// which uses the runtime's default directory resolution.
pub fn inject_shutdown(
    source: &str,
    runs_dir: Option<&str>,
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut injector = StringInjector::new();

    for item in &file.items {
        if let syn::Item::Fn(func) = item {
            if func.sig.ident != "main" {
                continue;
            }

            let is_async = func.sig.asyncness.is_some();
            let has_return_type = !matches!(&func.sig.output, syn::ReturnType::Default);

            let open = func.block.brace_token.span.open().start();
            let close = func.block.brace_token.span.close().start();
            let open_byte = line_col_to_byte(source, open) + 1;
            let close_byte = line_col_to_byte(source, close);

            // Build shutdown call text.
            let shutdown_text = match runs_dir {
                Some(dir) => {
                    format!("piano_runtime::shutdown_to(std::path::Path::new(\"{dir}\"));")
                }
                None => "piano_runtime::shutdown();".to_string(),
            };

            // Build optional set_runs_dir text.
            let set_dir_text = runs_dir.map(|dir| {
                format!("\n    piano_runtime::set_runs_dir(std::path::Path::new(\"{dir}\"));")
            });

            if is_async {
                // Async main: no catch_unwind.
                let mut prefix = String::from("\n    piano_runtime::init();");
                if let Some(ref sdr) = set_dir_text {
                    prefix.push_str(sdr);
                }

                if has_return_type && !func.block.stmts.is_empty() {
                    // Check if last statement is a tail expression (no semicolon).
                    if let Some(syn::Stmt::Expr(tail_expr, None)) = func.block.stmts.last() {
                        // Bind the tail expression, shutdown, return.
                        let tail_start = line_col_to_byte(source, tail_expr.span().start());
                        injector.insert(open_byte, prefix);
                        injector.insert(tail_start, "let __piano_result = ".to_string());
                        let suffix = format!(";\n    {shutdown_text}\n    __piano_result\n");
                        injector.insert(close_byte, suffix);
                    } else {
                        // Last statement has semicolon; just insert shutdown before }.
                        injector.insert(open_byte, prefix);
                        let suffix = format!("\n    {shutdown_text}\n");
                        injector.insert(close_byte, suffix);
                    }
                } else {
                    // No return type: insert shutdown before }.
                    // If the last stmt is a tail expression (no semicolon),
                    // insert a semicolon after it so shutdown can follow.
                    injector.insert(open_byte, prefix);
                    if let Some(syn::Stmt::Expr(tail_expr, None)) = func.block.stmts.last() {
                        let tail_end = line_col_to_byte(source, tail_expr.span().end());
                        injector.insert(tail_end, ";".to_string());
                    }
                    let suffix = format!("\n    {shutdown_text}\n");
                    injector.insert(close_byte, suffix);
                }
            } else {
                // Sync main: wrap body in catch_unwind.
                let mut prefix = String::from("\n    piano_runtime::init();");
                if let Some(ref sdr) = set_dir_text {
                    prefix.push_str(sdr);
                }
                prefix.push_str(
                    "\n    let __piano_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {",
                );
                injector.insert(open_byte, prefix);

                let mut suffix = format!("\n    }}));\n    {shutdown_text}");
                if has_return_type {
                    suffix.push_str(
                        "\n    match __piano_result {\n        Ok(__piano_val) => __piano_val,\n        Err(__piano_panic) => std::panic::resume_unwind(__piano_panic),\n    }\n",
                    );
                } else {
                    suffix.push_str(
                        "\n    if let Err(__piano_panic) = __piano_result {\n        std::panic::resume_unwind(__piano_panic);\n    }\n",
                    );
                }
                injector.insert(close_byte, suffix);
            }

            break;
        }
    }

    Ok(injector.apply(source))
}

/// Extract the type name from a `syn::Type` for qualified method names.
fn type_ident(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|seg| seg.ident.to_string())
            .unwrap_or_else(|| quote!(#ty).to_string()),
        _ => quote!(#ty).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let targets: HashSet<String> = ["walk".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter(\"walk\")"),
            "walk should be instrumented"
        );
        assert!(
            !result.contains("piano_runtime::enter(\"other\")"),
            "other should not be instrumented",
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
        let targets: HashSet<String> = ["Walker::walk".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter(\"Walker::walk\")"),
            "Walker::walk should be instrumented. Got:\n{result}",
        );
    }

    #[test]
    fn preserves_function_signature_and_body() {
        let source = r#"
fn compute(x: i32, y: i32) -> i32 {
    x + y
}
"#;
        let targets: HashSet<String> = ["compute".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("fn compute(x: i32, y: i32) -> i32"),
            "signature preserved"
        );
        assert!(result.contains("x + y"), "body preserved");
        assert!(
            result.contains("piano_runtime::enter(\"compute\")"),
            "guard injected"
        );
    }

    #[test]
    fn multiple_functions_instrumented() {
        let source = r#"
fn a() {}
fn b() {}
fn c() {}
"#;
        let targets: HashSet<String> = ["a".to_string(), "c".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter(\"a\")"),
            "a should be instrumented"
        );
        assert!(
            !result.contains("piano_runtime::enter(\"b\")"),
            "b should NOT be instrumented",
        );
        assert!(
            result.contains("piano_runtime::enter(\"c\")"),
            "c should be instrumented"
        );
    }

    #[test]
    fn injects_register_calls_in_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let names = vec!["walk".to_string(), "parse".to_string()];
        let (result, _map) = inject_registrations(source, &names).unwrap();
        assert!(
            result.contains("piano_runtime::register(\"walk\")"),
            "Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::register(\"parse\")"),
            "Got:\n{result}"
        );
    }

    #[test]
    fn detect_no_allocator() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        assert!(matches!(kind, AllocatorKind::Absent));
    }

    #[test]
    fn injects_global_allocator() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Absent).unwrap();
        assert!(
            result.contains("#[global_allocator]"),
            "should inject global_allocator attribute. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator"),
            "should use PianoAllocator. Got:\n{result}"
        );
        assert!(
            result.contains("std::alloc::System"),
            "should wrap System allocator. Got:\n{result}"
        );
    }

    #[test]
    fn wraps_existing_global_allocator() {
        let source = r#"
use std::alloc::System;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Unconditional).unwrap();
        assert!(
            result.contains("PianoAllocator"),
            "should wrap existing allocator. Got:\n{result}"
        );
    }

    #[test]
    fn cfg_gated_allocator_gets_fallback() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator<Jemalloc>"),
            "should wrap cfg-gated allocator. Got:\n{result}"
        );
        assert!(
            result.contains("not(target_os = \"linux\")"),
            "should inject negated cfg fallback. Got:\n{result}"
        );
        assert!(
            result.contains("_PIANO_ALLOC"),
            "fallback should use _PIANO_ALLOC name. Got:\n{result}"
        );
        assert!(
            result.contains("std::alloc::System"),
            "fallback should wrap System allocator. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_cfg_gated_allocators_get_combined_fallback() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC_LINUX: Jemalloc = Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static ALLOC_MAC: MiMalloc = MiMalloc;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator<Jemalloc>"),
            "should wrap linux allocator. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator<MiMalloc>"),
            "should wrap macos allocator. Got:\n{result}"
        );
        assert!(
            result.contains("not(any("),
            "fallback should use not(any(...)) for multiple cfgs. Got:\n{result}"
        );
        assert!(
            result.contains("_PIANO_ALLOC"),
            "fallback should use _PIANO_ALLOC. Got:\n{result}"
        );
    }

    #[test]
    fn uncfg_allocator_no_fallback() {
        let source = r#"
use std::alloc::System;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator"),
            "should wrap allocator. Got:\n{result}"
        );
        assert!(
            !result.contains("_PIANO_ALLOC"),
            "should NOT inject fallback for unconditional allocator. Got:\n{result}"
        );
    }

    #[test]
    fn injects_init_at_start_of_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();

        assert!(
            result.contains("piano_runtime::init()"),
            "should inject init(). Got:\n{result}"
        );

        // init() should appear before set_runs_dir and catch_unwind
        let init_pos = result.find("piano_runtime::init()").unwrap();
        let set_dir_pos = result.find("piano_runtime::set_runs_dir").unwrap();
        assert!(
            init_pos < set_dir_pos,
            "init() should come before set_runs_dir(). Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_with_catch_unwind() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            result.contains("catch_unwind"),
            "should wrap body in catch_unwind. Got:\n{result}"
        );
        assert!(
            result.contains("resume_unwind"),
            "should re-panic on caught panic. Got:\n{result}"
        );
        let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
        let do_stuff_pos = result.find("do_stuff()").unwrap();
        assert!(
            shutdown_pos > do_stuff_pos,
            "shutdown should come after existing code"
        );
    }

    #[test]
    fn injects_shutdown_to_with_dir() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) =
            inject_shutdown(source, Some("/tmp/my-project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown_to")
                && result.contains("std::path::Path::new(\"/tmp/my-project/target/piano/runs\")"),
            "should inject shutdown_to with Path::new. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_at_start_of_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::set_runs_dir"),
            "should inject set_runs_dir when runs_dir is provided. Got:\n{result}"
        );
        // set_runs_dir should appear BEFORE catch_unwind (i.e. before the body)
        let set_pos = result.find("set_runs_dir").unwrap();
        let catch_pos = result.find("catch_unwind").unwrap();
        assert!(
            set_pos < catch_pos,
            "set_runs_dir should come before catch_unwind. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_preserves_main_return_type() {
        let source = r#"
use std::process::ExitCode;
fn main() -> ExitCode {
    do_stuff();
    ExitCode::SUCCESS
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            result.contains("catch_unwind"),
            "should wrap body in catch_unwind for return-type main. Got:\n{result}"
        );
        // Must preserve ExitCode as the tail expression (not discard it)
        // The rewritten code should compile — ExitCode must be returned after shutdown.
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        // Find main function and verify it still has a return type
        let main_fn = parsed
            .items
            .iter()
            .find_map(|item| {
                if let syn::Item::Fn(f) = item {
                    if f.sig.ident == "main" {
                        return Some(f);
                    }
                }
                None
            })
            .expect("should have main fn");
        // The last statement in main should be an expression (the return value),
        // not a semicolon-terminated statement
        let last = main_fn
            .block
            .stmts
            .last()
            .expect("main should have statements");
        assert!(
            matches!(last, syn::Stmt::Expr(_, None)),
            "last statement should be a tail expression (no semicolon) for the return value. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_async_main_no_catch_unwind() {
        let source = r#"
async fn main() {
    do_stuff().await;
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "async main should NOT use catch_unwind. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_async_main_with_return_type() {
        let source = r#"
async fn main() -> ExitCode {
    do_stuff().await;
    ExitCode::SUCCESS
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "async main should NOT use catch_unwind. Got:\n{result}"
        );
        // Tail expression is bound to __piano_result, shutdown runs,
        // then __piano_result is returned as the tail expression.
        assert!(
            result.contains("__piano_result"),
            "tail expression should be bound to __piano_result. Got:\n{result}"
        );
        let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
        let return_pos = result.rfind("__piano_result").unwrap();
        assert!(
            shutdown_pos < return_pos,
            "shutdown should come before the return. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_in_async_main() {
        let source = r#"
async fn main() {
    do_stuff().await;
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::set_runs_dir"),
            "should inject set_runs_dir for async main. Got:\n{result}"
        );
        // set_runs_dir should come before the user's code
        let set_pos = result.find("set_runs_dir").unwrap();
        let stuff_pos = result.find("do_stuff").unwrap();
        assert!(
            set_pos < stuff_pos,
            "set_runs_dir should come before user code. Got:\n{result}"
        );
    }

    #[test]
    fn injects_fork_and_adopt_for_par_iter() {
        let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
        let targets: HashSet<String> = ["process_all".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter(\"process_all\")"),
            "should have guard. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt in closure. Got:\n{result}"
        );
    }

    #[test]
    fn skips_fork_for_thread_spawn() {
        let source = r#"
fn do_work() {
    std::thread::spawn(|| {
        heavy_computation();
    });
}
"#;
        let targets: HashSet<String> = ["do_work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

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
            result.source.contains("piano_runtime::enter(\"do_work\")"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        // Detached spawns should not report concurrency (no fork/adopt to act on)
        assert!(
            result.concurrency.is_empty(),
            "should not report concurrency for detached spawn. Got: {:?}",
            result.concurrency
        );
    }

    #[test]
    fn mixed_scope_and_thread_spawn() {
        let source = r#"
fn mixed() {
    rayon::scope(|s| {
        s.spawn(|_| { work_a(); });
    });
    std::thread::spawn(|| {
        work_b();
    });
}
"#;
        let targets: HashSet<String> = ["mixed".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

        // Fork should be injected (rayon::scope triggers it)
        assert!(
            result.source.contains("piano_runtime::fork()"),
            "should inject fork for rayon::scope. Got:\n{}",
            result.source
        );
        // Adopt should appear (for s.spawn inside scope)
        assert!(
            result.source.contains("piano_runtime::adopt"),
            "should inject adopt for scoped s.spawn. Got:\n{}",
            result.source
        );

        // Count adopt occurrences -- should be exactly 1 (in s.spawn, NOT in thread::spawn)
        let adopt_count = result.source.matches("piano_runtime::adopt").count();
        assert_eq!(
            adopt_count, 1,
            "should have exactly 1 adopt (in s.spawn), not in thread::spawn. Got {adopt_count} in:\n{}",
            result.source
        );
    }

    #[test]
    fn skips_fork_for_short_path_thread_spawn() {
        // `use std::thread::spawn; spawn(|| ...)` -- bare name, no path prefix
        let source = r#"
fn do_work() {
    spawn(|| {
        heavy_computation();
    });
}
"#;
        let targets: HashSet<String> = ["do_work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for bare spawn(). Got:\n{}",
            result.source
        );
    }

    #[test]
    fn no_concurrency_for_thread_spawn() {
        let source = r#"
fn do_work() {
    std::thread::spawn(|| { work(); });
}
"#;
        let targets: HashSet<String> = ["do_work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

        assert!(
            result.concurrency.is_empty(),
            "detached spawn should not report concurrency. Got: {:?}",
            result.concurrency
        );
    }

    #[test]
    fn skips_fork_for_rayon_spawn_free_function() {
        // rayon::spawn(|| ...) is a free function call (Call arm, not MethodCall).
        // It is detached ('static bound), so no fork/adopt should be injected.
        let source = r#"
fn do_work() {
    rayon::spawn(|| {
        heavy_computation();
    });
}
"#;
        let targets: HashSet<String> = ["do_work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

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
            result.source.contains("piano_runtime::enter(\"do_work\")"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        assert!(
            result.concurrency.is_empty(),
            "rayon::spawn free function should not report concurrency. Got: {:?}",
            result.concurrency
        );
    }

    #[test]
    fn nested_scope_inside_detached_spawn() {
        // A scoped concurrency primitive (rayon::scope) nested inside a
        // detached spawn (std::thread::spawn). The outer detached spawn
        // suppresses fork/adopt for the entire function body -- the inner
        // rayon::scope should not trigger fork/adopt injection.
        let source = r#"
fn work() {
    std::thread::spawn(|| {
        rayon::scope(|s| {
            s.spawn(|_| { inner(); });
        });
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

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
            result.source.contains("piano_runtime::enter(\"work\")"),
            "should still inject enter guard. Got:\n{}",
            result.source
        );
        assert!(
            result.concurrency.is_empty(),
            "nested scope inside detached spawn should not report concurrency. Got: {:?}",
            result.concurrency
        );
    }

    #[test]
    fn injects_adopt_in_rayon_scope_spawn() {
        let source = r#"
fn parallel_work() {
    rayon::scope(|s| {
        s.spawn(|_| { work_a(); });
        s.spawn(|_| { work_b(); });
    });
}
"#;
        let targets: HashSet<String> = ["parallel_work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt. Got:\n{result}"
        );
    }

    #[test]
    fn rayon_scope_spawn_in_loop_with_move_closures() {
        // Simulates real rayon pattern: s.spawn(move |_| { ... }) in a for loop.
        // `__piano_ctx` is `Option<&SpanContext>` which is Copy, so move closures work.
        let source = r#"
fn concurrent_discover() {
    rayon::scope(|s| {
        for i in 0..4 {
            s.spawn(move |_| {
                do_work(i);
            });
        }
    });
}
"#;
        let targets: HashSet<String> = ["concurrent_discover".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt in spawn closures. Got:\n{result}"
        );
        // The scope closure itself should NOT have an adopt (it's the coordinator).
        // The adopt should only be inside the s.spawn closures.
        // Verify by checking the generated code compiles structurally.
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn injects_adopt_in_let_binding_inside_for_loop() {
        // Concurrency closure assigned to a let binding inside a for loop.
        // Regression: inject_adopt_in_concurrency_closures only handled
        // Stmt::Expr, missing Stmt::Local (let bindings).
        let source = r#"
fn work() {
    rayon::scope(|s| {
        for item in items {
            let handle = s.spawn(|_| { compute(item); });
        }
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt inside spawn closure bound to let. Got:\n{result}"
        );
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn injects_adopt_in_match_arms_inside_scope() {
        // Concurrency closures inside match arms should get adopt injection.
        // Bug #331: match expressions were skipped by the catch-all.
        let source = r#"
fn work(kind: Kind) {
    rayon::scope(|s| {
        match kind {
            Kind::A => { s.spawn(|_| { work_a(); }); }
            Kind::B => { s.spawn(|_| { work_b(); }); }
        }
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt inside spawn closures in match arms. Got:\n{result}"
        );
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn injects_adopt_in_unsafe_block_inside_scope() {
        // Concurrency closures inside unsafe blocks should get adopt injection.
        // Bug #331: unsafe expressions were skipped by the catch-all.
        let source = r#"
fn work() {
    rayon::scope(|s| {
        unsafe {
            s.spawn(|_| { do_unsafe_work(); });
        }
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt inside spawn closure in unsafe block. Got:\n{result}"
        );
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn propagates_parallel_chain_through_control_flow() {
        // in_parallel_chain must propagate through if/else, for, while,
        // loop, block, match, and unsafe so that closures nested inside
        // control flow within a par_iter chain still get adopt injection.
        // Bug #344: inject_adopt_in_stmts hardcoded false, losing context.
        let source = r#"
fn work(items: &[Item]) {
    items.par_iter().for_each(|item| {
        if item.ready {
            item.parts.par_iter().for_each(|p| process(p));
        } else {
            item.fallbacks.par_iter().for_each(|f| fallback(f));
        }
        for sub in &item.subs {
            sub.entries.par_iter().for_each(|e| handle(e));
        }
        match item.kind {
            Kind::A => item.as_.par_iter().for_each(|a| do_a(a)),
            Kind::B => item.bs.par_iter().for_each(|b| do_b(b)),
        }
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap().source;

        // Count adopt injections: outer for_each closure + 5 inner par_iter closures
        // (if-branch, else-branch, for-body, match-arm-A, match-arm-B)
        let adopt_count = result.matches("piano_runtime::adopt").count();
        assert!(
            adopt_count >= 6,
            "expected at least 6 adopt injections (1 outer + 5 inner), got {adopt_count}. Got:\n{result}"
        );
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        assert!(!parsed.items.is_empty());
    }

    #[test]
    fn no_fork_inject_in_non_target_function() {
        let source = r#"
fn not_targeted() {
    items.par_iter().map(|x| x).collect()
}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            !result.contains("piano_runtime::fork()"),
            "should NOT inject fork in non-target function. Got:\n{result}"
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
        let targets: HashSet<String> = ["process_all".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["concurrent_discover".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["simple".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(result.concurrency.is_empty());
    }

    #[test]
    fn instruments_async_fn() {
        let source = r#"
async fn fetch_data(id: u32) -> String {
    format!("data-{id}")
}
"#;
        let targets: HashSet<String> = ["fetch_data".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"fetch_data\")"),
            "async function SHOULD be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
            "async fn should be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("async move"),
            "async fn body should use async move. Got:\n{}",
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
        let targets: HashSet<String> = ["compute".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

        assert!(
            result.source.contains("piano_runtime::enter(\"compute\")"),
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
        let targets: HashSet<String> = ["Client::fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"Client::fetch\")"),
            "async impl method SHOULD be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["Service::handle".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"Service::handle\")"),
            "async trait default method SHOULD be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
            "async trait method should be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn skips_uninstrumentable_trait_default_methods() {
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
        // Only the safe default method is targeted
        let targets: HashSet<String> = [
            "Processor::default_method".to_string(),
            "Processor::unsafe_default".to_string(),
        ]
        .into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"Processor::default_method\")"),
            "safe trait default method should be instrumented. Got:\n{}",
            result.source
        );
        assert!(
            !result
                .source
                .contains("piano_runtime::enter(\"Processor::unsafe_default\")"),
            "unsafe trait default method should NOT be instrumented. Got:\n{}",
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
        let targets: HashSet<String> = ["handler".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(result.source.contains("piano_runtime::PianoFuture::new"));
        assert!(result.source.contains("async move"));
        assert!(result.source.contains(".await"));
        assert!(result.source.contains("piano_runtime::enter"));
        // Should NOT have check(), save(), resume(), or AllocAccumulator
        assert!(!result.source.contains("__piano_guard.check()"));
        assert!(!result.source.contains("__piano_alloc"));
        assert!(!result.source.contains("AllocAccumulator"));
    }

    #[test]
    fn does_not_wrap_sync_fn_in_piano_future() {
        let source = r#"
fn compute(x: u64) -> u64 {
    x * 2
}
"#;
        let targets: HashSet<String> = ["compute".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::enter(\"compute\")"),
            "sync fn should be instrumented"
        );
        assert!(
            !result.source.contains("PianoFuture"),
            "sync fn should NOT be wrapped in PianoFuture. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn non_target_async_fn_not_wrapped() {
        let targets: HashSet<String> = ["other".to_string()].into_iter().collect();
        let source = r#"
async fn not_targeted() {
    fetch().await;
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            !result.source.contains("PianoFuture"),
            "non-target async fn should not be wrapped"
        );
        assert!(!result.source.contains("piano_runtime::enter"));
    }

    #[test]
    fn instruments_fn_in_macro_rules_metavar_name() {
        let source = r#"
macro_rules! make_handler {
    ($name:ident) => {
        fn $name() {
            do_work();
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains("stringify!"),
            "macro fn with metavar name should use stringify!. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::enter"),
            "macro fn should get a guard. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_macro_rules_literal_name() {
        let source = r#"
macro_rules! setup {
    () => {
        fn initialize() {
            startup();
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("initialize")"#),
            "macro fn with literal name should use string literal. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_pub_async_fn_in_macro_rules() {
        let source = r#"
macro_rules! make_async {
    ($name:ident) => {
        pub async fn $name() {
            work().await;
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter"),
            "pub async fn in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn skips_const_unsafe_extern_fn_in_macro_rules() {
        let source = r#"
macro_rules! special_fns {
    () => {
        const fn fixed() -> usize { 42 }
        unsafe fn danger() {}
        extern "C" fn ffi() {}
        pub const fn pub_fixed() -> usize { 42 }
        pub unsafe fn pub_danger() {}
        pub extern "C" fn pub_ffi() {}
        fn normal() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("normal")"#),
            "normal fn should be instrumented. Got:\n{result}"
        );
        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 1,
            "only normal fn should be instrumented, not const/unsafe/extern. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_extern_rust_fn_in_macro_rules() {
        let source = r#"
macro_rules! abi_fns {
    () => {
        extern "Rust" fn rust_abi() { work(); }
        extern "C" fn c_abi() {}
        pub extern "Rust" fn pub_rust_abi() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("rust_abi")"#),
            "extern \"Rust\" fn should be instrumented. Got:\n{result}"
        );
        assert!(
            result.contains(r#"piano_runtime::enter("pub_rust_abi")"#),
            "pub extern \"Rust\" fn should be instrumented. Got:\n{result}"
        );
        assert!(
            !result.contains(r#"piano_runtime::enter("c_abi")"#),
            "extern \"C\" fn should NOT be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn const_variable_does_not_swallow_next_fn() {
        let source = r#"
macro_rules! with_const {
    () => {
        const SIZE: usize = 42;
        fn process() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("process")"#),
            "fn after const variable should still be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_multiple_fns_in_one_macro_rule() {
        let source = r#"
macro_rules! make_pair {
    ($a:ident, $b:ident) => {
        fn $a() { work_a(); }
        fn $b() { work_b(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 2,
            "both fns in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_multiple_macro_rules_arms() {
        let source = r#"
macro_rules! multi {
    (one $name:ident) => {
        fn $name() { one(); }
    };
    (two $name:ident) => {
        fn $name() { two(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 2,
            "fn in each rule arm should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn macro_without_fn_unchanged() {
        let source = r#"
macro_rules! log {
    ($msg:expr) => {
        println!("{}", $msg);
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            !result.contains("piano_runtime::enter"),
            "macro without fn should not be modified. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_paren_delimited_macro_template() {
        let source = r#"
macro_rules! make {
    ($name:ident) => (
        fn $name() { work(); }
    );
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter"),
            "fn in paren-delimited template should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instrument_macros_false_skips_macros() {
        let source = r#"
macro_rules! make {
    ($name:ident) => {
        fn $name() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            !result.contains("piano_runtime::enter"),
            "instrument_macros=false should skip macro instrumentation. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_in_macro_rules() {
        let source = r#"
macro_rules! make_generic {
    ($name:ident) => {
        fn $name<T: std::fmt::Display>(val: T) -> String {
            format!("{}", val)
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains("piano_runtime::enter"),
            "generic fn in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_with_fn_trait_return_in_macro_rules() {
        let source = r#"
macro_rules! make {
    () => {
        fn process<F: Fn() -> bool>(f: F) {
            work();
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("process")"#),
            "generic fn with Fn() -> T bound in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_with_nested_return_type_in_macro_rules() {
        let source = r#"
macro_rules! make {
    () => {
        fn apply<F: Fn(i32) -> Option<bool>>(f: F) {
            work();
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("apply")"#),
            "generic fn with Fn() -> Option<bool> bound in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_impl_block_in_macro_rules() {
        let source = r#"
macro_rules! make_impl {
    ($ty:ident) => {
        impl $ty {
            fn process() {
                work();
            }
        }
    };
}
fn main() {}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, true).unwrap().source;

        assert!(
            result.contains(r#"piano_runtime::enter("process")"#),
            "fn inside impl block in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_cfg_fallback_negates_all_predicates() {
        let src = r#"
            #[cfg(target_os = "linux")]
            #[cfg(target_arch = "x86_64")]
            #[global_allocator]
            static ALLOC: Jemalloc = Jemalloc;
            fn main() {}
        "#;
        let kind = detect_allocator_kind(src).unwrap();
        let (result, _map) = inject_global_allocator(src, kind).unwrap();
        assert!(result.contains("PianoAllocator"), "should wrap allocator");
        assert!(result.contains("_PIANO_ALLOC"), "should inject fallback");
        assert!(
            result.contains("target_os") && result.contains("target_arch"),
            "fallback negation should reference both predicates"
        );
    }

    #[test]
    fn mixed_cfg_gated_and_unconditional_allocator_returns_unconditional() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC_LINUX: Jemalloc = Jemalloc;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        assert!(
            matches!(kind, AllocatorKind::Unconditional),
            "unconditional allocator should take precedence over cfg-gated. Got: {kind:?}"
        );
    }

    #[test]
    fn returns_future_detects_impl_future() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> impl Future<Output = i32>
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_impl_future_with_send() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> impl Future<Output = i32> + Send
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_impl_future_with_lifetime() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo(s: &str) -> impl Future<Output = usize> + '_
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn returns_future_detects_pin_box_dyn_future() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> Pin<Box<dyn Future<Output = i32>>>
        };
        assert_eq!(
            returns_future(&sig),
            Some(FutureReturnKind::PinBoxDynFuture)
        );
    }

    #[test]
    fn returns_future_detects_pin_box_dyn_future_send() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> Pin<Box<dyn Future<Output = i32> + Send>>
        };
        assert_eq!(
            returns_future(&sig),
            Some(FutureReturnKind::PinBoxDynFuture)
        );
    }

    #[test]
    fn returns_future_detects_box_future_alias() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> BoxFuture<'static, i32>
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::KnownAlias));
    }

    #[test]
    fn returns_future_detects_local_box_future_alias() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> LocalBoxFuture<'_, i32>
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::KnownAlias));
    }

    #[test]
    fn returns_future_rejects_plain_type() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> i32
        };
        assert_eq!(returns_future(&sig), None);
    }

    #[test]
    fn returns_future_rejects_result_of_future() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> Result<impl Future<Output = i32>, Error>
        };
        assert_eq!(returns_future(&sig), None);
    }

    #[test]
    fn returns_future_rejects_no_return_type() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo()
        };
        assert_eq!(returns_future(&sig), None);
    }

    #[test]
    fn returns_future_rejects_string_return() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> String
        };
        assert_eq!(returns_future(&sig), None);
    }

    #[test]
    fn returns_future_detects_fully_qualified_future() {
        let sig: syn::Signature = syn::parse_quote! {
            fn foo() -> impl std::future::Future<Output = i32>
        };
        assert_eq!(returns_future(&sig), Some(FutureReturnKind::ImplFuture));
    }

    #[test]
    fn instruments_impl_future_fn() {
        let source = r#"
use std::future::Future;

fn fetch() -> impl Future<Output = String> {
    async { "data".to_string() }
}
"#;
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
            "impl Future fn should be wrapped in PianoFuture. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains("piano_runtime::enter(\"fetch\")"),
            "guard should be inside PianoFuture wrapper. Got:\n{}",
            result.source,
        );
        assert!(
            result.source.contains(".await"),
            "trailing expression should be awaited inside async block. Got:\n{}",
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        let count = result.source.matches("piano_runtime::enter").count();
        assert_eq!(
            count, 1,
            "should have exactly one enter() call (inside PianoFuture). Got {} in:\n{}",
            count, result.source,
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
        let targets: HashSet<String> = ["delegator".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["Client::fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
            "impl method returning impl Future should be wrapped. Got:\n{}",
            result.source,
        );
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"Client::fetch\")"),
            "guard should use qualified name. Got:\n{}",
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
        let targets: HashSet<String> = ["Service::call".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::PianoFuture::new"),
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
        let targets: HashSet<String> = ["compute".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
        assert!(
            result.source.contains("piano_runtime::enter(\"compute\")"),
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();
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
        let targets: HashSet<String> = ["foo".to_string()].into();
        let result = instrument_source(source, &targets, false).unwrap();

        assert!(
            result.source.contains("piano_runtime::enter(\"foo\")"),
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
        let targets: HashSet<String> = ["target".to_string()].into_iter().collect();
        let result = instrument_source(source, &targets, false).unwrap();
        let result_lines: Vec<&str> = result.source.lines().collect();
        // "fn other()" is on original line 6. With 1 guard line injected for target(),
        // it should be at line index 6 (0-indexed).
        let other_pos = result_lines
            .iter()
            .position(|l| l.contains("fn other()"))
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
}
