pub(crate) mod allocator;
pub(crate) mod macro_rules;
pub(crate) mod shutdown;

use std::collections::HashSet;

use quote::quote;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;

use crate::resolve::{Classification, classify};
use crate::source_map::{SourceMap, StringInjector, skip_inner_attrs};

pub use allocator::{AllocatorKind, detect_allocator_kind, inject_global_allocator};
pub use shutdown::inject_shutdown;

use macro_rules::MacroInstrumenter;
use shutdown::type_ident;

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
    module_prefix: &str,
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
        module_prefix: module_prefix.to_string(),
    };
    collector.visit_file(&file);

    let (rewritten, source_map) = collector.injector.apply(source);

    // Macro instrumentation uses token-stream mutation (separate from the
    // string-injection path). Apply it on the rewritten source if needed.
    // prettyplease reformats the entire file, invalidating our source_map,
    // so we reset it to empty when this path is taken.
    let (final_source, final_map, macro_fn_names) = if instrument_macros {
        let mut rewritten_file: syn::File = syn::parse_str(&rewritten)?;
        let mut macro_visitor = MacroInstrumenter {
            module_prefix: module_prefix.to_string(),
            collected_names: Vec::new(),
        };
        macro_visitor.visit_file_mut(&mut rewritten_file);
        (
            prettyplease::unparse(&rewritten_file),
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
pub(super) fn line_col_to_byte(source: &str, lc: proc_macro2::LineColumn) -> usize {
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
    module_prefix: String,
}

impl<'s> InjectionCollector<'s> {
    fn qualify_name(&self, name: &str) -> String {
        crate::resolve::qualify(&self.module_prefix, name)
    }

    fn collect_guard(&mut self, block: &syn::Block, name: &str, sig: &syn::Signature) {
        if !self.targets.contains(name) {
            return;
        }
        let guard_name = self.qualify_name(name);
        let is_async = sig.asyncness.is_some();

        // For non-async functions returning futures, wrap only the trailing expression.
        if !is_async {
            if let Some(kind) = returns_future(sig) {
                self.collect_future_return_guard(block, name, sig, kind);
                return;
            }
        }

        let open_pos = block.brace_token.span.open().start();
        let open_byte = skip_inner_attrs(self.source, line_col_to_byte(self.source, open_pos) + 1);

        // Check for concurrency patterns.
        if let Some(pattern) = find_concurrency_pattern(block) {
            self.concurrency.push((guard_name.clone(), pattern));

            if is_async {
                // Async + concurrency: wrap entire body in PianoFuture with
                // guard + fork inside.
                let close_pos = block.brace_token.span.close().start();
                let close_byte = line_col_to_byte(self.source, close_pos);

                self.injector.insert(
                    open_byte,
                    format!(
                        "\npiano_runtime::PianoFuture::new(async move {{\
                         \n    let __piano_guard = piano_runtime::enter({guard_name:?});\
                         \n    let __piano_ctx_owned = piano_runtime::fork();\
                         \n    let __piano_ctx = __piano_ctx_owned.as_ref();"
                    ),
                );

                // Collect adopt injections for closures inside the block.
                let mut adopt = AdoptCollector {
                    in_parallel_chain: false,
                    in_scope_body: false,
                    source: self.source,
                    injector: &mut self.injector,
                };
                for stmt in &block.stmts {
                    adopt.visit_stmt(stmt);
                }

                self.injector.insert(close_byte, "\n}).await\n");
            } else {
                // Sync + concurrency: guard + fork at top.
                self.injector.insert(
                    open_byte,
                    format!(
                        "\n    let __piano_guard = piano_runtime::enter({guard_name:?});\
                         \n    let __piano_ctx_owned = piano_runtime::fork();\
                         \n    let __piano_ctx = __piano_ctx_owned.as_ref();"
                    ),
                );

                // Collect adopt injections for closures inside the block.
                let mut adopt = AdoptCollector {
                    in_parallel_chain: false,
                    in_scope_body: false,
                    source: self.source,
                    injector: &mut self.injector,
                };
                for stmt in &block.stmts {
                    adopt.visit_stmt(stmt);
                }
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
                     \n    let __piano_guard = piano_runtime::enter({guard_name:?});"
                ),
            );
            self.injector.insert(close_byte, "\n}).await\n");
        } else {
            // Sync function: just inject guard.
            self.injector.insert(
                open_byte,
                format!("\n    let __piano_guard = piano_runtime::enter({guard_name:?});"),
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
        let guard_name = self.qualify_name(name);

        // Find trailing expression.
        let Some(syn::Stmt::Expr(trailing_expr, None)) = block.stmts.last() else {
            // No trailing expression -- fall back to sync guard.
            let open_pos = block.brace_token.span.open().start();
            let open_byte =
                skip_inner_attrs(self.source, line_col_to_byte(self.source, open_pos) + 1);
            self.injector.insert(
                open_byte,
                format!("\n    let __piano_guard = piano_runtime::enter({guard_name:?});"),
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
                         \n        let __piano_guard = piano_runtime::enter({guard_name:?});\
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
                self.collect_return_wrappers(block, &guard_name, &return_type);

                self.injector.insert(
                    trailing_start,
                    format!(
                        "Box::pin(piano_runtime::PianoFuture::new(async move {{\
                         \n        let __piano_guard = piano_runtime::enter({guard_name:?});\
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
        let mut collector = ReturnWrapperCollector {
            name,
            return_type,
            source: self.source,
            injector: &mut self.injector,
        };
        for stmt in stmts {
            collector.visit_stmt(stmt);
        }
    }
}

struct ReturnWrapperCollector<'a> {
    name: &'a str,
    return_type: &'a str,
    source: &'a str,
    injector: &'a mut StringInjector,
}

impl<'ast, 'a> Visit<'ast> for ReturnWrapperCollector<'a> {
    fn visit_expr_return(&mut self, ret: &'ast syn::ExprReturn) {
        if let Some(inner) = &ret.expr {
            let inner_start = line_col_to_byte(self.source, inner.span().start());
            let inner_end = line_col_to_byte(self.source, inner.span().end());
            self.injector.insert(
                inner_start,
                format!(
                    "Box::pin(piano_runtime::PianoFuture::new(async move {{\
                     \n            let __piano_guard = piano_runtime::enter({:?});\
                     \n            let __piano_inner: {} = ",
                    self.name, self.return_type
                ),
            );
            self.injector
                .insert(inner_end, ";\n            __piano_inner.await\n        }))");
        }
        // Don't recurse into the return's inner expression -- it's already wrapped.
    }

    // Boundaries: return inside closures/async blocks is not our function's return.
    fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}
    fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}
}

/// Find the first concurrency pattern in a block and return its name.
///
/// For method calls matching PARALLEL_ITER_METHODS: returns the method name (e.g. "par_iter").
/// For method calls matching FORK_INJECTION_TRIGGERS: returns the method name (e.g. "scope").
/// For function calls matching FORK_INJECTION_TRIGGERS: returns the full path (e.g. "rayon::scope").
/// Stops recursion at detached spawn calls (method or function) since their
/// 'static boundary prevents fork/adopt from working.
fn find_concurrency_pattern(block: &syn::Block) -> Option<String> {
    let mut finder = ConcurrencyPatternFinder { found: None };
    for stmt in &block.stmts {
        finder.visit_stmt(stmt);
        if finder.found.is_some() {
            return finder.found;
        }
    }
    None
}

struct ConcurrencyPatternFinder {
    found: Option<String>,
}

impl<'ast> Visit<'ast> for ConcurrencyPatternFinder {
    fn visit_expr_method_call(&mut self, mc: &'ast syn::ExprMethodCall) {
        if self.found.is_some() {
            return;
        }
        let method = mc.method.to_string();
        if PARALLEL_ITER_METHODS.contains(&method.as_str()) {
            self.found = Some(method);
            return;
        }
        if FORK_INJECTION_TRIGGERS.contains(&method.as_str()) {
            self.found = Some(method);
            return;
        }
        // Don't recurse into detached .spawn() args -- anything inside
        // inherits the 'static boundary, so fork/adopt can't help.
        if method == "spawn" {
            return;
        }
        // Default recursion into receiver and args.
        syn::visit::visit_expr_method_call(self, mc);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if self.found.is_some() {
            return;
        }
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
                self.found = Some(full_path);
                return;
            }
        }
        // Don't recurse into detached spawn args.
        if name.as_deref() == Some("spawn") {
            return;
        }
        // Default recursion into func and args.
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_expr_closure(&mut self, c: &'ast syn::ExprClosure) {
        // Recurse into closure body (concurrency can be inside a closure arg).
        self.visit_expr(&c.body);
    }

    fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}
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

// ---------------------------------------------------------------------------
// String-injection adopt collection (read-only AST walk)
// ---------------------------------------------------------------------------

// Hard-coded 8-space indent — may not match user's style, but the
// instrumented source is transient (never shown to users).
const ADOPT_STMT_TEXT: &str =
    "\n        let __piano_adopt = __piano_ctx.map(|c| piano_runtime::adopt(c));";

struct AdoptCollector<'a> {
    in_parallel_chain: bool,
    in_scope_body: bool,
    source: &'a str,
    injector: &'a mut StringInjector,
}

impl<'a> AdoptCollector<'a> {
    fn visit_with_chain(&mut self, expr: &syn::Expr, chain: bool) {
        let prev = self.in_parallel_chain;
        self.in_parallel_chain = chain;
        self.visit_expr(expr);
        self.in_parallel_chain = prev;
    }
}

impl<'ast, 'a> Visit<'ast> for AdoptCollector<'a> {
    fn visit_expr_method_call(&mut self, mc: &'ast syn::ExprMethodCall) {
        let method = mc.method.to_string();
        let is_par = PARALLEL_ITER_METHODS.contains(&method.as_str());
        let is_spawn = ADOPT_INJECTION_TARGETS.contains(&method.as_str());
        let is_scope = SCOPE_FUNCTIONS.contains(&method.as_str());
        let is_detached = method == "spawn" && !self.in_scope_body;

        // Recurse into receiver first (preserving current chain state).
        self.visit_expr(&mc.receiver);

        let chain_active =
            self.in_parallel_chain || is_par || receiver_has_parallel_method(&mc.receiver);

        if is_detached {
            // Detached spawn -- don't inject adopt, don't recurse into args.
        } else if is_scope {
            // Scope closures are coordinators -- recurse body for nested spawns.
            for arg in &mc.args {
                if let syn::Expr::Closure(closure) = arg {
                    collect_adopt_in_scope_closure(closure, self.source, self.injector);
                } else {
                    let prev = self.in_parallel_chain;
                    self.in_parallel_chain = false;
                    self.visit_expr(arg);
                    self.in_parallel_chain = prev;
                }
            }
        } else if (chain_active && !is_par) || is_spawn {
            // Worker closures: inject adopt, then recurse body.
            for arg in &mc.args {
                if let syn::Expr::Closure(closure) = arg {
                    collect_adopt_at_closure_start(closure, self.source, self.injector);
                    if let syn::Expr::Block(block) = &*closure.body {
                        let prev_chain = self.in_parallel_chain;
                        self.in_parallel_chain = false;
                        for stmt in &block.block.stmts {
                            self.visit_stmt(stmt);
                        }
                        self.in_parallel_chain = prev_chain;
                    }
                } else {
                    let prev = self.in_parallel_chain;
                    self.in_parallel_chain = false;
                    self.visit_expr(arg);
                    self.in_parallel_chain = prev;
                }
            }
        } else {
            // Non-special method: propagate chain state through args.
            for arg in &mc.args {
                self.visit_with_chain(arg, chain_active);
            }
        }
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
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
                    collect_adopt_in_scope_closure(closure, self.source, self.injector);
                } else {
                    let prev = self.in_parallel_chain;
                    self.in_parallel_chain = false;
                    self.visit_expr(arg);
                    self.in_parallel_chain = prev;
                }
            }
        } else if is_spawn && !is_detached {
            for arg in &call.args {
                if let syn::Expr::Closure(closure) = arg {
                    collect_adopt_at_closure_start(closure, self.source, self.injector);
                    if let syn::Expr::Block(block) = &*closure.body {
                        let prev_chain = self.in_parallel_chain;
                        self.in_parallel_chain = false;
                        for stmt in &block.block.stmts {
                            self.visit_stmt(stmt);
                        }
                        self.in_parallel_chain = prev_chain;
                    }
                } else {
                    let prev = self.in_parallel_chain;
                    self.in_parallel_chain = false;
                    self.visit_expr(arg);
                    self.in_parallel_chain = prev;
                }
            }
        } else if is_detached {
            // Detached spawn -- don't recurse into args.
        } else {
            // Default: recurse into func and args, no chain propagation.
            for arg in &call.args {
                let prev = self.in_parallel_chain;
                self.in_parallel_chain = false;
                self.visit_expr(arg);
                self.in_parallel_chain = prev;
            }
        }
    }

    // Boundaries: closures and async blocks are separate scopes.
    fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}
    fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}
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
            let open_byte = skip_inner_attrs(source, line_col_to_byte(source, open_pos) + 1);
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
        let mut adopt = AdoptCollector {
            in_parallel_chain: false,
            in_scope_body: true,
            source,
            injector,
        };
        for stmt in &block.block.stmts {
            adopt.visit_stmt(stmt);
        }
    }
}

fn receiver_has_parallel_method(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            PARALLEL_ITER_METHODS.contains(&method.as_str())
                || receiver_has_parallel_method(&mc.receiver)
        }
        syn::Expr::Paren(p) => receiver_has_parallel_method(&p.expr),
        _ => false,
    }
}

impl<'s> Visit<'s> for InjectionCollector<'s> {
    fn visit_item_fn(&mut self, node: &'s syn::ItemFn) {
        if matches!(classify(&node.sig), Classification::Instrumentable) {
            let name = node.sig.ident.to_string();
            self.collect_guard(&node.block, &name, &node.sig);
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_impl(&mut self, node: &'s syn::ItemImpl) {
        let type_name = type_ident(&node.self_ty);
        let prev = self.current_impl.take();
        // For trait impls (impl Trait for Type), include trait name for disambiguation.
        // Format: "<Type as Trait>" so methods become "<Type as Trait>::method".
        // For inherent impls (plain impl Type), use just the type name.
        let impl_name = if let Some((_, ref trait_path, _)) = node.trait_ {
            if let Some(seg) = trait_path.segments.last() {
                format!("<{} as {}>", type_name, seg.ident)
            } else {
                type_name
            }
        } else {
            type_name
        };
        self.current_impl = Some(impl_name);
        syn::visit::visit_item_impl(self, node);
        self.current_impl = prev;
    }

    fn visit_impl_item_fn(&mut self, node: &'s syn::ImplItemFn) {
        if matches!(classify(&node.sig), Classification::Instrumentable) {
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
            if matches!(classify(&node.sig), Classification::Instrumentable) {
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

/// Inject `piano_runtime::register(name)` calls at the top of `fn main`.
///
/// This ensures every instrumented function appears in the output, even if it
/// was never called during the run.
pub fn inject_registrations(
    source: &str,
    names: &[(u32, &str)],
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut injector = StringInjector::new();

    let insert_pos = match file.attrs.last() {
        Some(attr) => line_col_to_byte(source, attr.span().end()),
        None => 0,
    };

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
    fn injects_fork_and_adopt_for_par_iter() {
        let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
        let targets: HashSet<String> = ["process_all".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
    fn par_iter_inside_async_block_not_detected() {
        // par_iter inside an async block is a separate scope -- it should NOT
        // cause fork/adopt injection in the enclosing function.
        let source = r#"
fn outer() {
    async {
        items.par_iter().for_each(|x| process(x));
    };
}
"#;
        let targets: HashSet<String> = ["outer".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();

        assert!(
            !result.source.contains("piano_runtime::fork()"),
            "should NOT inject fork for par_iter inside async block. Got:\n{}",
            result.source
        );
        assert!(
            !result.source.contains("piano_runtime::adopt"),
            "should NOT inject adopt for par_iter inside async block. Got:\n{}",
            result.source
        );
        assert!(
            result.concurrency.is_empty(),
            "par_iter inside async block should not report concurrency. Got: {:?}",
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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
        assert!(
            !result.source.contains("PianoFuture"),
            "non-target async fn should not be wrapped"
        );
        assert!(!result.source.contains("piano_runtime::enter"));
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let result = instrument_source(source, &targets, false, "").unwrap();

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
        let result = instrument_source(source, &targets, false, "").unwrap();
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

    #[test]
    fn module_prefix_qualifies_guard_name() {
        let source = r#"
fn validate_input(x: i32) -> bool {
    x > 0
}
"#;
        let targets: HashSet<String> = ["validate_input".to_string()].into();
        let result = instrument_source(source, &targets, false, "db::query")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("db::query::validate_input")"#),
            "guard should use module-qualified name. Got:\n{result}",
        );
    }

    #[test]
    fn module_prefix_qualifies_impl_method() {
        let source = r#"
struct Handler;
impl Handler {
    fn validate(&self) { }
}
"#;
        let targets: HashSet<String> = ["Handler::validate".to_string()].into();
        let result = instrument_source(source, &targets, false, "api")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("api::Handler::validate")"#),
            "guard should qualify impl method. Got:\n{result}",
        );
    }

    #[test]
    fn empty_module_prefix_preserves_bare_name() {
        let source = r#"
fn walk() {
    do_stuff();
}
"#;
        let targets: HashSet<String> = ["walk".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("walk")"#),
            "empty prefix should not change name. Got:\n{result}",
        );
    }

    #[test]
    fn module_prefix_qualifies_concurrency_info() {
        let source = r#"
fn process_all(data: &[i32]) {
    data.par_iter().for_each(|x| { let _ = x; });
}
"#;
        let targets: HashSet<String> = ["process_all".to_string()].into();
        let result = instrument_source(source, &targets, false, "worker").unwrap();
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
        let targets: HashSet<String> = [
            "<Point as Display>::fmt".to_string(),
            "<Point as Debug>::fmt".to_string(),
        ]
        .into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("<Point as Display>::fmt")"#),
            "Display::fmt should be instrumented with trait-qualified name"
        );
        assert!(
            result.contains(r#"piano_runtime::enter("<Point as Debug>::fmt")"#),
            "Debug::fmt should be instrumented with trait-qualified name"
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
        let targets: HashSet<String> = ["Walker::walk".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("Walker::walk")"#),
            "inherent methods should use Type::method format"
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

        // Must re-parse successfully.
        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));

        // Inner attr must precede the guard.
        let attr_pos = result.find("#![allow(unused_variables)]").unwrap();
        let guard_pos = result.find("piano_runtime::enter").unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
        let mut map = result.source_map;
        let mut current = result.source;

        let (s, m) = inject_registrations(&current, &[(0, "work")]).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_global_allocator(&current, AllocatorKind::Absent).unwrap();
        map.merge(m);
        current = s;

        let (s, m) = inject_shutdown(&current, "/tmp/piano/runs", false).unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
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
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "").unwrap();
        assert!(
            !result.concurrency.is_empty(),
            "should detect par_iter inside Assign expr. Got: {:?}\n{}",
            result.concurrency,
            result.source
        );
    }

    #[test]
    fn wraps_return_inside_assign_in_boxed_future() {
        // `return` inside an assignment expression -- previously missed by wildcard.
        // The Assign expr is not handled by the old walker, so the return inside
        // the if-else (which is the Assign's right-hand side) is never wrapped.
        let source = r#"
fn fetch(flag: bool) -> Pin<Box<dyn Future<Output = i32>>> {
    let mut x = 0;
    x = if flag { return Box::pin(async { 0 }); } else { 1 };
    Box::pin(async { x })
}
"#;
        let targets: HashSet<String> = ["fetch".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
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
    fn injects_adopt_in_try_expr_inside_scope() {
        // s.spawn wrapped in a Try (?) expression -- previously missed by wildcard.
        // The spawn is directly inside Ok(...)?, not behind a closure boundary.
        let source = r#"
fn work() {
    rayon::scope(|s| {
        Ok::<_, ()>(s.spawn(|_| { do_work(); })).unwrap();
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt inside spawn closure in unwrap() expr. Got:\n{result}"
        );
    }

    #[test]
    fn injects_adopt_in_tuple_inside_scope() {
        // s.spawn inside a tuple expression -- previously missed.
        let source = r#"
fn work() {
    rayon::scope(|s| {
        let _ = (s.spawn(|_| { work_a(); }), s.spawn(|_| { work_b(); }));
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        let adopt_count = result.matches("piano_runtime::adopt").count();
        assert_eq!(
            adopt_count, 2,
            "should inject adopt in both spawn closures inside tuple. Got {adopt_count} in:\n{result}"
        );
    }

    #[test]
    fn injects_adopt_in_reference_inside_scope() {
        // s.spawn inside a & expression -- previously missed.
        let source = r#"
fn work() {
    rayon::scope(|s| {
        let r = &s.spawn(|_| { do_work(); });
    });
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt inside spawn closure in Reference expr. Got:\n{result}"
        );
    }

    #[test]
    fn propagates_parallel_chain_through_paren_expr() {
        // par_iter chain wrapped in parentheses -- previously missed.
        let source = r#"
fn work(items: &[Item]) {
    (items.par_iter()).for_each(|item| process(item));
}
"#;
        let targets: HashSet<String> = ["work".to_string()].into();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt in for_each closure with parenthesized par_iter. Got:\n{result}"
        );
    }
}
