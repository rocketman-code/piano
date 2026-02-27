use std::collections::HashSet;

use quote::quote;
use syn::visit_mut::VisitMut;

use crate::resolve::is_instrumentable;

/// Result of instrumenting a source file.
pub struct InstrumentResult {
    pub source: String,
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
    let mut file: syn::File = syn::parse_str(source)?;
    let mut instrumenter = Instrumenter {
        targets: targets.clone(),
        instrument_macros,
        current_impl: None,
        current_trait: None,
        concurrency: Vec::new(),
    };
    instrumenter.visit_file_mut(&mut file);
    Ok(InstrumentResult {
        source: prettyplease::unparse(&file),
        concurrency: instrumenter.concurrency,
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

/// Function/method path segments that indicate spawn/scope boundaries.
const SPAWN_FUNCTIONS: &[&str] = &["spawn", "scope", "scope_fifo", "join"];
/// Scope-like functions whose closures coordinate workers (don't adopt, recurse body).
const SCOPE_FUNCTIONS: &[&str] = &["scope", "scope_fifo"];

/// Functions that create scoped concurrency boundaries where closures can
/// capture non-'static references, making fork/adopt injection safe.
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
const FORK_TRIGGER_FUNCTIONS: &[&str] = &["scope", "scope_fifo", "join"];

/// Check whether a statement contains `.await` at its own expression level,
/// excluding awaits inside sub-blocks of control-flow expressions (if, match,
/// loop, while, for). Those sub-blocks are handled by the recursive visitor.
///
/// Also excludes closures, async blocks, and nested fn items since those are
/// separate async contexts.
fn stmt_has_direct_await(stmt: &syn::Stmt) -> bool {
    struct AwaitFinder {
        found: bool,
    }
    impl<'ast> syn::visit::Visit<'ast> for AwaitFinder {
        fn visit_expr_await(&mut self, _: &'ast syn::ExprAwait) {
            self.found = true;
        }
        // Don't descend into closures -- separate async context.
        fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}
        // Don't descend into async blocks -- separate async context.
        fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}
        // Don't descend into nested fn items -- they have their own guards.
        fn visit_item_fn(&mut self, _: &'ast syn::ItemFn) {}
        // Don't descend into sub-blocks of control-flow expressions.
        // The recursive VisitMut will process those blocks separately.
        fn visit_block(&mut self, _: &'ast syn::Block) {}
    }
    let mut finder = AwaitFinder { found: false };
    syn::visit::Visit::visit_stmt(&mut finder, stmt);
    finder.found
}

/// Recursively inject `_piano_guard.check();` after each statement that
/// directly contains `.await`, at every nesting depth.
///
/// Uses `VisitMut` to walk into if/match/loop/while/for/block bodies so
/// that `check()` is placed immediately after the `.await`, not after the
/// enclosing top-level statement.
fn inject_await_checks(block: &mut syn::Block) {
    struct AwaitCheckInjector;

    impl AwaitCheckInjector {
        /// Process a block's statements: for each statement that directly
        /// contains `.await`, insert a `check()` call after it. Then recurse
        /// into nested blocks within each statement.
        fn process_block(&mut self, block: &mut syn::Block) {
            let mut new_stmts = Vec::with_capacity(block.stmts.len() * 2);
            for mut stmt in block.stmts.drain(..) {
                let has_await = stmt_has_direct_await(&stmt);
                // Recurse into nested blocks within this statement first.
                self.visit_stmt_mut(&mut stmt);
                new_stmts.push(stmt);
                if has_await {
                    let check_stmt: syn::Stmt = syn::parse_quote! {
                        _piano_guard.check();
                    };
                    new_stmts.push(check_stmt);
                }
            }
            block.stmts = new_stmts;
        }
    }

    impl VisitMut for AwaitCheckInjector {
        fn visit_block_mut(&mut self, block: &mut syn::Block) {
            self.process_block(block);
        }

        // Don't descend into closures -- separate async context.
        fn visit_expr_closure_mut(&mut self, _: &mut syn::ExprClosure) {}
        // Don't descend into async blocks -- separate async context.
        fn visit_expr_async_mut(&mut self, _: &mut syn::ExprAsync) {}
        // Don't descend into nested fn items -- they have their own guards.
        fn visit_item_fn_mut(&mut self, _: &mut syn::ItemFn) {}
    }

    AwaitCheckInjector.process_block(block);
}

struct Instrumenter {
    targets: HashSet<String>,
    instrument_macros: bool,
    current_impl: Option<String>,
    current_trait: Option<String>,
    /// Collected concurrency info: (function_name, pattern_name).
    concurrency: Vec<(String, String)>,
}

impl Instrumenter {
    fn inject_guard(&mut self, block: &mut syn::Block, name: &str, is_async: bool) {
        if !self.targets.contains(name) {
            return;
        }
        let guard_stmt: syn::Stmt = syn::parse_quote! {
            let _piano_guard = piano_runtime::enter(#name);
        };
        block.stmts.insert(0, guard_stmt);

        // For async functions, inject check() after each .await statement
        // so the phantom StackEntry is pushed on migration.
        if is_async {
            let guard_stmt = block.stmts.remove(0);
            inject_await_checks(block);
            block.stmts.insert(0, guard_stmt);
        }

        // If the function body contains concurrency calls, inject fork
        // and adopt in the relevant closures.
        if let Some(pattern) = find_concurrency_pattern(block) {
            self.concurrency.push((name.to_string(), pattern));
            let fork_owned_stmt: syn::Stmt = syn::parse_quote! {
                let _piano_ctx_owned = piano_runtime::fork();
            };
            let fork_ref_stmt: syn::Stmt = syn::parse_quote! {
                let _piano_ctx = _piano_ctx_owned.as_ref();
            };
            // Insert after the guard (position 1, 2)
            block.stmts.insert(1, fork_owned_stmt);
            block.stmts.insert(2, fork_ref_stmt);

            // Walk the remaining statements and inject adopt into closures
            for stmt in block.stmts.iter_mut().skip(3) {
                match stmt {
                    syn::Stmt::Expr(expr, _) => {
                        inject_adopt_in_concurrency_closures(expr, false);
                    }
                    syn::Stmt::Local(local) => {
                        if let Some(init) = &mut local.init {
                            inject_adopt_in_concurrency_closures(&mut init.expr, false);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Find the first concurrency pattern in a block and return its name.
///
/// For method calls matching PARALLEL_ITER_METHODS: returns the method name (e.g. "par_iter").
/// For method calls matching FORK_TRIGGER_FUNCTIONS: returns the method name (e.g. "scope").
/// For function calls matching FORK_TRIGGER_FUNCTIONS: returns the full path (e.g. "rayon::scope").
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

fn find_pattern_in_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            if PARALLEL_ITER_METHODS.contains(&method.as_str()) {
                return Some(method);
            }
            if FORK_TRIGGER_FUNCTIONS.contains(&method.as_str()) {
                return Some(method);
            }
            if let Some(p) = find_pattern_in_expr(&mc.receiver) {
                return Some(p);
            }
            mc.args.iter().find_map(find_pattern_in_expr)
        }
        syn::Expr::Call(call) => {
            if let syn::Expr::Path(path) = &*call.func {
                let last_seg = path.path.segments.last().map(|s| s.ident.to_string());
                if let Some(ref name) = last_seg
                    && FORK_TRIGGER_FUNCTIONS.contains(&name.as_str())
                {
                    // Build the full path, e.g. "rayon::scope"
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
            if let syn::Expr::Path(path) = &*call.func {
                let last = path.path.segments.last().map(|s| s.ident.to_string());
                if last.as_deref() == Some("spawn") {
                    return None;
                }
            }
            call.args.iter().find_map(find_pattern_in_expr)
        }
        syn::Expr::Block(b) => b.block.stmts.iter().find_map(find_pattern_in_stmt),
        syn::Expr::Closure(c) => find_pattern_in_expr(&c.body),
        _ => None,
    }
}

/// Walk an expression and inject adopt() at the start of closures that
/// are arguments to concurrency calls or methods chained after par_iter.
fn inject_adopt_in_concurrency_closures(expr: &mut syn::Expr, in_parallel_chain: bool) {
    match expr {
        syn::Expr::MethodCall(mc) => {
            let method = mc.method.to_string();
            let is_par = PARALLEL_ITER_METHODS.contains(&method.as_str());
            let is_spawn = SPAWN_FUNCTIONS.contains(&method.as_str());
            let is_scope = SCOPE_FUNCTIONS.contains(&method.as_str());

            // Recurse into receiver first
            inject_adopt_in_concurrency_closures(&mut mc.receiver, in_parallel_chain);

            // Determine if we're in a parallel chain
            let chain_active =
                in_parallel_chain || is_par || receiver_has_parallel_method(&mc.receiver);

            if is_scope {
                // Scope closures are coordinators, not workers — don't adopt,
                // but recurse into their body to find nested spawn calls.
                for arg in &mut mc.args {
                    if let syn::Expr::Closure(closure) = arg {
                        recurse_closure_body_for_spawns(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
            } else if (chain_active && !is_par) || is_spawn {
                // Worker closures: inject adopt so their time is attributed.
                for arg in &mut mc.args {
                    if let syn::Expr::Closure(closure) = arg {
                        inject_adopt_at_closure_start(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
            } else {
                // Recurse into args anyway for nested concurrency
                for arg in &mut mc.args {
                    inject_adopt_in_concurrency_closures(arg, chain_active);
                }
            }
        }
        syn::Expr::Call(call) => {
            let func_name = if let syn::Expr::Path(path) = &*call.func {
                path.path.segments.last().map(|s| s.ident.to_string())
            } else {
                None
            };
            let is_spawn = func_name
                .as_ref()
                .is_some_and(|n| SPAWN_FUNCTIONS.contains(&n.as_str()));
            let is_scope = func_name
                .as_ref()
                .is_some_and(|n| SCOPE_FUNCTIONS.contains(&n.as_str()));
            let is_detached = func_name.as_deref() == Some("spawn");

            if is_scope {
                for arg in &mut call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        recurse_closure_body_for_spawns(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
            } else if is_spawn && !is_detached {
                for arg in &mut call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        inject_adopt_at_closure_start(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
            } else if is_detached {
                // Detached spawn (std::thread::spawn, rayon::spawn, etc.)
                // Don't inject adopt -- can't cross 'static boundary.
                // Don't recurse into closure body -- nested scopes can't
                // use the parent's fork either.
            } else {
                for arg in &mut call.args {
                    inject_adopt_in_concurrency_closures(arg, false);
                }
            }
        }
        syn::Expr::Block(b) => {
            for stmt in &mut b.block.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
        }
        syn::Expr::ForLoop(f) => {
            for stmt in &mut f.body.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
        }
        syn::Expr::While(w) => {
            for stmt in &mut w.body.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
        }
        syn::Expr::Loop(l) => {
            for stmt in &mut l.body.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
        }
        syn::Expr::If(i) => {
            for stmt in &mut i.then_branch.stmts {
                if let syn::Stmt::Expr(e, _) = stmt {
                    inject_adopt_in_concurrency_closures(e, false);
                }
            }
            if let Some((_, else_branch)) = &mut i.else_branch {
                inject_adopt_in_concurrency_closures(else_branch, false);
            }
        }
        _ => {}
    }
}

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

fn inject_adopt_at_closure_start(closure: &mut syn::ExprClosure) {
    let adopt_stmt: syn::Stmt = syn::parse_quote! {
        let _piano_adopt = _piano_ctx.map(|c| piano_runtime::adopt(c));
    };
    match &mut *closure.body {
        syn::Expr::Block(block) => {
            block.block.stmts.insert(0, adopt_stmt);
        }
        other => {
            let existing = other.clone();
            *other = syn::parse_quote! {
                {
                    #adopt_stmt
                    #existing
                }
            };
        }
    }
}

/// Recurse into a closure body to find nested spawn calls and inject adopt.
/// Used for scope/scope_fifo closures where the closure is the coordinator,
/// not a worker — we don't adopt the coordinator, but its spawn calls need adopt.
fn recurse_closure_body_for_spawns(closure: &mut syn::ExprClosure) {
    if let syn::Expr::Block(block) = &mut *closure.body {
        inject_adopt_in_stmts(&mut block.block.stmts);
    }
}

/// Walk statements, injecting adopt into spawn closures.
/// `_piano_ctx` is `Option<&SpanContext>` which is `Copy`, so `move` closures
/// can capture it without ownership issues.
fn inject_adopt_in_stmts(stmts: &mut [syn::Stmt]) {
    for stmt in stmts.iter_mut() {
        match stmt {
            syn::Stmt::Expr(e, _) => {
                inject_adopt_in_concurrency_closures(e, false);
            }
            syn::Stmt::Local(local) => {
                if let Some(init) = &mut local.init {
                    inject_adopt_in_concurrency_closures(&mut init.expr, false);
                }
            }
            _ => {}
        }
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
/// (const fn, unsafe fn, extern fn, extern "ABI" fn), return the index just
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

    // Must see one of: const, unsafe, extern — then `fn` must follow immediately.
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
    } else if is_ident(tokens, pos, "extern") {
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
///   [pub [( ... )]]? [async]? fn NAME ( ... ) [-> ...]? { ... }
///
/// Rejects const fn, unsafe fn, and extern fn (matching is_instrumentable).
/// NAME can be an Ident (literal name) or $metavar (Punct('$') + Ident).
fn match_fn_pattern(tokens: &[proc_macro2::TokenTree], start: usize) -> Option<FnMatch> {
    let len = tokens.len();
    let mut pos = start;

    // Check for non-instrumentable prefixes: const, unsafe, extern.
    // If we see any of these before `fn`, skip this position.
    if is_ident(tokens, pos, "const")
        || is_ident(tokens, pos, "unsafe")
        || is_ident(tokens, pos, "extern")
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
            || is_ident(tokens, pos, "extern")
        {
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
                        '>' => depth -= 1,
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

/// Build the guard statement tokens for a function name.
///
/// For a literal name like `initialize`:
///   `let _piano_guard = piano_runtime::enter("initialize");`
///
/// For a metavar name like `$name`:
///   `let _piano_guard = piano_runtime::enter(stringify!($name));`
fn make_guard_tokens(name_tokens: &[proc_macro2::TokenTree]) -> proc_macro2::TokenStream {
    let is_metavar = name_tokens.len() == 2
        && matches!(&name_tokens[0], proc_macro2::TokenTree::Punct(p) if p.as_char() == '$');

    if is_metavar {
        // For metavar names, we need to build:
        //   let _piano_guard = piano_runtime::enter(stringify!($name));
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

        // Build: let _piano_guard = piano_runtime::enter(stringify!($name));
        let enter_arg =
            proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, stringify_call);

        let span = proc_macro2::Span::call_site();
        vec![
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("let", span)),
            proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("_piano_guard", span)),
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
            let _piano_guard = piano_runtime::enter(#name_str);
        }
    }
}

impl VisitMut for Instrumenter {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if is_instrumentable(&node.sig) {
            let name = node.sig.ident.to_string();
            let is_async = node.sig.asyncness.is_some();
            self.inject_guard(&mut node.block, &name, is_async);
        }
        syn::visit_mut::visit_item_fn_mut(self, node);
    }

    fn visit_item_impl_mut(&mut self, node: &mut syn::ItemImpl) {
        let type_name = type_ident(&node.self_ty);
        let prev = self.current_impl.take();
        self.current_impl = Some(type_name);
        syn::visit_mut::visit_item_impl_mut(self, node);
        self.current_impl = prev;
    }

    fn visit_impl_item_fn_mut(&mut self, node: &mut syn::ImplItemFn) {
        if is_instrumentable(&node.sig) {
            let method = node.sig.ident.to_string();
            let qualified = match &self.current_impl {
                Some(ty) => format!("{ty}::{method}"),
                None => method,
            };
            let is_async = node.sig.asyncness.is_some();
            self.inject_guard(&mut node.block, &qualified, is_async);
        }
        syn::visit_mut::visit_impl_item_fn_mut(self, node);
    }

    fn visit_item_trait_mut(&mut self, node: &mut syn::ItemTrait) {
        let trait_name = node.ident.to_string();
        let prev = self.current_trait.take();
        self.current_trait = Some(trait_name);
        syn::visit_mut::visit_item_trait_mut(self, node);
        self.current_trait = prev;
    }

    fn visit_trait_item_fn_mut(&mut self, node: &mut syn::TraitItemFn) {
        if let Some(block) = &mut node.default {
            if is_instrumentable(&node.sig) {
                let method = node.sig.ident.to_string();
                let qualified = match &self.current_trait {
                    Some(trait_name) => format!("{trait_name}::{method}"),
                    None => method,
                };
                let is_async = node.sig.asyncness.is_some();
                self.inject_guard(block, &qualified, is_async);
            }
        }
        syn::visit_mut::visit_trait_item_fn_mut(self, node);
    }

    fn visit_item_macro_mut(&mut self, node: &mut syn::ItemMacro) {
        if self.instrument_macros && is_macro_rules(node) {
            instrument_macro_tokens(&mut node.mac.tokens);
        }
        syn::visit_mut::visit_item_macro_mut(self, node);
    }
}

/// Inject `piano_runtime::register(name)` calls at the top of `fn main`.
///
/// This ensures every instrumented function appears in the output, even if it
/// was never called during the run.
pub fn inject_registrations(source: &str, names: &[String]) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;
    let mut injector = RegistrationInjector {
        names: names.to_vec(),
    };
    injector.visit_file_mut(&mut file);
    Ok(prettyplease::unparse(&file))
}

struct RegistrationInjector {
    names: Vec<String>,
}

impl VisitMut for RegistrationInjector {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if node.sig.ident == "main" {
            for name in self.names.iter().rev() {
                let stmt: syn::Stmt = syn::parse_quote! {
                    piano_runtime::register(#name);
                };
                node.block.stmts.insert(0, stmt);
            }
        }
        syn::visit_mut::visit_item_fn_mut(self, node);
    }
}

/// Inject a `#[global_allocator]` static using `PianoAllocator` wrapping System.
///
/// If `existing_allocator_type` is provided, the source already has a
/// `#[global_allocator]` that should be wrapped rather than replaced.
pub fn inject_global_allocator(
    source: &str,
    existing_allocator_type: Option<&str>,
) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;

    match existing_allocator_type {
        None => {
            let item: syn::Item = syn::parse_quote! {
                #[global_allocator]
                static _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>
                    = piano_runtime::PianoAllocator::new(std::alloc::System);
            };
            file.items.insert(0, item);
        }
        Some(_) => {
            for item in &mut file.items {
                if let syn::Item::Static(static_item) = item {
                    let has_global_alloc = static_item
                        .attrs
                        .iter()
                        .any(|a| a.path().is_ident("global_allocator"));
                    if has_global_alloc {
                        let orig_ty = &static_item.ty;
                        let orig_expr = &static_item.expr;
                        *static_item.ty = syn::parse_quote! {
                            piano_runtime::PianoAllocator<#orig_ty>
                        };
                        *static_item.expr = syn::parse_quote! {
                            piano_runtime::PianoAllocator::new(#orig_expr)
                        };
                        break;
                    }
                }
            }
        }
    }

    Ok(prettyplease::unparse(&file))
}

/// Wrap `fn main`'s body in `catch_unwind` and inject `piano_runtime::shutdown()`.
///
/// When `runs_dir` is `Some`, emits `shutdown_to(dir)` to write run data to
/// the given project-local directory. When `None`, falls back to `shutdown()`
/// which uses the runtime's default directory resolution.
pub fn inject_shutdown(source: &str, runs_dir: Option<&str>) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;
    let mut injector = ShutdownInjector {
        runs_dir: runs_dir.map(String::from),
    };
    injector.visit_file_mut(&mut file);
    Ok(prettyplease::unparse(&file))
}

struct ShutdownInjector {
    runs_dir: Option<String>,
}

impl ShutdownInjector {
    fn shutdown_stmt(&self) -> syn::Stmt {
        match &self.runs_dir {
            Some(dir) => syn::parse_quote! {
                piano_runtime::shutdown_to(#dir);
            },
            None => syn::parse_quote! {
                piano_runtime::shutdown();
            },
        }
    }

    fn set_runs_dir_stmt(&self) -> Option<syn::Stmt> {
        self.runs_dir.as_ref().map(|dir| {
            syn::parse_quote! {
                piano_runtime::set_runs_dir(#dir);
            }
        })
    }
}

impl VisitMut for ShutdownInjector {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if node.sig.ident == "main" {
            let is_async = node.sig.asyncness.is_some();
            let has_return_type = !matches!(&node.sig.output, syn::ReturnType::Default);

            let existing_stmts = std::mem::take(&mut node.block.stmts);
            let shutdown_stmt = self.shutdown_stmt();
            let set_dir_stmt = self.set_runs_dir_stmt();

            if is_async {
                // Async main: can't use catch_unwind (sync closure can't contain .await).
                // Insert shutdown before the tail expression if there is one.
                let mut stmts: Vec<syn::Stmt> = Vec::new();
                stmts.extend(set_dir_stmt);
                if has_return_type && !existing_stmts.is_empty() {
                    let (body, tail) = existing_stmts.split_at(existing_stmts.len() - 1);
                    stmts.extend(body.to_vec());
                    stmts.push(shutdown_stmt);
                    stmts.extend(tail.to_vec());
                } else {
                    stmts.extend(existing_stmts);
                    stmts.push(shutdown_stmt);
                }
                node.block.stmts = stmts;
            } else {
                // Sync main: wrap body in catch_unwind so guards drop on panic
                // (recording timing data), then shutdown collects it, then re-panic.
                let catch_stmt: syn::Stmt = syn::parse_quote! {
                    let __piano_result = std::panic::catch_unwind(
                        std::panic::AssertUnwindSafe(|| { #(#existing_stmts)* })
                    );
                };
                let mut stmts: Vec<syn::Stmt> = Vec::new();
                stmts.extend(set_dir_stmt);
                stmts.push(catch_stmt);
                stmts.push(shutdown_stmt);
                if has_return_type {
                    let tail: syn::Stmt = syn::Stmt::Expr(
                        syn::parse_quote! {
                            match __piano_result {
                                Ok(__piano_val) => __piano_val,
                                Err(__piano_panic) => std::panic::resume_unwind(__piano_panic),
                            }
                        },
                        None,
                    );
                    stmts.push(tail);
                } else {
                    let resume_stmt: syn::Stmt = syn::parse_quote! {
                        if let Err(__piano_panic) = __piano_result {
                            std::panic::resume_unwind(__piano_panic);
                        }
                    };
                    stmts.push(resume_stmt);
                }
                node.block.stmts = stmts;
            }
        }
        syn::visit_mut::visit_item_fn_mut(self, node);
    }
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
        let result = inject_registrations(source, &names).unwrap();
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
    fn injects_global_allocator() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let result = inject_global_allocator(source, None).unwrap();
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
        let result = inject_global_allocator(source, Some("System")).unwrap();
        assert!(
            result.contains("PianoAllocator"),
            "should wrap existing allocator. Got:\n{result}"
        );
    }

    #[test]
    fn does_not_inject_init() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets, false).unwrap().source;

        assert!(
            !result.contains("piano_runtime::init()"),
            "should NOT inject init (init is a no-op)"
        );
    }

    #[test]
    fn injects_shutdown_with_catch_unwind() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let result = inject_shutdown(source, None).unwrap();
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
        let result = inject_shutdown(source, Some("/tmp/my-project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown_to(\"/tmp/my-project/target/piano/runs\")"),
            "should inject shutdown_to with dir. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_at_start_of_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let result = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
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
        let result = inject_shutdown(source, None).unwrap();
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
        let result = inject_shutdown(source, None).unwrap();
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
        let result = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "async main should NOT use catch_unwind. Got:\n{result}"
        );
        // shutdown should come before the tail expression
        let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
        let exit_code_pos = result.rfind("ExitCode::SUCCESS").unwrap();
        assert!(
            shutdown_pos < exit_code_pos,
            "shutdown should come before the tail expression. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_in_async_main() {
        let source = r#"
async fn main() {
    do_stuff().await;
}
"#;
        let result = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
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
        // `_piano_ctx` is `Option<&SpanContext>` which is Copy, so move closures work.
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
    }

    #[test]
    fn injects_check_after_await_in_async_fn() {
        let targets: HashSet<String> = ["do_work".to_string()].into_iter().collect();
        let source = r#"
async fn do_work() {
    setup();
    fetch().await;
    process();
    save().await;
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        let check_count = result.source.matches("_piano_guard.check()").count();
        assert_eq!(
            check_count, 2,
            "should inject check() after each .await statement. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn no_check_injection_in_sync_fn() {
        let targets: HashSet<String> = ["do_work".to_string()].into_iter().collect();
        let source = r#"
fn do_work() {
    setup();
    process();
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_eq!(
            result.source.matches("_piano_guard.check()").count(),
            0,
            "sync fn should not get check() calls. Got:\n{}",
            result.source
        );
    }

    #[test]
    fn no_check_injection_for_non_target_async_fn() {
        let targets: HashSet<String> = ["other".to_string()].into_iter().collect();
        let source = r#"
async fn not_targeted() {
    fetch().await;
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_eq!(
            result.source.matches("_piano_guard.check()").count(),
            0,
            "non-target async fn should not get check() calls. Got:\n{}",
            result.source
        );
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

    // -- Nested .await check() injection tests --
    //
    // These tests verify check() is injected INSIDE nested blocks, directly
    // after the .await, not just after the enclosing top-level statement.

    /// Assert that `needle_a` appears before `needle_b` in `haystack`.
    fn assert_appears_before(haystack: &str, needle_a: &str, needle_b: &str, context: &str) {
        let pos_a = haystack
            .find(needle_a)
            .unwrap_or_else(|| panic!("{needle_a} not found in output. {context}:\n{haystack}"));
        let pos_b = haystack
            .find(needle_b)
            .unwrap_or_else(|| panic!("{needle_b} not found in output. {context}:\n{haystack}"));
        assert!(
            pos_a < pos_b,
            "{needle_a} should appear before {needle_b} ({context}). Got:\n{haystack}",
        );
    }

    #[test]
    fn injects_check_inside_if_block_not_after_it() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        // The .await is inside the if block. check() must appear between
        // fetch().await and process(), not after the closing brace of `if`.
        let source = r#"
async fn example() {
    if condition {
        fetch().await;
        process();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        // check() must appear before process() (i.e. inside the if block),
        // not after the if statement.
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "process()",
            "inside if block",
        );
    }

    #[test]
    fn injects_check_inside_match_arm() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example(x: u32) {
    match x {
        0 => {
            fetch().await;
            process();
        }
        _ => {}
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "process()",
            "inside match arm",
        );
    }

    #[test]
    fn injects_check_inside_loop() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    loop {
        fetch().await;
        process();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "process()",
            "inside loop",
        );
    }

    #[test]
    fn injects_check_at_every_nesting_depth() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    if condition {
        fetch().await;
        if other {
            save().await;
            cleanup();
        }
    }
    send().await;
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        let check_count = result.source.matches("_piano_guard.check()").count();
        assert_eq!(
            check_count, 3,
            "should inject check() after every .await at any depth. Got:\n{}",
            result.source
        );
        // Verify the innermost check() is between save().await and cleanup()
        let save_pos = result.source.find("save()").unwrap();
        let cleanup_pos = result.source.find("cleanup()").unwrap();
        let check_between = result.source[save_pos..cleanup_pos].contains("_piano_guard.check()");
        assert!(
            check_between,
            "check() should appear between save().await and cleanup(). Got:\n{}",
            result.source,
        );
    }

    #[test]
    fn injects_check_inside_else_block() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    if condition {
        sync_work();
    } else {
        fetch().await;
        process();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "process()",
            "inside else block",
        );
    }

    #[test]
    fn injects_check_inside_while_let_body() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    while let Some(item) = iter.next() {
        process(item).await;
        log();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "log()",
            "inside while let body",
        );
    }

    #[test]
    fn injects_check_inside_for_loop_body() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    for item in items {
        process(item).await;
        log();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "log()",
            "inside for loop body",
        );
    }

    #[test]
    fn injects_check_inside_bare_block() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    {
        fetch().await;
        process();
    }
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "process()",
            "inside bare block",
        );
    }

    #[test]
    fn injects_check_after_await_in_condition_expression() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    if stream.next().await.is_some() {
        process();
    }
    cleanup();
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        // The .await is in the condition, so check() goes after the whole if statement
        assert_eq!(
            result.source.matches("_piano_guard.check()").count(),
            1,
            "expected exactly 1 check() call",
        );
        assert_appears_before(
            &result.source,
            "process()",
            "_piano_guard.check()",
            "check() should be after the if block containing process()",
        );
        assert_appears_before(
            &result.source,
            "_piano_guard.check()",
            "cleanup()",
            "after if with .await in condition",
        );
    }

    #[test]
    fn no_check_injection_inside_closure_or_async_block() {
        let targets: HashSet<String> = ["example".to_string()].into_iter().collect();
        let source = r#"
async fn example() {
    let f = || async { fetch().await; };
    let g = async { save().await; };
    send().await;
}
"#;
        let result = instrument_source(source, &targets, false).unwrap();
        // Only one check() -- after send().await. The closure and async block
        // have separate async contexts with their own guards.
        let check_count = result.source.matches("_piano_guard.check()").count();
        assert_eq!(
            check_count, 1,
            "expected 1 check() (after send().await only), got {check_count}:\n{}",
            result.source,
        );
    }
}
