use std::collections::HashSet;

use quote::quote;
use syn::visit_mut::VisitMut;

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
) -> Result<InstrumentResult, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;
    let mut instrumenter = Instrumenter {
        targets: targets.clone(),
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

struct Instrumenter {
    targets: HashSet<String>,
    current_impl: Option<String>,
    current_trait: Option<String>,
    /// Collected concurrency info: (function_name, pattern_name).
    concurrency: Vec<(String, String)>,
}

impl Instrumenter {
    fn inject_guard(&mut self, block: &mut syn::Block, name: &str) {
        if !self.targets.contains(name) {
            return;
        }
        let guard_stmt: syn::Stmt = syn::parse_quote! {
            let _piano_guard = piano_runtime::enter(#name);
        };
        block.stmts.insert(0, guard_stmt);

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
/// For method calls matching SPAWN_FUNCTIONS: returns the method name (e.g. "spawn").
/// For function calls matching SPAWN_FUNCTIONS: returns the full path (e.g. "rayon::scope").
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
            if SPAWN_FUNCTIONS.contains(&method.as_str()) {
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
                    && SPAWN_FUNCTIONS.contains(&name.as_str())
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

            if is_scope {
                for arg in &mut call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        recurse_closure_body_for_spawns(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
            } else if is_spawn {
                for arg in &mut call.args {
                    if let syn::Expr::Closure(closure) = arg {
                        inject_adopt_at_closure_start(closure);
                    } else {
                        inject_adopt_in_concurrency_closures(arg, false);
                    }
                }
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

impl VisitMut for Instrumenter {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        let name = node.sig.ident.to_string();
        self.inject_guard(&mut node.block, &name);
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
        let method = node.sig.ident.to_string();
        let qualified = match &self.current_impl {
            Some(ty) => format!("{ty}::{method}"),
            None => method,
        };
        self.inject_guard(&mut node.block, &qualified);
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
            let method = node.sig.ident.to_string();
            let qualified = match &self.current_trait {
                Some(trait_name) => format!("{trait_name}::{method}"),
                None => method,
            };
            self.inject_guard(block, &qualified);
        }
        syn::visit_mut::visit_trait_item_fn_mut(self, node);
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
}

impl VisitMut for ShutdownInjector {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        if node.sig.ident == "main" {
            let is_async = node.sig.asyncness.is_some();
            let has_return_type = !matches!(&node.sig.output, syn::ReturnType::Default);

            let existing_stmts = std::mem::take(&mut node.block.stmts);
            let shutdown_stmt = self.shutdown_stmt();

            if is_async {
                // Async main: can't use catch_unwind (sync closure can't contain .await).
                // Insert shutdown before the tail expression if there is one.
                if has_return_type && !existing_stmts.is_empty() {
                    let (body, tail) = existing_stmts.split_at(existing_stmts.len() - 1);
                    let mut stmts: Vec<syn::Stmt> = body.to_vec();
                    stmts.push(shutdown_stmt);
                    stmts.extend(tail.to_vec());
                    node.block.stmts = stmts;
                } else {
                    let mut stmts = existing_stmts;
                    stmts.push(shutdown_stmt);
                    node.block.stmts = stmts;
                }
            } else {
                // Sync main: wrap body in catch_unwind so guards drop on panic
                // (recording timing data), then shutdown collects it, then re-panic.
                let catch_stmt: syn::Stmt = syn::parse_quote! {
                    let __piano_result = std::panic::catch_unwind(
                        std::panic::AssertUnwindSafe(|| { #(#existing_stmts)* })
                    );
                };
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
                    node.block.stmts = vec![catch_stmt, shutdown_stmt, tail];
                } else {
                    let resume_stmt: syn::Stmt = syn::parse_quote! {
                        if let Err(__piano_panic) = __piano_result {
                            std::panic::resume_unwind(__piano_panic);
                        }
                    };
                    node.block.stmts = vec![catch_stmt, shutdown_stmt, resume_stmt];
                }
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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
    fn injects_fork_and_adopt_for_par_iter() {
        let source = r#"
fn process_all(items: &[Item]) -> Vec<Result> {
    items.par_iter()
         .map(|item| transform(item))
         .collect()
}
"#;
        let targets: HashSet<String> = ["process_all".to_string()].into();
        let result = instrument_source(source, &targets).unwrap().source;

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
    fn injects_fork_and_adopt_for_thread_spawn() {
        let source = r#"
fn do_work() {
    std::thread::spawn(|| {
        heavy_computation();
    });
}
"#;
        let targets: HashSet<String> = ["do_work".to_string()].into();
        let result = instrument_source(source, &targets).unwrap().source;

        assert!(
            result.contains("piano_runtime::fork()"),
            "should inject fork. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::adopt"),
            "should inject adopt in spawn closure. Got:\n{result}"
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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap().source;

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
        let result = instrument_source(source, &targets).unwrap();
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
        let result = instrument_source(source, &targets).unwrap();
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
        let result = instrument_source(source, &targets).unwrap();
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
        let result = instrument_source(source, &targets).unwrap();
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
        let result = instrument_source(source, &targets).unwrap();

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
        let result = instrument_source(source, &targets).unwrap();
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
        let result = instrument_source(source, &targets).unwrap();
        assert!(
            result
                .source
                .contains("piano_runtime::enter(\"Service::handle\")"),
            "async trait default method SHOULD be instrumented. Got:\n{}",
            result.source
        );
    }
}
