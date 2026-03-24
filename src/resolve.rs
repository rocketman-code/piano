use std::path::{Path, PathBuf};

use ra_ap_syntax::ast::HasAttrs;
use ra_ap_syntax::ast::HasModuleItem;
use ra_ap_syntax::ast::HasName;
use ra_ap_syntax::{AstNode, AstToken, Edition, SyntaxKind, ast};

use crate::error::{Error, io_context};

/// What the user asked to instrument.
#[derive(Debug, Clone)]
pub enum TargetSpec {
    /// Match against function names (--fn). Substring by default, exact with --exact.
    Fn(String),
    /// All functions in a specific file (--file).
    File(PathBuf),
    /// All functions in a module directory (--mod).
    Mod(String),
}

/// A file and the functions within it that matched.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub file: PathBuf,
    pub functions: Vec<crate::naming::QualifiedFunction>,
}

/// Why a function was excluded from instrumentation.
///
/// Only structurally uninstrumentable signatures belong here.
/// unsafe fn is NOT skipped -- it receives ctx and guard like any other function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    Const,
    ExternAbi,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::Const => write!(f, "const"),
            SkipReason::ExternAbi => write!(f, "extern"),
        }
    }
}

/// Whether a function signature is safe to instrument.
///
/// Explicit enum with no implicit "everything else is fine" default.
/// Every code path through `classify()` returns a named variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Classification {
    /// Safe to instrument with standard guard or PianoFuture.
    Instrumentable,
    /// Must not be instrumented, with reason.
    Skip(SkipReason),
}

/// A function that was in the target set but excluded from instrumentation.
#[derive(Debug, Clone)]
pub struct SkippedFunction {
    pub name: String,
    pub reason: SkipReason,
    /// File path relative to the project root (e.g. "src/ffi.rs").
    pub path: PathBuf,
}

/// Result of target resolution: resolved targets plus any skipped functions.
#[derive(Debug)]
pub struct ResolveResult {
    /// Functions selected for measurement (matched by selectors, or all when no selectors).
    pub targets: Vec<ResolvedTarget>,
    /// Functions excluded from instrumentation (const fn, extern fn).
    pub skipped: Vec<SkippedFunction>,
    /// ALL instrumentable functions across the source tree, regardless of selectors.
    /// Used to build the pass-through set: functions that receive ctx but no guard.
    /// When selectors are empty, this equals the union of all targets.
    pub all_functions: Vec<ResolvedTarget>,
}

/// Resolve user-provided target specs against the source tree rooted at `src_dir`.
///
/// Returns one `ResolvedTarget` per file that contains at least one matching function.
/// When `specs` is empty, collects all functions from all files (instrument-everything mode).
/// When `specs` is non-empty, errors if no functions match any spec.
/// When `exact` is true, `Fn` patterns match bare or qualified names exactly
/// instead of by substring.
pub fn resolve_targets(
    src_dir: &Path,
    specs: &[TargetSpec],
    exact: bool,
) -> Result<ResolveResult, Error> {
    let rs_files = walk_rs_files(src_dir)?;

    // Compute a relative path like "src/main.rs" from an absolute file path.
    let project_root = src_dir.parent().unwrap_or(src_dir);
    let rel_path = |file: &Path| -> PathBuf {
        file.strip_prefix(project_root)
            .unwrap_or(file)
            .to_path_buf()
    };

    let mut results: Vec<ResolvedTarget> = Vec::new();
    let mut skipped: Vec<SkippedFunction> = Vec::new();
    // Collect all function names seen during resolution for suggestion hints,
    // avoiding a redundant re-parse in build_suggestion_hint.
    let mut all_seen_names: Vec<String> = Vec::new();

    // Always collect ALL instrumentable functions for pass-through support.
    // When selectors are active, non-selected functions become pass-through:
    // they receive ctx but no guard (zero profiling overhead for pass-through).
    let mut all_functions: Vec<ResolvedTarget> = Vec::new();
    let mut all_skipped: Vec<SkippedFunction> = Vec::new();
    for file in &rs_files {
        let source = std::fs::read_to_string(file).map_err(|source| Error::RunReadError {
            path: file.clone(),
            source,
        })?;
        let (all_fns, file_skipped) = extract_functions(&source, rel_path(file));
        if !all_fns.is_empty() {
            merge_into(&mut all_functions, file, all_fns);
        }
        all_skipped.extend(file_skipped);
    }

    if specs.is_empty() {
        // No specs = instrument all functions in all files.
        // Reuse the already-collected all_functions and all_skipped.
        results = all_functions.clone();
        skipped = all_skipped;
    } else {
        for spec in specs {
            match spec {
                TargetSpec::Fn(pattern) => {
                    for file in &rs_files {
                        let source = std::fs::read_to_string(file).map_err(|source| {
                            Error::RunReadError {
                                path: file.clone(),
                                source,
                            }
                        })?;
                        let (all_fns, file_skipped) =
                            extract_functions(&source, rel_path(file));
                        all_seen_names.extend(all_fns.iter().map(|qf| qf.minimal.clone()));
                        let matched: Vec<crate::naming::QualifiedFunction> = all_fns
                            .into_iter()
                            .filter(|qf| {
                                let name = &qf.minimal;
                                // Match against the bare function name (after any Type:: prefix).
                                let bare = name.rsplit("::").next().unwrap_or(name);
                                if exact {
                                    bare == pattern.as_str() || name == pattern.as_str()
                                } else {
                                    bare.contains(pattern.as_str())
                                        || name.contains(pattern.as_str())
                                }
                            })
                            .collect();
                        if !matched.is_empty() {
                            merge_into(&mut results, file, matched);
                        }
                        let matched_skipped: Vec<SkippedFunction> = file_skipped
                            .into_iter()
                            .filter(|s| {
                                let bare = s.name.rsplit("::").next().unwrap_or(&s.name);
                                if exact {
                                    bare == pattern.as_str() || s.name == pattern.as_str()
                                } else {
                                    bare.contains(pattern.as_str())
                                        || s.name.contains(pattern.as_str())
                                }
                            })
                            .collect();
                        skipped.extend(matched_skipped);
                    }
                }
                TargetSpec::File(file_path) => {
                    // Find files whose path ends with the given relative path.
                    let matching_files: Vec<&PathBuf> =
                        rs_files.iter().filter(|f| f.ends_with(file_path)).collect();
                    for file in matching_files {
                        let source = std::fs::read_to_string(file).map_err(|source| {
                            Error::RunReadError {
                                path: file.clone(),
                                source,
                            }
                        })?;
                        let (all_fns, file_skipped) =
                            extract_functions(&source, rel_path(file));
                        all_seen_names.extend(all_fns.iter().map(|qf| qf.minimal.clone()));
                        if !all_fns.is_empty() {
                            merge_into(&mut results, file, all_fns);
                        }
                        skipped.extend(file_skipped);
                    }
                }
                TargetSpec::Mod(module_name) => {
                    // Look for files under a directory named `module_name` (e.g. walker/mod.rs,
                    // walker/sub.rs) or a file named `module_name.rs`.
                    for file in &rs_files {
                        let is_mod_file = file
                            .parent()
                            .and_then(|p| p.file_name())
                            .is_some_and(|dir| dir == module_name.as_str());
                        let is_named_file = file
                            .file_stem()
                            .is_some_and(|stem| stem == module_name.as_str());

                        if !is_mod_file && !is_named_file {
                            continue;
                        }

                        let source = std::fs::read_to_string(file).map_err(|source| {
                            Error::RunReadError {
                                path: file.clone(),
                                source,
                            }
                        })?;
                        let (all_fns, file_skipped) =
                            extract_functions(&source, rel_path(file));
                        all_seen_names.extend(all_fns.iter().map(|qf| qf.minimal.clone()));
                        if !all_fns.is_empty() {
                            merge_into(&mut results, file, all_fns);
                        }
                        skipped.extend(file_skipped);
                    }
                }
            }
        }

        if results.is_empty() {
            let desc = specs
                .iter()
                .map(|s| match s {
                    TargetSpec::Fn(p) => format!("--fn {p}"),
                    TargetSpec::File(p) => format!("--file {}", p.display()),
                    TargetSpec::Mod(m) => format!("--mod {m}"),
                })
                .collect::<Vec<_>>()
                .join(", ");

            let hint = if skipped.is_empty() {
                build_suggestion_hint(specs, &all_seen_names)
            } else {
                let reasons = skipped
                    .iter()
                    .map(|s| s.reason.to_string())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    ". All {} matched function(s) were skipped ({}) -- piano cannot instrument these",
                    skipped.len(),
                    reasons
                )
            };
            return Err(Error::NoTargetsFound { specs: desc, hint });
        }
    }

    // Sort by file path for deterministic output.
    results.sort_by(|a, b| a.file.cmp(&b.file));
    for r in &mut results {
        r.functions.sort_by(|a, b| a.full.cmp(&b.full));
        r.functions.dedup_by(|a, b| a.full == b.full);
    }

    skipped.sort_by(|a, b| a.name.cmp(&b.name));
    skipped.dedup_by(|a, b| a.name == b.name);

    // Sort all_functions the same way as results.
    all_functions.sort_by(|a, b| a.file.cmp(&b.file));
    for r in &mut all_functions {
        r.functions.sort_by(|a, b| a.full.cmp(&b.full));
        r.functions.dedup_by(|a, b| a.full == b.full);
    }

    Ok(ResolveResult {
        targets: results,
        skipped,
        all_functions,
    })
}

/// Merge matched functions into the results vec, coalescing by file path.
fn merge_into(
    results: &mut Vec<ResolvedTarget>,
    file: &Path,
    functions: Vec<crate::naming::QualifiedFunction>,
) {
    if let Some(existing) = results.iter_mut().find(|r| r.file == file) {
        existing.functions.extend(functions);
    } else {
        results.push(ResolvedTarget {
            file: file.to_path_buf(),
            functions,
        });
    }
}

/// Recursively find all `.rs` files under `dir`.
fn walk_rs_files(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut files = Vec::new();
    walk_rs_files_inner(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn walk_rs_files_inner(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Error> {
    let entries = std::fs::read_dir(dir).map_err(io_context("read directory", dir))?;
    for entry in entries {
        let entry = entry.map_err(io_context("read directory entry", dir))?;
        let path = entry.path();
        if path.is_dir() {
            walk_rs_files_inner(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Parse a Rust source file and extract all function names.
///
/// Top-level functions are returned as bare names.
/// Impl methods are returned as "Type::method".
/// Default trait methods are returned as "Trait::method".
///
/// Functions annotated with `#[test]` and items inside `#[cfg(test)]` modules
/// are excluded -- they are not useful instrumentation targets.
pub(crate) fn extract_functions(
    source: &str,
    rel_path: PathBuf,
) -> (Vec<crate::naming::QualifiedFunction>, Vec<SkippedFunction>) {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);

    // ra_ap_syntax does error recovery, so we always get a tree.
    // If there are errors, warn but continue (like syn did -- we just get
    // fewer functions from broken regions).
    if !parse.errors().is_empty() {
        eprintln!(
            "warning: parse errors in {} ({} errors, continuing with recovered tree)",
            rel_path.display(),
            parse.errors().len()
        );
    }

    let file = parse.tree();
    let mut collector = FnCollector {
        functions: Vec::new(),
        skipped: Vec::new(),
        path: rel_path,
        current_impl: None,
        current_trait: None,
        scope: crate::naming::ScopeState::new(),
    };
    collector.walk_source_file(&file);
    (collector.functions, collector.skipped)
}

/// Check whether a CST node's attributes contain a specific simple attribute (e.g. `#[test]`).
fn has_attr_cst(attrs: impl Iterator<Item = ast::Attr>, name: &str) -> bool {
    attrs.into_iter().any(|a| {
        // Simple path attribute: `#[test]` has a single path segment.
        a.path()
            .and_then(|p| p.segment())
            .and_then(|seg| seg.name_ref())
            .is_some_and(|n| n.text() == name)
    })
}

/// Check whether a CST node's attributes contain `#[cfg(test)]`.
fn has_cfg_test_cst(attrs: impl Iterator<Item = ast::Attr>) -> bool {
    attrs.into_iter().any(|a| {
        let path_is_cfg = a
            .path()
            .and_then(|p| p.segment())
            .and_then(|seg| seg.name_ref())
            .is_some_and(|n| n.text() == "cfg");
        if !path_is_cfg {
            return false;
        }
        // Check if the token tree content is just "test".
        // The token tree text is "(test)" -- strip parens and check.
        a.token_tree()
            .is_some_and(|tt| {
                let text = tt.syntax().text().to_string();
                let inner = text.trim_start_matches('(').trim_end_matches(')').trim();
                inner == "test"
            })
    })
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

/// Classify whether a function is safe to instrument based on primitive properties.
///
/// Takes parser-agnostic bools and an optional ABI string so that both the
/// resolver and the rewriter call the same implementation. One function, one proof.
///
/// `abi` semantics: `None` means no explicit ABI (default Rust ABI, instrumentable).
/// `Some("Rust")` is also instrumentable. Any other value (including `Some("")`
/// for bare `extern fn`) causes a skip.
///
/// Defense against the "wrong function filtering" bug class:
/// - This function: signature modifiers via is_const, abi
/// - Return type: `returns_future()` in the strategy layer (rewrite/mod.rs)
/// - Attributes: caller-level checks (#[test]) + structural coverage
/// - Syntactic context: item walking (FnCollector/InjectionCollector)
///
/// unsafe fn is NOT skipped -- it receives ctx parameter and guard like any
/// other instrumentable function.
///
/// Built-in attribute audit (Rust 1.88, 26 attrs on functions):
///   Safe: cfg, cfg_attr, ignore, should_panic, allow, expect, warn, deny,
///     forbid, deprecated, must_use, inline, cold, track_caller,
///     instruction_set, no_mangle, export_name, link_section, doc
///   Policy skip: test (handled by caller)
///   Caught by signature: naked (extern), target_feature (safe to instrument)
///   Unreachable: proc_macro, proc_macro_derive, proc_macro_attribute,
///     panic_handler (no_std only)
pub(crate) fn classify(is_const: bool, abi: Option<&str>) -> Classification {
    if is_const {
        return Classification::Skip(SkipReason::Const);
    }
    if let Some(abi_str) = abi {
        if abi_str != "Rust" {
            return Classification::Skip(SkipReason::ExternAbi);
        }
    }
    Classification::Instrumentable
}

/// Classify a CST function node using primitive properties extracted from the CST.
fn classify_cst_fn(func: &ast::Fn) -> Classification {
    let cst_abi = extract_cst_abi(func);
    classify(func.const_token().is_some(), cst_abi.as_deref())
}

/// CST-based collector that walks module items to extract function names.
struct FnCollector {
    functions: Vec<crate::naming::QualifiedFunction>,
    skipped: Vec<SkippedFunction>,
    /// File path relative to the project root (e.g. "src/core.rs").
    path: PathBuf,
    /// When inside an `impl` block, holds the type name (e.g. "Resolver").
    current_impl: Option<String>,
    /// When inside a `trait` block, holds the trait name.
    current_trait: Option<String>,
    /// Scope tracking (mod, fn, block).
    scope: crate::naming::ScopeState,
}

impl FnCollector {
    fn walk_source_file(&mut self, file: &ast::SourceFile) {
        for item in file.items() {
            self.walk_item(&item);
        }
    }

    fn walk_item(&mut self, item: &ast::Item) {
        match item {
            ast::Item::Module(module) => self.visit_module(module),
            ast::Item::Fn(func) => self.visit_top_level_fn(func),
            ast::Item::Impl(imp) => self.visit_impl(imp),
            ast::Item::Trait(tr) => self.visit_trait(tr),
            ast::Item::MacroRules(mac) => self.visit_macro_rules(mac),
            _ => {}
        }
    }

    fn walk_item_list(&mut self, item_list: &ast::ItemList) {
        for item in item_list.items() {
            self.walk_item(&item);
        }
    }

    fn visit_module(&mut self, module: &ast::Module) {
        if has_cfg_test_cst(module.attrs()) {
            return; // skip entire #[cfg(test)] module
        }
        let mod_name = module
            .name()
            .map(|n| n.text().to_string())
            .unwrap_or_else(|| "_".to_string());
        self.scope.push_mod(&mod_name);
        if let Some(item_list) = module.item_list() {
            self.walk_item_list(&item_list);
        }
        self.scope.pop();
    }

    fn visit_top_level_fn(&mut self, func: &ast::Fn) {
        if !has_attr_cst(func.attrs(), "test") {
            let name = func
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_default();
            self.record_function(func, &name);
        }
        // Push fn scope for nested items, then walk the body for nested fns.
        let fn_name = func
            .name()
            .map(|n| n.text().to_string())
            .unwrap_or_default();
        self.scope.push_fn(&fn_name);
        if let Some(body) = func.body() {
            self.walk_fn_body(&body);
        }
        self.scope.pop();
    }

    fn visit_impl(&mut self, imp: &ast::Impl) {
        let self_ty = imp.self_ty();
        let trait_ty = imp.trait_();
        let impl_name = crate::naming::render_impl_name(
            self_ty.as_ref().expect("impl should have self type"),
            trait_ty.as_ref(),
        );
        let prev = self.current_impl.replace(impl_name);
        if let Some(assoc_list) = imp.assoc_item_list() {
            for assoc in assoc_list.assoc_items() {
                if let ast::AssocItem::Fn(func) = assoc {
                    self.visit_impl_fn(&func);
                }
            }
        }
        self.current_impl = prev;
    }

    fn visit_impl_fn(&mut self, func: &ast::Fn) {
        if !has_attr_cst(func.attrs(), "test") {
            let method_name = func
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_default();
            let impl_qualified = if let Some(ref impl_name) = self.current_impl {
                format!("{impl_name}::{method_name}")
            } else {
                method_name
            };
            self.record_function(func, &impl_qualified);
        }
        // Push fn scope for nested items.
        let fn_name = func
            .name()
            .map(|n| n.text().to_string())
            .unwrap_or_default();
        self.scope.push_fn(&fn_name);
        if let Some(body) = func.body() {
            self.walk_fn_body(&body);
        }
        self.scope.pop();
    }

    fn visit_trait(&mut self, tr: &ast::Trait) {
        let trait_name = tr
            .name()
            .map(|n| n.text().to_string())
            .unwrap_or_else(|| "_".to_string());
        let prev = self.current_trait.replace(trait_name);
        if let Some(assoc_list) = tr.assoc_item_list() {
            for assoc in assoc_list.assoc_items() {
                if let ast::AssocItem::Fn(func) = assoc {
                    self.visit_trait_fn(&func);
                }
            }
        }
        self.current_trait = prev;
    }

    fn visit_trait_fn(&mut self, func: &ast::Fn) {
        // Only collect trait fns with default bodies.
        if func.body().is_some() {
            let method_name = func
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_default();
            let trait_qualified = if let Some(ref trait_name) = self.current_trait {
                format!("{trait_name}::{method_name}")
            } else {
                method_name
            };
            self.record_function(func, &trait_qualified);
            // Push fn scope for nested items in the default body.
            let fn_name = func
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_default();
            self.scope.push_fn(&fn_name);
            if let Some(body) = func.body() {
                self.walk_fn_body(&body);
            }
            self.scope.pop();
        }
    }

    /// Walk token trees inside a macro_rules! body to find literal fn items.
    ///
    /// ra_ap_syntax does not parse macro bodies into AST nodes. The body is a
    /// flat token stream. We scan for the pattern `fn IDENT` and record each
    /// as an instrumentable function (unless preceded by `const`).
    fn visit_macro_rules(&mut self, mac: &ast::MacroRules) {
        let Some(body) = mac.token_tree() else { return };

        let tokens: Vec<_> = body
            .syntax()
            .descendants_with_tokens()
            .filter_map(|e| e.into_token())
            .collect();

        let mut i = 0;
        while i < tokens.len() {
            if tokens[i].kind() != SyntaxKind::FN_KW {
                i += 1;
                continue;
            }

            // Skip const fn
            if i >= 2
                && tokens[i - 1].kind() == SyntaxKind::WHITESPACE
                && tokens[i - 2].kind() == SyntaxKind::CONST_KW
            {
                i += 1;
                continue;
            }
            if i > 0 && tokens[i - 1].kind() == SyntaxKind::CONST_KW {
                i += 1;
                continue;
            }

            // Find next IDENT (function name), skipping whitespace
            let mut j = i + 1;
            while j < tokens.len() && tokens[j].kind() == SyntaxKind::WHITESPACE {
                j += 1;
            }
            if j < tokens.len() && tokens[j].kind() == SyntaxKind::IDENT {
                let fn_name = tokens[j].text().to_string();
                let minimal = self.scope.render_minimal(&fn_name);
                let medium = self.scope.render_medium(&fn_name);
                let full = self.scope.render_full(&fn_name);
                self.functions.push(crate::naming::QualifiedFunction::new(
                    &minimal, &medium, &full,
                ));
            }

            i = j + 1;
        }
    }

    /// Walk descendants of a function body to find nested items
    /// (inner fns, impls, traits, modules defined inside function bodies).
    fn walk_fn_body(&mut self, body: &ast::BlockExpr) {
        // Walk direct statement-level items in the block.
        // We need to handle nested items like:
        //   fn outer() {
        //     struct S;
        //     impl S { fn m() {} }
        //     fn inner() {}
        //   }
        // Use descendants to find nested items at any depth within the block.
        for node in body.syntax().descendants() {
            if node.kind() == SyntaxKind::FN {
                // Skip the function itself (body's parent fn is already handled).
                if node.text_range() == body.syntax().text_range() {
                    continue;
                }
                if let Some(inner_fn) = ast::Fn::cast(node.clone()) {
                    // Only process if the parent is NOT another FN we already handle
                    // via recursive visit. Check if this fn is a direct item
                    // (not inside another nested fn's body that would be visited
                    // by its own walk_fn_body call).
                    // The ancestor walking approach: find the enclosing context.
                    let context = find_ancestor_impl_context(&node);
                    self.visit_nested_fn(&inner_fn, context.as_deref());
                }
            }
        }
    }

    /// Process a nested function found inside another function's body.
    fn visit_nested_fn(&mut self, func: &ast::Fn, impl_context: Option<&str>) {
        if !has_attr_cst(func.attrs(), "test") {
            let name = func
                .name()
                .map(|n| n.text().to_string())
                .unwrap_or_default();
            let fn_qualified = match impl_context {
                Some(prefix) => format!("{prefix}::{name}"),
                None => name,
            };
            self.record_function(func, &fn_qualified);
        }
    }

    /// Classify a function and record it as instrumentable or skipped.
    /// Single site for classification -> push, so adding a Classification
    /// variant or SkippedFunction field can't silently break one of the callers.
    fn record_function(&mut self, func: &ast::Fn, qualified: &str) {
        let minimal = self.scope.render_minimal(qualified);
        match classify_cst_fn(func) {
            Classification::Skip(reason) => {
                self.skipped.push(SkippedFunction {
                    name: minimal,
                    reason,
                    path: self.path.clone(),
                });
            }
            Classification::Instrumentable => {
                let medium = self.scope.render_medium(qualified);
                let full = self.scope.render_full(qualified);
                self.functions.push(crate::naming::QualifiedFunction::new(
                    &minimal, &medium, &full,
                ));
            }
        }
    }
}

/// Walk up the syntax tree from a FN node to find the nearest enclosing `impl`
/// block and extract its qualified type name (e.g. "Local" or "<Local as Trait>").
fn find_ancestor_impl_context(fn_node: &ra_ap_syntax::SyntaxNode) -> Option<String> {
    let mut current = fn_node.parent();
    while let Some(node) = current {
        if let Some(imp) = ast::Impl::cast(node.clone()) {
            let self_ty = imp.self_ty()?;
            let trait_ty = imp.trait_();
            return Some(crate::naming::render_impl_name(&self_ty, trait_ty.as_ref()));
        }
        current = node.parent();
    }
    None
}

/// Levenshtein edit distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let b_len = b.len();
    let mut row: Vec<usize> = (0..=b_len).collect();

    for (i, a_ch) in a.chars().enumerate() {
        let mut prev = i;
        row[0] = i + 1;
        for (j, b_ch) in b.chars().enumerate() {
            let cost = if a_ch == b_ch { prev } else { prev + 1 };
            prev = row[j + 1];
            row[j + 1] = cost.min(row[j] + 1).min(prev + 1);
        }
    }

    row[b_len]
}

/// Build a recovery hint for the NoTargetsFound error.
///
/// For `--fn` specs: computes Levenshtein suggestions against all function names.
/// Falls back to showing function count with guidance when no close matches exist.
/// Accepts pre-collected function names from the resolution pass to avoid re-parsing.
fn build_suggestion_hint(specs: &[TargetSpec], seen_names: &[String]) -> String {
    // Collect --fn patterns only; other spec types don't get suggestions.
    let fn_patterns: Vec<&str> = specs
        .iter()
        .filter_map(|s| match s {
            TargetSpec::Fn(p) => Some(p.as_str()),
            _ => None,
        })
        .collect();

    if fn_patterns.is_empty() {
        return String::new();
    }

    // Deduplicate the pre-collected names.
    let mut all_names: Vec<&String> = seen_names.iter().collect();
    all_names.sort();
    all_names.dedup();

    let total_count = all_names.len();

    // Find Levenshtein-close matches for each pattern.
    let mut suggestions: Vec<String> = Vec::new();
    for pattern in &fn_patterns {
        let threshold = pattern.len() / 3;
        let mut scored: Vec<(usize, &str)> = all_names
            .iter()
            .filter_map(|name| {
                let bare = name.rsplit("::").next().unwrap_or(name);
                let dist = levenshtein(pattern, bare).min(levenshtein(pattern, name));
                if dist <= threshold && dist > 0 {
                    Some((dist, name.as_str()))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by_key(|(d, _)| *d);
        suggestions.extend(scored.iter().take(5).map(|(_, name)| (*name).to_owned()));
    }
    suggestions.sort();
    suggestions.dedup();

    if !suggestions.is_empty() {
        format!(". Did you mean: {}?", suggestions.join(", "))
    } else {
        format!(". Found {total_count} functions, none matched. Run without --fn to instrument all")
    }
}

/// Derive a Rust module path prefix from a file path relative to `src/`.
///
/// - `main.rs` / `lib.rs` -> `""` (crate root)
/// - `mod.rs` -> parent directory components (e.g., `db/mod.rs` -> `"db"`)
/// - other -> parent components + file stem (e.g., `db/query.rs` -> `"db::query"`)
pub fn module_prefix(relative: &Path) -> String {
    let stem = relative.file_stem().and_then(|s| s.to_str()).unwrap_or("_");

    let parent_components: Vec<&str> = relative
        .parent()
        .map(|p| {
            p.components()
                .map(|c| c.as_os_str().to_str().unwrap_or("_"))
                .collect()
        })
        .unwrap_or_default();

    if stem == "main" || stem == "lib" || stem == "mod" {
        parent_components.join("::")
    } else {
        let mut parts = parent_components;
        parts.push(stem);
        parts.join("::")
    }
}

/// Qualify a function name with a module prefix. Returns the name unchanged
/// if the prefix is empty.
pub fn qualify(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}::{name}")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    /// Build the synthetic test project inside `dir/src/`.
    fn create_test_project(dir: &Path) {
        let src = dir.join("src");
        fs::create_dir_all(src.join("walker")).unwrap();

        fs::write(src.join("main.rs"), "fn main() { walk(); }\nfn walk() {}\n").unwrap();

        fs::write(
            src.join("resolver.rs"),
            "\
struct Resolver;
impl Resolver {
    pub fn resolve(&self) -> bool { true }
    fn internal_resolve(&self) {}
}
fn helper() {}
",
        )
        .unwrap();

        fs::write(
            src.join("walker").join("mod.rs"),
            "pub fn walk_dir() {}\nfn scan() {}\n",
        )
        .unwrap();

        fs::write(
            src.join("special_fns.rs"),
            "\
const fn fixed_size() -> usize { 42 }
unsafe fn dangerous() -> i32 { 0 }
extern \"C\" fn ffi_callback() {}
fn normal_fn() {}

struct Widget;
impl Widget {
    const fn none() -> Option<Self> { None }
    unsafe fn raw_ptr(&self) -> *const u8 { std::ptr::null() }
    fn valid_method(&self) {}
}

trait Processor {
    fn process(&self);
    fn default_method(&self) { }
    unsafe fn unsafe_default(&self) { }
    fn required_method(&self);
}
",
        )
        .unwrap();

        fs::write(
            src.join("with_tests.rs"),
            "\
fn production_fn() {}

#[test]
fn test_something() {}

#[cfg(test)]
mod tests {
    fn test_helper() {}

    #[test]
    fn it_works() {}
}
",
        )
        .unwrap();
    }

    #[test]
    fn resolve_fn_by_substring() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("walk".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(all_fns.contains(&"walk"), "should match exact 'walk'");
        assert!(
            all_fns.contains(&"walk_dir"),
            "should match 'walk_dir' (substring)"
        );
        assert!(!all_fns.contains(&"helper"), "should not match 'helper'");
        assert!(!all_fns.contains(&"scan"), "should not match 'scan'");
    }

    #[test]
    fn resolve_fn_finds_impl_methods() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("resolve".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(
            all_fns.contains(&"Resolver::resolve"),
            "should match impl method 'resolve'"
        );
        assert!(
            all_fns.contains(&"Resolver::internal_resolve"),
            "should match impl method 'internal_resolve'"
        );
    }

    #[test]
    fn resolve_file_gets_all_functions() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("resolver.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        assert_eq!(result.targets.len(), 1);
        let fns: Vec<&str> = result.targets[0].functions.iter().map(|qf| qf.minimal.as_str()).collect();
        assert!(fns.contains(&"helper"));
        assert!(fns.contains(&"Resolver::internal_resolve"));
        assert!(fns.contains(&"Resolver::resolve"));
    }

    #[test]
    fn resolve_mod_gets_directory_module() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Mod("walker".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        assert_eq!(result.targets.len(), 1);
        let fns: Vec<&str> = result.targets[0].functions.iter().map(|qf| qf.minimal.as_str()).collect();
        assert!(fns.contains(&"walk_dir"));
        assert!(fns.contains(&"scan"));
    }

    #[test]
    fn no_match_returns_error() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("nonexistent_xyz".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false);

        assert!(result.is_err(), "should error when no functions match");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent_xyz"),
            "error should mention the pattern: {err}"
        );
        assert!(
            err.contains("Found 14 functions"),
            "error should show function count: {err}"
        );
        assert!(
            err.contains("Run without --fn"),
            "error should suggest running without --fn: {err}"
        );
    }

    #[test]
    fn resolve_skips_test_functions_and_cfg_test_modules() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("with_tests.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(
            all_fns.contains(&"production_fn"),
            "should include production function"
        );
        assert!(
            !all_fns.contains(&"test_something"),
            "should skip #[test] function"
        );
        assert!(
            !all_fns.contains(&"test_helper"),
            "should skip function inside #[cfg(test)] module"
        );
        assert!(
            !all_fns.contains(&"it_works"),
            "should skip #[test] inside #[cfg(test)] module"
        );
    }

    #[test]
    fn resolve_skips_unparseable_files() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // Add a file that looks like .rs but isn't valid Rust (e.g. a Tera template).
        let src = tmp.path().join("src");
        fs::write(
            src.join("template.tera.rs"),
            "{% for variant in variants %}\nfn {{ variant }}() {}\n{% endfor %}\n",
        )
        .unwrap();

        // Should succeed despite the unparseable file.
        let specs = [TargetSpec::Fn("walk".into())];
        let result = resolve_targets(&src, &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(
            all_fns.contains(&"walk"),
            "should still find valid functions"
        );
        assert!(
            all_fns.contains(&"walk_dir"),
            "should still find valid functions"
        );
    }

    #[test]
    fn resolve_empty_specs_returns_all_functions() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs: Vec<TargetSpec> = vec![];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        // Should find functions from all files: main.rs, resolver.rs, walker/mod.rs, with_tests.rs
        assert!(all_fns.contains(&"main"), "should include main");
        assert!(
            all_fns.contains(&"walk"),
            "should include walk from main.rs"
        );
        assert!(
            all_fns.contains(&"helper"),
            "should include helper from resolver.rs"
        );
        assert!(
            all_fns.contains(&"Resolver::resolve"),
            "should include impl methods"
        );
        assert!(
            all_fns.contains(&"walk_dir"),
            "should include walk_dir from walker"
        );
        assert!(all_fns.contains(&"scan"), "should include scan from walker");
        assert!(
            all_fns.contains(&"production_fn"),
            "should include production_fn"
        );

        // Should still skip test functions
        assert!(!all_fns.contains(&"test_something"), "should skip #[test]");
        assert!(
            !all_fns.contains(&"it_works"),
            "should skip test in cfg(test)"
        );
    }

    #[test]
    fn resolve_skips_const_and_extern_functions() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("special_fns.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        // Should include normal functions
        assert!(all_fns.contains(&"normal_fn"), "should include normal_fn");
        assert!(
            all_fns.contains(&"Widget::valid_method"),
            "should include Widget::valid_method"
        );

        // unsafe fn IS instrumentable -- guard is pure safe code
        assert!(all_fns.contains(&"dangerous"), "should include unsafe fn");
        assert!(
            all_fns.contains(&"Widget::raw_ptr"),
            "should include unsafe impl method"
        );

        // Should skip const/extern
        assert!(!all_fns.contains(&"fixed_size"), "should skip const fn");
        assert!(!all_fns.contains(&"ffi_callback"), "should skip extern fn");
        assert!(
            !all_fns.contains(&"Widget::none"),
            "should skip const impl method"
        );
    }

    #[test]
    fn resolve_instruments_unsafe_trait_default_methods() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("special_fns.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        // Trait default methods should be included
        assert!(
            all_fns.contains(&"Processor::default_method"),
            "should include safe trait default method"
        );

        // unsafe trait default methods ARE instrumentable -- guard is pure safe code
        assert!(
            all_fns.contains(&"Processor::unsafe_default"),
            "should include unsafe trait default method"
        );

        // Trait methods without default bodies should not appear at all
        assert!(
            !all_fns.contains(&"Processor::process"),
            "should not include trait method without default body"
        );
        assert!(
            !all_fns.contains(&"Processor::required_method"),
            "should not include trait method without default body"
        );

        // unsafe_default should NOT be in the skipped list
        let skipped_names: Vec<&str> = result.skipped.iter().map(|s| s.name.as_str()).collect();
        assert!(
            !skipped_names.contains(&"Processor::unsafe_default"),
            "unsafe trait default method should not be skipped"
        );
    }

    #[test]
    fn no_match_error_includes_suggestions_for_typo() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "heper" is close to "helper" (distance 1, threshold = 5/3 = 1, so distance <= 1 passes).
        let specs = [TargetSpec::Fn("heper".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("helper"),
            "error should suggest 'helper' for typo 'heper': {err}"
        );
        assert!(
            err.contains("Did you mean"),
            "error should include 'Did you mean' phrasing: {err}"
        );
    }

    #[test]
    fn levenshtein_basic_cases() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("parse", "prase"), 2); // swap = 2 edits
        assert_eq!(levenshtein("walk", "wlak"), 2);
        assert_eq!(levenshtein("a", "b"), 1);
    }

    #[test]
    fn no_match_error_shows_count_for_large_project() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();

        // Create a file with 20 functions to verify the count fallback path.
        let mut code = String::new();
        for i in 0..20 {
            code.push_str(&format!("fn func_{i}() {{}}\n"));
        }
        fs::write(src.join("many.rs"), &code).unwrap();

        let specs = [TargetSpec::Fn("nonexistent".into())];
        let result = resolve_targets(&src, &specs, false);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Found 20 functions"),
            "error should show function count: {err}"
        );
        assert!(
            err.contains("Run without --fn"),
            "error should suggest running without --fn: {err}"
        );
    }

    #[test]
    fn no_match_error_shows_clean_patterns() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("zzz_nonexistent".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false);
        let err = result.unwrap_err().to_string();
        assert!(
            err.starts_with("no functions matched"),
            "error should start with 'no functions matched': {err}"
        );
        assert!(
            err.contains("--fn zzz_nonexistent"),
            "error should include the spec: {err}"
        );
    }

    #[test]
    fn resolve_fn_substring_matches_qualified_name() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "Resolver" appears in the qualified name "Resolver::resolve" but not in the bare
        // name "resolve". Substring matching should check both.
        let specs = [TargetSpec::Fn("Resolver".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(
            all_fns.contains(&"Resolver::resolve"),
            "should match 'Resolver::resolve' via qualified name substring"
        );
        assert!(
            all_fns.contains(&"Resolver::internal_resolve"),
            "should match 'Resolver::internal_resolve' via qualified name substring"
        );
        assert!(
            !all_fns.contains(&"helper"),
            "should not match unrelated 'helper'"
        );
    }

    #[test]
    fn resolve_fn_exact_match() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "walk" with exact=true should match only "walk", not "walk_dir".
        let specs = [TargetSpec::Fn("walk".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, true).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(all_fns.contains(&"walk"), "should match exact 'walk'");
        assert!(
            !all_fns.contains(&"walk_dir"),
            "should NOT match 'walk_dir' in exact mode"
        );
    }

    #[test]
    fn resolve_fn_exact_match_qualified() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // Exact match should also work with qualified names like "Resolver::resolve".
        let specs = [TargetSpec::Fn("Resolver::resolve".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, true).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();

        assert!(
            all_fns.contains(&"Resolver::resolve"),
            "should match qualified 'Resolver::resolve'"
        );
        assert!(
            !all_fns.contains(&"Resolver::internal_resolve"),
            "should NOT match 'Resolver::internal_resolve' in exact mode"
        );
    }

    #[test]
    fn resolve_fn_exact_no_match_shows_error() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "wal" is a substring of "walk" but not an exact match.
        let specs = [TargetSpec::Fn("wal".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, true);

        assert!(result.is_err(), "partial match should fail in exact mode");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no functions matched"),
            "error should say no functions matched: {err}"
        );
    }

    #[test]
    fn resolve_finds_unsafe_fn_by_pattern() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "dangerous" matches the unsafe fn "dangerous" -- unsafe fn is instrumentable.
        let specs = [TargetSpec::Fn("dangerous".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();
        assert!(
            all_fns.contains(&"dangerous"),
            "should find and instrument unsafe fn 'dangerous': {all_fns:?}"
        );
    }

    #[test]
    fn resolve_reports_skipped_functions_with_reasons() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("special_fns.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let skipped_names: Vec<(&str, &SkipReason)> = result
            .skipped
            .iter()
            .map(|s| (s.name.as_str(), &s.reason))
            .collect();

        assert!(
            skipped_names.contains(&("fixed_size", &SkipReason::Const)),
            "should report const fn as skipped: {skipped_names:?}"
        );
        assert!(
            skipped_names.contains(&("ffi_callback", &SkipReason::ExternAbi)),
            "should report extern fn as skipped: {skipped_names:?}"
        );
        assert!(
            skipped_names.contains(&("Widget::none", &SkipReason::Const)),
            "should report const impl method as skipped: {skipped_names:?}"
        );

        // unsafe fns should NOT be in the skipped list -- guard is pure safe code
        assert!(
            !skipped_names.iter().any(|(name, _)| *name == "dangerous"),
            "unsafe fn should not be skipped: {skipped_names:?}"
        );
        assert!(
            !skipped_names.iter().any(|(name, _)| *name == "Widget::raw_ptr"),
            "unsafe impl method should not be skipped: {skipped_names:?}"
        );

        // All skipped functions from this file should have the correct relative path.
        for s in &result.skipped {
            assert_eq!(
                s.path,
                Path::new("src/special_fns.rs"),
                "skipped fn '{}' should have relative path src/special_fns.rs",
                s.name
            );
        }

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();
        assert!(all_fns.contains(&"normal_fn"));
        assert!(all_fns.contains(&"Widget::valid_method"));
        // unsafe fns should be in the instrumented list
        assert!(all_fns.contains(&"dangerous"));
        assert!(all_fns.contains(&"Widget::raw_ptr"));
    }

    #[test]
    fn hint_empty_when_only_file_and_mod_specs() {
        // Only --file and --mod specs, no --fn specs.
        let specs = [
            TargetSpec::File("lib.rs".into()),
            TargetSpec::Mod("mymod".into()),
        ];
        let hint = build_suggestion_hint(&specs, &[]);
        assert!(
            hint.is_empty(),
            "hint should be empty when no --fn specs are present, got: {hint:?}"
        );
    }

    #[test]
    fn module_prefix_crate_roots() {
        assert_eq!(module_prefix(Path::new("main.rs")), "");
        assert_eq!(module_prefix(Path::new("lib.rs")), "");
    }

    #[test]
    fn module_prefix_simple_files() {
        assert_eq!(module_prefix(Path::new("db.rs")), "db");
        assert_eq!(module_prefix(Path::new("utils.rs")), "utils");
    }

    #[test]
    fn module_prefix_mod_rs() {
        assert_eq!(module_prefix(Path::new("db/mod.rs")), "db");
        assert_eq!(
            module_prefix(Path::new("api/handlers/mod.rs")),
            "api::handlers"
        );
    }

    #[test]
    fn module_prefix_nested_files() {
        assert_eq!(module_prefix(Path::new("db/query.rs")), "db::query");
        assert_eq!(
            module_prefix(Path::new("api/handlers/user.rs")),
            "api::handlers::user"
        );
    }

    #[cfg(unix)]
    #[test]
    fn module_prefix_non_utf8_fallback() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        // 0x80 is not valid UTF-8; build a path like "<invalid>/valid.rs"
        let bad_dir = OsStr::from_bytes(b"\x80");
        let mut path = std::path::PathBuf::from(bad_dir);
        path.push("query.rs");
        assert_eq!(module_prefix(&path), "_::query");

        // Non-UTF-8 file stem: "valid/<invalid>.rs"
        let bad_stem = OsStr::from_bytes(b"\x80.rs");
        let path2 = Path::new("api").join(bad_stem);
        assert_eq!(module_prefix(&path2), "api::_");
    }

    #[test]
    fn qualify_with_empty_prefix() {
        assert_eq!(qualify("", "walk"), "walk");
        assert_eq!(qualify("", "Walker::walk"), "Walker::walk");
    }

    #[test]
    fn qualify_with_module_prefix() {
        assert_eq!(qualify("db", "execute"), "db::execute");
        assert_eq!(
            qualify("db::query", "validate_input"),
            "db::query::validate_input"
        );
        assert_eq!(
            qualify("api", "Handler::validate_input"),
            "api::Handler::validate_input"
        );
    }

    #[test]
    fn trait_impl_methods_get_disambiguated_names() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(
            src.join("main.rs"),
            r#"
use std::fmt;

struct Point;

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "point")
    }
}

impl fmt::Debug for Point {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "point")
    }
}

fn main() {}
"#,
        )
        .unwrap();

        let result = resolve_targets(&src, &[], false).unwrap();
        let names: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|t| t.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();
        assert!(
            names.contains(&"<Point as Display>::fmt"),
            "should have Display-qualified name: {names:?}"
        );
        assert!(
            names.contains(&"<Point as Debug>::fmt"),
            "should have Debug-qualified name: {names:?}"
        );
        // There should be two separate entries, not one merged "Point::fmt"
        let fmt_count = names.iter().filter(|n| n.ends_with("fmt")).count();
        assert_eq!(
            fmt_count, 2,
            "should have 2 distinct fmt entries: {names:?}"
        );
    }

    #[test]
    fn classify_returns_explicit_classification() {
        // Normal fn -> Instrumentable
        assert!(matches!(
            classify(false, None),
            Classification::Instrumentable
        ));

        // const fn -> Skip(Const)
        assert!(matches!(
            classify(true, None),
            Classification::Skip(SkipReason::Const)
        ));

        // extern "C" fn -> Skip(ExternAbi)
        assert!(matches!(
            classify(false, Some("C")),
            Classification::Skip(SkipReason::ExternAbi)
        ));

        // extern fn (bare extern = empty ABI) -> Skip(ExternAbi)
        assert!(matches!(
            classify(false, Some("")),
            Classification::Skip(SkipReason::ExternAbi)
        ));

        // extern "Rust" fn -> Instrumentable (explicit Rust ABI is fine)
        assert!(matches!(
            classify(false, Some("Rust")),
            Classification::Instrumentable
        ));

        // async fn -> Instrumentable (async affects strategy, not filtering)
        // (asyncness is not a classify parameter -- it is a strategy concern)
        assert!(matches!(
            classify(false, None),
            Classification::Instrumentable
        ));
    }

    #[test]
    fn classify_cst_fn_delegates_to_classify() {
        /// Parse a function signature string into a CST `ast::Fn` node.
        fn parse_fn(sig: &str) -> ast::Fn {
            let source = format!("{sig} {{}}");
            let parse = ast::SourceFile::parse(&source, Edition::Edition2024);
            let file = parse.tree();
            file.syntax()
                .descendants()
                .find_map(ast::Fn::cast)
                .expect("should parse fn")
        }

        // Verify classify_cst_fn extracts properties from CST ast::Fn
        // and delegates to the unified classify().
        let func = parse_fn("fn foo()");
        assert!(matches!(classify_cst_fn(&func), Classification::Instrumentable));

        let func = parse_fn("const fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Skip(SkipReason::Const)
        ));

        // unsafe fn -> Instrumentable (guard is pure safe code)
        let func = parse_fn("unsafe fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Instrumentable
        ));

        let func = parse_fn("extern \"C\" fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Skip(SkipReason::ExternAbi)
        ));

        let func = parse_fn("extern fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Skip(SkipReason::ExternAbi)
        ));

        let func = parse_fn("extern \"Rust\" fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Instrumentable
        ));

        let func = parse_fn("async fn foo()");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Instrumentable
        ));

        let func = parse_fn("fn foo<T: Clone>(x: T) -> T");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Instrumentable
        ));

        let func = parse_fn("fn foo() -> impl std::future::Future<Output = i32>");
        assert!(matches!(
            classify_cst_fn(&func),
            Classification::Instrumentable
        ));
    }

    // --- Pass-through / all_functions tests ---

    /// resolve_targets with selectors returns all_functions containing every
    /// instrumentable function, while targets contains only the selected subset.
    #[test]
    fn all_functions_includes_unselected() {
        let dir = TempDir::new().unwrap();
        create_test_project(dir.path());
        let src = dir.path().join("src");

        // Use --fn to select only "walk" (in walker.rs).
        let specs = vec![TargetSpec::Fn("walk".to_string())];
        let result = resolve_targets(&src, &specs, false).unwrap();

        // targets should contain only matched functions.
        let measured_names: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|t| t.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();
        assert!(
            measured_names.iter().all(|n| n.contains("walk")),
            "targets should only contain walk-matching functions. Got: {measured_names:?}"
        );

        // all_functions should contain functions from all files, not just matched ones.
        let all_names: Vec<&str> = result
            .all_functions
            .iter()
            .flat_map(|t| t.functions.iter().map(|qf| qf.minimal.as_str()))
            .collect();
        assert!(
            all_names.len() >= measured_names.len(),
            "all_functions should be >= targets. all={}, targets={}",
            all_names.len(),
            measured_names.len()
        );

        // all_functions should include functions not in targets.
        let has_non_walk = all_names.iter().any(|n| !n.contains("walk"));
        assert!(
            has_non_walk,
            "all_functions should include non-walk functions for pass-through. Got: {all_names:?}"
        );
    }

    /// When specs are empty (default), all_functions equals targets.
    #[test]
    fn all_functions_equals_targets_when_no_specs() {
        let dir = TempDir::new().unwrap();
        create_test_project(dir.path());
        let src = dir.path().join("src");

        let specs: Vec<TargetSpec> = Vec::new();
        let result = resolve_targets(&src, &specs, false).unwrap();

        let target_names: std::collections::BTreeSet<String> = result
            .targets
            .iter()
            .flat_map(|t| t.functions.iter().map(|qf| qf.minimal.clone()))
            .collect();
        let all_names: std::collections::BTreeSet<String> = result
            .all_functions
            .iter()
            .flat_map(|t| t.functions.iter().map(|qf| qf.minimal.clone()))
            .collect();
        assert_eq!(
            target_names, all_names,
            "when no selectors, all_functions should equal targets"
        );
    }
}
