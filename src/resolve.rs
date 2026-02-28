use std::path::{Path, PathBuf};

use syn::visit::Visit;

use crate::error::Error;

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
    pub functions: Vec<String>,
}

/// Why a function was excluded from instrumentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    Unsafe,
    Const,
    ExternAbi,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::Unsafe => write!(f, "unsafe"),
            SkipReason::Const => write!(f, "const"),
            SkipReason::ExternAbi => write!(f, "extern"),
        }
    }
}

/// A function that was in the target set but excluded from instrumentation.
#[derive(Debug, Clone)]
pub struct SkippedFunction {
    pub name: String,
    pub reason: SkipReason,
}

/// Result of target resolution: resolved targets plus any skipped functions.
#[derive(Debug)]
pub struct ResolveResult {
    pub targets: Vec<ResolvedTarget>,
    pub skipped: Vec<SkippedFunction>,
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

    let mut results: Vec<ResolvedTarget> = Vec::new();
    let mut skipped: Vec<SkippedFunction> = Vec::new();

    if specs.is_empty() {
        // No specs = instrument all functions in all files.
        for file in &rs_files {
            let source = std::fs::read_to_string(file).map_err(|source| Error::RunReadError {
                path: file.clone(),
                source,
            })?;
            let (all_fns, file_skipped) = extract_functions(&source, file);
            if !all_fns.is_empty() {
                merge_into(&mut results, file, all_fns);
            }
            skipped.extend(file_skipped);
        }
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
                        let (all_fns, file_skipped) = extract_functions(&source, file);
                        let matched: Vec<String> = all_fns
                            .into_iter()
                            .filter(|name| {
                                // Match against the bare function name (after any Type:: prefix).
                                let bare = name.rsplit("::").next().unwrap_or(name);
                                if exact {
                                    bare == pattern.as_str() || name == pattern.as_str()
                                } else {
                                    bare.contains(pattern.as_str())
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
                        let (all_fns, file_skipped) = extract_functions(&source, file);
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
                        let (all_fns, file_skipped) = extract_functions(&source, file);
                        if !all_fns.is_empty() {
                            merge_into(&mut results, file, all_fns);
                        }
                        skipped.extend(file_skipped);
                    }
                }
            }
        }

        if results.is_empty() && skipped.is_empty() {
            let desc = specs
                .iter()
                .map(|s| match s {
                    TargetSpec::Fn(p) => format!("--fn {p}"),
                    TargetSpec::File(p) => format!("--file {}", p.display()),
                    TargetSpec::Mod(m) => format!("--mod {m}"),
                })
                .collect::<Vec<_>>()
                .join(", ");

            let hint = build_suggestion_hint(specs, &rs_files);
            return Err(Error::NoTargetsFound { specs: desc, hint });
        }
    }

    // Sort by file path for deterministic output.
    results.sort_by(|a, b| a.file.cmp(&b.file));
    for r in &mut results {
        r.functions.sort();
        r.functions.dedup();
    }

    skipped.sort_by(|a, b| a.name.cmp(&b.name));
    skipped.dedup_by(|a, b| a.name == b.name);

    Ok(ResolveResult {
        targets: results,
        skipped,
    })
}

/// Merge matched functions into the results vec, coalescing by file path.
fn merge_into(results: &mut Vec<ResolvedTarget>, file: &Path, functions: Vec<String>) {
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
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
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
fn extract_functions(source: &str, path: &Path) -> (Vec<String>, Vec<SkippedFunction>) {
    let syntax = match syn::parse_file(source) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("warning: skipping {}: {e}", path.display());
            return (Vec::new(), Vec::new());
        }
    };

    let mut collector = FnCollector::default();
    collector.visit_file(&syntax);
    (collector.functions, collector.skipped)
}

/// Check whether an attribute list contains a specific simple attribute (e.g. `#[test]`).
fn has_attr(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

/// Check whether an attribute list contains `#[cfg(test)]`.
fn has_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        if !a.path().is_ident("cfg") {
            return false;
        }
        a.parse_args::<syn::Ident>()
            .map(|id| id == "test")
            .unwrap_or(false)
    })
}

/// Returns false for function signatures that cannot be instrumented:
/// const fn (enter() is not const), unsafe fn, extern fn (non-Rust ABI).
pub(crate) fn is_instrumentable(sig: &syn::Signature) -> bool {
    classify_skip(sig).is_none()
}

/// Classify why a function signature cannot be instrumented.
/// Returns None if the function is instrumentable.
pub(crate) fn classify_skip(sig: &syn::Signature) -> Option<SkipReason> {
    if sig.unsafety.is_some() {
        return Some(SkipReason::Unsafe);
    }
    if sig.constness.is_some() {
        return Some(SkipReason::Const);
    }
    if let Some(abi) = &sig.abi {
        let is_rust_abi = abi.name.as_ref().is_some_and(|name| name.value() == "Rust");
        if !is_rust_abi {
            return Some(SkipReason::ExternAbi);
        }
    }
    None
}

/// AST visitor that collects function names from a parsed Rust file.
#[derive(Default)]
struct FnCollector {
    functions: Vec<String>,
    skipped: Vec<SkippedFunction>,
    /// When inside an `impl` block, holds the type name (e.g. "Resolver").
    current_impl: Option<String>,
    /// When inside a `trait` block, holds the trait name.
    current_trait: Option<String>,
}

impl<'ast> Visit<'ast> for FnCollector {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if has_cfg_test(&node.attrs) {
            return; // skip entire #[cfg(test)] module
        }
        syn::visit::visit_item_mod(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !has_attr(&node.attrs, "test") {
            if let Some(reason) = classify_skip(&node.sig) {
                self.skipped.push(SkippedFunction {
                    name: node.sig.ident.to_string(),
                    reason,
                });
            } else {
                self.functions.push(node.sig.ident.to_string());
            }
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let type_name = type_name_from_type(&node.self_ty);
        let prev = self.current_impl.replace(type_name);
        syn::visit::visit_item_impl(self, node);
        self.current_impl = prev;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if !has_attr(&node.attrs, "test") {
            let method_name = node.sig.ident.to_string();
            let qualified = if let Some(ref impl_name) = self.current_impl {
                format!("{impl_name}::{method_name}")
            } else {
                method_name
            };
            if let Some(reason) = classify_skip(&node.sig) {
                self.skipped.push(SkippedFunction {
                    name: qualified,
                    reason,
                });
            } else {
                self.functions.push(qualified);
            }
        }
        syn::visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        let trait_name = node.ident.to_string();
        let prev = self.current_trait.replace(trait_name);
        syn::visit::visit_item_trait(self, node);
        self.current_trait = prev;
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if node.default.is_some() {
            let method_name = node.sig.ident.to_string();
            let qualified = if let Some(ref trait_name) = self.current_trait {
                format!("{trait_name}::{method_name}")
            } else {
                method_name
            };
            if let Some(reason) = classify_skip(&node.sig) {
                self.skipped.push(SkippedFunction {
                    name: qualified,
                    reason,
                });
            } else {
                self.functions.push(qualified);
            }
        }
        syn::visit::visit_trait_item_fn(self, node);
    }
}

/// Extract a human-readable type name from a `syn::Type` (best-effort).
fn type_name_from_type(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|seg| seg.ident.to_string())
            .unwrap_or_else(|| "_".to_string()),
        _ => "_".to_string(),
    }
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
fn build_suggestion_hint(specs: &[TargetSpec], rs_files: &[PathBuf]) -> String {
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

    // Collect all instrumentable function names across all files.
    let mut all_names: Vec<String> = Vec::new();
    for file in rs_files {
        let Ok(source) = std::fs::read_to_string(file) else {
            continue;
        };
        let (fns, _skipped) = extract_functions(&source, file);
        all_names.extend(fns);
    }
    all_names.sort();
    all_names.dedup();

    let total_count = all_names.len();

    // Find Levenshtein-close matches for each pattern.
    let mut suggestions: Vec<String> = Vec::new();
    for pattern in &fn_patterns {
        let threshold = pattern.len() / 3;
        let mut scored: Vec<(usize, &String)> = all_names
            .iter()
            .filter_map(|name| {
                let bare = name.rsplit("::").next().unwrap_or(name);
                let dist = levenshtein(pattern, bare).min(levenshtein(pattern, name));
                if dist <= threshold && dist > 0 {
                    Some((dist, name))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by_key(|(d, _)| *d);
        suggestions.extend(scored.iter().take(5).map(|(_, name)| (*name).clone()));
    }
    suggestions.sort();
    suggestions.dedup();

    if !suggestions.is_empty() {
        format!(". Did you mean: {}?", suggestions.join(", "))
    } else {
        format!(". Found {total_count} functions, none matched. Run without --fn to instrument all")
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
        let fns = &result.targets[0].functions;
        assert!(fns.contains(&"helper".to_string()));
        assert!(fns.contains(&"Resolver::internal_resolve".to_string()));
        assert!(fns.contains(&"Resolver::resolve".to_string()));
    }

    #[test]
    fn resolve_mod_gets_directory_module() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Mod("walker".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        assert_eq!(result.targets.len(), 1);
        let fns = &result.targets[0].functions;
        assert!(fns.contains(&"walk_dir".to_string()));
        assert!(fns.contains(&"scan".to_string()));
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
            err.contains("Found 10 functions"),
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
    fn resolve_skips_const_unsafe_extern_functions() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::File("special_fns.rs".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(String::as_str))
            .collect();

        // Should include normal functions
        assert!(all_fns.contains(&"normal_fn"), "should include normal_fn");
        assert!(
            all_fns.contains(&"Widget::valid_method"),
            "should include Widget::valid_method"
        );

        // Should skip const/unsafe/extern
        assert!(!all_fns.contains(&"fixed_size"), "should skip const fn");
        assert!(!all_fns.contains(&"dangerous"), "should skip unsafe fn");
        assert!(!all_fns.contains(&"ffi_callback"), "should skip extern fn");
        assert!(
            !all_fns.contains(&"Widget::none"),
            "should skip const impl method"
        );
        assert!(
            !all_fns.contains(&"Widget::raw_ptr"),
            "should skip unsafe impl method"
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
    fn resolve_fn_exact_match() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "walk" with exact=true should match only "walk", not "walk_dir".
        let specs = [TargetSpec::Fn("walk".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, true).unwrap();

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
            .flat_map(|r| r.functions.iter().map(String::as_str))
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
    fn resolve_skipped_filtered_by_fn_pattern() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        // "dangerous" matches the unsafe fn "dangerous" but not "fixed_size" or "ffi_callback"
        let specs = [TargetSpec::Fn("dangerous".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs, false).unwrap();

        assert_eq!(
            result.skipped.len(),
            1,
            "only one skipped fn matches 'dangerous'"
        );
        assert_eq!(result.skipped[0].name, "dangerous");
        assert_eq!(result.skipped[0].reason, SkipReason::Unsafe);
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
            skipped_names.contains(&("dangerous", &SkipReason::Unsafe)),
            "should report unsafe fn as skipped: {skipped_names:?}"
        );
        assert!(
            skipped_names.contains(&("ffi_callback", &SkipReason::ExternAbi)),
            "should report extern fn as skipped: {skipped_names:?}"
        );
        assert!(
            skipped_names.contains(&("Widget::none", &SkipReason::Const)),
            "should report const impl method as skipped: {skipped_names:?}"
        );
        assert!(
            skipped_names.contains(&("Widget::raw_ptr", &SkipReason::Unsafe)),
            "should report unsafe impl method as skipped: {skipped_names:?}"
        );

        let all_fns: Vec<&str> = result
            .targets
            .iter()
            .flat_map(|r| r.functions.iter().map(String::as_str))
            .collect();
        assert!(all_fns.contains(&"normal_fn"));
        assert!(all_fns.contains(&"Widget::valid_method"));
    }
}
