use std::path::{Path, PathBuf};

use syn::visit::Visit;

use crate::error::Error;

/// What the user asked to instrument.
#[derive(Debug, Clone)]
pub enum TargetSpec {
    /// Substring match against function names (--fn).
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

/// Resolve user-provided target specs against the source tree rooted at `src_dir`.
///
/// Returns one `ResolvedTarget` per file that contains at least one matching function.
/// Errors if no functions match any spec.
pub fn resolve_targets(src_dir: &Path, specs: &[TargetSpec]) -> Result<Vec<ResolvedTarget>, Error> {
    let rs_files = walk_rs_files(src_dir)?;

    let mut results: Vec<ResolvedTarget> = Vec::new();

    for spec in specs {
        match spec {
            TargetSpec::Fn(pattern) => {
                for file in &rs_files {
                    let source =
                        std::fs::read_to_string(file).map_err(|source| Error::RunReadError {
                            path: file.clone(),
                            source,
                        })?;
                    let all_fns = extract_functions(&source, file)?;
                    let matched: Vec<String> = all_fns
                        .into_iter()
                        .filter(|name| {
                            // Match against the bare function name (after any Type:: prefix).
                            let bare = name.rsplit("::").next().unwrap_or(name);
                            bare.contains(pattern.as_str())
                        })
                        .collect();
                    if !matched.is_empty() {
                        merge_into(&mut results, file, matched);
                    }
                }
            }
            TargetSpec::File(file_path) => {
                // Find files whose path ends with the given relative path.
                let matching_files: Vec<&PathBuf> =
                    rs_files.iter().filter(|f| f.ends_with(file_path)).collect();
                for file in matching_files {
                    let source =
                        std::fs::read_to_string(file).map_err(|source| Error::RunReadError {
                            path: file.clone(),
                            source,
                        })?;
                    let all_fns = extract_functions(&source, file)?;
                    if !all_fns.is_empty() {
                        merge_into(&mut results, file, all_fns);
                    }
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

                    let source =
                        std::fs::read_to_string(file).map_err(|source| Error::RunReadError {
                            path: file.clone(),
                            source,
                        })?;
                    let all_fns = extract_functions(&source, file)?;
                    if !all_fns.is_empty() {
                        merge_into(&mut results, file, all_fns);
                    }
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
        return Err(Error::NoTargetsFound(desc));
    }

    // Sort by file path for deterministic output.
    results.sort_by(|a, b| a.file.cmp(&b.file));
    for r in &mut results {
        r.functions.sort();
        r.functions.dedup();
    }

    Ok(results)
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
fn extract_functions(source: &str, path: &Path) -> Result<Vec<String>, Error> {
    let syntax = syn::parse_file(source).map_err(|source| Error::ParseError {
        path: path.to_path_buf(),
        source,
    })?;

    let mut collector = FnCollector::default();
    collector.visit_file(&syntax);
    Ok(collector.functions)
}

/// AST visitor that collects function names from a parsed Rust file.
#[derive(Default)]
struct FnCollector {
    functions: Vec<String>,
    /// When inside an `impl` block, holds the type name (e.g. "Resolver").
    current_impl: Option<String>,
    /// When inside a `trait` block, holds the trait name.
    current_trait: Option<String>,
}

impl<'ast> Visit<'ast> for FnCollector {
    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        self.functions.push(node.sig.ident.to_string());
        // Continue visiting nested items (closures etc. are not collected).
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let type_name = type_name_from_type(&node.self_ty);
        let prev = self.current_impl.replace(type_name);
        syn::visit::visit_item_impl(self, node);
        self.current_impl = prev;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let method_name = node.sig.ident.to_string();
        if let Some(ref impl_name) = self.current_impl {
            self.functions.push(format!("{impl_name}::{method_name}"));
        } else {
            self.functions.push(method_name);
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
        // Only collect if the method has a default body.
        if node.default.is_some() {
            let method_name = node.sig.ident.to_string();
            if let Some(ref trait_name) = self.current_trait {
                self.functions.push(format!("{trait_name}::{method_name}"));
            } else {
                self.functions.push(method_name);
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
    }

    #[test]
    fn resolve_fn_by_substring() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("walk".into())];
        let results = resolve_targets(&tmp.path().join("src"), &specs).unwrap();

        let all_fns: Vec<&str> = results
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
        let results = resolve_targets(&tmp.path().join("src"), &specs).unwrap();

        let all_fns: Vec<&str> = results
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
        let results = resolve_targets(&tmp.path().join("src"), &specs).unwrap();

        assert_eq!(results.len(), 1);
        let fns = &results[0].functions;
        assert!(fns.contains(&"helper".to_string()));
        assert!(fns.contains(&"Resolver::internal_resolve".to_string()));
        assert!(fns.contains(&"Resolver::resolve".to_string()));
    }

    #[test]
    fn resolve_mod_gets_directory_module() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Mod("walker".into())];
        let results = resolve_targets(&tmp.path().join("src"), &specs).unwrap();

        assert_eq!(results.len(), 1);
        let fns = &results[0].functions;
        assert!(fns.contains(&"walk_dir".to_string()));
        assert!(fns.contains(&"scan".to_string()));
    }

    #[test]
    fn no_match_returns_error() {
        let tmp = TempDir::new().unwrap();
        create_test_project(tmp.path());

        let specs = [TargetSpec::Fn("nonexistent_xyz".into())];
        let result = resolve_targets(&tmp.path().join("src"), &specs);

        assert!(result.is_err(), "should error when no functions match");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent_xyz"),
            "error should mention the pattern"
        );
    }
}
