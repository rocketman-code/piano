use std::collections::{HashMap, HashSet};

use quote::quote;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::visit_mut::VisitMut;

pub struct ChannelRewriteResult {
    pub source: String,
    pub detected_crates: HashSet<String>,
}

struct ChannelCreator {
    crate_id: &'static str,
    qualifiers: &'static [&'static str],
    fn_name: &'static str,
    wrap_fn: &'static str,
}

const CHANNEL_CREATORS: &[ChannelCreator] = &[
    // tokio
    ChannelCreator {
        crate_id: "tokio",
        qualifiers: &["tokio", "sync", "mpsc"],
        fn_name: "channel",
        wrap_fn: "piano_runtime::wrap_tokio_bounded",
    },
    ChannelCreator {
        crate_id: "tokio",
        qualifiers: &["tokio", "sync", "mpsc"],
        fn_name: "unbounded_channel",
        wrap_fn: "piano_runtime::wrap_tokio_unbounded",
    },
    // crossbeam
    ChannelCreator {
        crate_id: "crossbeam",
        qualifiers: &["crossbeam_channel"],
        fn_name: "bounded",
        wrap_fn: "piano_runtime::wrap_crossbeam_bounded",
    },
    ChannelCreator {
        crate_id: "crossbeam",
        qualifiers: &["crossbeam_channel"],
        fn_name: "unbounded",
        wrap_fn: "piano_runtime::wrap_crossbeam_unbounded",
    },
    // std
    ChannelCreator {
        crate_id: "std",
        qualifiers: &["std", "sync", "mpsc"],
        fn_name: "channel",
        wrap_fn: "piano_runtime::wrap_std_channel",
    },
    ChannelCreator {
        crate_id: "std",
        qualifiers: &["std", "sync", "mpsc"],
        fn_name: "sync_channel",
        wrap_fn: "piano_runtime::wrap_std_sync_channel",
    },
    // async_channel
    ChannelCreator {
        crate_id: "async_channel",
        qualifiers: &["async_channel"],
        fn_name: "bounded",
        wrap_fn: "piano_runtime::wrap_async_bounded",
    },
    ChannelCreator {
        crate_id: "async_channel",
        qualifiers: &["async_channel"],
        fn_name: "unbounded",
        wrap_fn: "piano_runtime::wrap_async_unbounded",
    },
    // futures_channel
    ChannelCreator {
        crate_id: "futures",
        qualifiers: &["futures_channel", "mpsc"],
        fn_name: "channel",
        wrap_fn: "piano_runtime::wrap_futures_bounded",
    },
    ChannelCreator {
        crate_id: "futures",
        qualifiers: &["futures_channel", "mpsc"],
        fn_name: "unbounded",
        wrap_fn: "piano_runtime::wrap_futures_unbounded",
    },
];

/// Scan use declarations to build an import map: bare name -> full path segments.
struct UseScanner {
    imports: HashMap<String, Vec<String>>,
}

impl UseScanner {
    fn new() -> Self {
        UseScanner {
            imports: HashMap::new(),
        }
    }

    fn collect_use_tree(&mut self, tree: &syn::UseTree, prefix: &[String]) {
        match tree {
            syn::UseTree::Path(p) => {
                let mut new_prefix = prefix.to_vec();
                new_prefix.push(p.ident.to_string());
                self.collect_use_tree(&p.tree, &new_prefix);
            }
            syn::UseTree::Name(n) => {
                let name = n.ident.to_string();
                let mut full = prefix.to_vec();
                full.push(name.clone());
                self.imports.insert(name, full);
            }
            syn::UseTree::Rename(r) => {
                let alias = r.rename.to_string();
                let mut full = prefix.to_vec();
                full.push(r.ident.to_string());
                self.imports.insert(alias, full);
            }
            syn::UseTree::Group(g) => {
                for tree in &g.items {
                    self.collect_use_tree(tree, prefix);
                }
            }
            syn::UseTree::Glob(_) => {
                // Glob imports not supported for channel detection.
            }
        }
    }
}

impl<'ast> Visit<'ast> for UseScanner {
    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        self.collect_use_tree(&node.tree, &[]);
        syn::visit::visit_item_use(self, node);
    }
}

/// Extract path segments from an expression's function call path.
fn extract_call_path(expr: &syn::Expr) -> Option<Vec<String>> {
    match expr {
        syn::Expr::Path(ep) => {
            let segs: Vec<String> = ep
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            Some(segs)
        }
        _ => None,
    }
}

/// Check if a path matches a channel creator (fully qualified or via import map).
fn match_creator<'a>(
    segments: &[String],
    imports: &HashMap<String, Vec<String>>,
) -> Option<&'a ChannelCreator> {
    // Try fully qualified match first.
    for creator in CHANNEL_CREATORS {
        let mut expected: Vec<&str> = creator.qualifiers.to_vec();
        expected.push(creator.fn_name);

        if segments.len() == expected.len()
            && segments.iter().zip(expected.iter()).all(|(a, b)| a == b)
        {
            return Some(creator);
        }
    }

    // Try bare name via import map.
    if segments.len() == 1 {
        let bare = &segments[0];
        if let Some(full_path) = imports.get(bare) {
            // Recursively match the resolved full path.
            return match_creator(full_path, &HashMap::new());
        }
    }

    // Try partial qualification (e.g., `mpsc::channel` with `use tokio::sync::mpsc`).
    if segments.len() >= 2 {
        let first = &segments[0];
        if let Some(prefix_path) = imports.get(first) {
            let mut full: Vec<String> = prefix_path.clone();
            full.extend(segments[1..].iter().cloned());
            return match_creator(&full, &HashMap::new());
        }
    }

    None
}

struct ChannelRewriter {
    file_path: String,
    imports: HashMap<String, Vec<String>>,
    detected_crates: HashSet<String>,
}

impl VisitMut for ChannelRewriter {
    fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
        // Visit children first.
        syn::visit_mut::visit_expr_mut(self, expr);

        // Check if this is a function call to a channel creator.
        if let syn::Expr::Call(call) = expr {
            if let Some(segments) = extract_call_path(&call.func) {
                if let Some(creator) = match_creator(&segments, &self.imports) {
                    self.detected_crates.insert(creator.crate_id.to_string());

                    let line = call.func.span().start().line;
                    let label = format!("{}:{}", self.file_path, line);

                    let wrap_path: syn::ExprPath =
                        syn::parse_str(creator.wrap_fn).expect("invalid wrap fn path");

                    let original = call.clone();

                    // Build the label as a string literal.
                    let label_lit = syn::LitStr::new(&label, proc_macro2::Span::call_site());

                    // Determine if the wrap function needs a capacity arg.
                    let needs_cap = creator.wrap_fn.contains("bounded")
                        && !creator.wrap_fn.contains("unbounded");

                    let replacement = if needs_cap {
                        // For bounded channels, extract the capacity arg from the original call.
                        // The capacity is typically the first (or only numeric) argument.
                        let cap_arg = if !original.args.is_empty() {
                            let first_arg = &original.args[0];
                            quote! { #first_arg }
                        } else {
                            quote! { 0 }
                        };
                        syn::parse_quote! {
                            #wrap_path(#original, #label_lit, #cap_arg as usize)
                        }
                    } else {
                        syn::parse_quote! {
                            #wrap_path(#original, #label_lit)
                        }
                    };

                    *expr = replacement;
                }
            }
        }
    }
}

pub fn rewrite_channels(source: &str, file_path: &str) -> Result<ChannelRewriteResult, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;

    // First pass: scan use declarations.
    let mut scanner = UseScanner::new();
    scanner.visit_file(&file);

    // Second pass: rewrite channel creation expressions.
    let mut rewriter = ChannelRewriter {
        file_path: file_path.to_string(),
        imports: scanner.imports,
        detected_crates: HashSet::new(),
    };
    rewriter.visit_file_mut(&mut file);

    let source = prettyplease::unparse(&file);

    Ok(ChannelRewriteResult {
        source,
        detected_crates: rewriter.detected_crates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_tokio_mpsc_channel() {
        let source = r#"
fn main() {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
}
"#;
        let result = rewrite_channels(source, "src/main.rs").unwrap();
        assert!(result.source.contains("piano_runtime::wrap_tokio_bounded"));
        assert!(result.detected_crates.contains("tokio"));
    }

    #[test]
    fn detects_std_mpsc_channel() {
        let source = r#"
fn main() {
    let (tx, rx) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::sync_channel(5);
}
"#;
        let result = rewrite_channels(source, "src/main.rs").unwrap();
        assert!(result.source.contains("piano_runtime::wrap_std_channel"));
        assert!(
            result
                .source
                .contains("piano_runtime::wrap_std_sync_channel")
        );
        assert!(result.detected_crates.contains("std"));
    }

    #[test]
    fn does_not_rewrite_non_channel_calls() {
        let source = r#"
fn main() {
    let result = some_other::channel(42);
    let foo = bar::bounded(10);
}
"#;
        let result = rewrite_channels(source, "src/main.rs").unwrap();
        assert!(result.detected_crates.is_empty());
        assert!(result.source.contains("some_other::channel(42)"));
    }

    #[test]
    fn detects_all_five_crate_types() {
        let source = r#"
fn main() {
    let a = tokio::sync::mpsc::channel(10);
    let b = crossbeam_channel::bounded(5);
    let c = std::sync::mpsc::channel();
    let d = async_channel::bounded(8);
    let e = futures_channel::mpsc::channel(4);
}
"#;
        let result = rewrite_channels(source, "src/main.rs").unwrap();
        assert_eq!(
            result.detected_crates.len(),
            5,
            "detected: {:?}",
            result.detected_crates
        );
    }

    #[test]
    fn resolves_bare_names_via_use_map() {
        let source = r#"
use tokio::sync::mpsc::channel;

fn main() {
    let (tx, rx) = channel(10);
}
"#;
        let result = rewrite_channels(source, "src/main.rs").unwrap();
        assert!(result.source.contains("piano_runtime::wrap_tokio_bounded"));
        assert!(result.detected_crates.contains("tokio"));
    }
}
