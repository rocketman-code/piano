use std::collections::HashSet;

use quote::quote;
use syn::visit_mut::VisitMut;

/// Rewrite `source` so that every function whose name (or qualified name) is in
/// `targets` gets an RAII timing guard injected as its first statement.
///
/// Top-level functions match by bare name (e.g. "walk"). Impl methods match by
/// "Type::method" (e.g. "Walker::walk"). Trait default methods match by "Trait::method" (e.g. "Drawable::draw").
///
pub fn instrument_source(source: &str, targets: &HashSet<String>) -> Result<String, syn::Error> {
    let mut file: syn::File = syn::parse_str(source)?;
    let mut instrumenter = Instrumenter {
        targets: targets.clone(),
        current_impl: None,
        current_trait: None,
    };
    instrumenter.visit_file_mut(&mut file);
    Ok(prettyplease::unparse(&file))
}

struct Instrumenter {
    targets: HashSet<String>,
    current_impl: Option<String>,
    current_trait: Option<String>,
}

impl Instrumenter {
    fn inject_guard(&self, block: &mut syn::Block, name: &str) {
        if !self.targets.contains(name) {
            return;
        }
        let guard_stmt: syn::Stmt = syn::parse_quote! {
            let _piano_guard = piano_runtime::enter(#name);
        };
        block.stmts.insert(0, guard_stmt);
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
        let result = instrument_source(source, &targets).unwrap();

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
        let result = instrument_source(source, &targets).unwrap();

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
        let result = instrument_source(source, &targets).unwrap();

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
        let result = instrument_source(source, &targets).unwrap();

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
    fn does_not_inject_init() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let targets: HashSet<String> = HashSet::new();
        let result = instrument_source(source, &targets).unwrap();

        assert!(
            !result.contains("piano_runtime::init()"),
            "should NOT inject init (init is a no-op)"
        );
    }
}
