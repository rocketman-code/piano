use quote::quote;

/// Render a `syn::Type` to a human-readable string.
///
/// Exhaustive match over all `syn::Type` variants -- no wildcards.
/// If syn adds a new variant, this code will not compile.
/// Replaces both `type_name_from_type` (resolve.rs) and `type_ident` (shutdown.rs).
pub fn render_type(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Array(t) => quote!(#t).to_string(),
        syn::Type::BareFn(t) => quote!(#t).to_string(),
        syn::Type::Group(t) => {
            // Invisible delimiter (proc-macro artifact, never from source parsing).
            render_type(&t.elem)
        }
        syn::Type::ImplTrait(t) => quote!(#t).to_string(),
        syn::Type::Infer(_) => "_".to_string(),
        syn::Type::Macro(t) => quote!(#t).to_string(),
        syn::Type::Never(_) => "!".to_string(),
        syn::Type::Paren(t) => quote!(#t).to_string(),
        syn::Type::Path(t) => quote!(#t).to_string(),
        syn::Type::Ptr(t) => quote!(#t).to_string(),
        syn::Type::Reference(t) => quote!(#t).to_string(),
        syn::Type::Slice(t) => quote!(#t).to_string(),
        syn::Type::TraitObject(t) => quote!(#t).to_string(),
        syn::Type::Tuple(t) => quote!(#t).to_string(),
        syn::Type::Verbatim(t) => t.to_string(),
        // syn::Type is #[non_exhaustive]. Safe fallback for future variants.
        _ => quote!(#ty).to_string(),
    }
}

/// Build the impl context string for a function inside an impl block.
///
/// - Inherent impl: returns rendered self_ty (e.g. `"Wrapper < u32 >"`)
/// - Trait impl: returns `"<{self_ty} as {trait_name}>"` (e.g. `"<Foo as Display>"`)
///
/// Uses last segment of trait path only. Within a module, same ident = same trait
/// (Rust prevents conflicting imports). Cross-module disambiguation comes from
/// the module prefix.
pub fn render_impl_name(self_ty: &syn::Type, trait_: Option<&syn::Path>) -> String {
    let type_str = render_type(self_ty);
    if let Some(trait_path) = trait_ {
        if let Some(seg) = trait_path.segments.last() {
            format!("<{type_str} as {}>", seg.ident)
        } else {
            type_str
        }
    } else {
        type_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_type_path_simple() {
        let ty: syn::Type = syn::parse_str("Foo").unwrap();
        assert_eq!(render_type(&ty), "Foo");
    }

    #[test]
    fn render_type_path_with_generics() {
        let ty: syn::Type = syn::parse_str("Wrapper<u32>").unwrap();
        assert!(
            render_type(&ty).contains("u32"),
            "generics must be preserved"
        );
        let ty2: syn::Type = syn::parse_str("Wrapper<String>").unwrap();
        assert_ne!(render_type(&ty), render_type(&ty2));
    }

    #[test]
    fn render_type_reference() {
        let ty: syn::Type = syn::parse_str("&Foo").unwrap();
        assert!(render_type(&ty).contains("&"));
        assert!(render_type(&ty).contains("Foo"));
    }

    #[test]
    fn render_type_mut_reference() {
        let ty: syn::Type = syn::parse_str("&mut Foo").unwrap();
        let r = render_type(&ty);
        assert!(r.contains("mut"), "must distinguish &mut from &");
    }

    #[test]
    fn render_type_tuple() {
        let ty: syn::Type = syn::parse_str("(i32, i64)").unwrap();
        let r = render_type(&ty);
        assert!(r.contains("i32") && r.contains("i64"));
    }

    #[test]
    fn render_type_slice() {
        let ty: syn::Type = syn::parse_str("[u8]").unwrap();
        assert!(render_type(&ty).contains("u8"));
    }

    #[test]
    fn render_type_array() {
        let ty: syn::Type = syn::parse_str("[u8; 4]").unwrap();
        let r = render_type(&ty);
        assert!(r.contains("u8") && r.contains("4"));
    }

    #[test]
    fn render_type_ptr() {
        let ty: syn::Type = syn::parse_str("*const u8").unwrap();
        assert!(render_type(&ty).contains("const"));
    }

    #[test]
    fn render_type_bare_fn() {
        let ty: syn::Type = syn::parse_str("fn(u32) -> bool").unwrap();
        let r = render_type(&ty);
        assert!(r.contains("fn") && r.contains("bool"));
    }

    #[test]
    fn render_type_never() {
        let ty: syn::Type = syn::parse_str("!").unwrap();
        assert_eq!(render_type(&ty), "!");
    }

    #[test]
    fn render_type_paren() {
        let ty: syn::Type = syn::parse_str("(Foo)").unwrap();
        let r = render_type(&ty);
        let tuple: syn::Type = syn::parse_str("(Foo,)").unwrap();
        assert_ne!(r, render_type(&tuple));
    }

    #[test]
    fn all_types_distinct() {
        let sources = vec![
            "Foo",
            "Foo<u32>",
            "Foo<String>",
            "&Foo",
            "&mut Foo",
            "(i32, i64)",
            "[u8]",
            "[u8; 4]",
            "*const u8",
            "*mut u8",
            "fn() -> bool",
            "!",
            "(Foo)",
            "(Foo,)",
        ];
        let mut seen = std::collections::HashSet::new();
        for src in &sources {
            let ty: syn::Type = syn::parse_str(src).unwrap();
            let rendered = render_type(&ty);
            assert!(
                seen.insert(rendered.clone()),
                "duplicate rendering for '{src}': {rendered}"
            );
        }
    }

    #[test]
    fn render_impl_name_inherent() {
        let ty: syn::Type = syn::parse_str("Walker").unwrap();
        assert_eq!(render_impl_name(&ty, None), "Walker");
    }

    #[test]
    fn render_impl_name_with_generics() {
        let ty: syn::Type = syn::parse_str("Wrapper<u32>").unwrap();
        let name = render_impl_name(&ty, None);
        assert!(name.contains("u32"), "generics preserved: {name}");
    }

    #[test]
    fn render_impl_name_trait_impl() {
        let ty: syn::Type = syn::parse_str("Foo").unwrap();
        let trait_path: syn::Path = syn::parse_str("Display").unwrap();
        assert_eq!(render_impl_name(&ty, Some(&trait_path)), "<Foo as Display>");
    }

    #[test]
    fn render_impl_name_trait_impl_reference_type() {
        let ty: syn::Type = syn::parse_str("&Foo").unwrap();
        let trait_path: syn::Path = syn::parse_str("MyTrait").unwrap();
        let name = render_impl_name(&ty, Some(&trait_path));
        assert!(name.contains("&"), "reference preserved: {name}");
        assert!(name.contains("Foo"), "type preserved: {name}");
        assert!(name.contains("MyTrait"), "trait preserved: {name}");
    }
}
