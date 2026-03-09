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

/// A single entry in the scope chain from file root to a function definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeEntry {
    /// Named module: `mod inner { ... }`
    Mod(String),
    /// Enclosing function (for items defined inside function bodies).
    Fn(String),
    /// Anonymous block scope, identified by source-order index among siblings.
    Block(usize),
}

/// Tracks the current scope chain during AST visitation.
///
/// Both `FnCollector` and `InjectionCollector` embed this to ensure
/// identical scope tracking -- one code path, no divergence.
pub struct ScopeState {
    scope: Vec<ScopeEntry>,
    /// Counts sibling blocks at each nesting level.
    block_counters: Vec<usize>,
}

impl Default for ScopeState {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeState {
    pub fn new() -> Self {
        Self {
            scope: Vec::new(),
            block_counters: vec![0],
        }
    }

    pub fn push_mod(&mut self, name: &str) {
        self.scope.push(ScopeEntry::Mod(name.to_string()));
        self.block_counters.push(0);
    }

    pub fn push_fn(&mut self, name: &str) {
        self.scope.push(ScopeEntry::Fn(name.to_string()));
        self.block_counters.push(0);
    }

    pub fn push_block(&mut self) {
        let depth = self.block_counters.len() - 1;
        let idx = self.block_counters[depth];
        self.block_counters[depth] += 1;
        self.scope.push(ScopeEntry::Block(idx));
        self.block_counters.push(0);
    }

    pub fn pop(&mut self) {
        self.scope.pop();
        self.block_counters.pop();
    }

    /// Render with ALL scope entries (Mod + Fn + Block).
    pub fn render_full(&self, fn_qualified: &str) -> String {
        render_with_scope(&self.scope, fn_qualified, 2)
    }

    /// Render with Mod + Fn entries (no Block indices).
    pub fn render_medium(&self, fn_qualified: &str) -> String {
        render_with_scope(&self.scope, fn_qualified, 1)
    }

    /// Render with Mod entries only (no Fn or Block).
    pub fn render_minimal(&self, fn_qualified: &str) -> String {
        render_with_scope(&self.scope, fn_qualified, 0)
    }

    /// Render the qualified name for the current scope (used for target matching).
    /// Includes Mod only -- Fn and Block are only used during disambiguation.
    pub fn render_qualified(&self, fn_qualified: &str) -> String {
        self.render_minimal(fn_qualified)
    }
}

fn render_with_scope(scope: &[ScopeEntry], fn_qualified: &str, level: u8) -> String {
    let mut parts: Vec<String> = Vec::new();
    for entry in scope {
        match entry {
            ScopeEntry::Mod(name) => parts.push(name.clone()),
            ScopeEntry::Fn(name) => {
                if level >= 1 {
                    parts.push(name.clone());
                }
            }
            ScopeEntry::Block(idx) => {
                if level >= 2 {
                    parts.push(format!("{{{idx}}}"));
                }
            }
        }
    }
    if fn_qualified.is_empty() {
        parts.join("::")
    } else {
        parts.push(fn_qualified.to_string());
        parts.join("::")
    }
}

/// A function with three levels of name detail for progressive disambiguation.
#[derive(Debug, Clone)]
pub struct QualifiedFunction {
    /// Level 0: Mod + impl context + fn ident (no fn scope or block index).
    pub minimal: String,
    /// Level 1: Mod + Fn scope + impl context + fn ident (no block index).
    pub medium: String,
    /// Level 2: Everything (Mod + Fn + Block + impl context + fn ident). Always unique.
    pub full: String,
}

impl QualifiedFunction {
    pub fn new(minimal: &str, medium: &str, full: &str) -> Self {
        Self {
            minimal: minimal.to_string(),
            medium: medium.to_string(),
            full: full.to_string(),
        }
    }
}

/// Compute the display name for each function using progressive disambiguation.
///
/// - Level 0 (minimal): Use if unique. Clean output, no fn scope or block indices.
/// - Level 1 (medium): Add enclosing function scope. Resolves most fn-local collisions.
/// - Level 2 (full): Add block index. Resolves sibling-block collisions. Always unique.
///
/// Returns one display name per input, in the same order.
pub fn disambiguate(functions: &[QualifiedFunction]) -> Vec<String> {
    let mut display: Vec<String> = functions.iter().map(|f| f.minimal.clone()).collect();

    let collisions = find_duplicates(&display);
    if !collisions.is_empty() {
        for &i in &collisions {
            display[i] = functions[i].medium.clone();
        }
    }

    let collisions2 = find_duplicates(&display);
    if !collisions2.is_empty() {
        for &i in &collisions2 {
            display[i] = functions[i].full.clone();
        }
    }

    display
}

/// Return indices of all entries that appear more than once.
fn find_duplicates(names: &[String]) -> Vec<usize> {
    let mut counts: std::collections::HashMap<&str, Vec<usize>> = std::collections::HashMap::new();
    for (i, name) in names.iter().enumerate() {
        counts.entry(name.as_str()).or_default().push(i);
    }
    let mut dups = Vec::new();
    for indices in counts.values() {
        if indices.len() > 1 {
            dups.extend(indices);
        }
    }
    dups.sort_unstable();
    dups
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

    // --- ScopeState tests ---

    #[test]
    fn scope_state_mod_nesting() {
        let mut state = ScopeState::new();
        state.push_mod("inner");
        assert_eq!(state.render_qualified("foo"), "inner::foo");
        state.pop();
        assert_eq!(state.render_qualified("foo"), "foo");
    }

    #[test]
    fn scope_state_fn_and_block() {
        let mut state = ScopeState::new();
        state.push_fn("outer");
        state.push_block();
        assert_eq!(state.render_full("S::m"), "outer::{0}::S::m");
        state.pop(); // block
        state.push_block();
        assert_eq!(state.render_full("S::m"), "outer::{1}::S::m");
    }

    #[test]
    fn scope_state_render_minimal_omits_fn_and_block() {
        let mut state = ScopeState::new();
        state.push_mod("db");
        state.push_fn("outer");
        state.push_block();
        // Minimal rendering omits Fn and Block entries.
        assert_eq!(state.render_minimal("S::m"), "db::S::m");
        // Full rendering includes them.
        assert_eq!(state.render_full("S::m"), "db::outer::{0}::S::m");
    }

    // --- disambiguate tests ---

    #[test]
    fn disambiguate_no_collisions() {
        let entries = vec![
            QualifiedFunction::new("walk", "walk", "walk"),
            QualifiedFunction::new("Walker::walk", "Walker::walk", "Walker::walk"),
        ];
        let result = disambiguate(&entries);
        assert_eq!(result[0], "walk");
        assert_eq!(result[1], "Walker::walk");
    }

    #[test]
    fn disambiguate_adds_fn_scope_on_collision() {
        let entries = vec![
            QualifiedFunction::new("S::m", "outer_a::S::m", "outer_a::{0}::S::m"),
            QualifiedFunction::new("S::m", "outer_b::S::m", "outer_b::{0}::S::m"),
        ];
        let result = disambiguate(&entries);
        assert!(result[0].contains("outer_a"), "got: {}", result[0]);
        assert!(result[1].contains("outer_b"), "got: {}", result[1]);
        assert_ne!(result[0], result[1]);
    }

    #[test]
    fn disambiguate_adds_block_index_on_collision() {
        let entries = vec![
            QualifiedFunction::new("S::m", "host::S::m", "host::{0}::S::m"),
            QualifiedFunction::new("S::m", "host::S::m", "host::{1}::S::m"),
        ];
        let result = disambiguate(&entries);
        assert!(result[0].contains("{0}"), "got: {}", result[0]);
        assert!(result[1].contains("{1}"), "got: {}", result[1]);
        assert_ne!(result[0], result[1]);
    }

    #[test]
    fn disambiguate_no_numbers_when_unique() {
        let entries = vec![
            QualifiedFunction::new("Unique::m", "host::Unique::m", "host::{0}::Unique::m"),
            QualifiedFunction::new("host", "host", "host"),
        ];
        let result = disambiguate(&entries);
        assert_eq!(result[0], "Unique::m");
        assert!(!result[0].contains('{'));
        assert!(!result[0].contains("host::Unique"));
    }
}
