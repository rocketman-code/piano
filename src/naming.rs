use ra_ap_syntax::AstNode;
use ra_ap_syntax::ast;

/// Render a CST type node to a human-readable string.
///
/// Uses the exact source text from the CST node, which preserves the
/// original formatting (generics, references, etc.). Called by
/// render_impl_name, which is called by qualified_name_for_fn.
pub fn render_type(ty: &ast::Type) -> String {
    ty.syntax().text().to_string()
}

/// Build the impl context string for a function inside an impl block.
///
/// - Inherent impl: returns rendered self_ty (e.g. `"Wrapper<u32>"`)
/// - Trait impl: returns `"<{self_ty} as {trait_name}>"` (e.g. `"<Foo as Display>"`)
///
/// Uses last segment of trait path only. Within a module, same ident = same trait
/// (Rust prevents conflicting imports). Cross-module disambiguation comes from
/// the module prefix.
///
/// For trait_ty: pass the trait type from `ast::Impl::trait_()`.
pub fn render_impl_name(self_ty: &ast::Type, trait_ty: Option<&ast::Type>) -> String {
    let type_str = render_type(self_ty);
    if let Some(trait_type) = trait_ty {
        let trait_name = trait_last_segment(trait_type);
        format!("<{type_str} as {trait_name}>")
    } else {
        type_str
    }
}

/// Derive the impl/trait-qualified name for a function from its AST position.
///
/// Walks up from the function node to find the nearest enclosing `impl` or
/// `trait` block and prepends the appropriate context:
/// - Inherent impl: `"S::method"`
/// - Trait impl: `"<S as T>::method"`
/// - Trait definition: `"T::default_method"`
/// - Standalone / nested inside another fn: bare name only
///
/// Stops walking at an enclosing `fn` node (nested fns are bare).
pub fn qualified_name_for_fn(func: &ast::Fn) -> String {
    use ast::HasName;
    let bare = func
        .name()
        .map(|n| n.text().to_string())
        .unwrap_or_default();
    let mut node = func.syntax().parent();
    while let Some(n) = node {
        if let Some(imp) = ast::Impl::cast(n.clone()) {
            if let Some(self_ty) = imp.self_ty() {
                let ctx = render_impl_name(&self_ty, imp.trait_().as_ref());
                return format!("{ctx}::{bare}");
            }
        }
        if let Some(tr) = ast::Trait::cast(n.clone()) {
            if let Some(name) = tr.name() {
                return format!("{}::{bare}", name.text());
            }
        }
        if ast::Fn::can_cast(n.kind()) {
            break;
        }
        node = n.parent();
    }
    bare
}

/// Extract the last segment name from a trait type in a CST.
///
/// For `fmt::Display`, returns `"Display"`. For `MyTrait`, returns `"MyTrait"`.
/// Uses the last segment of the trait path only.
pub(crate) fn trait_last_segment(ty: &ast::Type) -> String {
    if let ast::Type::PathType(path_type) = ty {
        if let Some(path) = path_type.path() {
            // ra_ap_syntax: `.segment()` returns the last segment directly.
            if let Some(seg) = path.segment() {
                if let Some(name_ref) = seg.name_ref() {
                    return name_ref.text().to_string();
                }
            }
        }
    }
    ty.syntax().text().to_string()
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
/// Embedded by `FnCollector` for module-level scope tracking.
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
    use ra_ap_syntax::{Edition, SourceFile};

    /// Parse a type string into a CST `ast::Type` by embedding it in a
    /// dummy function return type and extracting the type node.
    fn parse_type(s: &str) -> ast::Type {
        let source = format!("fn __dummy() -> {s} {{}}");
        let parse = ast::SourceFile::parse(&source, Edition::Edition2024);
        let file = parse.tree();
        let func = file
            .syntax()
            .descendants()
            .find_map(ast::Fn::cast)
            .expect("should parse dummy fn");
        func.ret_type()
            .expect("should have return type")
            .ty()
            .expect("should have type")
    }

    /// Parse an impl block and extract the self_ty and optional trait type.
    fn parse_impl(source: &str) -> (ast::Type, Option<ast::Type>) {
        let full = format!("{source} {{}}");
        let parse = ast::SourceFile::parse(&full, Edition::Edition2024);
        let file = parse.tree();
        let imp = file
            .syntax()
            .descendants()
            .find_map(ast::Impl::cast)
            .expect("should parse impl");
        let self_ty = imp.self_ty().expect("should have self type");
        let trait_ty = imp.trait_();
        (self_ty, trait_ty)
    }

    #[test]
    fn render_type_path_simple() {
        let ty = parse_type("Foo");
        assert_eq!(render_type(&ty), "Foo");
    }

    #[test]
    fn render_type_path_with_generics() {
        let ty = parse_type("Wrapper<u32>");
        assert!(
            render_type(&ty).contains("u32"),
            "generics must be preserved"
        );
        let ty2 = parse_type("Wrapper<String>");
        assert_ne!(render_type(&ty), render_type(&ty2));
    }

    #[test]
    fn render_type_reference() {
        let ty = parse_type("&Foo");
        assert!(render_type(&ty).contains("&"));
        assert!(render_type(&ty).contains("Foo"));
    }

    #[test]
    fn render_type_mut_reference() {
        let ty = parse_type("&mut Foo");
        let r = render_type(&ty);
        assert!(r.contains("mut"), "must distinguish &mut from &");
    }

    #[test]
    fn render_type_tuple() {
        let ty = parse_type("(i32, i64)");
        let r = render_type(&ty);
        assert!(r.contains("i32") && r.contains("i64"));
    }

    #[test]
    fn render_type_slice() {
        let ty = parse_type("[u8]");
        assert!(render_type(&ty).contains("u8"));
    }

    #[test]
    fn render_type_array() {
        let ty = parse_type("[u8; 4]");
        let r = render_type(&ty);
        assert!(r.contains("u8") && r.contains("4"));
    }

    #[test]
    fn render_type_ptr() {
        let ty = parse_type("*const u8");
        assert!(render_type(&ty).contains("const"));
    }

    #[test]
    fn render_type_bare_fn() {
        let ty = parse_type("fn(u32) -> bool");
        let r = render_type(&ty);
        assert!(r.contains("fn") && r.contains("bool"));
    }

    #[test]
    fn render_type_never() {
        let ty = parse_type("!");
        assert_eq!(render_type(&ty), "!");
    }

    #[test]
    fn render_type_paren() {
        let ty = parse_type("(Foo)");
        let r = render_type(&ty);
        let tuple = parse_type("(Foo,)");
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
            let ty = parse_type(src);
            let rendered = render_type(&ty);
            assert!(
                seen.insert(rendered.clone()),
                "duplicate rendering for '{src}': {rendered}"
            );
        }
    }

    #[test]
    fn render_impl_name_inherent() {
        let (self_ty, _) = parse_impl("impl Walker");
        assert_eq!(render_impl_name(&self_ty, None), "Walker");
    }

    #[test]
    fn render_impl_name_with_generics() {
        let (self_ty, _) = parse_impl("impl Wrapper<u32>");
        let name = render_impl_name(&self_ty, None);
        assert!(name.contains("u32"), "generics preserved: {name}");
    }

    #[test]
    fn render_impl_name_trait_impl() {
        let (self_ty, trait_ty) = parse_impl("impl Display for Foo");
        assert_eq!(
            render_impl_name(&self_ty, trait_ty.as_ref()),
            "<Foo as Display>"
        );
    }

    #[test]
    fn render_impl_name_trait_impl_reference_type() {
        let (self_ty, trait_ty) = parse_impl("impl MyTrait for &Foo");
        let name = render_impl_name(&self_ty, trait_ty.as_ref());
        assert!(name.contains("&"), "reference preserved: {name}");
        assert!(name.contains("Foo"), "type preserved: {name}");
        assert!(name.contains("MyTrait"), "trait preserved: {name}");
    }

    // --- qualified_name_for_fn tests ---

    /// Helper: parse source and find the Nth `ast::Fn` (0-indexed).
    fn find_fn(source: &str, index: usize) -> ast::Fn {
        let parse = ast::SourceFile::parse(source, Edition::Edition2024);
        let file = parse.tree();
        file.syntax()
            .descendants()
            .filter_map(ast::Fn::cast)
            .nth(index)
            .expect("should find fn at given index")
    }

    #[test]
    fn qualified_name_standalone_fn() {
        let func = find_fn("fn standalone() {}", 0);
        assert_eq!(qualified_name_for_fn(&func), "standalone");
    }

    #[test]
    fn qualified_name_inherent_impl_method() {
        let func = find_fn("struct S; impl S { fn method() {} }", 0);
        assert_eq!(qualified_name_for_fn(&func), "S::method");
    }

    #[test]
    fn qualified_name_trait_impl_method() {
        let func = find_fn(
            "trait T { fn m(); } struct S; impl T for S { fn m() {} }",
            1,
        );
        assert_eq!(qualified_name_for_fn(&func), "<S as T>::m");
    }

    #[test]
    fn qualified_name_trait_default_method() {
        let func = find_fn("trait T { fn default_method() { let _ = 1; } }", 0);
        assert_eq!(qualified_name_for_fn(&func), "T::default_method");
    }

    #[test]
    fn qualified_name_nested_fn_breaks_at_enclosing_fn() {
        let source = "struct S; impl S { fn method() { fn helper() {} } }";
        // helper is the second fn (index 1)
        let func = find_fn(source, 1);
        assert_eq!(qualified_name_for_fn(&func), "helper");
    }

    #[test]
    fn qualified_name_inner_impl_inside_fn_body() {
        let source = "struct Outer; impl Outer { fn method() { struct Inner; impl Inner { fn inner_method() {} } } }";
        // inner_method is the second fn (index 1)
        let func = find_fn(source, 1);
        assert_eq!(qualified_name_for_fn(&func), "Inner::inner_method");
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

    // --- Cross-path agreement tests ---

    #[test]
    fn resolve_and_rewriter_agree_on_qualified_names() {
        use std::collections::BTreeSet;
        use std::path::PathBuf;

        let source = r#"
fn standalone() { let _ = 1; }
struct S;
impl S {
    fn inherent(&self) { let _ = 1; }
    fn with_nested() {
        fn nested_bare() { let _ = 1; }
    }
}
trait T {
    fn default_method(&self) { let _ = 1; }
}
impl T for S {
    fn trait_method(&self) { let _ = 1; }
}
struct W<U>(U);
impl W<u32> {
    fn generic_method(&self) { let _ = 1; }
}
"#;

        let (resolve_fns, _) = crate::resolve::extract_functions(source, PathBuf::from("test.rs"));
        let resolve_names: BTreeSet<String> =
            resolve_fns.iter().map(|qf| qf.minimal.clone()).collect();

        let file = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2024);
        let rewriter_names: BTreeSet<String> = file
            .tree()
            .syntax()
            .descendants()
            .filter_map(ast::Fn::cast)
            .filter(|f| f.body().is_some())
            .map(|f| qualified_name_for_fn(&f))
            .collect();

        assert_eq!(
            resolve_names, rewriter_names,
            "resolve and rewriter must produce identical qualified names.\n\
             resolve:  {resolve_names:?}\n\
             rewriter: {rewriter_names:?}"
        );
    }

    // --- Collision regression tests ---

    #[test]
    fn collision_regression_generic_types() {
        let (ty_u32, _) = parse_impl("impl W<u32>");
        let (ty_string, _) = parse_impl("impl W<String>");
        let name_u32 = render_impl_name(&ty_u32, None);
        let name_string = render_impl_name(&ty_string, None);
        assert_ne!(name_u32, name_string, "generic args must distinguish impls");

        let entries = vec![
            QualifiedFunction::new(
                &format!("{name_u32}::go"),
                &format!("{name_u32}::go"),
                &format!("{name_u32}::go"),
            ),
            QualifiedFunction::new(
                &format!("{name_string}::go"),
                &format!("{name_string}::go"),
                &format!("{name_string}::go"),
            ),
        ];
        let display = disambiguate(&entries);
        assert_ne!(display[0], display[1]);
        assert!(display[0].contains("u32"), "got: {}", display[0]);
        assert!(display[1].contains("String"), "got: {}", display[1]);
    }

    #[test]
    fn collision_regression_non_path_type() {
        let (self_ty, trait_ty) = parse_impl("impl MyTrait for &Foo");
        let name = render_impl_name(&self_ty, trait_ty.as_ref());
        assert!(
            name.contains("&") && name.contains("Foo") && name.contains("MyTrait"),
            "expected & + Foo + MyTrait in: {name}"
        );
    }

    /// Fn-local types in different functions must disambiguate.
    #[test]
    fn collision_regression_fn_local_types() {
        // fn outer_a() { struct S; impl S { fn m() {} } }
        // fn outer_b() { struct S; impl S { fn m() {} } }
        let entries = vec![
            QualifiedFunction::new("S::m", "outer_a::S::m", "outer_a::{0}::S::m"),
            QualifiedFunction::new("S::m", "outer_b::S::m", "outer_b::{0}::S::m"),
        ];
        let display = disambiguate(&entries);
        assert_ne!(display[0], display[1]);
        assert!(display[0].contains("outer_a"), "got: {}", display[0]);
        assert!(display[1].contains("outer_b"), "got: {}", display[1]);
    }

    /// Sibling blocks in same function must disambiguate via block index.
    #[test]
    fn collision_regression_sibling_blocks() {
        // Two blocks in same fn with same type name.
        let entries = vec![
            QualifiedFunction::new("S::m", "host::S::m", "host::{0}::S::m"),
            QualifiedFunction::new("S::m", "host::S::m", "host::{1}::S::m"),
        ];
        let display = disambiguate(&entries);
        assert_ne!(display[0], display[1]);
        assert!(display[0].contains("{0}"), "got: {}", display[0]);
        assert!(display[1].contains("{1}"), "got: {}", display[1]);
    }

    /// Inline mod vs top-level must produce different names.
    #[test]
    fn collision_regression_inline_mod() {
        let entries = vec![
            QualifiedFunction::new("foo", "foo", "foo"),
            QualifiedFunction::new("inner::foo", "inner::foo", "inner::foo"),
        ];
        let display = disambiguate(&entries);
        assert_ne!(display[0], display[1]);
        assert_eq!(display[0], "foo");
        assert_eq!(display[1], "inner::foo");
    }

    /// When there is no collision, the minimal name is used (no fn scope
    /// or block index clutter).
    #[test]
    fn no_unnecessary_disambiguation() {
        let entries = vec![
            QualifiedFunction::new("Unique::m", "host::Unique::m", "host::{0}::Unique::m"),
            QualifiedFunction::new("Other::m", "Other::m", "Other::m"),
        ];
        let display = disambiguate(&entries);
        assert_eq!(display[0], "Unique::m");
        assert_eq!(display[1], "Other::m");
    }
}
