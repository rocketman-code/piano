use ra_ap_syntax::ast::HasAttrs;
use ra_ap_syntax::ast::HasModuleItem;
use ra_ap_syntax::ast::HasName;
use ra_ap_syntax::{AstNode, Edition, SyntaxKind, ast};

use crate::source_map::{SourceMap, StringInjector};

/// Byte offset in `source` just after the last file-level inner attribute.
/// Uses ra_ap_syntax for position finding. Correctly handles all inner-attribute
/// forms: `#![...]`, `//!`, and `/*!...*/`.
fn after_file_inner_attrs(source: &str) -> usize {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();

    // Walk the CST children of the SourceFile. Inner attributes appear as Attr
    // nodes with `#!` syntax. Inner doc comments appear as COMMENT tokens with
    // `//!` or `/*!` prefix. We skip past them all.
    let mut pos = 0usize;
    for child in file.syntax().children_with_tokens() {
        match &child {
            ra_ap_syntax::NodeOrToken::Token(token) => {
                if token.kind() == SyntaxKind::WHITESPACE {
                    let end: usize = token.text_range().end().into();
                    if end > pos {
                        pos = end;
                    }
                    continue;
                }
                if token.kind() == SyntaxKind::COMMENT {
                    let text = token.text();
                    if text.starts_with("//!") || text.starts_with("/*!") {
                        let end: usize = token.text_range().end().into();
                        if text.starts_with("//!") {
                            let after = end;
                            if after < source.len() && source.as_bytes()[after] == b'\n' {
                                pos = after + 1;
                            } else {
                                pos = end;
                            }
                        } else {
                            pos = end;
                        }
                        continue;
                    }
                    // Regular comments between inner attrs -- skip them.
                    let end: usize = token.text_range().end().into();
                    if end > pos {
                        pos = end;
                    }
                    continue;
                }
                // Any other token -- stop.
                break;
            }
            ra_ap_syntax::NodeOrToken::Node(node) => {
                if let Some(attr) = ast::Attr::cast(node.clone()) {
                    if attr.excl_token().is_some() {
                        let end: usize = node.text_range().end().into();
                        if end > pos {
                            pos = end;
                        }
                        continue;
                    }
                }
                // Any other node (item, etc.) -- stop.
                break;
            }
        }
    }
    pos
}

/// Classification of the user's `#[global_allocator]` declaration.
pub enum AllocatorKind {
    /// No `#[global_allocator]` found in the source.
    Absent,
    /// `#[global_allocator]` without any `#[cfg(...)]` gate.
    Unconditional,
    /// One or more `#[global_allocator]` statics, each behind a `#[cfg(...)]`.
    /// The `Vec` contains the cfg predicate text from each `#[cfg(pred)]`.
    CfgGated(Vec<String>),
}

impl std::fmt::Debug for AllocatorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocatorKind::Absent => write!(f, "Absent"),
            AllocatorKind::Unconditional => write!(f, "Unconditional"),
            AllocatorKind::CfgGated(preds) => {
                write!(f, "CfgGated({} predicates)", preds.len())
            }
        }
    }
}

/// Check whether a static item has a `#[global_allocator]` attribute,
/// either directly or inside a `#[cfg_attr(condition, global_allocator)]`.
fn has_global_allocator_attr(static_item: &ast::Static) -> bool {
    static_item.attrs().any(|a| {
        if let Some(name) = a.simple_name() {
            if name == "global_allocator" {
                return true;
            }
            if name == "cfg_attr" {
                return cfg_attr_contains_global_allocator(&a);
            }
        }
        false
    })
}

/// Check whether a `#[cfg_attr(...)]` attribute contains `global_allocator`
/// among its conditional attributes.
///
/// `cfg_attr` has the form `cfg_attr(condition, attr1, attr2, ...)`.
/// We check the token tree text for `global_allocator` after the first
/// top-level comma.
fn cfg_attr_contains_global_allocator(attr: &ast::Attr) -> bool {
    let tt_text = match attr.meta().and_then(|m| m.token_tree()) {
        Some(tt) => tt.syntax().text().to_string(),
        None => return false,
    };
    // Token tree text is e.g. "(condition, global_allocator)" or
    // "(condition, global_allocator, other_attr)".
    // Find the first top-level comma, then check if "global_allocator"
    // appears in the remainder.
    let inner = tt_text.strip_prefix('(').and_then(|s| s.strip_suffix(')'));
    let inner = match inner {
        Some(s) => s,
        None => return false,
    };
    // Find the first top-level comma (tracking paren depth).
    let comma_pos = match find_top_level_comma(inner) {
        Some(pos) => pos,
        None => return false,
    };
    let after_condition = &inner[comma_pos + 1..];
    // Check if any comma-separated segment after the condition is "global_allocator".
    after_condition
        .split(',')
        .any(|seg| seg.trim() == "global_allocator")
}

/// Extract the cfg condition text from a `#[cfg_attr(condition, ...)]` that
/// contains `global_allocator`. Returns `None` if the attribute is not a
/// matching `cfg_attr`.
fn cfg_attr_global_allocator_condition(attr: &ast::Attr) -> Option<String> {
    let name = attr.simple_name()?;
    if name != "cfg_attr" {
        return None;
    }
    let tt_text = attr.meta()?.token_tree()?.syntax().text().to_string();
    let inner = tt_text.strip_prefix('(')?.strip_suffix(')')?;
    let comma_pos = find_top_level_comma(inner)?;
    let after_condition = &inner[comma_pos + 1..];
    let has_global_alloc = after_condition
        .split(',')
        .any(|seg| seg.trim() == "global_allocator");
    if has_global_alloc {
        Some(inner[..comma_pos].trim().to_string())
    } else {
        None
    }
}

/// Extract the predicate text from a `#[cfg(predicate)]` attribute's token tree.
/// Returns the text inside the parentheses, e.g. `target_os = "linux"`.
fn cfg_predicate_text(attr: &ast::Attr) -> Option<String> {
    let tt_text = attr.meta()?.token_tree()?.syntax().text().to_string();
    let inner = tt_text.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner.trim().to_string())
}

/// Find the position of the first top-level comma in `s`, tracking parenthesis
/// depth so commas inside nested parens are ignored.
fn find_top_level_comma(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Find the first child `Expr` node of a `Static` item in the CST.
/// ra_ap_syntax does not provide a direct `.expr()` accessor for `Static`,
/// so we walk the CST children to find the initializer expression.
fn static_initializer(static_node: &ast::Static) -> Option<ast::Expr> {
    static_node
        .syntax()
        .children()
        .find_map(ast::Expr::cast)
}

/// Wrap a `#[global_allocator]` static's type and initializer with `PianoAllocator`
/// using string replacement. Finds the static by name in the ra_ap_syntax tree
/// and extracts byte ranges for the type and expression.
///
/// Returns a list of (start, end, replacement) edits.
fn wrap_allocator_edits(
    source: &str,
    static_name: &str,
) -> Vec<(usize, usize, String)> {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();

    for item in file.items() {
        if let ast::Item::Static(cst_static) = item {
            let name = match cst_static.name() {
                Some(n) => n,
                None => continue,
            };
            if name.text() != static_name {
                continue;
            }

            let mut edits = Vec::new();

            // Wrap the type: T -> piano_runtime::PianoAllocator<T>
            if let Some(ty) = cst_static.ty() {
                let ty_start: usize = ty.syntax().text_range().start().into();
                let ty_end: usize = ty.syntax().text_range().end().into();
                let orig_ty = &source[ty_start..ty_end];
                edits.push((
                    ty_start,
                    ty_end,
                    format!("piano_runtime::PianoAllocator<{orig_ty}>"),
                ));
            }

            // Wrap the expr: E -> piano_runtime::PianoAllocator::new(E)
            if let Some(expr) = static_initializer(&cst_static) {
                let expr_start: usize = expr.syntax().text_range().start().into();
                let expr_end: usize = expr.syntax().text_range().end().into();
                let orig_expr = &source[expr_start..expr_end];
                edits.push((
                    expr_start,
                    expr_end,
                    format!("piano_runtime::PianoAllocator::new({orig_expr})"),
                ));
            }

            return edits;
        }
    }

    Vec::new()
}

/// Walk the parsed file and classify any `#[global_allocator]` statics.
pub fn detect_allocator_kind(source: &str) -> Result<AllocatorKind, String> {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();
    let mut cfg_predicates: Vec<String> = Vec::new();

    for item in file.items() {
        if let ast::Item::Static(static_item) = item {
            if !has_global_allocator_attr(&static_item) {
                continue;
            }

            let mut item_cfgs: Vec<String> = Vec::new();
            for a in static_item.attrs() {
                let attr_name = match a.simple_name() {
                    Some(n) => n,
                    None => continue,
                };
                if attr_name == "cfg" {
                    match cfg_predicate_text(&a) {
                        Some(pred) => item_cfgs.push(pred),
                        None => {
                            return Err("malformed #[cfg(...)] attribute".to_string());
                        }
                    }
                } else if attr_name == "cfg_attr" {
                    if let Some(condition) = cfg_attr_global_allocator_condition(&a) {
                        item_cfgs.push(condition);
                    }
                }
            }

            if item_cfgs.is_empty() {
                return Ok(AllocatorKind::Unconditional);
            }

            // Multiple #[cfg] on the same item is semantically #[cfg(all(...))].
            let combined = if item_cfgs.len() == 1 {
                item_cfgs.remove(0)
            } else {
                format!("all({})", item_cfgs.join(", "))
            };
            cfg_predicates.push(combined);
        }
    }

    if cfg_predicates.is_empty() {
        Ok(AllocatorKind::Absent)
    } else {
        Ok(AllocatorKind::CfgGated(cfg_predicates))
    }
}

/// Inject a `#[global_allocator]` static using `PianoAllocator`.
///
/// Behavior depends on `AllocatorKind`:
/// - `Absent`: inject `PianoAllocator<System>` unconditionally.
/// - `Unconditional`: wrap the existing allocator with `PianoAllocator`.
/// - `CfgGated`: wrap each cfg-gated allocator AND inject a cfg-negated
///   `PianoAllocator<System>` fallback for platforms where none match.
pub fn inject_global_allocator(
    source: &str,
    kind: AllocatorKind,
) -> Result<(String, SourceMap), String> {
    match kind {
        AllocatorKind::Absent => {
            let text = "\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n";
            let mut injector = StringInjector::new();
            injector.insert(after_file_inner_attrs(source), text);
            Ok(injector.apply(source))
        }
        AllocatorKind::Unconditional => {
            let parse = ast::SourceFile::parse(source, Edition::Edition2024);
            let file = parse.tree();
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in file.items() {
                if let ast::Item::Static(static_item) = item {
                    if has_global_allocator_attr(&static_item) {
                        if let Some(name) = static_item.name() {
                            edits = wrap_allocator_edits(source, name.text().as_ref());
                            break;
                        }
                    }
                }
            }
            Ok((apply_replacements(source, &edits), SourceMap::default()))
        }
        AllocatorKind::CfgGated(ref predicates) => {
            let parse = ast::SourceFile::parse(source, Edition::Edition2024);
            let file = parse.tree();
            // Collect type/expr edits for each cfg-gated allocator.
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in file.items() {
                if let ast::Item::Static(static_item) = item {
                    if has_global_allocator_attr(&static_item) {
                        if let Some(name) = static_item.name() {
                            edits.extend(wrap_allocator_edits(
                                source,
                                name.text().as_ref(),
                            ));
                        }
                    }
                }
            }

            // Build the cfg-negated fallback text.
            // Predicates are already stored as text strings, preserving
            // original formatting from the source.
            let negated_str = if predicates.len() == 1 {
                format!("not({})", predicates[0])
            } else {
                format!("not(any({}))", predicates.join(", "))
            };
            let fallback = format!(
                "\n#[cfg({negated_str})]\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n"
            );

            // Apply type/expr replacements first, then insert fallback after
            // any inner attributes so `#![...]` stays at the top of the file.
            // The insertion offset is computed from the original parse -- edits
            // only touch allocator statics below inner attrs, so the offset
            // is valid for the replaced string too.
            let replaced = apply_replacements(source, &edits);
            let insert_pos = after_file_inner_attrs(source);

            let fallback_newlines = fallback.bytes().filter(|&b| b == b'\n').count() as u32;
            let mut map = SourceMap::new();
            if fallback_newlines > 0 {
                let line_at_insert = replaced[..insert_pos]
                    .bytes()
                    .filter(|&b| b == b'\n')
                    .count() as u32
                    + 1;
                let none_span = if fallback.ends_with('\n') {
                    fallback_newlines - 1
                } else {
                    fallback_newlines
                };
                map.record(line_at_insert, fallback_newlines, none_span);
            }
            let mut result = String::with_capacity(replaced.len() + fallback.len());
            result.push_str(&replaced[..insert_pos]);
            result.push_str(&fallback);
            result.push_str(&replaced[insert_pos..]);
            Ok((result, map))
        }
    }
}

/// Apply a set of non-overlapping (start, end, replacement) edits to source.
/// Edits must not overlap. They are applied in offset order.
fn apply_replacements(source: &str, edits: &[(usize, usize, String)]) -> String {
    let mut sorted: Vec<&(usize, usize, String)> = edits.iter().collect();
    sorted.sort_by_key(|(start, _, _)| *start);

    let mut result = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for (start, end, replacement) in &sorted {
        debug_assert!(
            cursor <= *start,
            "overlapping edits: cursor {cursor} > start {start}"
        );
        result.push_str(&source[cursor..*start]);
        result.push_str(replacement);
        cursor = *end;
    }
    result.push_str(&source[cursor..]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the result is valid Rust by re-parsing with ra_ap_syntax.
    /// Checks for parse errors (we don't require zero errors since type/path
    /// resolution is not available, but structural parse must succeed).
    fn assert_parses(source: &str) {
        let parse = ast::SourceFile::parse(source, Edition::Edition2024);
        let errors = parse.errors();
        assert!(
            errors.is_empty(),
            "source has parse errors: {errors:?}\n\n{source}"
        );
    }

    #[test]
    fn detect_no_allocator() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        assert!(matches!(kind, AllocatorKind::Absent));
    }

    #[test]
    fn injects_global_allocator() {
        let source = r#"
fn main() {
    println!("hello");
}
"#;
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Absent).unwrap();
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
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Unconditional).unwrap();
        assert!(
            result.contains("PianoAllocator"),
            "should wrap existing allocator. Got:\n{result}"
        );
    }

    #[test]
    fn cfg_gated_allocator_gets_fallback() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator<Jemalloc>"),
            "should wrap cfg-gated allocator. Got:\n{result}"
        );
        assert!(
            result.contains("not(target_os = \"linux\")"),
            "should inject negated cfg fallback. Got:\n{result}"
        );
        assert!(
            result.contains("_PIANO_ALLOC"),
            "fallback should use _PIANO_ALLOC name. Got:\n{result}"
        );
        assert!(
            result.contains("std::alloc::System"),
            "fallback should wrap System allocator. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_cfg_gated_allocators_get_combined_fallback() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC_LINUX: Jemalloc = Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static ALLOC_MAC: MiMalloc = MiMalloc;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator<Jemalloc>"),
            "should wrap linux allocator. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator<MiMalloc>"),
            "should wrap macos allocator. Got:\n{result}"
        );
        assert!(
            result.contains("not(any("),
            "fallback should use not(any(...)) for multiple cfgs. Got:\n{result}"
        );
        assert!(
            result.contains("_PIANO_ALLOC"),
            "fallback should use _PIANO_ALLOC. Got:\n{result}"
        );
    }

    #[test]
    fn uncfg_allocator_no_fallback() {
        let source = r#"
use std::alloc::System;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.contains("PianoAllocator"),
            "should wrap allocator. Got:\n{result}"
        );
        assert!(
            !result.contains("_PIANO_ALLOC"),
            "should NOT inject fallback for unconditional allocator. Got:\n{result}"
        );
    }

    #[test]
    fn multiple_cfg_fallback_negates_all_predicates() {
        let src = r#"
            #[cfg(target_os = "linux")]
            #[cfg(target_arch = "x86_64")]
            #[global_allocator]
            static ALLOC: Jemalloc = Jemalloc;
            fn main() {}
        "#;
        let kind = detect_allocator_kind(src).unwrap();
        let (result, _map) = inject_global_allocator(src, kind).unwrap();
        assert!(result.contains("PianoAllocator"), "should wrap allocator");
        assert!(result.contains("_PIANO_ALLOC"), "should inject fallback");
        assert!(
            result.contains("target_os") && result.contains("target_arch"),
            "fallback negation should reference both predicates"
        );
    }

    #[test]
    fn mixed_cfg_gated_and_unconditional_allocator_returns_unconditional() {
        let source = r#"
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC_LINUX: Jemalloc = Jemalloc;

#[global_allocator]
static ALLOC: System = System;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        assert!(
            matches!(kind, AllocatorKind::Unconditional),
            "unconditional allocator should take precedence over cfg-gated. Got: {kind:?}"
        );
    }

    #[test]
    fn absent_allocator_preserves_inner_attrs() {
        let source = r#"#![cfg_attr(test, feature(test))]
#![allow(unused)]

fn main() {
    println!("hello");
}
"#;
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Absent).unwrap();
        // Inner attrs must remain at the top of the file.
        assert!(
            result.starts_with("#![cfg_attr"),
            "inner attrs must stay at top. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator"),
            "should inject allocator. Got:\n{result}"
        );
        // The result must re-parse successfully.
        assert_parses(&result);
    }

    #[test]
    fn cfg_gated_fallback_preserves_inner_attrs() {
        let source = r#"#![cfg_attr(test, feature(test))]

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

fn main() {}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind).unwrap();
        assert!(
            result.starts_with("#![cfg_attr"),
            "inner attrs must stay at top. Got:\n{result}"
        );
        assert!(
            result.contains("_PIANO_ALLOC"),
            "fallback should be injected. Got:\n{result}"
        );
        assert_parses(&result);
    }

    #[test]
    fn cfg_gated_qualified_path_allocator() {
        // Reproduces issue #546: ripgrep uses tikv_jemallocator::Jemalloc (qualified path)
        let source = r#"use std::process::ExitCode;

#[cfg(all(target_env = "musl", target_pointer_width = "64"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    ExitCode::SUCCESS
}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        assert!(
            matches!(&kind, AllocatorKind::CfgGated(preds) if preds.len() == 1),
            "should detect CfgGated with 1 predicate. Got: {kind:?}"
        );
        let (result, _map) = inject_global_allocator(source, kind)
            .unwrap_or_else(|e| panic!("inject_global_allocator failed: {e}\n\nSource:\n{source}"));
        assert!(
            result.contains("PianoAllocator<tikv_jemallocator::Jemalloc>"),
            "should wrap qualified path allocator. Got:\n{result}"
        );
        // ra_ap_syntax preserves original formatting: "all(" not "all ("
        assert!(
            result.contains("not(all("),
            "should negate compound cfg predicate. Got:\n{result}"
        );
        assert_parses(&result);
    }

    #[test]
    fn cfg_gated_with_inner_doc_comment() {
        // Regression test for issue #546: inner doc comments (/*! ... */)
        // must stay at the top of the file, before any injected items.
        let source = r#"/*!
Module-level documentation.
*/
use std::process::ExitCode;

#[cfg(all(target_env = "musl", target_pointer_width = "64"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    ExitCode::SUCCESS
}
"#;
        let kind = detect_allocator_kind(source).unwrap();
        let (result, _map) = inject_global_allocator(source, kind)
            .unwrap_or_else(|e| panic!("inject_global_allocator failed: {e}"));
        assert!(
            result.starts_with("/*!"),
            "inner doc comment must stay at top of file. Got:\n{result}"
        );
        assert_parses(&result);
    }

    #[test]
    fn absent_allocator_with_inner_doc_comment() {
        // Absent allocator + inner doc comment -- the other code path.
        let source = r#"/*!
Module docs.
*/

fn main() {}
"#;
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Absent).unwrap();
        assert!(
            result.starts_with("/*!"),
            "inner doc comment must stay at top. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator"),
            "should inject allocator. Got:\n{result}"
        );
        assert_parses(&result);
    }

    #[test]
    fn absent_allocator_with_line_inner_doc_comments() {
        let source = "//! Line doc comment.\n//! Another line.\n\nfn main() {}\n";
        let (result, _map) = inject_global_allocator(source, AllocatorKind::Absent).unwrap();
        assert!(
            result.starts_with("//!"),
            "line doc comments must stay at top. Got:\n{result}"
        );
        assert_parses(&result);
    }
}
