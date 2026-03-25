//! Macro expansion for macro_rules! function discovery and instrumentation.
//!
//! Uses ra_ap_mbe (rust-analyzer's macro-by-example expander) to expand
//! macro_rules! invocations and find the function definitions they generate.
//!
//! The pipeline:
//! 1. Find macro_rules! definitions and their invocations in the CST.
//! 2. Expand each invocation using ra_ap_mbe.
//! 3. Parse the expansion to find fn items.
//! 4. Safety fence: skip invocations whose expansion contains no fn items.
//!
//! This module is the leaf. Both resolve (function discovery) and rewrite
//! (guard injection) depend on it.

use ra_ap_mbe::DeclarativeMacro;
use ra_ap_span::{Edition, EditionedFileId, ErasedFileAstId, FileId, Span, SpanAnchor, SyntaxContext};
use ra_ap_syntax::{AstNode, SourceFile, SyntaxKind, SyntaxNode, ast};
use ra_ap_syntax::ast::HasName;
use ra_ap_tt::TextRange;

/// A macro_rules! definition found in the source.
pub(crate) struct MacroDef {
    pub name: String,
    /// The rule text between the outer braces: `(pattern) => { template };`
    pub body_text: String,
}

/// A macro invocation found in the source.
pub(crate) struct MacroCall {
    pub name: String,
    /// The argument text inside the invocation delimiters.
    pub args_text: String,
    /// Byte range of the entire MACRO_CALL node (including trailing semicolon).
    pub byte_start: usize,
    pub byte_end: usize,
}

/// Result of expanding a macro invocation.
pub(crate) struct MacroExpansion {
    /// The expanded source text.
    pub expanded_text: String,
    /// Function names found in the expansion.
    pub fn_names: Vec<String>,
    /// Index into the calls list that produced this expansion.
    pub call_idx: usize,
}

/// Find all macro_rules! definitions in a syntax tree.
pub(crate) fn find_macro_defs(root: &SyntaxNode) -> Vec<MacroDef> {
    let mut defs = Vec::new();
    for node in root.descendants() {
        if node.kind() != SyntaxKind::MACRO_RULES {
            continue;
        }
        let Some(mac) = ast::MacroRules::cast(node) else { continue };
        let name = mac.name().map(|n| n.text().to_string()).unwrap_or_default();
        let Some(tt) = mac.token_tree() else { continue };
        let tt_text = tt.syntax().text().to_string();
        let body = strip_outer_delimiters(&tt_text);
        defs.push(MacroDef { name, body_text: body });
    }
    defs
}

/// Find all macro invocations in a syntax tree.
pub(crate) fn find_macro_calls(root: &SyntaxNode) -> Vec<MacroCall> {
    let mut calls = Vec::new();
    for node in root.descendants() {
        if node.kind() != SyntaxKind::MACRO_CALL {
            continue;
        }
        let Some(mac) = ast::MacroCall::cast(node.clone()) else { continue };
        let name = mac.path().map(|p| p.syntax().text().to_string()).unwrap_or_default();
        let args_text = mac.token_tree()
            .map(|tt| strip_outer_delimiters(&tt.syntax().text().to_string()))
            .unwrap_or_default();

        let range = node.text_range();
        let mut end: usize = range.end().into();

        // Include trailing semicolon if present.
        let full_text = root.text().to_string();
        if end < full_text.len() && full_text.as_bytes()[end] == b';' {
            end += 1;
        }

        calls.push(MacroCall {
            name,
            args_text,
            byte_start: range.start().into(),
            byte_end: end,
        });
    }
    calls
}

/// Expand a macro invocation using ra_ap_mbe.
///
/// Returns the expanded source text, or an error message.
pub(crate) fn expand_macro(def: &MacroDef, call: &MacroCall) -> Result<String, String> {
    let edition = Edition::Edition2021;
    let ctx = SyntaxContext::root(Edition::CURRENT);

    let def_anchor = SpanAnchor {
        file_id: EditionedFileId::new(FileId::from_raw(0), edition),
        ast_id: ErasedFileAstId::from_raw(0),
    };

    let def_tt = ra_ap_syntax_bridge::parse_to_token_tree(edition, def_anchor, ctx, &def.body_text)
        .ok_or_else(|| format!("failed to parse macro body for '{}'", def.name))?;

    let mac = DeclarativeMacro::parse_macro_rules(&def_tt, |_| edition);
    if let Some(err) = mac.err() {
        return Err(format!("macro parse error for '{}': {err}", def.name));
    }

    let call_anchor = SpanAnchor {
        file_id: EditionedFileId::new(FileId::from_raw(1), edition),
        ast_id: ErasedFileAstId::from_raw(0),
    };

    let arg_tt = ra_ap_syntax_bridge::parse_to_token_tree(edition, call_anchor, ctx, &call.args_text)
        .ok_or_else(|| format!("failed to parse invocation args for '{}'", call.name))?;

    let call_site = Span {
        range: TextRange::up_to(ra_ap_tt::TextSize::of(&call.args_text)),
        anchor: call_anchor,
        ctx,
    };

    let result = mac.expand(&arg_tt, |_| (), call_site, edition);
    if let Some(err) = &result.err {
        return Err(format!("expand error for '{}': {err}", call.name));
    }

    let (parse, _span_map) = ra_ap_syntax_bridge::token_tree_to_syntax_node(
        &result.value.0,
        ra_ap_parser::TopEntryPoint::SourceFile,
        &mut |_| edition,
        edition,
    );

    #[allow(deprecated)]
    let pretty = ra_ap_syntax_bridge::prettify_macro_expansion::prettify_macro_expansion(
        parse.syntax_node(),
        &mut |it: &ra_ap_syntax::SyntaxToken| it.clone(),
    );

    Ok(pretty.to_string())
}

/// Find function names in a source text fragment.
pub(crate) fn find_fn_names(text: &str) -> Vec<String> {
    let parse = SourceFile::parse(text, Edition::Edition2021);
    let mut names = Vec::new();
    for node in parse.tree().syntax().descendants() {
        if let Some(func) = ast::Fn::cast(node) {
            if let Some(name) = func.name() {
                names.push(name.text().to_string());
            }
        }
    }
    names
}

/// Expand all invocations of locally-defined macros and return expansions
/// that contain fn items. Invocations whose expansion has no fn items are
/// skipped (safety fence: expression-position macros are never touched).
pub(crate) fn expand_fn_generating_macros(
    root: &SyntaxNode,
) -> (Vec<MacroExpansion>, Vec<MacroDef>, Vec<MacroCall>) {
    let defs = find_macro_defs(root);
    let calls = find_macro_calls(root);

    let def_map: std::collections::HashMap<&str, &MacroDef> =
        defs.iter().map(|d| (d.name.as_str(), d)).collect();

    let mut expansions = Vec::new();

    for (i, call) in calls.iter().enumerate() {
        let Some(def) = def_map.get(call.name.as_str()) else { continue };
        let Ok(expanded) = expand_macro(def, call) else { continue };
        let fn_names = find_fn_names(&expanded);

        // Safety fence: skip if expansion contains no fn items.
        if fn_names.is_empty() {
            continue;
        }

        expansions.push(MacroExpansion {
            expanded_text: expanded,
            fn_names,
            call_idx: i,
        });
    }

    (expansions, defs, calls)
}

fn strip_outer_delimiters(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() < 2 {
        return trimmed.to_string();
    }
    let first = trimmed.as_bytes()[0];
    let last = trimmed.as_bytes()[trimmed.len() - 1];
    if matches!((first, last), (b'{', b'}') | (b'(', b')') | (b'[', b']')) {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_expand(source: &str) -> (Vec<MacroExpansion>, Vec<MacroCall>) {
        let parse = SourceFile::parse(source, Edition::Edition2021);
        let root = parse.tree().syntax().clone();
        let (expansions, _defs, calls) = expand_fn_generating_macros(&root);
        (expansions, calls)
    }

    #[test]
    fn literal_template_expands() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! setup { () => { fn process() { let _ = 1; } }; }
            setup!();
        "#);
        assert_eq!(exps.len(), 1);
        assert_eq!(exps[0].fn_names, vec!["process"]);
    }

    #[test]
    fn ident_metavar_expands() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! make_fn { ($name:ident) => { fn $name() { let _ = 1; } }; }
            make_fn!(compute);
            make_fn!(helper);
        "#);
        assert_eq!(exps.len(), 2);
        assert_eq!(exps[0].fn_names, vec!["compute"]);
        assert_eq!(exps[1].fn_names, vec!["helper"]);
    }

    #[test]
    fn multiple_fns_in_template() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! pair { () => { fn alpha() {} fn beta() {} }; }
            pair!();
        "#);
        assert_eq!(exps.len(), 1);
        assert_eq!(exps[0].fn_names, vec!["alpha", "beta"]);
    }

    #[test]
    fn expr_metavar_expands() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! make_fn {
                ($name:ident, $body:expr) => { fn $name() -> u64 { $body } };
            }
            make_fn!(compute, { let mut s = 0u64; for i in 0..100 { s += i; } s });
        "#);
        assert_eq!(exps.len(), 1);
        assert_eq!(exps[0].fn_names, vec!["compute"]);
    }

    #[test]
    fn safety_fence_skips_expression_macros() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! compute_value { ($x:expr) => { { let h = $x * 2; h + 1 } }; }
            fn main() { let r = compute_value!(5); }
        "#);
        assert!(exps.is_empty(), "expression-position macro should be skipped");
    }

    #[test]
    fn impl_block_macro_expands() {
        let (exps, _) = parse_and_expand(r#"
            struct S;
            macro_rules! add_method { ($name:ident) => { fn $name(&self) -> u32 { 42 } }; }
            impl S { add_method!(get_value); }
        "#);
        assert_eq!(exps.len(), 1);
        assert_eq!(exps[0].fn_names, vec!["get_value"]);
    }

    #[test]
    fn external_macro_call_skipped() {
        let (exps, _) = parse_and_expand(r#"
            fn main() { println!("hello"); }
        "#);
        assert!(exps.is_empty(), "external macro call with no local def should produce no expansions");
    }

    #[test]
    fn expand_failure_silently_skipped() {
        // Macro expects one ident arg, but invocation provides two -- expansion should
        // fail silently (no panic), and the call is skipped.
        let (exps, _) = parse_and_expand(r#"
            macro_rules! make_fn { ($name:ident) => { fn $name() {} }; }
            make_fn!(a, b);
        "#);
        assert!(exps.is_empty(), "failed expansion should be silently skipped");
    }

    #[test]
    fn empty_source_produces_no_expansions() {
        let (exps, calls) = parse_and_expand("fn main() { let x = 1; }");
        assert!(exps.is_empty(), "source with no macros should produce no expansions");
        assert!(calls.is_empty(), "source with no macros should produce no calls");
    }

    #[test]
    fn defs_without_calls_produce_no_expansions() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! unused { () => { fn ghost() {} }; }
            fn main() {}
        "#);
        assert!(exps.is_empty(), "defined but never invoked macro should produce no expansions");
    }

    #[test]
    fn multiple_rules_macro() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! make {
                (fast $name:ident) => { fn $name() { let _ = "fast"; } };
                (slow $name:ident) => { fn $name() { let _ = "slow"; } };
            }
            make!(fast speedy);
            make!(slow turtle);
        "#);
        assert_eq!(exps.len(), 2, "both invocations should expand");
        assert_eq!(exps[0].fn_names, vec!["speedy"]);
        assert_eq!(exps[1].fn_names, vec!["turtle"]);
    }

    #[test]
    fn repetition_pattern_expands() {
        let (exps, _) = parse_and_expand(r#"
            macro_rules! make_fns {
                ($($name:ident),*) => { $(fn $name() { let _ = 1; })* };
            }
            make_fns!(a, b, c);
        "#);
        assert_eq!(exps.len(), 1, "single invocation should produce one expansion");
        assert_eq!(exps[0].fn_names, vec!["a", "b", "c"]);
    }
}
