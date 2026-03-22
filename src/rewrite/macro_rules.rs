use std::collections::HashMap;

use ra_ap_syntax::{AstNode, Edition, SyntaxKind, SyntaxNode, SyntaxToken};

use crate::source_map::StringInjector;

type NodeOrToken = ra_ap_syntax::NodeOrToken<SyntaxNode, SyntaxToken>;

/// Whether the impl block's Self type is a literal identifier or a metavar.
enum MacroImplType {
    /// A literal type name, e.g. `impl Cruncher { ... }`.
    Literal(String),
    /// A metavar type, e.g. `impl $ty { ... }`.
    #[allow(dead_code)]
    Metavar,
}

/// Result of matching a fn pattern in a flat TOKEN_TREE children list.
struct MacroFnMatch {
    /// The literal function name (None for metavar names).
    name: Option<String>,
    /// Whether the name is a metavar ($name) -- these are skipped.
    is_metavar: bool,
    /// Byte offset of the closing ')' of the param list.
    params_close_offset: usize,
    /// Whether the param list has existing parameters.
    has_params: bool,
    /// Byte offset just after the opening '{' of the body.
    body_open_offset: usize,
    /// Index in the children array of the body TOKEN_TREE (for advancing past it).
    body_child_index: usize,
}

/// Pre-scan source for literal function names inside macro_rules! bodies.
/// Returns qualified names (with module_prefix) for all literal-name functions
/// found. Metavar names ($name) are excluded.
///
/// This is the read-only counterpart to instrument_macro_fns -- same walk
/// logic, but only collects names without modifying anything.
pub fn discover_macro_fn_names(source: &str, module_prefix: &str) -> Result<Vec<String>, String> {
    let parse = ra_ap_syntax::SourceFile::parse(source, Edition::Edition2024);
    let root = parse.tree().syntax().clone();

    let mut names = Vec::new();
    for node in root.descendants() {
        if node.kind() == SyntaxKind::MACRO_RULES {
            collect_names_from_macro_rules(&node, module_prefix, &mut names);
        }
    }
    Ok(names)
}

/// Walk a MACRO_RULES node collecting literal fn names from rule arm templates.
fn collect_names_from_macro_rules(
    macro_node: &SyntaxNode,
    module_prefix: &str,
    names: &mut Vec<String>,
) {
    let outer_tt = match macro_node.children().find(|n| n.kind() == SyntaxKind::TOKEN_TREE) {
        Some(tt) => tt,
        None => return,
    };

    let children: Vec<NodeOrToken> = outer_tt.children_with_tokens().collect();
    let len = children.len();
    let mut i = 0;

    while i < len {
        // Look for EQ + R_ANGLE (the fat arrow `=>`).
        if i + 1 < len
            && is_kind(&children[i], SyntaxKind::EQ)
            && is_kind(&children[i + 1], SyntaxKind::R_ANGLE)
        {
            let mut j = i + 2;
            j = skip_ws(&children, j);
            if j < len {
                if let ra_ap_syntax::NodeOrToken::Node(template_tt) = &children[j] {
                    if template_tt.kind() == SyntaxKind::TOKEN_TREE {
                        collect_fn_names_in_tt(template_tt, None, module_prefix, names);
                    }
                }
            }
            i = j + 1;
        } else {
            i += 1;
        }
    }
}

/// Scan a TOKEN_TREE for fn definitions and collect their literal names.
/// Recurses into brace TOKEN_TREE children for impl blocks.
fn collect_fn_names_in_tt(
    tt: &SyntaxNode,
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
    names: &mut Vec<String>,
) {
    let children: Vec<NodeOrToken> = tt.children_with_tokens().collect();
    let len = children.len();
    let mut i = 0;

    while i < len {
        if let Some(skip_to) = skip_non_instrumentable_fn(&children, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_in_children(&children, i) {
            if !fm.is_metavar {
                if let Some(ref fn_name) = fm.name {
                    let qualified = match impl_type {
                        Some(MacroImplType::Literal(ty)) => {
                            crate::resolve::qualify(module_prefix, &format!("{ty}::{fn_name}"))
                        }
                        None => crate::resolve::qualify(module_prefix, fn_name),
                        Some(MacroImplType::Metavar) => {
                            crate::resolve::qualify(module_prefix, fn_name)
                        }
                    };
                    if !qualified.is_empty() {
                        names.push(qualified);
                    }
                }
            }
            i = fm.body_child_index + 1;
        } else {
            // Recurse into brace TOKEN_TREEs for impl blocks.
            if let ra_ap_syntax::NodeOrToken::Node(n) = &children[i] {
                if n.kind() == SyntaxKind::TOKEN_TREE
                    && starts_with_kind(n, SyntaxKind::L_CURLY)
                {
                    let detected = detect_impl_type(&children, i);
                    let recurse_impl = detected.as_ref().or(impl_type);
                    collect_fn_names_in_tt(n, recurse_impl, module_prefix, names);
                }
            }
            i += 1;
        }
    }
}

/// Instrument all macro_rules! bodies in the source, collecting injection
/// points into the provided StringInjector. Also collects literal fn names
/// found in macro templates (for registration).
///
/// This operates on the same CST as the main InjectionCollector, using
/// byte-offset splicing via StringInjector. No re-parse needed.
pub(super) fn instrument_macro_fns(
    source: &str,
    injector: &mut StringInjector,
    module_prefix: &str,
    name_ids: &HashMap<String, u32>,
    collected_names: &mut Vec<String>,
) {
    let parse = ra_ap_syntax::SourceFile::parse(source, Edition::Edition2024);
    let root = parse.tree().syntax().clone();

    for node in root.descendants() {
        if node.kind() == SyntaxKind::MACRO_RULES {
            process_macro_rules(&node, injector, module_prefix, name_ids, collected_names);
        }
    }
}

/// Process a MACRO_RULES node: find template TOKEN_TREEs (after =>) and scan
/// for fn patterns, injecting ctx params and guards.
fn process_macro_rules(
    macro_node: &SyntaxNode,
    injector: &mut StringInjector,
    module_prefix: &str,
    name_ids: &HashMap<String, u32>,
    collected_names: &mut Vec<String>,
) {
    let outer_tt = match macro_node.children().find(|n| n.kind() == SyntaxKind::TOKEN_TREE) {
        Some(tt) => tt,
        None => return,
    };

    let children: Vec<NodeOrToken> = outer_tt.children_with_tokens().collect();
    let len = children.len();
    let mut i = 0;

    while i < len {
        // Look for EQ + R_ANGLE (the fat arrow `=>`).
        if i + 1 < len
            && is_kind(&children[i], SyntaxKind::EQ)
            && is_kind(&children[i + 1], SyntaxKind::R_ANGLE)
        {
            let mut j = i + 2;
            j = skip_ws(&children, j);
            if j < len {
                if let ra_ap_syntax::NodeOrToken::Node(template_tt) = &children[j] {
                    if template_tt.kind() == SyntaxKind::TOKEN_TREE {
                        scan_tt_for_fns(
                            template_tt,
                            None,
                            injector,
                            module_prefix,
                            name_ids,
                            collected_names,
                        );
                    }
                }
            }
            i = j + 1;
        } else {
            i += 1;
        }
    }
}

/// Scan direct children of a TOKEN_TREE for fn definitions and inject
/// ctx params + guards. Recurses into brace TOKEN_TREEs for impl blocks.
fn scan_tt_for_fns(
    tt: &SyntaxNode,
    impl_type: Option<&MacroImplType>,
    injector: &mut StringInjector,
    module_prefix: &str,
    name_ids: &HashMap<String, u32>,
    collected_names: &mut Vec<String>,
) {
    let children: Vec<NodeOrToken> = tt.children_with_tokens().collect();
    let len = children.len();
    let mut i = 0;

    while i < len {
        if let Some(skip_to) = skip_non_instrumentable_fn(&children, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_in_children(&children, i) {
            if fm.is_metavar {
                // Metavar-named fns are skipped -- name is unknown at rewrite time.
                i = fm.body_child_index + 1;
                continue;
            }

            let fn_name = fm.name.as_deref().unwrap_or("unknown");

            // Determine qualified name for name_id lookup.
            let qualified = match impl_type {
                Some(MacroImplType::Literal(ty)) => {
                    crate::resolve::qualify(module_prefix, &format!("{ty}::{fn_name}"))
                }
                None => crate::resolve::qualify(module_prefix, fn_name),
                Some(MacroImplType::Metavar) => {
                    crate::resolve::qualify(module_prefix, fn_name)
                }
            };

            if !qualified.is_empty() {
                collected_names.push(qualified.clone());
            }

            // Injection 1: ctx parameter before the closing ')'.
            let ctx_param = if fm.has_params {
                ", __piano_ctx: piano_runtime::ctx::Ctx"
            } else {
                "__piano_ctx: piano_runtime::ctx::Ctx"
            };
            injector.insert(fm.params_close_offset, ctx_param);

            // Injection 2: guard call just after the opening '{' of the body,
            // if this function has a name_id.
            if let Some(&name_id) = name_ids.get(&qualified) {
                let guard = format!(
                    " let (__piano_guard, __piano_ctx) = __piano_ctx.enter({name_id});",
                );
                injector.insert(fm.body_open_offset, &guard);
            }

            i = fm.body_child_index + 1;
        } else {
            // Recurse into brace TOKEN_TREEs for impl blocks.
            if let ra_ap_syntax::NodeOrToken::Node(n) = &children[i] {
                if n.kind() == SyntaxKind::TOKEN_TREE
                    && starts_with_kind(n, SyntaxKind::L_CURLY)
                {
                    let detected = detect_impl_type(&children, i);
                    let recurse_impl = detected.as_ref().or(impl_type);
                    scan_tt_for_fns(
                        n,
                        recurse_impl,
                        injector,
                        module_prefix,
                        name_ids,
                        collected_names,
                    );
                }
            }
            i += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Pattern matching helpers
// ---------------------------------------------------------------------------

/// Try to match a fn pattern starting at position `start` in the children list.
/// Pattern: [pub [(...)]]? [unsafe]? [extern "Rust"]? [async]? fn NAME (...) [-> ...]? { ... }
///
/// Rejects const fn and extern fn with non-Rust ABI (matching classify()).
/// unsafe fn IS accepted -- the guard is pure safe code.
/// NAME can be an IDENT (literal name) or DOLLAR + IDENT (metavar).
fn match_fn_in_children(children: &[NodeOrToken], start: usize) -> Option<MacroFnMatch> {
    let len = children.len();
    let mut pos = start;

    pos = skip_ws(children, pos);
    if pos >= len {
        return None;
    }

    // Check for non-instrumentable prefixes at start.
    if is_const_at(children, pos) || is_non_rust_extern(children, pos) {
        return None;
    }

    // Optional: `pub`
    if is_kind(&children[pos], SyntaxKind::PUB_KW) {
        pos += 1;
        pos = skip_ws(children, pos);
        if pos >= len {
            return None;
        }

        // Optional: visibility restriction `(crate)` etc. -- a paren TOKEN_TREE.
        if let ra_ap_syntax::NodeOrToken::Node(n) = &children[pos] {
            if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_PAREN) {
                pos += 1;
                pos = skip_ws(children, pos);
                if pos >= len {
                    return None;
                }
            }
        }

        // After pub, check for non-instrumentable prefixes.
        if is_const_at(children, pos) || is_non_rust_extern(children, pos) {
            return None;
        }
    }

    // Optional: `unsafe`
    if pos < len && is_kind(&children[pos], SyntaxKind::UNSAFE_KW) {
        pos += 1;
        pos = skip_ws(children, pos);
        if pos >= len {
            return None;
        }
    }

    // Optional: `extern "Rust"` (Rust ABI is instrumentable).
    if pos < len && is_kind(&children[pos], SyntaxKind::EXTERN_KW) {
        pos += 1;
        pos = skip_ws(children, pos);
        // Skip optional ABI string literal.
        if pos < len {
            if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
                if t.kind() == SyntaxKind::STRING {
                    pos += 1;
                    pos = skip_ws(children, pos);
                }
            }
        }
        if pos >= len {
            return None;
        }
    }

    // Optional: `async`
    if pos < len && is_kind(&children[pos], SyntaxKind::ASYNC_KW) {
        pos += 1;
        pos = skip_ws(children, pos);
        if pos >= len {
            return None;
        }
    }

    // Required: `fn`
    if !is_kind(&children[pos], SyntaxKind::FN_KW) {
        return None;
    }
    pos += 1;
    pos = skip_ws(children, pos);
    if pos >= len {
        return None;
    }

    // Required: NAME -- either an IDENT or DOLLAR + IDENT (metavar).
    let mut is_metavar = false;
    let mut name: Option<String> = None;

    if is_kind(&children[pos], SyntaxKind::DOLLAR) {
        is_metavar = true;
        pos += 1;
        // The ident follows (possibly without whitespace).
        if pos < len {
            if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
                if t.kind() == SyntaxKind::IDENT {
                    pos += 1;
                }
            }
        }
        pos = skip_ws(children, pos);
        if pos >= len {
            return None;
        }
    } else if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
        if t.kind() == SyntaxKind::IDENT {
            name = Some(t.text().to_string());
            pos += 1;
            pos = skip_ws(children, pos);
            if pos >= len {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    }

    // Optional: generic parameters <...> -- flat tokens in macro bodies.
    if pos < len && is_kind(&children[pos], SyntaxKind::L_ANGLE) {
        let mut depth = 1u32;
        pos += 1;
        while pos < len && depth > 0 {
            if is_kind(&children[pos], SyntaxKind::L_ANGLE) {
                depth += 1;
            } else if is_kind(&children[pos], SyntaxKind::R_ANGLE) {
                // Don't decrement for `->` (return type arrow).
                // In ra_ap_syntax `->` tokenizes as MINUS + R_ANGLE within TOKEN_TREE,
                // but only within generic params we might see R_ANGLE. Check previous token.
                let is_arrow = pos > 0
                    && is_kind(&children[pos - 1], SyntaxKind::MINUS);
                if !is_arrow {
                    depth -= 1;
                }
            }
            pos += 1;
        }
        pos = skip_ws(children, pos);
        if pos >= len {
            return None;
        }
    }

    // Required: parameter list -- a TOKEN_TREE starting with '('.
    let (params_close_offset, has_params) = match &children[pos] {
        ra_ap_syntax::NodeOrToken::Node(n)
            if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_PAREN) =>
        {
            let close_offset = find_close_paren_offset(n)?;
            let has_p = has_params(n);
            (close_offset, has_p)
        }
        _ => return None,
    };
    pos += 1;
    pos = skip_ws(children, pos);

    // Optional: return type `-> ...` -- skip tokens until we find a brace TOKEN_TREE.
    while pos < len {
        if let ra_ap_syntax::NodeOrToken::Node(n) = &children[pos] {
            if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_CURLY) {
                break;
            }
        }
        pos += 1;
    }
    if pos >= len {
        return None;
    }

    // Required: body -- a TOKEN_TREE starting with '{'.
    let body_open_offset = match &children[pos] {
        ra_ap_syntax::NodeOrToken::Node(n)
            if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_CURLY) =>
        {
            find_open_brace_end_offset(n)?
        }
        _ => return None,
    };

    Some(MacroFnMatch {
        name,
        is_metavar,
        params_close_offset,
        has_params,
        body_open_offset,
        body_child_index: pos,
    })
}

/// If the token sequence starting at `i` starts a non-instrumentable fn
/// definition (const fn, extern fn with non-Rust ABI, unsafe extern non-Rust
/// ABI fn), return the index just past the body brace group so the caller can
/// skip the entire definition. Returns None if this is not a non-instrumentable fn.
///
/// Unlike match_fn_in_children, this requires `fn` to immediately follow the
/// modifier(s). A `const SIZE: usize = 42;` is NOT a const fn -- we must not
/// greedily scan forward and swallow the next real fn.
fn skip_non_instrumentable_fn(children: &[NodeOrToken], i: usize) -> Option<usize> {
    let len = children.len();
    let mut pos = i;

    pos = skip_ws(children, pos);
    if pos >= len {
        return None;
    }

    // Optional: pub [(...)]
    if is_kind(&children[pos], SyntaxKind::PUB_KW) {
        pos += 1;
        pos = skip_ws(children, pos);
        if pos < len {
            if let ra_ap_syntax::NodeOrToken::Node(n) = &children[pos] {
                if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_PAREN) {
                    pos += 1;
                    pos = skip_ws(children, pos);
                }
            }
        }
    }

    if pos >= len {
        return None;
    }

    // Must see one of: const, unsafe extern (non-Rust ABI), extern (non-Rust ABI)
    // -- then `fn` must follow immediately.
    if is_const_at(children, pos) {
        pos += 1;
        pos = skip_ws(children, pos);
    } else if is_kind(&children[pos], SyntaxKind::UNSAFE_KW) && is_non_rust_extern(children, skip_ws(children, pos + 1)) {
        pos += 1; // skip `unsafe`
        pos = skip_ws(children, pos);
        pos += 1; // skip `extern`
        pos = skip_ws(children, pos);
        // skip optional ABI string
        if pos < len {
            if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
                if t.kind() == SyntaxKind::STRING {
                    pos += 1;
                    pos = skip_ws(children, pos);
                }
            }
        }
    } else if is_non_rust_extern(children, pos) {
        pos += 1; // skip `extern`
        pos = skip_ws(children, pos);
        // skip optional ABI string
        if pos < len {
            if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
                if t.kind() == SyntaxKind::STRING {
                    pos += 1;
                    pos = skip_ws(children, pos);
                }
            }
        }
    } else {
        return None;
    }

    if pos >= len {
        return None;
    }

    // `fn` must be the very next token -- no greedy scanning.
    if !is_kind(&children[pos], SyntaxKind::FN_KW) {
        return None;
    }

    // Skip past fn, name, params, return type to find the body brace group.
    pos += 1;
    while pos < len {
        if let ra_ap_syntax::NodeOrToken::Node(n) = &children[pos] {
            if n.kind() == SyntaxKind::TOKEN_TREE && starts_with_kind(n, SyntaxKind::L_CURLY) {
                return Some(pos + 1);
            }
        }
        pos += 1;
    }

    None
}

/// Scan backward from position `brace_idx` to detect whether a brace group
/// is the body of an `impl Type { ... }` block.
fn detect_impl_type(children: &[NodeOrToken], brace_idx: usize) -> Option<MacroImplType> {
    if brace_idx == 0 {
        return None;
    }

    let mut pos = brace_idx - 1;
    pos = skip_ws_backward(children, pos);

    // Skip trailing type generics: `Foo<T> { ... }` -- skip past `<...>`.
    pos = skip_angle_brackets_backward(children, pos);

    // The type name is now at `pos`. It's either an IDENT or a metavar ($ident).
    let (type_result, type_start) = match &children[pos] {
        ra_ap_syntax::NodeOrToken::Token(t) if t.kind() == SyntaxKind::IDENT => {
            // Check if this ident is preceded by DOLLAR (metavar).
            let prev = if pos > 0 { skip_ws_backward(children, pos - 1) } else { 0 };
            if pos > 0 && is_kind(&children[prev], SyntaxKind::DOLLAR) {
                (MacroImplType::Metavar, prev)
            } else {
                (MacroImplType::Literal(t.text().to_string()), pos)
            }
        }
        _ => return None,
    };

    if type_start == 0 {
        return None;
    }

    let mut scan = type_start - 1;
    scan = skip_ws_backward(children, scan);

    // Check for `for` keyword -- indicates trait impl.
    // Inside TOKEN_TREE, `for` is tokenized as FOR_KW.
    if is_kind(&children[scan], SyntaxKind::FOR_KW) {
        if scan == 0 {
            return None;
        }
        scan -= 1;
        scan = skip_ws_backward(children, scan);

        // Skip the trait name (and optional trailing generics).
        scan = skip_angle_brackets_backward(children, scan);

        // Now at trait name ident.
        match &children[scan] {
            ra_ap_syntax::NodeOrToken::Token(t) if t.kind() == SyntaxKind::IDENT => {
                // Check if preceded by $ (metavar trait).
                if scan > 0 {
                    let prev = skip_ws_backward(children, scan - 1);
                    if is_kind(&children[prev], SyntaxKind::DOLLAR) {
                        scan = prev;
                    }
                }
            }
            _ => return None,
        }

        if scan == 0 {
            return None;
        }
        scan -= 1;
        scan = skip_ws_backward(children, scan);

        // Skip optional generic params on impl: `impl<T>`.
        scan = skip_angle_brackets_backward(children, scan);

        // Inside TOKEN_TREE, `impl` is tokenized as IMPL_KW.
        if is_kind(&children[scan], SyntaxKind::IMPL_KW) {
            return Some(type_result);
        }
        return None;
    }

    // No `for` -- this is a direct impl: `impl [<T>] Type { ... }`.
    scan = skip_angle_brackets_backward(children, scan);

    // Inside TOKEN_TREE, `impl` is tokenized as IMPL_KW.
    if is_kind(&children[scan], SyntaxKind::IMPL_KW) {
        return Some(type_result);
    }

    None
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

/// Check if a NodeOrToken is a token with the given kind.
fn is_kind(child: &NodeOrToken, kind: SyntaxKind) -> bool {
    matches!(child, ra_ap_syntax::NodeOrToken::Token(t) if t.kind() == kind)
}

/// Check if position `pos` has a CONST_KW token.
fn is_const_at(children: &[NodeOrToken], pos: usize) -> bool {
    pos < children.len() && is_kind(&children[pos], SyntaxKind::CONST_KW)
}

/// Check if `extern` at position `pos` has a non-Rust ABI (i.e. should be skipped).
/// Returns true for `extern fn`, `extern "C" fn`, etc.
/// Returns false for `extern "Rust" fn` (instrumentable, matching AST-level behavior).
fn is_non_rust_extern(children: &[NodeOrToken], pos: usize) -> bool {
    if pos >= children.len() || !is_kind(&children[pos], SyntaxKind::EXTERN_KW) {
        return false;
    }
    // Peek at next non-whitespace token: if it's a "Rust" string literal, this is instrumentable.
    let next = skip_ws(children, pos + 1);
    if next < children.len() {
        if let ra_ap_syntax::NodeOrToken::Token(t) = &children[next] {
            if t.kind() == SyntaxKind::STRING {
                let s = t.text();
                if s == "\"Rust\"" {
                    return false;
                }
            }
        }
    }
    true
}

/// Skip whitespace tokens starting at `pos`.
fn skip_ws(children: &[NodeOrToken], mut pos: usize) -> usize {
    while pos < children.len() {
        if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
            if t.kind() == SyntaxKind::WHITESPACE {
                pos += 1;
                continue;
            }
        }
        break;
    }
    pos
}

/// Skip whitespace tokens going backward from `pos`.
fn skip_ws_backward(children: &[NodeOrToken], mut pos: usize) -> usize {
    while pos > 0 {
        if let ra_ap_syntax::NodeOrToken::Token(t) = &children[pos] {
            if t.kind() == SyntaxKind::WHITESPACE {
                pos -= 1;
                continue;
            }
        }
        break;
    }
    pos
}

/// Check if a node starts with a token of the given kind.
fn starts_with_kind(node: &SyntaxNode, kind: SyntaxKind) -> bool {
    node.children_with_tokens()
        .next()
        .is_some_and(|c| matches!(c, ra_ap_syntax::NodeOrToken::Token(t) if t.kind() == kind))
}

/// Find the byte offset of the closing ')' inside a paren TOKEN_TREE.
fn find_close_paren_offset(tt: &SyntaxNode) -> Option<usize> {
    let mut offset = None;
    for child in tt.children_with_tokens() {
        if let ra_ap_syntax::NodeOrToken::Token(t) = child {
            if t.kind() == SyntaxKind::R_PAREN {
                offset = Some(usize::from(t.text_range().start()));
            }
        }
    }
    offset
}

/// Check if a paren TOKEN_TREE has any actual parameters (non-whitespace, non-paren content).
fn has_params(tt: &SyntaxNode) -> bool {
    tt.children_with_tokens().any(|child| match child {
        ra_ap_syntax::NodeOrToken::Token(t) => {
            t.kind() != SyntaxKind::L_PAREN
                && t.kind() != SyntaxKind::R_PAREN
                && t.kind() != SyntaxKind::WHITESPACE
        }
        ra_ap_syntax::NodeOrToken::Node(_) => true,
    })
}

/// Find the byte offset just after the opening '{' of a brace TOKEN_TREE.
fn find_open_brace_end_offset(tt: &SyntaxNode) -> Option<usize> {
    for child in tt.children_with_tokens() {
        if let ra_ap_syntax::NodeOrToken::Token(t) = child {
            if t.kind() == SyntaxKind::L_CURLY {
                return Some(usize::from(t.text_range().end()));
            }
        }
    }
    None
}

/// Scan backward from `pos` over angle-bracket generics like `<T>` or `<K, V>`.
fn skip_angle_brackets_backward(children: &[NodeOrToken], pos: usize) -> usize {
    if !is_kind(&children[pos], SyntaxKind::R_ANGLE) {
        return pos;
    }

    let mut depth: usize = 1;
    let mut cur = pos;
    while depth > 0 {
        if cur == 0 {
            return pos;
        }
        cur -= 1;
        if is_kind(&children[cur], SyntaxKind::R_ANGLE) {
            depth += 1;
        } else if is_kind(&children[cur], SyntaxKind::L_ANGLE) {
            depth -= 1;
        }
    }
    // `cur` is now at the `<`. Return position before it.
    if cur == 0 {
        return 0;
    }
    let prev = skip_ws_backward(children, cur - 1);
    prev
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::rewrite::instrument_source;

    /// Test helper: calls instrument_source with all_instrumentable derived from
    /// measured keys (sufficient for tests that don't exercise call-site injection).
    /// Pre-scans for macro function names and assigns IDs starting after the
    /// highest ID in `measured`.
    fn instrument(
        source: &str,
        measured: &HashMap<String, u32>,
        instrument_macros: bool,
        module_prefix: &str,
    ) -> Result<crate::rewrite::InstrumentResult, String> {
        let all_instrumentable: HashSet<String> = measured.keys().cloned().collect();
        // Build macro name_ids by pre-scanning, mirroring main.rs behavior.
        let mut macro_name_ids: HashMap<String, u32> = measured.clone();
        let mut next_id: u32 = measured.values().copied().max().map_or(0, |m| m + 1);
        if instrument_macros {
            if let Ok(names) = super::discover_macro_fn_names(source, module_prefix) {
                for name in names {
                    macro_name_ids.entry(name).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    });
                }
            }
        }
        instrument_source(source, measured, &all_instrumentable, instrument_macros, module_prefix, &macro_name_ids)
    }

    /// Check if the output contains the ctx parameter, tolerating whitespace
    /// differences.
    fn contains_ctx_param(source: &str) -> bool {
        let stripped: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        stripped.contains("__piano_ctx:piano_runtime::ctx::Ctx")
    }

    /// Count occurrences of profiling guards (`__piano_ctx.enter(`), tolerating
    /// line breaks and spaces.
    fn count_profiling_guards(source: &str) -> usize {
        let stripped: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        stripped.matches("__piano_ctx.enter(").count()
    }

    /// Count occurrences of the ctx parameter in function signatures.
    fn count_ctx_params(source: &str) -> usize {
        let stripped: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        stripped.matches("__piano_ctx:piano_runtime::ctx::Ctx").count()
    }

    #[test]
    fn skips_fn_in_macro_rules_metavar_name() {
        let source = r#"
macro_rules! make_handler {
    ($name:ident) => {
        fn $name() {
            do_work();
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Metavar-name functions are skipped -- name is unknown at rewrite time.
        assert!(
            !result.contains("__piano_ctx"),
            "macro fn with metavar name should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_macro_rules_literal_name() {
        let source = r#"
macro_rules! setup {
    () => {
        fn initialize() {
            startup();
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            count_profiling_guards(&result) > 0,
            "macro fn with literal name should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "macro fn with literal name should get ctx parameter. Got:\n{result}"
        );
    }

    #[test]
    fn skips_pub_async_fn_with_metavar_name_in_macro_rules() {
        let source = r#"
macro_rules! make_async {
    ($name:ident) => {
        pub async fn $name() {
            work().await;
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Metavar-name functions are skipped -- name is unknown at rewrite time.
        assert!(
            !result.contains("__piano_ctx"),
            "pub async fn with metavar name should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn skips_const_and_extern_fn_in_macro_rules() {
        let source = r#"
macro_rules! special_fns {
    () => {
        const fn fixed() -> usize { 42 }
        unsafe fn danger() {}
        extern "C" fn ffi() {}
        pub const fn pub_fixed() -> usize { 42 }
        pub unsafe fn pub_danger() {}
        pub extern "C" fn pub_ffi() {}
        fn normal() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // unsafe fn IS instrumentable (guard is safe code): normal + danger + pub_danger = 3
        let enter_count = count_profiling_guards(&result);
        assert_eq!(
            enter_count, 3,
            "normal + unsafe fns should be instrumented, not const/extern. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_extern_rust_fn_in_macro_rules() {
        let source = r#"
macro_rules! abi_fns {
    () => {
        extern "Rust" fn rust_abi() { work(); }
        extern "C" fn c_abi() {}
        pub extern "Rust" fn pub_rust_abi() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Both Rust-ABI fns should get ctx params and guards.
        let enter_count = count_profiling_guards(&result);
        assert_eq!(
            enter_count, 2,
            "extern \"Rust\" fns should be instrumented with profiling guards. Got:\n{result}"
        );
        let ctx_count = count_ctx_params(&result);
        assert_eq!(
            ctx_count, 2,
            "extern \"Rust\" fns should get ctx params. Got:\n{result}"
        );
    }

    #[test]
    fn const_variable_does_not_swallow_next_fn() {
        let source = r#"
macro_rules! with_const {
    () => {
        const SIZE: usize = 42;
        fn process() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            count_profiling_guards(&result) > 0,
            "fn after const variable should still be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn skips_multiple_metavar_fns_in_one_macro_rule() {
        let source = r#"
macro_rules! make_pair {
    ($a:ident, $b:ident) => {
        fn $a() { work_a(); }
        fn $b() { work_b(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            !result.contains("__piano_ctx"),
            "both metavar-name fns should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn mixed_metavar_and_literal_fn_names() {
        let source = r#"
macro_rules! mixed {
    ($dyn_name:ident) => {
        fn $dyn_name() { dynamic_work(); }
        fn fixed_name() { static_work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Only fixed_name should be instrumented; $dyn_name should be skipped.
        let enter_count = count_profiling_guards(&result);
        assert_eq!(
            enter_count, 1,
            "only literal-name fn should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn impl_block_in_macro_gets_qualified_name() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Cruncher {
            fn crunch(&self) { work(); }
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "");
        let ir = result.unwrap();

        assert!(
            ir.macro_fn_names.contains(&"Cruncher::crunch".to_string()),
            "impl method should have qualified name. Got: {:?}",
            ir.macro_fn_names,
        );
    }

    #[test]
    fn discover_collects_literal_names_only() {
        let source = r#"
macro_rules! m {
    ($name:ident) => {
        fn $name() { body(); }
        fn fixed() { body2(); }
    };
}
"#;
        let names = super::discover_macro_fn_names(source, "").unwrap();
        assert_eq!(names, vec!["fixed"]);
    }

    #[test]
    fn discover_with_module_prefix() {
        let source = r#"
macro_rules! m {
    () => {
        fn helper() { work(); }
    };
}
"#;
        let names = super::discover_macro_fn_names(source, "mymod").unwrap();
        assert_eq!(names, vec!["mymod::helper"]);
    }

    #[test]
    fn discover_impl_qualified_names() {
        let source = r#"
macro_rules! m {
    () => {
        impl Widget {
            fn render(&self) { draw(); }
        }
    };
}
"#;
        let names = super::discover_macro_fn_names(source, "").unwrap();
        assert_eq!(names, vec!["Widget::render"]);
    }

    #[test]
    fn multiple_arms_instrumented() {
        let source = r#"
macro_rules! multi {
    (a) => {
        fn arm_a() { work_a(); }
    };
    (b) => {
        fn arm_b() { work_b(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        let enter_count = count_profiling_guards(&result);
        assert_eq!(
            enter_count, 2,
            "both arms should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn fn_with_existing_params_gets_comma() {
        let source = r#"
macro_rules! m {
    () => {
        fn process(x: i32, y: &str) { work(x, y); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(", __piano_ctx: piano_runtime::ctx::Ctx"),
            "should have comma before ctx param when params exist. Got:\n{result}"
        );
    }

    #[test]
    fn fn_with_return_type_instrumented() {
        let source = r#"
macro_rules! m {
    () => {
        fn compute() -> u64 { 42 }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            contains_ctx_param(&result),
            "fn with return type should be instrumented. Got:\n{result}"
        );
        assert!(
            result.contains("-> u64"),
            "return type should be preserved. Got:\n{result}"
        );
    }

    #[test]
    fn pub_crate_fn_instrumented() {
        let source = r#"
macro_rules! m {
    () => {
        pub(crate) fn internal() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            contains_ctx_param(&result),
            "pub(crate) fn should be instrumented. Got:\n{result}"
        );
    }
}
