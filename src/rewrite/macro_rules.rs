use std::collections::HashMap;

use quote::quote;
use syn::visit_mut::VisitMut;

/// Result of matching a fn pattern in a flat token vector.
struct FnMatch {
    /// The tokens that form the function name (either a single Ident for
    /// literal names, or [Punct('$'), Ident] for metavar names).
    name_tokens: Vec<proc_macro2::TokenTree>,
    /// Index of the Parenthesis Group containing the parameter list.
    params_index: usize,
    /// Index of the brace Group containing the function body.
    body_index: usize,
}

/// Check whether an `ItemMacro` is a `macro_rules!` definition.
fn is_macro_rules(item: &syn::ItemMacro) -> bool {
    item.mac.path.is_ident("macro_rules")
}

/// Pre-scan source for literal function names inside macro_rules! bodies.
/// Returns qualified names (with module_prefix) for all literal-name functions
/// found. Metavar names ($name) are excluded.
///
/// This is the read-only counterpart to instrument_macro_tokens -- same walk
/// logic, but only collects names without modifying anything.
pub fn discover_macro_fn_names(source: &str, module_prefix: &str) -> Result<Vec<String>, syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut names = Vec::new();
    for item in &file.items {
        if let syn::Item::Macro(item_macro) = item {
            if is_macro_rules(item_macro) {
                collect_names_from_macro_tokens(&item_macro.mac.tokens, module_prefix, &mut names);
            }
        }
    }
    Ok(names)
}

/// Walk a macro_rules! token stream collecting literal fn names from rule
/// arm templates. Same structure as instrument_macro_tokens but read-only.
fn collect_names_from_macro_tokens(
    tokens: &proc_macro2::TokenStream,
    module_prefix: &str,
    names: &mut Vec<String>,
) {
    let tts: Vec<proc_macro2::TokenTree> = tokens.clone().into_iter().collect();
    let len = tts.len();
    let mut i = 0;

    while i < len {
        if is_fat_arrow(&tts, i) {
            let template_idx = i + 2;
            if template_idx < len {
                if let proc_macro2::TokenTree::Group(ref group) = tts[template_idx] {
                    let inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    collect_fn_names_in_tokens(&inner, None, module_prefix, names);
                }
            }
            i += 3;
        } else {
            i += 1;
        }
    }
}

/// Scan a flat token vector collecting literal fn names. Recurses into brace
/// groups for impl blocks. Read-only counterpart to inject_fn_guards_in_tokens.
fn collect_fn_names_in_tokens(
    tokens: &[proc_macro2::TokenTree],
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
    names: &mut Vec<String>,
) {
    let mut i = 0;
    while i < tokens.len() {
        if let Some(skip_to) = skip_non_instrumentable_fn(tokens, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_pattern(tokens, i) {
            let body_idx = fm.body_index;
            let is_metavar_name = fm.name_tokens.len() == 2
                && matches!(&fm.name_tokens[0], proc_macro2::TokenTree::Punct(p) if p.as_char() == '$');

            if !is_metavar_name {
                let method_str = match &fm.name_tokens[0] {
                    proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
                    _ => unreachable!(
                        "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
                    ),
                };
                let qualified = match impl_type {
                    Some(MacroImplType::Literal(ty)) => {
                        crate::resolve::qualify(module_prefix, &format!("{ty}::{method_str}"))
                    }
                    None => crate::resolve::qualify(module_prefix, &method_str),
                    Some(MacroImplType::Metavar(_)) => {
                        // Can't resolve at rewrite time -- use bare method name.
                        crate::resolve::qualify(module_prefix, &method_str)
                    }
                };
                if !qualified.is_empty() {
                    names.push(qualified);
                }
            }
            i = body_idx + 1;
        } else {
            if let proc_macro2::TokenTree::Group(ref group) = tokens[i] {
                if group.delimiter() == proc_macro2::Delimiter::Brace {
                    let detected = detect_impl_type(tokens, i);
                    let recurse_impl = detected.as_ref().or(impl_type);
                    let inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    collect_fn_names_in_tokens(&inner, recurse_impl, module_prefix, names);
                }
            }
            i += 1;
        }
    }
}

/// Top-level entry: scan a macro_rules! token stream for rule arms and
/// instrument any fn items found in each arm's template body.
fn instrument_macro_tokens(
    tokens: &mut proc_macro2::TokenStream,
    module_prefix: &str,
    collected_names: &mut Vec<String>,
    name_ids: &HashMap<String, u32>,
) {
    let mut tts: Vec<proc_macro2::TokenTree> = tokens.clone().into_iter().collect();
    let len = tts.len();
    let mut i = 0;
    let mut modified = false;

    while i < len {
        // Look for `=> { ... }` — the pattern separator followed by the
        // template group. The `=>` is two Punct tokens: `=` then `>`.
        if is_fat_arrow(&tts, i) {
            let template_idx = i + 2;
            if template_idx < len {
                if let proc_macro2::TokenTree::Group(ref group) = tts[template_idx] {
                    // Templates can use brace, paren, or bracket delimiters.
                    let mut inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    inject_fn_guards_in_tokens(
                        &mut inner,
                        None,
                        module_prefix,
                        collected_names,
                        name_ids,
                    );
                    let new_stream: proc_macro2::TokenStream = inner.into_iter().collect();
                    let mut new_group = proc_macro2::Group::new(group.delimiter(), new_stream);
                    new_group.set_span(group.span());
                    tts[template_idx] = proc_macro2::TokenTree::Group(new_group);
                    modified = true;
                }
            }
            i += 3;
        } else {
            i += 1;
        }
    }

    if modified {
        *tokens = tts.into_iter().collect();
    }
}

/// Check if tokens[i..i+2] form a fat arrow `=>`.
fn is_fat_arrow(tokens: &[proc_macro2::TokenTree], i: usize) -> bool {
    if i + 1 >= tokens.len() {
        return false;
    }
    matches!(
        (&tokens[i], &tokens[i + 1]),
        (
            proc_macro2::TokenTree::Punct(eq),
            proc_macro2::TokenTree::Punct(gt),
        ) if eq.as_char() == '=' && eq.spacing() == proc_macro2::Spacing::Joint && gt.as_char() == '>'
    )
}

/// Whether the impl block's Self type is a literal identifier or a metavar.
enum MacroImplType {
    /// A literal type name, e.g. `impl Cruncher { ... }`.
    Literal(String),
    /// A metavar type, e.g. `impl $ty { ... }`.
    #[allow(dead_code)]
    Metavar(proc_macro2::TokenTree),
}

/// Scan backward from `pos` over angle-bracket generics like `<T>` or `<K, V>`.
/// If the token at `pos` is `>`, walks backward counting nesting depth until the
/// matching `<` is found, then returns the position before the `<`.
/// If the token at `pos` is not `>`, returns `pos` unchanged.
fn skip_angle_brackets_backward(tokens: &[proc_macro2::TokenTree], pos: usize) -> usize {
    let is_close_angle =
        matches!(&tokens[pos], proc_macro2::TokenTree::Punct(p) if p.as_char() == '>');
    if !is_close_angle {
        return pos;
    }

    let mut depth: usize = 1;
    let mut cur = pos;
    while depth > 0 {
        if cur == 0 {
            // Unbalanced — give up and return original position.
            return pos;
        }
        cur -= 1;
        match &tokens[cur] {
            proc_macro2::TokenTree::Punct(p) if p.as_char() == '>' => depth += 1,
            proc_macro2::TokenTree::Punct(p) if p.as_char() == '<' => depth -= 1,
            _ => {}
        }
    }
    // `cur` is now at the `<`. Return position before it.
    if cur == 0 {
        return 0;
    }
    cur - 1
}

/// Scan backward from position `brace_idx` to detect whether a brace group
/// is the body of an `impl Type { ... }` block. Handles:
///   - `impl Foo { ... }` -> Literal("Foo")
///   - `impl $ty { ... }` -> Metavar(ty_ident)
///   - `impl<T> Foo<T> { ... }` -> Literal("Foo")
///   - `impl Trait for Foo { ... }` -> Literal("Foo")
///   - `impl $trait for $ty { ... }` -> Metavar(ty_ident)
///   - `impl<T> Trait<T> for Foo<T> { ... }` -> Literal("Foo")
fn detect_impl_type(tokens: &[proc_macro2::TokenTree], brace_idx: usize) -> Option<MacroImplType> {
    if brace_idx == 0 {
        return None;
    }

    let mut pos = brace_idx - 1;

    // Skip trailing type generics: `Foo<T> { ... }` — skip past `<T>`.
    // When parsing source text, `<T>` is tokenized as individual `<`, `T`, `>`
    // Punct tokens, not as an invisible group.
    pos = skip_angle_brackets_backward(tokens, pos);

    // The type name is now at `pos`. It's either an Ident or a metavar ($ident).
    let (type_result, type_start) = match &tokens[pos] {
        proc_macro2::TokenTree::Ident(ident) => {
            // Check if this ident is preceded by `$` (metavar).
            if pos > 0 {
                if let proc_macro2::TokenTree::Punct(p) = &tokens[pos - 1] {
                    if p.as_char() == '$' {
                        (MacroImplType::Metavar(tokens[pos].clone()), pos - 1)
                    } else {
                        (MacroImplType::Literal(ident.to_string()), pos)
                    }
                } else {
                    (MacroImplType::Literal(ident.to_string()), pos)
                }
            } else {
                (MacroImplType::Literal(ident.to_string()), pos)
            }
        }
        _ => return None,
    };

    // Now scan backward from type_start to find `impl`.
    // Possible patterns before the type:
    //   `impl`                     (direct impl)
    //   `impl<T>`                  (generic impl)
    //   `impl Trait for`           (trait impl)
    //   `impl<T> Trait<T> for`     (generic trait impl)
    //   `impl $trait for`          (metavar trait impl)
    if type_start == 0 {
        return None;
    }

    let mut scan = type_start - 1;

    // Check for `for` keyword — indicates trait impl.
    if is_ident(tokens, scan, "for") {
        // This is `impl [Trait] for Type { ... }`.
        // We already have the type — just verify `impl` is somewhere before `for`.
        if scan == 0 {
            return None;
        }
        scan -= 1;

        // Skip the trait name (and optional trailing generics).
        // Trait could be: Ident, Ident<T>, $trait ($ Ident), $trait<T>.
        scan = skip_angle_brackets_backward(tokens, scan);

        // Now at trait name ident.
        match &tokens[scan] {
            proc_macro2::TokenTree::Ident(_) => {
                // Check if preceded by $ (metavar trait).
                if scan > 0 {
                    if let proc_macro2::TokenTree::Punct(p) = &tokens[scan - 1] {
                        if p.as_char() == '$' {
                            scan -= 1; // skip the $
                        }
                    }
                }
            }
            _ => return None,
        }

        if scan == 0 {
            return None;
        }
        scan -= 1;

        // Skip optional generic params on impl: `impl<T>`.
        scan = skip_angle_brackets_backward(tokens, scan);

        // Must be `impl`.
        if is_ident(tokens, scan, "impl") {
            return Some(type_result);
        }
        return None;
    }

    // No `for` — this is a direct impl: `impl [<T>] Type { ... }`.
    // Skip optional generic params on impl.
    scan = skip_angle_brackets_backward(tokens, scan);

    // Must be `impl`.
    if is_ident(tokens, scan, "impl") {
        return Some(type_result);
    }

    None
}

/// Scan a flat token vector for fn patterns and inject guards + ctx
/// parameters into each matched function. Recurses into brace groups to
/// handle fn items inside impl blocks within macro templates.
fn inject_fn_guards_in_tokens(
    tokens: &mut [proc_macro2::TokenTree],
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
    collected_names: &mut Vec<String>,
    name_ids: &HashMap<String, u32>,
) {
    let mut i = 0;
    while i < tokens.len() {
        // Skip non-instrumentable fn definitions (const fn, extern fn with non-Rust ABI)
        // so we don't accidentally match the inner `fn` keyword.
        if let Some(skip_to) = skip_non_instrumentable_fn(tokens, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_pattern(tokens, i) {
            let params_idx = fm.params_index;
            let body_idx = fm.body_index;

            // Collect literal (non-metavar) function names for registration.
            let is_metavar_name = fm.name_tokens.len() == 2
                && matches!(&fm.name_tokens[0], proc_macro2::TokenTree::Punct(p) if p.as_char() == '$');

            if !is_metavar_name {
                let method_str = match &fm.name_tokens[0] {
                    proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
                    _ => unreachable!(
                        "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
                    ),
                };
                let qualified = match impl_type {
                    Some(MacroImplType::Literal(ty)) => {
                        crate::resolve::qualify(module_prefix, &format!("{ty}::{method_str}"))
                    }
                    None => crate::resolve::qualify(module_prefix, &method_str),
                    Some(MacroImplType::Metavar(_)) => {
                        // Can't resolve qualified name -- use bare method name.
                        crate::resolve::qualify(module_prefix, &method_str)
                    }
                };
                if !qualified.is_empty() {
                    collected_names.push(qualified.clone());
                }

                // Build the profiling guard if we have a name_id for this function.
                let guard_opt = make_guard_tokens(
                    &fm.name_tokens,
                    impl_type,
                    module_prefix,
                    name_ids,
                );

                // Inject ctx parameter into the parenthesized param list.
                inject_ctx_param(tokens, params_idx);

                // Inject guard tokens at the start of the body brace group.
                if let Some(guard) = guard_opt {
                    let guard_tts: Vec<proc_macro2::TokenTree> = guard.into_iter().collect();
                    if let proc_macro2::TokenTree::Group(ref group) = tokens[body_idx] {
                        let mut body_tts: Vec<proc_macro2::TokenTree> =
                            group.stream().into_iter().collect();
                        for (j, tt) in guard_tts.into_iter().enumerate() {
                            body_tts.insert(j, tt);
                        }
                        let new_stream: proc_macro2::TokenStream = body_tts.into_iter().collect();
                        let mut new_group =
                            proc_macro2::Group::new(proc_macro2::Delimiter::Brace, new_stream);
                        new_group.set_span(group.span());
                        tokens[body_idx] = proc_macro2::TokenTree::Group(new_group);
                    }
                }
            }
            // Advance past the body group.
            i = body_idx + 1;
        } else {
            // Recurse into brace groups (handles fn inside impl blocks, etc.).
            if let proc_macro2::TokenTree::Group(ref group) = tokens[i] {
                if group.delimiter() == proc_macro2::Delimiter::Brace {
                    let detected = detect_impl_type(tokens, i);
                    let recurse_impl = detected.as_ref().or(impl_type);
                    let mut inner: Vec<proc_macro2::TokenTree> =
                        group.stream().into_iter().collect();
                    inject_fn_guards_in_tokens(
                        &mut inner,
                        recurse_impl,
                        module_prefix,
                        collected_names,
                        name_ids,
                    );
                    let new_stream: proc_macro2::TokenStream = inner.into_iter().collect();
                    let mut new_group =
                        proc_macro2::Group::new(proc_macro2::Delimiter::Brace, new_stream);
                    new_group.set_span(group.span());
                    tokens[i] = proc_macro2::TokenTree::Group(new_group);
                }
            }
            i += 1;
        }
    }
}

/// Inject `__piano_ctx: piano_runtime::ctx::Ctx` as the last parameter in
/// the parenthesized parameter list at `params_idx`.
fn inject_ctx_param(tokens: &mut [proc_macro2::TokenTree], params_idx: usize) {
    if let proc_macro2::TokenTree::Group(ref group) = tokens[params_idx] {
        let existing: proc_macro2::TokenStream = group.stream();
        let ctx_param = quote! { __piano_ctx: piano_runtime::ctx::Ctx };
        let new_stream = if existing.is_empty() {
            ctx_param
        } else {
            let comma = proc_macro2::Punct::new(',', proc_macro2::Spacing::Alone);
            let mut tts: Vec<proc_macro2::TokenTree> = existing.into_iter().collect();
            tts.push(proc_macro2::TokenTree::Punct(comma));
            tts.extend(ctx_param);
            tts.into_iter().collect()
        };
        let mut new_group =
            proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, new_stream);
        new_group.set_span(group.span());
        tokens[params_idx] = proc_macro2::TokenTree::Group(new_group);
    }
}

/// If the token at position `i` starts a non-instrumentable fn definition
/// (const fn, extern fn with non-Rust ABI, unsafe extern non-Rust ABI fn),
/// return the index just past the body brace group so the caller can skip
/// the entire definition. Returns None if this is not a non-instrumentable fn.
/// unsafe fn alone is instrumentable (guard is pure safe code) and handled
/// by match_fn_pattern.
///
/// Unlike match_fn_pattern, this requires `fn` to immediately follow the
/// modifier(s). A `const SIZE: usize = 42;` is NOT a const fn -- we must not
/// greedily scan forward and swallow the next real fn.
fn skip_non_instrumentable_fn(tokens: &[proc_macro2::TokenTree], i: usize) -> Option<usize> {
    let len = tokens.len();
    let mut pos = i;

    // Optional: pub [(...)]
    if is_ident(tokens, pos, "pub") {
        pos += 1;
        if pos < len {
            if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
                if g.delimiter() == proc_macro2::Delimiter::Parenthesis {
                    pos += 1;
                }
            }
        }
    }

    // Must see one of: const, unsafe extern (non-Rust ABI), extern (non-Rust ABI)
    // — then `fn` must follow immediately.
    // unsafe fn alone is instrumentable (guard is pure safe code) and handled by match_fn_pattern.
    if is_ident(tokens, pos, "const") {
        pos += 1;
    } else if is_ident(tokens, pos, "unsafe") && is_non_rust_extern(tokens, pos + 1) {
        pos += 1; // skip `unsafe`
        pos += 1; // skip `extern`
        if pos < len {
            if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                pos += 1; // skip ABI string like "C"
            }
        }
    } else if is_non_rust_extern(tokens, pos) {
        pos += 1;
        // extern "ABI" fn
        if pos < len {
            if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                pos += 1; // skip ABI string like "C"
            }
        }
    } else {
        return None;
    }

    // `fn` must be the very next token — no greedy scanning.
    if !is_ident(tokens, pos, "fn") {
        return None;
    }

    // Skip past fn, name, params, return type to find the body brace group.
    pos += 1;
    while pos < len {
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Brace {
                return Some(pos + 1);
            }
        }
        pos += 1;
    }

    None
}

/// Try to match a fn pattern starting at position `start`:
///   [pub [( ... )]]? [unsafe]? [extern "Rust"]? [async]? fn NAME ( ... ) [-> ...]? { ... }
///
/// Rejects const fn and extern fn with non-Rust ABI (matching classify()).
/// unsafe fn IS accepted -- the guard is pure safe code, so unsafe fn is instrumentable.
/// NAME can be an Ident (literal name) or $metavar (Punct('$') + Ident).
fn match_fn_pattern(tokens: &[proc_macro2::TokenTree], start: usize) -> Option<FnMatch> {
    let len = tokens.len();
    let mut pos = start;

    // Check for non-instrumentable prefixes: const, extern (non-Rust ABI).
    // If we see any of these before `fn`, skip this position.
    if is_ident(tokens, pos, "const")
        || is_non_rust_extern(tokens, pos)
    {
        return None;
    }

    // Optional: `pub` with optional visibility group like `pub(crate)`.
    if is_ident(tokens, pos, "pub") {
        pos += 1;
        if pos >= len {
            return None;
        }
        // Skip optional visibility restriction group: `(crate)`, `(super)`, etc.
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Parenthesis {
                pos += 1;
                if pos >= len {
                    return None;
                }
            }
        }

        // After pub, check for non-instrumentable prefixes.
        if is_ident(tokens, pos, "const")
            || is_non_rust_extern(tokens, pos)
        {
            return None;
        }
    }

    // Optional: `unsafe`.
    if is_ident(tokens, pos, "unsafe") {
        pos += 1;
        if pos >= len {
            return None;
        }
    }

    // Optional: `extern "Rust"` (Rust ABI is instrumentable).
    if is_ident(tokens, pos, "extern") {
        pos += 1; // skip `extern`
        if pos < len {
            if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                pos += 1; // skip `"Rust"`
            }
        }
        if pos >= len {
            return None;
        }
    }

    // Optional: `async`.
    if is_ident(tokens, pos, "async") {
        pos += 1;
        if pos >= len {
            return None;
        }
    }

    // Required: `fn`.
    if !is_ident(tokens, pos, "fn") {
        return None;
    }
    pos += 1;
    if pos >= len {
        return None;
    }

    // Required: NAME — either a plain Ident or $metavar (Punct('$') + Ident).
    let name_tokens: Vec<proc_macro2::TokenTree>;
    if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
        if p.as_char() == '$' {
            // Metavar: $name
            if pos + 1 >= len {
                return None;
            }
            if let proc_macro2::TokenTree::Ident(_) = &tokens[pos + 1] {
                name_tokens = vec![tokens[pos].clone(), tokens[pos + 1].clone()];
                pos += 2;
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else if let proc_macro2::TokenTree::Ident(_) = &tokens[pos] {
        name_tokens = vec![tokens[pos].clone()];
        pos += 1;
    } else {
        return None;
    }

    if pos >= len {
        return None;
    }

    // Optional: generic parameters `<...>`. In macro token streams, angle
    // brackets are individual Punct tokens, so we track nesting depth to skip
    // past the entire generic parameter list.
    if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
        if p.as_char() == '<' {
            let mut depth = 1u32;
            pos += 1;
            while pos < len && depth > 0 {
                if let proc_macro2::TokenTree::Punct(p) = &tokens[pos] {
                    match p.as_char() {
                        '<' => depth += 1,
                        '>' => {
                            // Don't decrement for `->` (return type arrow).
                            // In proc_macro2, `->` is two separate Punct tokens:
                            // `-` then `>`. Check the previous token.
                            let is_arrow = pos > 0
                                && matches!(
                                    &tokens[pos - 1],
                                    proc_macro2::TokenTree::Punct(prev)
                                        if prev.as_char() == '-' && prev.spacing() == proc_macro2::Spacing::Joint
                                );
                            if !is_arrow {
                                depth -= 1;
                            }
                        }
                        _ => {}
                    }
                }
                pos += 1;
            }
            if depth > 0 || pos >= len {
                return None;
            }
        }
    }

    // Required: parameter list (parenthesized group).
    let params_pos = pos;
    if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
        if g.delimiter() != proc_macro2::Delimiter::Parenthesis {
            return None;
        }
    } else {
        return None;
    }
    pos += 1;

    // Optional: return type `-> ...` — skip tokens until we find a brace group.
    while pos < len {
        if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
            if g.delimiter() == proc_macro2::Delimiter::Brace {
                break;
            }
        }
        pos += 1;
    }

    if pos >= len {
        return None;
    }

    // Required: body (brace group).
    if let proc_macro2::TokenTree::Group(g) = &tokens[pos] {
        if g.delimiter() == proc_macro2::Delimiter::Brace {
            return Some(FnMatch {
                name_tokens,
                params_index: params_pos,
                body_index: pos,
            });
        }
    }

    None
}

/// Check if tokens[i] is an Ident matching the given name.
fn is_ident(tokens: &[proc_macro2::TokenTree], i: usize, name: &str) -> bool {
    if i >= tokens.len() {
        return false;
    }
    matches!(&tokens[i], proc_macro2::TokenTree::Ident(ident) if *ident == name)
}

/// Check if `extern` at position `i` has a non-Rust ABI (i.e. should be skipped).
/// Returns true for `extern fn`, `extern "C" fn`, etc.
/// Returns false for `extern "Rust" fn` (instrumentable, matching AST-level behavior).
fn is_non_rust_extern(tokens: &[proc_macro2::TokenTree], i: usize) -> bool {
    if !is_ident(tokens, i, "extern") {
        return false;
    }
    // Peek at next token: if it's a "Rust" string literal, this is instrumentable.
    if i + 1 < tokens.len() {
        if let proc_macro2::TokenTree::Literal(lit) = &tokens[i + 1] {
            let s = lit.to_string();
            if s == "\"Rust\"" {
                return false;
            }
        }
    }
    true
}

/// Build the profiling guard: `let (__piano_guard, __piano_ctx) = __piano_ctx.enter(N);`
fn build_profiling_guard(name_id: u32) -> proc_macro2::TokenStream {
    quote! {
        let (__piano_guard, __piano_ctx) = __piano_ctx.enter(#name_id);
    }
}

/// Build the guard statement tokens for a literal-name function, looking up
/// the name_id in the provided map. Returns None if the function name has no
/// assigned ID (guard generation is skipped).
///
/// Metavar-name functions are never passed to this function -- they are
/// skipped entirely by the caller (metavar names are unknown at rewrite time).
fn make_guard_tokens(
    name_tokens: &[proc_macro2::TokenTree],
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
    name_ids: &HashMap<String, u32>,
) -> Option<proc_macro2::TokenStream> {
    let method_str = match &name_tokens[0] {
        proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
        _ => unreachable!(
            "make_guard_tokens is only called for literal names"
        ),
    };

    // Determine the qualified name to look up in name_ids.
    let qualified = match impl_type {
        Some(MacroImplType::Literal(ty)) => {
            crate::resolve::qualify(module_prefix, &format!("{ty}::{method_str}"))
        }
        Some(MacroImplType::Metavar(_)) => {
            // Can't resolve qualified name at rewrite time -- try bare method name.
            crate::resolve::qualify(module_prefix, &method_str)
        }
        None => crate::resolve::qualify(module_prefix, &method_str),
    };

    name_ids.get(&qualified).map(|&id| build_profiling_guard(id))
}

/// Separate VisitMut that only handles macro_rules! instrumentation.
/// Kept as VisitMut because macro instrumentation modifies token streams directly.
pub(super) struct MacroInstrumenter {
    pub(super) module_prefix: String,
    pub(super) collected_names: Vec<String>,
    pub(super) name_ids: HashMap<String, u32>,
}

impl VisitMut for MacroInstrumenter {
    fn visit_item_macro_mut(&mut self, node: &mut syn::ItemMacro) {
        if is_macro_rules(node) {
            instrument_macro_tokens(
                &mut node.mac.tokens,
                &self.module_prefix,
                &mut self.collected_names,
                &self.name_ids,
            );
        }
        syn::visit_mut::visit_item_macro_mut(self, node);
    }
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
    /// differences from prettyplease.
    fn contains_ctx_param(source: &str) -> bool {
        let stripped: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        stripped.contains("__piano_ctx:piano_runtime::ctx::Ctx")
    }

    /// Count occurrences of profiling guards (`__piano_ctx.enter(`), tolerating
    /// prettyplease line breaks and spaces.
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

        // Metavar-name functions are skipped -- name is unknown at rewrite time.
        assert!(
            !result.contains("__piano_ctx"),
            "metavar-name fns should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn skips_multiple_metavar_macro_rules_arms() {
        let source = r#"
macro_rules! multi {
    (one $name:ident) => {
        fn $name() { one(); }
    };
    (two $name:ident) => {
        fn $name() { two(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Metavar-name fns in each arm are skipped.
        assert!(
            !result.contains("__piano_ctx"),
            "metavar-name fns in each rule arm should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn macro_without_fn_unchanged() {
        let source = r#"
macro_rules! log {
    ($msg:expr) => {
        println!("{}", $msg);
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
            "macro without fn should not be modified. Got:\n{result}"
        );
    }

    #[test]
    fn skips_fn_in_paren_delimited_macro_template_metavar() {
        let source = r#"
macro_rules! make {
    ($name:ident) => (
        fn $name() { work(); }
    );
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Metavar-name fn is skipped even in paren-delimited template.
        assert!(
            !result.contains("__piano_ctx"),
            "metavar fn in paren-delimited template should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn instrument_macros_false_skips_macros() {
        let source = r#"
macro_rules! make {
    ($name:ident) => {
        fn $name() { work(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, false, "")
            .unwrap()
            .source;

        assert!(
            !result.contains("__piano_ctx"),
            "instrument_macros=false should skip macro instrumentation. Got:\n{result}"
        );
    }

    #[test]
    fn skips_generic_fn_with_metavar_name_in_macro_rules() {
        let source = r#"
macro_rules! make_generic {
    ($name:ident) => {
        fn $name<T: std::fmt::Display>(val: T) -> String {
            format!("{}", val)
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Metavar-name functions are skipped.
        assert!(
            !result.contains("__piano_ctx"),
            "generic fn with metavar name should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_with_fn_trait_return_in_macro_rules() {
        let source = r#"
macro_rules! make {
    () => {
        fn process<F: Fn() -> bool>(f: F) {
            work();
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
            "generic fn with Fn() -> T bound in macro should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "generic fn with Fn() -> T bound in macro should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_with_nested_return_type_in_macro_rules() {
        let source = r#"
macro_rules! make {
    () => {
        fn apply<F: Fn(i32) -> Option<bool>>(f: F) {
            work();
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
            "generic fn with Fn() -> Option<bool> bound should get profiling guard. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_impl_metavar_type_in_macro_rules() {
        let source = r#"
macro_rules! make_impl {
    ($ty:ident) => {
        impl $ty {
            fn process() {
                work();
            }
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;

        // Literal method name in impl $ty -- gets profiling guard and ctx param.
        assert!(
            count_profiling_guards(&result) > 0,
            "fn inside impl $ty block should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "fn inside impl $ty block should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_literal_type_qualifies_method_name() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Cruncher {
            fn process() {
                work();
            }
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
            "method in impl with literal type should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "method in impl with literal type should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_generic_type_qualifies_method_name() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl<T> Container<T> {
            fn process() {
                work();
            }
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
            "generic impl method should get profiling guard. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_multiple_methods_all_instrumented() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Cruncher {
            fn new() -> Self { Self }
            pub fn crunch_small() -> u64 { 42 }
            pub fn crunch_large() -> u64 { 999 }
        }
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
            enter_count, 3,
            "all 3 methods should get profiling guards. Got:\n{result}"
        );
        let ctx_count = count_ctx_params(&result);
        assert_eq!(
            ctx_count, 3,
            "all 3 methods should get ctx params. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_trait_for_type_uses_type_name() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Display for Cruncher {
            fn fmt() {
                work();
            }
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
            "impl Trait for Type method should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "impl Trait for Type method should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_metavar_type_instruments_literal_method() {
        let source = r#"
macro_rules! make_impl {
    ($ty:ident) => {
        impl $ty {
            fn process() {
                work();
            }
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;
        // Literal method name gets profiling guard + ctx param even with metavar type.
        assert!(
            count_profiling_guards(&result) > 0,
            "method in impl $ty should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "method in impl $ty should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_metavar_type_and_metavar_name_skipped() {
        let source = r#"
macro_rules! make_impl {
    ($ty:ident, $method:ident) => {
        impl $ty {
            fn $method() {
                work();
            }
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "")
            .unwrap()
            .source;
        // Metavar method name is skipped -- name is unknown at rewrite time.
        assert!(
            !result.contains("__piano_ctx"),
            "metavar method name should be skipped. Got:\n{result}"
        );
    }

    #[test]
    fn module_prefix_qualifies_macro_fn_name() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Handler {
            fn validate() { }
        }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "api")
            .unwrap()
            .source;
        assert!(
            count_profiling_guards(&result) > 0,
            "macro fn with module prefix should get profiling guard. Got:\n{result}"
        );
        assert!(
            contains_ctx_param(&result),
            "macro fn with module prefix should get ctx param. Got:\n{result}"
        );
    }

    #[test]
    fn module_prefix_qualifies_macro_top_level_fn() {
        let source = r#"
macro_rules! make_fn {
    () => {
        fn process() { }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "worker")
            .unwrap()
            .source;
        assert!(
            count_profiling_guards(&result) > 0,
            "top-level macro fn with module prefix should get profiling guard. Got:\n{result}"
        );
    }

    #[test]
    fn macro_fn_names_collects_literal_names() {
        let source = r#"
macro_rules! make_impl {
    () => {
        impl Handler {
            fn validate() { }
            fn process() { }
        }
    };
}
macro_rules! make_fn {
    () => {
        fn standalone() { }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "api").unwrap();
        let mut names = result.macro_fn_names.clone();
        names.sort();
        assert_eq!(
            names,
            vec![
                "api::Handler::process".to_string(),
                "api::Handler::validate".to_string(),
                "api::standalone".to_string(),
            ],
            "should collect qualified literal names from macros"
        );
    }

    #[test]
    fn macro_fn_names_excludes_metavar_names() {
        let source = r#"
macro_rules! make_fn {
    ($name:ident) => {
        fn $name() { }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, u32> = HashMap::new();
        let result = instrument(source, &targets, true, "").unwrap();
        assert!(
            result.macro_fn_names.is_empty(),
            "metavar names should not be collected. Got: {:?}",
            result.macro_fn_names
        );
    }

    #[test]
    fn macro_filter_agrees_with_classify() {
        // Both paths must agree: classify() on primitive properties and
        // match_fn_pattern() on the equivalent token stream must make
        // the same accept/reject decision.
        use crate::resolve::{Classification, classify};

        // (code, is_const, abi, expected_instrumentable)
        let cases: &[(&str, bool, Option<&str>, bool)] = &[
            ("fn foo() {}", false, None, true),
            ("pub fn foo() {}", false, None, true),
            ("async fn foo() {}", false, None, true),
            ("pub async fn foo() {}", false, None, true),
            ("extern \"Rust\" fn foo() {}", false, Some("Rust"), true),
            ("pub extern \"Rust\" fn foo() {}", false, Some("Rust"), true),
            ("unsafe fn foo() {}", false, None, true),
            ("pub unsafe fn foo() {}", false, None, true),
            ("const fn foo() {}", true, None, false),
            ("extern \"C\" fn foo() {}", false, Some("C"), false),
            ("extern fn foo() {}", false, Some(""), false),
            ("pub const fn foo() {}", true, None, false),
            ("pub extern \"C\" fn foo() {}", false, Some("C"), false),
        ];

        for &(code, is_const, abi, expected_instrumentable) in cases {
            // Path 1: classify() on primitive properties
            let classify_says =
                matches!(classify(is_const, abi), Classification::Instrumentable);

            // Path 2: match_fn_pattern() on token stream
            let tokens: proc_macro2::TokenStream = code.parse().unwrap();
            let flat: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
            let macro_says = super::match_fn_pattern(&flat, 0).is_some();

            assert_eq!(
                classify_says, macro_says,
                "classify() and match_fn_pattern() disagree on '{code}': \
                 classify={classify_says}, macro={macro_says}"
            );
            assert_eq!(
                classify_says, expected_instrumentable,
                "unexpected result for '{code}': got {classify_says}, expected {expected_instrumentable}"
            );
        }
    }
}
