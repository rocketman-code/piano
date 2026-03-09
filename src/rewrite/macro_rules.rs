use quote::quote;
use syn::visit_mut::VisitMut;

/// Result of matching a fn pattern in a flat token vector.
struct FnMatch {
    /// The tokens that form the function name (either a single Ident for
    /// literal names, or [Punct('$'), Ident] for metavar names).
    name_tokens: Vec<proc_macro2::TokenTree>,
    /// Index of the brace Group containing the function body.
    body_index: usize,
}

/// Check whether an `ItemMacro` is a `macro_rules!` definition.
fn is_macro_rules(item: &syn::ItemMacro) -> bool {
    item.mac.path.is_ident("macro_rules")
}

/// Top-level entry: scan a macro_rules! token stream for rule arms and
/// instrument any fn items found in each arm's template body.
fn instrument_macro_tokens(
    tokens: &mut proc_macro2::TokenStream,
    module_prefix: &str,
    collected_names: &mut Vec<String>,
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
                    inject_fn_guards_in_tokens(&mut inner, None, module_prefix, collected_names);
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

use super::token_util::is_fat_arrow;

/// Whether the impl block's Self type is a literal identifier or a metavar.
enum MacroImplType {
    /// A literal type name, e.g. `impl Cruncher { ... }`.
    Literal(String),
    /// A metavar type, e.g. `impl $ty { ... }`.
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

/// Scan a flat token vector for fn patterns and inject guards into each
/// matched function body. Recurses into brace groups to handle fn items
/// inside impl blocks within macro templates.
fn inject_fn_guards_in_tokens(
    tokens: &mut [proc_macro2::TokenTree],
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
    collected_names: &mut Vec<String>,
) {
    let mut i = 0;
    while i < tokens.len() {
        // Skip non-instrumentable fn definitions (const fn, unsafe fn, extern fn)
        // so we don't accidentally match the inner `fn` keyword.
        if let Some(skip_to) = skip_non_instrumentable_fn(tokens, i) {
            i = skip_to;
            continue;
        }

        if let Some(fm) = match_fn_pattern(tokens, i) {
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
                    Some(MacroImplType::Metavar(_)) => String::new(),
                };
                if !qualified.is_empty() {
                    collected_names.push(qualified);
                }
            }

            // Build the guard token stream for this function name.
            let guard = make_guard_tokens(&fm.name_tokens, impl_type, module_prefix);
            let guard_tts: Vec<proc_macro2::TokenTree> = guard.into_iter().collect();

            // Inject guard tokens at the start of the body brace group.
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

/// If the token at position `i` starts a non-instrumentable fn definition
/// (const fn, unsafe fn, extern fn with non-Rust ABI), return the index just
/// past the body brace group so the caller can skip the entire definition.
/// Returns None if this is not a non-instrumentable fn.
///
/// Unlike match_fn_pattern, this requires `fn` to immediately follow the
/// modifier(s). A `const SIZE: usize = 42;` is NOT a const fn — we must not
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

    // Must see one of: const, unsafe, extern (non-Rust ABI) — then `fn` must follow immediately.
    if is_ident(tokens, pos, "const") {
        pos += 1;
    } else if is_ident(tokens, pos, "unsafe") {
        pos += 1;
        // unsafe extern "ABI" fn
        if is_ident(tokens, pos, "extern") {
            pos += 1;
            if pos < len {
                if let proc_macro2::TokenTree::Literal(_) = &tokens[pos] {
                    pos += 1; // skip ABI string like "C"
                }
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
///   [pub [( ... )]]? [extern "Rust"]? [async]? fn NAME ( ... ) [-> ...]? { ... }
///
/// Rejects const fn, unsafe fn, and extern fn with non-Rust ABI (matching classify()).
/// NAME can be an Ident (literal name) or $metavar (Punct('$') + Ident).
fn match_fn_pattern(tokens: &[proc_macro2::TokenTree], start: usize) -> Option<FnMatch> {
    let len = tokens.len();
    let mut pos = start;

    // Check for non-instrumentable prefixes: const, unsafe, extern (non-Rust ABI).
    // If we see any of these before `fn`, skip this position.
    if is_ident(tokens, pos, "const")
        || is_ident(tokens, pos, "unsafe")
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
            || is_ident(tokens, pos, "unsafe")
            || is_non_rust_extern(tokens, pos)
        {
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

/// Build a `stringify!($metavar)` token stream. `quote!` cannot emit `$`
/// tokens, so we construct manually.
fn build_stringify_call(metavar_ident: &proc_macro2::TokenTree) -> proc_macro2::TokenStream {
    let span = proc_macro2::Span::call_site();
    let dollar = proc_macro2::Punct::new('$', proc_macro2::Spacing::Alone);
    let inner: proc_macro2::TokenStream =
        vec![proc_macro2::TokenTree::Punct(dollar), metavar_ident.clone()]
            .into_iter()
            .collect();
    vec![
        proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("stringify", span)),
        proc_macro2::TokenTree::Punct(proc_macro2::Punct::new('!', proc_macro2::Spacing::Alone)),
        proc_macro2::TokenTree::Group(proc_macro2::Group::new(
            proc_macro2::Delimiter::Parenthesis,
            inner,
        )),
    ]
    .into_iter()
    .collect()
}

/// Build the full `let __piano_guard = piano_runtime::enter(ARG);` statement,
/// where `enter_arg_stream` is the token stream to place inside the parens.
fn build_enter_guard(enter_arg_stream: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let span = proc_macro2::Span::call_site();
    let enter_arg = proc_macro2::Group::new(proc_macro2::Delimiter::Parenthesis, enter_arg_stream);
    vec![
        proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("let", span)),
        proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("__piano_guard", span)),
        proc_macro2::TokenTree::Punct(proc_macro2::Punct::new('=', proc_macro2::Spacing::Alone)),
        proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("piano_runtime", span)),
        proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(':', proc_macro2::Spacing::Joint)),
        proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(':', proc_macro2::Spacing::Alone)),
        proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("enter", span)),
        proc_macro2::TokenTree::Group(enter_arg),
        proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(';', proc_macro2::Spacing::Alone)),
    ]
    .into_iter()
    .collect()
}

/// Prepend `"prefix::", ` to a concat! argument list when module_prefix is non-empty.
fn prepend_module_prefix_tokens(tts: &mut Vec<proc_macro2::TokenTree>, module_prefix: &str) {
    if !module_prefix.is_empty() {
        let prefix_with_sep = format!("{module_prefix}::");
        tts.extend(quote! { #prefix_with_sep });
        tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
            ',',
            proc_macro2::Spacing::Alone,
        )));
    }
}

/// Build the guard statement tokens for a function name, optionally qualified
/// with the enclosing impl type.
///
/// For a literal name like `initialize` in `impl Cruncher`:
///   `let __piano_guard = piano_runtime::enter("Cruncher::initialize");`
///
/// For a metavar name like `$name`:
///   `let __piano_guard = piano_runtime::enter(stringify!($name));`
///
/// For a literal name in `impl $ty`:
///   `let __piano_guard = piano_runtime::enter(concat!(stringify!($ty), "::", "method"));`
///
/// For a metavar name in `impl $ty`:
///   `let __piano_guard = piano_runtime::enter(concat!(stringify!($ty), "::", stringify!($name)));`
fn make_guard_tokens(
    name_tokens: &[proc_macro2::TokenTree],
    impl_type: Option<&MacroImplType>,
    module_prefix: &str,
) -> proc_macro2::TokenStream {
    let is_name_metavar = name_tokens.len() == 2
        && matches!(&name_tokens[0], proc_macro2::TokenTree::Punct(p) if p.as_char() == '$');

    match (impl_type, is_name_metavar) {
        // Literal type + literal name: "Type::method"
        (Some(MacroImplType::Literal(type_name)), false) => {
            let method_name = match &name_tokens[0] {
                proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
                _ => unreachable!(
                    "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
                ),
            };
            let qualified =
                crate::resolve::qualify(module_prefix, &format!("{type_name}::{method_name}"));
            quote! {
                let __piano_guard = piano_runtime::enter(#qualified);
            }
        }
        // Metavar type + literal name: concat!(stringify!($ty), "::", "method")
        (Some(MacroImplType::Metavar(ty_ident)), false) => {
            let method_name = match &name_tokens[0] {
                proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
                _ => unreachable!(
                    "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
                ),
            };
            let stringify_call = build_stringify_call(ty_ident);
            let separator: proc_macro2::TokenStream = quote! { "::" };
            let method_lit: proc_macro2::TokenStream = quote! { #method_name };
            // Build: concat!("prefix::", stringify!($ty), "::", "method")
            let span = proc_macro2::Span::call_site();
            let concat_inner: proc_macro2::TokenStream = {
                let mut tts = Vec::new();
                prepend_module_prefix_tokens(&mut tts, module_prefix);
                tts.extend(stringify_call);
                tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    ',',
                    proc_macro2::Spacing::Alone,
                )));
                tts.extend(separator);
                tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    ',',
                    proc_macro2::Spacing::Alone,
                )));
                tts.extend(method_lit);
                tts.into_iter().collect()
            };
            let concat_call: proc_macro2::TokenStream = vec![
                proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("concat", span)),
                proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    '!',
                    proc_macro2::Spacing::Alone,
                )),
                proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                    proc_macro2::Delimiter::Parenthesis,
                    concat_inner,
                )),
            ]
            .into_iter()
            .collect();
            build_enter_guard(concat_call)
        }
        // Literal type + metavar name: concat!("Type::", stringify!($name))
        (Some(MacroImplType::Literal(type_name)), true) => {
            let qualified_type = crate::resolve::qualify(module_prefix, type_name);
            let prefix = format!("{qualified_type}::");
            let stringify_call = build_stringify_call(&name_tokens[1]);
            let prefix_lit: proc_macro2::TokenStream = quote! { #prefix };
            let span = proc_macro2::Span::call_site();
            let concat_inner: proc_macro2::TokenStream = {
                let mut tts = Vec::new();
                tts.extend(prefix_lit);
                tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    ',',
                    proc_macro2::Spacing::Alone,
                )));
                tts.extend(stringify_call);
                tts.into_iter().collect()
            };
            let concat_call: proc_macro2::TokenStream = vec![
                proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("concat", span)),
                proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    '!',
                    proc_macro2::Spacing::Alone,
                )),
                proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                    proc_macro2::Delimiter::Parenthesis,
                    concat_inner,
                )),
            ]
            .into_iter()
            .collect();
            build_enter_guard(concat_call)
        }
        // Metavar type + metavar name: concat!(stringify!($ty), "::", stringify!($name))
        (Some(MacroImplType::Metavar(ty_ident)), true) => {
            let ty_stringify = build_stringify_call(ty_ident);
            let name_stringify = build_stringify_call(&name_tokens[1]);
            let separator: proc_macro2::TokenStream = quote! { "::" };
            let span = proc_macro2::Span::call_site();
            let concat_inner: proc_macro2::TokenStream = {
                let mut tts = Vec::new();
                prepend_module_prefix_tokens(&mut tts, module_prefix);
                tts.extend(ty_stringify);
                tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    ',',
                    proc_macro2::Spacing::Alone,
                )));
                tts.extend(separator);
                tts.push(proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    ',',
                    proc_macro2::Spacing::Alone,
                )));
                tts.extend(name_stringify);
                tts.into_iter().collect()
            };
            let concat_call: proc_macro2::TokenStream = vec![
                proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("concat", span)),
                proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                    '!',
                    proc_macro2::Spacing::Alone,
                )),
                proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                    proc_macro2::Delimiter::Parenthesis,
                    concat_inner,
                )),
            ]
            .into_iter()
            .collect();
            build_enter_guard(concat_call)
        }
        // No impl type, metavar name: stringify!($name) or concat!("prefix::", stringify!($name))
        (None, true) => {
            let stringify_call = build_stringify_call(&name_tokens[1]);
            if module_prefix.is_empty() {
                build_enter_guard(stringify_call)
            } else {
                let span = proc_macro2::Span::call_site();
                let concat_inner: proc_macro2::TokenStream = {
                    let mut tts = Vec::new();
                    prepend_module_prefix_tokens(&mut tts, module_prefix);
                    tts.extend(stringify_call);
                    tts.into_iter().collect()
                };
                let concat_call: proc_macro2::TokenStream = vec![
                    proc_macro2::TokenTree::Ident(proc_macro2::Ident::new("concat", span)),
                    proc_macro2::TokenTree::Punct(proc_macro2::Punct::new(
                        '!',
                        proc_macro2::Spacing::Alone,
                    )),
                    proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                        proc_macro2::Delimiter::Parenthesis,
                        concat_inner,
                    )),
                ]
                .into_iter()
                .collect();
                build_enter_guard(concat_call)
            }
        }
        // No impl type, literal name: "name"
        (None, false) => {
            let name_str = match &name_tokens[0] {
                proc_macro2::TokenTree::Ident(ident) => ident.to_string(),
                _ => unreachable!(
                    "match_fn_pattern guarantees name_tokens[0] is Ident for literal names"
                ),
            };
            let qualified = crate::resolve::qualify(module_prefix, &name_str);
            quote! {
                let __piano_guard = piano_runtime::enter(#qualified);
            }
        }
    }
}

/// Separate VisitMut that only handles macro_rules! instrumentation.
/// Kept as VisitMut because macro instrumentation modifies token streams directly.
pub(super) struct MacroInstrumenter {
    pub(super) module_prefix: String,
    pub(super) collected_names: Vec<String>,
}

impl VisitMut for MacroInstrumenter {
    fn visit_item_macro_mut(&mut self, node: &mut syn::ItemMacro) {
        if is_macro_rules(node) {
            instrument_macro_tokens(
                &mut node.mac.tokens,
                &self.module_prefix,
                &mut self.collected_names,
            );
        }
        syn::visit_mut::visit_item_macro_mut(self, node);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::rewrite::instrument_source;

    #[test]
    fn instruments_fn_in_macro_rules_metavar_name() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains("stringify!"),
            "macro fn with metavar name should use stringify!. Got:\n{result}"
        );
        assert!(
            result.contains("piano_runtime::enter"),
            "macro fn should get a guard. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("initialize")"#),
            "macro fn with literal name should use string literal. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_pub_async_fn_in_macro_rules() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains("piano_runtime::enter"),
            "pub async fn in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn skips_const_unsafe_extern_fn_in_macro_rules() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("normal")"#),
            "normal fn should be instrumented. Got:\n{result}"
        );
        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 1,
            "only normal fn should be instrumented, not const/unsafe/extern. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("rust_abi")"#),
            "extern \"Rust\" fn should be instrumented. Got:\n{result}"
        );
        assert!(
            result.contains(r#"piano_runtime::enter("pub_rust_abi")"#),
            "pub extern \"Rust\" fn should be instrumented. Got:\n{result}"
        );
        assert!(
            !result.contains(r#"piano_runtime::enter("c_abi")"#),
            "extern \"C\" fn should NOT be instrumented. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("process")"#),
            "fn after const variable should still be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_multiple_fns_in_one_macro_rule() {
        let source = r#"
macro_rules! make_pair {
    ($a:ident, $b:ident) => {
        fn $a() { work_a(); }
        fn $b() { work_b(); }
    };
}
fn main() {}
"#;
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 2,
            "both fns in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_multiple_macro_rules_arms() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        let enter_count = result.matches("piano_runtime::enter").count();
        assert_eq!(
            enter_count, 2,
            "fn in each rule arm should be instrumented. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            !result.contains("piano_runtime::enter"),
            "macro without fn should not be modified. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_paren_delimited_macro_template() {
        let source = r#"
macro_rules! make {
    ($name:ident) => (
        fn $name() { work(); }
    );
}
fn main() {}
"#;
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains("piano_runtime::enter"),
            "fn in paren-delimited template should be instrumented. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, false, "")
            .unwrap()
            .source;

        assert!(
            !result.contains("piano_runtime::enter"),
            "instrument_macros=false should skip macro instrumentation. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_generic_fn_in_macro_rules() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains("piano_runtime::enter"),
            "generic fn in macro should be instrumented. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("process")"#),
            "generic fn with Fn() -> T bound in macro should be instrumented. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains(r#"piano_runtime::enter("apply")"#),
            "generic fn with Fn() -> Option<bool> bound in macro should be instrumented. Got:\n{result}"
        );
    }

    #[test]
    fn instruments_fn_in_impl_block_in_macro_rules() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;

        assert!(
            result.contains("concat") && result.contains("stringify") && result.contains("process"),
            "fn inside impl $ty block should be qualified via concat!(stringify!($ty), ...). Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("Cruncher::process")"#),
            "method in impl with literal type should be qualified. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("Container::process")"#),
            "generic impl should still qualify method name. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_multiple_methods_all_qualified() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        for method in &["new", "crunch_small", "crunch_large"] {
            assert!(
                result.contains(&format!(r#"piano_runtime::enter("Cruncher::{method}")"#)),
                "method '{method}' should be qualified as Cruncher::{method}. Got:\n{result}"
            );
        }
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("Cruncher::fmt")"#),
            "impl Trait for Type should qualify with Type, not Trait. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_metavar_type_uses_concat_stringify() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        // concat!(stringify!($ty), "::", "process") — prettyplease may reformat spacing.
        assert!(
            result.contains("concat") && result.contains("stringify") && result.contains("process"),
            "method in impl $ty should use concat+stringify. Got:\n{result}"
        );
        // Must NOT contain the bare unqualified name.
        assert!(
            !result.contains(r#"piano_runtime::enter("process")"#),
            "should not have bare unqualified name. Got:\n{result}"
        );
    }

    #[test]
    fn macro_impl_metavar_type_and_metavar_name() {
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "")
            .unwrap()
            .source;
        // Both type and method are metavars — should use concat!(stringify!($ty), "::", stringify!($method)).
        assert!(
            result.contains("concat") && result.contains("stringify"),
            "both metavar type+name should use concat+stringify. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "api")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("api::Handler::validate")"#),
            "macro fn should use module prefix. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "worker")
            .unwrap()
            .source;
        assert!(
            result.contains(r#"piano_runtime::enter("worker::process")"#),
            "top-level macro fn should use module prefix. Got:\n{result}"
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "api").unwrap();
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
        let targets: HashMap<String, String> = HashMap::new();
        let result = instrument_source(source, &targets, true, "").unwrap();
        assert!(
            result.macro_fn_names.is_empty(),
            "metavar names should not be collected. Got: {:?}",
            result.macro_fn_names
        );
    }

    #[test]
    fn macro_filter_agrees_with_classify() {
        // Both paths must agree: classify() on a parsed signature and
        // match_fn_pattern() on the equivalent token stream must make
        // the same accept/reject decision.
        use crate::resolve::{Classification, classify};

        let cases = [
            ("fn foo() {}", true),
            ("pub fn foo() {}", true),
            ("async fn foo() {}", true),
            ("pub async fn foo() {}", true),
            ("extern \"Rust\" fn foo() {}", true),
            ("pub extern \"Rust\" fn foo() {}", true),
            ("const fn foo() {}", false),
            ("unsafe fn foo() {}", false),
            ("extern \"C\" fn foo() {}", false),
            ("extern fn foo() {}", false),
            ("pub const fn foo() {}", false),
            ("pub unsafe fn foo() {}", false),
            ("pub extern \"C\" fn foo() {}", false),
        ];

        for (code, expected_instrumentable) in cases {
            // Path 1: classify() on parsed signature
            let item: syn::ItemFn = syn::parse_str(code).unwrap_or_else(|e| {
                panic!("failed to parse '{code}': {e}");
            });
            let classify_says = matches!(classify(&item.sig), Classification::Instrumentable);

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
