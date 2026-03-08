use quote::quote;
use syn::spanned::Spanned;

use super::line_col_to_byte;
use crate::source_map::{SourceMap, StringInjector, skip_inner_attrs};

/// Classification of the user's `#[global_allocator]` declaration.
pub enum AllocatorKind {
    /// No `#[global_allocator]` found in the source.
    Absent,
    /// `#[global_allocator]` without any `#[cfg(...)]` gate.
    Unconditional,
    /// One or more `#[global_allocator]` statics, each behind a `#[cfg(...)]`.
    /// The `Vec` contains the cfg predicate `Meta` from each `#[cfg(pred)]`.
    CfgGated(Vec<syn::Meta>),
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
fn has_global_allocator_attr(static_item: &syn::ItemStatic) -> bool {
    static_item.attrs.iter().any(|a| {
        if a.path().is_ident("global_allocator") {
            return true;
        }
        if a.path().is_ident("cfg_attr") {
            return cfg_attr_contains_global_allocator(a);
        }
        false
    })
}

/// Check whether a `#[cfg_attr(...)]` attribute contains `global_allocator`
/// among its conditional attributes.
///
/// `cfg_attr` has the form `cfg_attr(condition, attr1, attr2, ...)`.
/// We parse the token stream and check if any attr after the condition
/// is the path `global_allocator`.
fn cfg_attr_contains_global_allocator(attr: &syn::Attribute) -> bool {
    let Ok(nested) = attr.parse_args_with(
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
    ) else {
        return false;
    };
    // First element is the condition; remaining elements are the conditional attributes.
    nested
        .iter()
        .skip(1)
        .any(|meta| meta.path().is_ident("global_allocator"))
}

/// Extract the condition `Meta` from a `#[cfg_attr(condition, ...)]` that
/// contains `global_allocator`. Returns `None` if the attribute is not a
/// matching `cfg_attr`.
fn cfg_attr_global_allocator_condition(attr: &syn::Attribute) -> Option<syn::Meta> {
    if !attr.path().is_ident("cfg_attr") {
        return None;
    }
    let nested = attr
        .parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
        .ok()?;
    let has_global_alloc = nested
        .iter()
        .skip(1)
        .any(|meta| meta.path().is_ident("global_allocator"));
    if has_global_alloc {
        nested.into_iter().next()
    } else {
        None
    }
}

/// Extract byte range for a span in the source string.
fn span_byte_range(source: &str, span: proc_macro2::Span) -> (usize, usize) {
    let start = line_col_to_byte(source, span.start());
    let end = line_col_to_byte(source, span.end());
    (start, end)
}

/// Wrap a `#[global_allocator]` static's type and initializer with `PianoAllocator`
/// using string replacement. Returns a list of (start, end, replacement) edits.
fn wrap_allocator_edits(
    source: &str,
    static_item: &syn::ItemStatic,
) -> Vec<(usize, usize, String)> {
    let mut edits = Vec::new();

    // Wrap the type: T -> piano_runtime::PianoAllocator<T>
    let (ty_start, ty_end) = span_byte_range(source, static_item.ty.span());
    let orig_ty = &source[ty_start..ty_end];
    edits.push((
        ty_start,
        ty_end,
        format!("piano_runtime::PianoAllocator<{orig_ty}>"),
    ));

    // Wrap the expr: E -> piano_runtime::PianoAllocator::new(E)
    let (expr_start, expr_end) = span_byte_range(source, static_item.expr.span());
    let orig_expr = &source[expr_start..expr_end];
    edits.push((
        expr_start,
        expr_end,
        format!("piano_runtime::PianoAllocator::new({orig_expr})"),
    ));

    edits
}

/// Walk the parsed file and classify any `#[global_allocator]` statics.
pub fn detect_allocator_kind(source: &str) -> Result<AllocatorKind, syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut cfg_predicates: Vec<syn::Meta> = Vec::new();

    for item in &file.items {
        if let syn::Item::Static(static_item) = item {
            if !has_global_allocator_attr(static_item) {
                continue;
            }

            let mut item_cfgs: Vec<syn::Meta> = Vec::new();
            for a in &static_item.attrs {
                if a.path().is_ident("cfg") {
                    item_cfgs.push(a.parse_args::<syn::Meta>()?);
                } else if let Some(condition) = cfg_attr_global_allocator_condition(a) {
                    item_cfgs.push(condition);
                }
            }

            if item_cfgs.is_empty() {
                return Ok(AllocatorKind::Unconditional);
            }

            // Multiple #[cfg] on the same item is semantically #[cfg(all(...))].
            let combined: syn::Meta = if item_cfgs.len() == 1 {
                item_cfgs.remove(0)
            } else {
                syn::parse_quote! { all(#(#item_cfgs),*) }
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
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;

    match kind {
        AllocatorKind::Absent => {
            let text = "\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n";
            let mut injector = StringInjector::new();
            injector.insert(skip_inner_attrs(source, 0), text);
            Ok(injector.apply(source))
        }
        AllocatorKind::Unconditional => {
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in &file.items {
                if let syn::Item::Static(static_item) = item {
                    if has_global_allocator_attr(static_item) {
                        edits = wrap_allocator_edits(source, static_item);
                        break;
                    }
                }
            }
            Ok((apply_replacements(source, &edits), SourceMap::default()))
        }
        AllocatorKind::CfgGated(ref predicates) => {
            // Collect type/expr edits for each cfg-gated allocator.
            let mut edits: Vec<(usize, usize, String)> = Vec::new();
            for item in &file.items {
                if let syn::Item::Static(static_item) = item {
                    if has_global_allocator_attr(static_item) {
                        edits.extend(wrap_allocator_edits(source, static_item));
                    }
                }
            }

            // Build the cfg-negated fallback text.
            // Format predicates compactly (quote! adds unwanted spaces).
            let pred_strs: Vec<String> =
                predicates.iter().map(|p| quote!(#p).to_string()).collect();
            let negated_str = if pred_strs.len() == 1 {
                format!("not({})", pred_strs[0])
            } else {
                format!("not(any({}))", pred_strs.join(", "))
            };
            let fallback = format!(
                "\n#[cfg({negated_str})]\n#[global_allocator]\nstatic _PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n"
            );

            // Apply type/expr replacements first, then insert fallback after
            // any inner attributes so `#![...]` stays at the top of the file.
            let replaced = apply_replacements(source, &edits);
            let insert_pos = skip_inner_attrs(&replaced, 0);

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
        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));
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
        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));
    }
}
