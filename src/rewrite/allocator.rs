//! Allocator injection: wrap the user's global allocator in PianoAllocator.
//!
//! Three cases (from Rust reference, global.rs):
//! 1. No #[global_allocator] → inject PianoAllocator<System>
//! 2. #[global_allocator] without #[cfg] → wrap in PianoAllocator
//! 3. #[global_allocator] with #[cfg(...)] → wrap under same cfg + fallback

use crate::source_map::{SourceMap, StringInjector};

/// Detect and wrap the global allocator in a source file.
/// Returns the modified source and source map.
pub fn inject_global_allocator(source: &str) -> Result<(String, SourceMap), String> {
    // Find #[global_allocator] by text search. The attribute must
    // appear as a standalone line (standard formatting) or inline.
    // We search for the attribute text, then find the static item after it.
    let Some(attr_pos) = source.find("#[global_allocator]") else {
        // Case 1: no allocator. Inject PianoAllocator<System>.
        let mut injector = StringInjector::new();
        injector.insert(0, concat!(
            "#[global_allocator]\n",
            "static __PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>\n",
            "    = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
        ));
        return Ok(injector.apply(source));
    };

    // Find the full extent of the static item (from first attribute to semicolon)
    // Walk backwards from #[global_allocator] to find any preceding #[cfg(...)]
    let before_attr = &source[..attr_pos];
    let mut item_start = attr_pos;

    // Check for #[cfg(...)] on preceding lines
    let mut cfg_attr: Option<String> = None;
    for line in before_attr.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if trimmed.starts_with("#[cfg(") || trimmed.starts_with("#[cfg_attr(") {
            cfg_attr = Some(trimmed.to_string());
            item_start = attr_pos - (before_attr.len() - before_attr.rfind(trimmed).unwrap_or(before_attr.len()));
            break;
        }
        if !trimmed.starts_with("#[") {
            break;
        }
    }

    // Find the end of the static item (the semicolon after the initializer)
    let after_attr = &source[attr_pos..];
    let Some(semi_offset) = after_attr.find(';') else {
        return Err("no semicolon found after #[global_allocator] static".into());
    };
    let item_end = attr_pos + semi_offset + 1; // include the semicolon

    // Extract the static item text
    let item_text = &source[item_start..item_end];

    // Parse the static: extract name, type, and initializer
    // Pattern: static NAME: TYPE = INIT;
    let static_keyword_pos = item_text.find("static ").ok_or("no 'static' keyword found")?;
    let after_static = &item_text[static_keyword_pos + 7..]; // skip "static "
    let colon_pos = after_static.find(':').ok_or("no ':' in static item")?;
    let name = after_static[..colon_pos].trim();

    let after_colon = &after_static[colon_pos + 1..];
    let eq_pos = after_colon.find('=').ok_or("no '=' in static item")?;
    let type_expr = after_colon[..eq_pos].trim();

    let after_eq = &after_colon[eq_pos + 1..];
    let init_expr = after_eq.trim_end_matches(';').trim();

    // Build the replacement
    let mut replacement = String::new();

    if let Some(ref cfg) = cfg_attr {
        // Case 3: cfg-gated allocator
        // Wrap under same cfg + add fallback
        replacement.push_str(cfg);
        replacement.push('\n');
        replacement.push_str(&format!(
            "#[global_allocator]\n\
             static {name}: piano_runtime::PianoAllocator<{type_expr}>\n\
             \x20   = piano_runtime::PianoAllocator::new({init_expr});\n"
        ));

        // Add fallback under #[cfg(not(...))]
        let neg_cfg = negate_cfg(cfg);
        replacement.push_str(&neg_cfg);
        replacement.push('\n');
        replacement.push_str(&format!(
            "#[global_allocator]\n\
             static {name}: piano_runtime::PianoAllocator<std::alloc::System>\n\
             \x20   = piano_runtime::PianoAllocator::new(std::alloc::System);"
        ));
    } else {
        // Case 2: no cfg, simple wrap
        replacement.push_str(&format!(
            "#[global_allocator]\n\
             static {name}: piano_runtime::PianoAllocator<{type_expr}>\n\
             \x20   = piano_runtime::PianoAllocator::new({init_expr});"
        ));
    }

    // Replace the item in the source
    let mut result = String::with_capacity(source.len() + replacement.len());
    result.push_str(&source[..item_start]);
    result.push_str(&replacement);
    result.push_str(&source[item_end..]);

    Ok((result, SourceMap::new()))
}

/// Negate a #[cfg(...)] attribute to #[cfg(not(...))].
fn negate_cfg(cfg: &str) -> String {
    // #[cfg(condition)] → #[cfg(not(condition))]
    // #[cfg_attr(condition, ...)] → handled separately
    if let Some(inner) = cfg.strip_prefix("#[cfg(").and_then(|s| s.strip_suffix(")]")) {
        format!("#[cfg(not({inner}))]")
    } else {
        // Can't negate complex cfg_attr, fall back to not(any())
        "#[cfg(not(any()))]".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Case 1: no allocator
    #[test]
    fn injects_system_allocator_when_absent() {
        let source = "fn main() {}\n";
        let (result, _) = inject_global_allocator(source).unwrap();
        assert!(result.contains("PianoAllocator<std::alloc::System>"));
        assert!(result.contains("#[global_allocator]"));
        assert!(result.contains("fn main()"), "original code preserved");
    }

    // Case 2: allocator present, no cfg
    #[test]
    fn wraps_existing_allocator() {
        let source = "#[global_allocator]\nstatic ALLOC: MyAlloc = MyAlloc::new();\n\nfn main() {}\n";
        let (result, _) = inject_global_allocator(source).unwrap();
        assert!(result.contains("PianoAllocator<MyAlloc>"));
        assert!(result.contains("PianoAllocator::new(MyAlloc::new())"));
        assert!(!result.contains("#[cfg(not("), "no fallback for non-cfg case");
    }

    // Case 3: allocator with cfg gate
    #[test]
    fn wraps_cfg_gated_allocator_with_fallback() {
        let source = "#[cfg(target_os = \"linux\")]\n#[global_allocator]\nstatic ALLOC: Jemalloc = Jemalloc;\n\nfn main() {}\n";
        let (result, _) = inject_global_allocator(source).unwrap();
        // Should have the wrapped allocator under the original cfg
        assert!(
            result.contains("#[cfg(target_os = \"linux\")]"),
            "original cfg preserved"
        );
        assert!(
            result.contains("PianoAllocator<Jemalloc>"),
            "original allocator wrapped"
        );
        // Should have a fallback under #[cfg(not(...))]
        assert!(
            result.contains("#[cfg(not(target_os = \"linux\"))]"),
            "negated cfg for fallback. Got:\n{result}"
        );
        assert!(
            result.contains("PianoAllocator<std::alloc::System>"),
            "System fallback present"
        );
        assert!(result.contains("fn main()"), "original code preserved");
    }

    // Edge: cfg negation
    #[test]
    fn negate_cfg_simple() {
        assert_eq!(
            negate_cfg("#[cfg(target_os = \"linux\")]"),
            "#[cfg(not(target_os = \"linux\"))]"
        );
    }

    #[test]
    fn negate_cfg_compound() {
        assert_eq!(
            negate_cfg("#[cfg(all(target_os = \"linux\", target_arch = \"x86_64\"))]"),
            "#[cfg(not(all(target_os = \"linux\", target_arch = \"x86_64\")))]"
        );
    }
}
