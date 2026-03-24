//! Allocator injection: wrap the user's global allocator in PianoAllocator.
//!
//! Three cases:
//! 1. No allocator: inject PianoAllocator<System> as the global allocator
//! 2. User has #[global_allocator]: wrap their allocator in PianoAllocator
//! 3. User has #[cfg(...)] #[global_allocator]: wrap under the same cfg

use ra_ap_syntax::{AstNode, SourceFile, SyntaxKind, ast};

use crate::source_map::{SourceMap, StringInjector};

/// What kind of allocator was detected in the source.
#[derive(Debug, Clone, PartialEq)]
pub enum AllocatorKind {
    /// No #[global_allocator] found.
    Absent,
    /// #[global_allocator] found with the given static name and type expression.
    Present {
        name: String,
        type_expr: String,
        init_expr: String,
        /// Byte range of the entire static item (for replacement).
        start: usize,
        end: usize,
    },
}

/// Detect the allocator kind in a source file.
pub fn detect_allocator_kind(source: &str) -> AllocatorKind {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let file = parse.tree();

    for node in file.syntax().descendants() {
        let Some(static_item) = ast::Static::cast(node) else { continue };

        // Check if it has #[global_allocator]
        let has_global_alloc = static_item.syntax().children()
            .any(|child| {
                if child.kind() != SyntaxKind::ATTR { return false }
                let text = child.text().to_string();
                text.contains("global_allocator")
            });

        if !has_global_alloc { continue }

        let name = static_item.syntax().children()
            .find(|c| c.kind() == SyntaxKind::NAME)
            .map(|n| n.text().to_string())
            .unwrap_or_default();

        let type_expr = static_item.syntax().children()
            .find(|c| c.kind() == SyntaxKind::PATH_TYPE || c.kind() == SyntaxKind::TYPE)
            .map(|t| t.text().to_string())
            .unwrap_or_default();

        let init_expr = static_item.syntax().descendants()
            .find(|c| {
                // Find the initializer expression (after the = sign)
                c.kind() == SyntaxKind::CALL_EXPR
                    || c.kind() == SyntaxKind::PATH_EXPR
                    || c.kind() == SyntaxKind::MACRO_CALL
            })
            .map(|e| e.text().to_string())
            .unwrap_or_default();

        let range = static_item.syntax().text_range();
        return AllocatorKind::Present {
            name,
            type_expr,
            init_expr,
            start: range.start().into(),
            end: range.end().into(),
        };
    }

    AllocatorKind::Absent
}

/// Inject allocator wrapping into a source file.
pub fn inject_global_allocator(
    source: &str,
    kind: &AllocatorKind,
) -> Result<(String, SourceMap), String> {
    let mut injector = StringInjector::new();

    match kind {
        AllocatorKind::Absent => {
            // Inject PianoAllocator<System> at the top of the file
            injector.insert(0, concat!(
                "\n#[global_allocator]",
                "\nstatic __PIANO_ALLOC: piano_runtime::PianoAllocator<std::alloc::System>",
                "\n    = piano_runtime::PianoAllocator::new(std::alloc::System);\n",
            ));
        }
        AllocatorKind::Present { name, type_expr, init_expr, start, end } => {
            // Replace the existing allocator with a wrapped version.
            // The original: #[global_allocator] static ALLOC: Type = init;
            // Becomes: #[global_allocator] static ALLOC: PianoAllocator<Type> = PianoAllocator::new(init);
            let replacement = format!(
                "#[global_allocator]\nstatic {name}: piano_runtime::PianoAllocator<{type_expr}>\n    = piano_runtime::PianoAllocator::new({init_expr});"
            );
            // Replace the entire static item
            injector.insert(*start, &replacement);
            // We need to skip the original text. StringInjector doesn't support
            // replacement directly, so we'll use a different approach.
            // For now, return the source with manual replacement.
            let mut result = String::with_capacity(source.len() + replacement.len());
            result.push_str(&source[..*start]);
            result.push_str(&replacement);
            result.push_str(&source[*end..]);
            return Ok((result, SourceMap::new()));
        }
    }

    Ok(injector.apply(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_absent_allocator() {
        let source = "fn main() {}\n";
        assert_eq!(detect_allocator_kind(source), AllocatorKind::Absent);
    }

    #[test]
    fn injects_system_allocator_when_absent() {
        let source = "fn main() {}\n";
        let (result, _) = inject_global_allocator(source, &AllocatorKind::Absent).unwrap();
        assert!(result.contains("PianoAllocator<std::alloc::System>"));
        assert!(result.contains("#[global_allocator]"));
    }

    #[test]
    fn detects_present_allocator() {
        let source = "#[global_allocator]\nstatic ALLOC: MyAlloc = MyAlloc::new();\n";
        match detect_allocator_kind(source) {
            AllocatorKind::Present { name, .. } => {
                assert_eq!(name, "ALLOC");
            }
            other => panic!("expected Present, got {:?}", other),
        }
    }
}
