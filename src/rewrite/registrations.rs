//! Name table injection: PIANO_NAMES const at file level.
//!
//! Injects `const PIANO_NAMES: &[(u32, &str)] = &[...];` after any
//! file-level inner attributes.

use ra_ap_syntax::{AstNode, SourceFile, SyntaxKind};

use crate::source_map::{SourceMap, StringInjector};

/// Inject the PIANO_NAMES constant at the top of a source file.
pub fn inject_registrations(
    source: &str,
    name_table: &[(u32, &str)],
) -> Result<(String, SourceMap), String> {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let file = parse.tree();

    let mut inject_offset: usize = 0;

    // Skip file-level inner attributes (#![...])
    for child in file.syntax().children() {
        if child.kind() == SyntaxKind::ATTR {
            let text = child.text().to_string();
            if text.starts_with("#!") {
                inject_offset = child.text_range().end().into();
            }
        }
    }

    let mut entries = String::new();
    for (id, name) in name_table {
        if !entries.is_empty() {
            entries.push_str(", ");
        }
        entries.push_str(&format!("({id}, \"{name}\")"));
    }

    let injection = format!(
        "\nconst PIANO_NAMES: &[(u32, &str)] = &[{entries}];\n"
    );

    let mut injector = StringInjector::new();
    injector.insert(inject_offset, injection);
    Ok(injector.apply(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_name_table() {
        let source = "fn main() {}\n";
        let (result, _) = inject_registrations(source, &[(0, "work"), (1, "helper")]).unwrap();
        assert!(result.contains("PIANO_NAMES"));
        assert!(result.contains("(0, \"work\")"));
        assert!(result.contains("(1, \"helper\")"));
    }

    #[test]
    fn empty_table() {
        let source = "fn main() {}\n";
        let (result, _) = inject_registrations(source, &[]).unwrap();
        assert!(result.contains("PIANO_NAMES: &[(u32, &str)] = &[]"));
    }

    #[test]
    fn skips_inner_attrs() {
        let source = "#![allow(unused)]\nfn main() {}\n";
        let (result, _) = inject_registrations(source, &[(0, "work")]).unwrap();
        let attr_pos = result.find("#![allow(unused)]").unwrap();
        let names_pos = result.find("PIANO_NAMES").unwrap();
        assert!(names_pos > attr_pos);
    }
}
