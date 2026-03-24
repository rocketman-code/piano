//! Lifecycle injection: ProfileSession::init in fn main().
//!
//! Finds fn main(), injects file sink creation + ProfileSession::init
//! after the opening brace (skipping inner attributes).

use ra_ap_syntax::{AstNode, SourceFile, ast, T};
use ra_ap_syntax::ast::HasName;

use crate::source_map::{SourceMap, StringInjector};

const FILE_COLLISION_RETRIES: u32 = 4;

/// Build the lifecycle code injected at the top of main().
fn build_lifecycle_prefix(runs_dir: &str, cpu_time: bool) -> String {
    let mut s = String::new();
    s.push_str("\n    use std::io::Write as _;");
    s.push_str("\n    let __piano_sink = {");
    s.push_str(&format!(
        "\n        let __piano_dir = std::path::PathBuf::from(std::env::var(\"PIANO_RUNS_DIR\").unwrap_or_else(|_| \"{runs_dir}\".into()));"
    ));
    s.push_str("\n        match std::fs::create_dir_all(&__piano_dir) {");
    s.push_str("\n            Ok(()) => {");
    s.push_str("\n                let __piano_ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis();");
    s.push_str("\n                let __piano_pid = std::process::id();");
    s.push_str("\n                let mut __piano_file = None;");
    s.push_str("\n                let mut __piano_warned = false;");
    s.push_str(&format!(
        "\n                for __piano_suffix in 0u32..{FILE_COLLISION_RETRIES} {{"
    ));
    s.push_str("\n                    let __piano_name = if __piano_suffix == 0 {");
    s.push_str("\n                        format!(\"{}-{}.ndjson\", __piano_ts, __piano_pid)");
    s.push_str("\n                    } else {");
    s.push_str("\n                        format!(\"{}-{}-{}.ndjson\", __piano_ts, __piano_pid, __piano_suffix)");
    s.push_str("\n                    };");
    s.push_str("\n                    let __piano_path = __piano_dir.join(&__piano_name);");
    s.push_str("\n                    match std::fs::OpenOptions::new().write(true).create_new(true).open(&__piano_path) {");
    s.push_str("\n                        Ok(f) => { __piano_file = Some(f); break; }");
    s.push_str("\n                        Err(_) => {}");
    s.push_str("\n                    }");
    s.push_str("\n                }");
    s.push_str("\n                if __piano_file.is_none() && !__piano_warned {");
    s.push_str("\n                    let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output (all suffixes exhausted)\");");
    s.push_str("\n                }");
    s.push_str("\n                __piano_file.map(|f| std::sync::Arc::new(piano_runtime::file_sink::FileSink::new(f)))");
    s.push_str("\n            }");
    s.push_str("\n            Err(e) => {");
    s.push_str("\n                let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output directory {}: {}\", __piano_dir.display(), e);");
    s.push_str("\n                None");
    s.push_str("\n            }");
    s.push_str("\n        }");
    s.push_str("\n    };");
    s.push_str(&format!(
        "\n    piano_runtime::session::ProfileSession::init(__piano_sink, {cpu_time}, &PIANO_NAMES);"
    ));
    s
}

/// Inject profiling lifecycle code at the top of fn main().
pub fn inject_shutdown(
    source: &str,
    runs_dir: &str,
    cpu_time: bool,
) -> Result<(String, SourceMap), String> {
    let parse = SourceFile::parse(source, ra_ap_syntax::Edition::Edition2021);
    let file = parse.tree();

    let mut injector = StringInjector::new();
    let mut found_main = false;

    for node in file.syntax().descendants() {
        let Some(func) = ast::Fn::cast(node) else { continue };
        let Some(name) = func.name() else { continue };
        if name.text() != "main" { continue }
        let Some(body) = func.body() else { continue };
        let Some(stmt_list) = body.stmt_list() else { continue };

        let open_brace = stmt_list.syntax().children_with_tokens()
            .find(|t| t.kind() == T!['{'])
            .ok_or("no opening brace in main")?;
        let mut inject_offset: usize = open_brace.text_range().end().into();

        // Skip inner attributes
        for child in stmt_list.syntax().children() {
            if child.kind() == ra_ap_syntax::SyntaxKind::ATTR {
                let text = child.text().to_string();
                if text.starts_with("#!") {
                    inject_offset = child.text_range().end().into();
                }
            }
        }

        injector.insert(inject_offset, build_lifecycle_prefix(runs_dir, cpu_time));
        found_main = true;
        break;
    }

    if !found_main {
        return Ok((source.to_string(), SourceMap::new()));
    }

    Ok(injector.apply(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_in_sync_main() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert!(result.contains("ProfileSession::init"));
        assert!(result.contains("PIANO_NAMES"));
        assert!(result.contains("PIANO_RUNS_DIR"));
    }

    #[test]
    fn injects_in_async_main() {
        let source = "async fn main() {\n    do_stuff().await;\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert!(result.contains("ProfileSession::init"));
    }

    #[test]
    fn passes_cpu_time_flag() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", true).unwrap();
        assert!(result.contains("ProfileSession::init(__piano_sink, true,"));
    }

    #[test]
    fn no_op_when_no_main() {
        let source = "fn not_main() { }\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert_eq!(result, source);
    }

    #[test]
    fn skips_inner_attrs() {
        let source = "fn main() {\n    #![allow(unused)]\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        let attr_pos = result.find("#![allow(unused)]").unwrap();
        let init_pos = result.find("ProfileSession::init").unwrap();
        assert!(init_pos > attr_pos);
    }
}
