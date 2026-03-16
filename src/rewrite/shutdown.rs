use quote::quote;

use super::line_col_to_byte;
use crate::source_map::{SourceMap, StringInjector, skip_inner_attrs};

/// Build the lifecycle prefix that creates a file sink and a root `Ctx`.
fn build_lifecycle_prefix(runs_dir: &str, cpu_time: bool) -> String {
    let mut s = String::new();
    s.push_str("\n    use std::io::Write as _;");
    s.push_str("\n    let __piano_sink = {");
    s.push_str(&format!(
        "\n        let __piano_dir = std::path::PathBuf::from({runs_dir:?});"
    ));
    s.push_str("\n        match std::fs::create_dir_all(&__piano_dir) {");
    s.push_str("\n            Ok(()) => {");
    s.push_str("\n                let __piano_ts = std::time::SystemTime::now()");
    s.push_str("\n                    .duration_since(std::time::UNIX_EPOCH)");
    s.push_str("\n                    .map(|d| d.as_millis())");
    s.push_str("\n                    .unwrap_or(0);");
    s.push_str("\n                let __piano_pid = std::process::id();");
    s.push_str("\n                let __piano_path = __piano_dir.join(format!(\"{}-{}.ndjson\", __piano_ts, __piano_pid));");
    s.push_str("\n                match std::fs::OpenOptions::new().write(true).create_new(true).open(&__piano_path) {");
    s.push_str("\n                    Ok(f) => Some(std::sync::Arc::new(piano_runtime::file_sink::FileSink::new(f))),");
    s.push_str("\n                    Err(e) => {");
    s.push_str("\n                        let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output {}: {} -- no profiling data will be collected\", __piano_path.display(), e);");
    s.push_str("\n                        None");
    s.push_str("\n                    }");
    s.push_str("\n                }");
    s.push_str("\n            }");
    s.push_str("\n            Err(e) => {");
    s.push_str("\n                let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output directory {}: {} -- no profiling data will be collected\", __piano_dir.display(), e);");
    s.push_str("\n                None");
    s.push_str("\n            }");
    s.push_str("\n        }");
    s.push_str("\n    };");
    s.push_str(&format!(
        "\n    let __piano_ctx = piano_runtime::ctx::Ctx::new(__piano_sink, {cpu_time}, &PIANO_NAMES);"
    ));
    s
}

/// Inject CBC lifecycle code at the top of `fn main`.
///
/// Creates a file sink and root `Ctx` at the start of main's body.
/// The root `Ctx::drop` handles shutdown automatically -- no explicit
/// shutdown call or catch_unwind wrapping is needed.
pub fn inject_shutdown(
    source: &str,
    runs_dir: &str,
    cpu_time: bool,
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut injector = StringInjector::new();

    for item in &file.items {
        if let syn::Item::Fn(func) = item {
            if func.sig.ident != "main" {
                continue;
            }

            let open = func.block.brace_token.span.open().start();
            let open_byte = skip_inner_attrs(source, line_col_to_byte(source, open) + 1);

            injector.insert(open_byte, build_lifecycle_prefix(runs_dir, cpu_time));
            break;
        }
    }

    Ok(injector.apply(source))
}

/// Extract the type name from a `syn::Type` for qualified method names.
pub(super) fn type_ident(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(tp) => tp
            .path
            .segments
            .last()
            .map(|seg| seg.ident.to_string())
            .unwrap_or_else(|| quote!(#ty).to_string()),
        _ => quote!(#ty).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_ctx_new_in_sync_main() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert!(
            result.contains("piano_runtime::ctx::Ctx::new"),
            "should inject Ctx::new. Got:\n{result}"
        );
        assert!(
            result.contains("PIANO_NAMES"),
            "should reference PIANO_NAMES. Got:\n{result}"
        );
        assert!(
            result.contains("FileSink::new"),
            "should create FileSink. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "should NOT have catch_unwind. Got:\n{result}"
        );
        assert!(
            !result.contains("piano_runtime::init"),
            "should NOT have init(). Got:\n{result}"
        );
        assert!(
            !result.contains("piano_runtime::shutdown"),
            "should NOT have shutdown(). Got:\n{result}"
        );
    }

    #[test]
    fn injects_ctx_new_in_async_main() {
        let source = "async fn main() {\n    do_stuff().await;\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert!(
            result.contains("piano_runtime::ctx::Ctx::new"),
            "should inject Ctx::new. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "should NOT have catch_unwind for async. Got:\n{result}"
        );
    }

    #[test]
    fn injects_with_cpu_time_flag() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", true).unwrap();
        assert!(
            result.contains("Ctx::new(__piano_sink, true,"),
            "should pass true for cpu_time. Got:\n{result}"
        );
    }

    #[test]
    fn includes_runs_dir_in_path() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) =
            inject_shutdown(source, "/project/target/piano/runs", false).unwrap();
        assert!(
            result.contains("/project/target/piano/runs"),
            "should include runs_dir. Got:\n{result}"
        );
    }

    #[test]
    fn no_op_when_no_main() {
        let source = "fn not_main() { }\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert_eq!(result, source, "no main = no changes");
    }

    #[test]
    fn handles_inner_attrs() {
        let source = "fn main() {\n    #![allow(unused)]\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        let allow_pos = result.find("#![allow(unused)]").unwrap();
        let ctx_pos = result.find("Ctx::new").unwrap();
        assert!(
            ctx_pos > allow_pos,
            "Ctx::new should come after inner attrs. Got:\n{result}"
        );
    }

    #[test]
    fn handles_main_with_return_type() {
        let source =
            "fn main() -> Result<(), Box<dyn std::error::Error>> {\n    do_stuff()?;\n    Ok(())\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false).unwrap();
        assert!(
            result.contains("piano_runtime::ctx::Ctx::new"),
            "should inject Ctx::new for main with return type. Got:\n{result}"
        );
        // No special return-type handling needed -- Ctx drops naturally
        assert!(
            !result.contains("catch_unwind"),
            "should NOT have catch_unwind. Got:\n{result}"
        );
    }
}
