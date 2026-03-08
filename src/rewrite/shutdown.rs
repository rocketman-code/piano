use quote::quote;
use syn::spanned::Spanned;

use super::line_col_to_byte;
use crate::source_map::{SourceMap, StringInjector, skip_inner_attrs};

/// Wrap `fn main`'s body in `catch_unwind` and inject `piano_runtime::shutdown()`.
///
/// When `runs_dir` is `Some`, emits `shutdown_to(dir)` to write run data to
/// the given project-local directory. When `None`, falls back to `shutdown()`
/// which uses the runtime's default directory resolution.
pub fn inject_shutdown(
    source: &str,
    runs_dir: Option<&str>,
) -> Result<(String, SourceMap), syn::Error> {
    let file: syn::File = syn::parse_str(source)?;
    let mut injector = StringInjector::new();

    for item in &file.items {
        if let syn::Item::Fn(func) = item {
            if func.sig.ident != "main" {
                continue;
            }

            let is_async = func.sig.asyncness.is_some();
            let has_return_type = !matches!(&func.sig.output, syn::ReturnType::Default);

            let open = func.block.brace_token.span.open().start();
            let close = func.block.brace_token.span.close().start();
            let open_byte = skip_inner_attrs(source, line_col_to_byte(source, open) + 1);
            let close_byte = line_col_to_byte(source, close);

            // Build shutdown call text.
            let shutdown_text = match runs_dir {
                Some(dir) => {
                    format!("piano_runtime::shutdown_to(std::path::Path::new(\"{dir}\"));")
                }
                None => "piano_runtime::shutdown();".to_string(),
            };

            // Build optional set_runs_dir text.
            let set_dir_text = runs_dir.map(|dir| {
                format!("\n    piano_runtime::set_runs_dir(std::path::Path::new(\"{dir}\"));")
            });

            if is_async {
                // Async main: no catch_unwind.
                let mut prefix = String::from("\n    piano_runtime::init();");
                if let Some(ref sdr) = set_dir_text {
                    prefix.push_str(sdr);
                }

                if has_return_type && !func.block.stmts.is_empty() {
                    // Check if last statement is a tail expression (no semicolon).
                    if let Some(syn::Stmt::Expr(tail_expr, None)) = func.block.stmts.last() {
                        // Bind the tail expression, shutdown, return.
                        let tail_start = line_col_to_byte(source, tail_expr.span().start());
                        injector.insert(open_byte, prefix);
                        injector.insert(tail_start, "let __piano_result = ".to_string());
                        let suffix = format!(";\n    {shutdown_text}\n    __piano_result\n");
                        injector.insert(close_byte, suffix);
                    } else {
                        // Last statement has semicolon; just insert shutdown before }.
                        injector.insert(open_byte, prefix);
                        let suffix = format!("\n    {shutdown_text}\n");
                        injector.insert(close_byte, suffix);
                    }
                } else {
                    // No return type: insert shutdown before }.
                    // If the last stmt is a tail expression (no semicolon),
                    // insert a semicolon after it so shutdown can follow.
                    injector.insert(open_byte, prefix);
                    if let Some(syn::Stmt::Expr(tail_expr, None)) = func.block.stmts.last() {
                        let tail_end = line_col_to_byte(source, tail_expr.span().end());
                        injector.insert(tail_end, ";".to_string());
                    }
                    let suffix = format!("\n    {shutdown_text}\n");
                    injector.insert(close_byte, suffix);
                }
            } else {
                // Sync main: wrap body in catch_unwind.
                let mut prefix = String::from("\n    piano_runtime::init();");
                if let Some(ref sdr) = set_dir_text {
                    prefix.push_str(sdr);
                }
                prefix.push_str(
                    "\n    let __piano_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {",
                );
                injector.insert(open_byte, prefix);

                let mut suffix = format!("\n    }}));\n    {shutdown_text}");
                if has_return_type {
                    suffix.push_str(
                        "\n    match __piano_result {\n        Ok(__piano_val) => __piano_val,\n        Err(__piano_panic) => std::panic::resume_unwind(__piano_panic),\n    }\n",
                    );
                } else {
                    suffix.push_str(
                        "\n    if let Err(__piano_panic) = __piano_result {\n        std::panic::resume_unwind(__piano_panic);\n    }\n",
                    );
                }
                injector.insert(close_byte, suffix);
            }

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
    fn injects_init_at_start_of_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();

        assert!(
            result.contains("piano_runtime::init()"),
            "should inject init(). Got:\n{result}"
        );

        // init() should appear before set_runs_dir and catch_unwind
        let init_pos = result.find("piano_runtime::init()").unwrap();
        let set_dir_pos = result.find("piano_runtime::set_runs_dir").unwrap();
        assert!(
            init_pos < set_dir_pos,
            "init() should come before set_runs_dir(). Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_with_catch_unwind() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            result.contains("catch_unwind"),
            "should wrap body in catch_unwind. Got:\n{result}"
        );
        assert!(
            result.contains("resume_unwind"),
            "should re-panic on caught panic. Got:\n{result}"
        );
        let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
        let do_stuff_pos = result.find("do_stuff()").unwrap();
        assert!(
            shutdown_pos > do_stuff_pos,
            "shutdown should come after existing code"
        );
    }

    #[test]
    fn injects_shutdown_to_with_dir() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) =
            inject_shutdown(source, Some("/tmp/my-project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown_to")
                && result.contains("std::path::Path::new(\"/tmp/my-project/target/piano/runs\")"),
            "should inject shutdown_to with Path::new. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_at_start_of_main() {
        let source = r#"
fn main() {
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::set_runs_dir"),
            "should inject set_runs_dir when runs_dir is provided. Got:\n{result}"
        );
        // set_runs_dir should appear BEFORE catch_unwind (i.e. before the body)
        let set_pos = result.find("set_runs_dir").unwrap();
        let catch_pos = result.find("catch_unwind").unwrap();
        assert!(
            set_pos < catch_pos,
            "set_runs_dir should come before catch_unwind. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_preserves_main_return_type() {
        let source = r#"
use std::process::ExitCode;
fn main() -> ExitCode {
    do_stuff();
    ExitCode::SUCCESS
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            result.contains("catch_unwind"),
            "should wrap body in catch_unwind for return-type main. Got:\n{result}"
        );
        // Must preserve ExitCode as the tail expression (not discard it)
        // The rewritten code should compile — ExitCode must be returned after shutdown.
        let parsed: syn::File = syn::parse_str(&result)
            .unwrap_or_else(|e| panic!("rewritten code should parse: {e}\n\n{result}"));
        // Find main function and verify it still has a return type
        let main_fn = parsed
            .items
            .iter()
            .find_map(|item| {
                if let syn::Item::Fn(f) = item {
                    if f.sig.ident == "main" {
                        return Some(f);
                    }
                }
                None
            })
            .expect("should have main fn");
        // The last statement in main should be an expression (the return value),
        // not a semicolon-terminated statement
        let last = main_fn
            .block
            .stmts
            .last()
            .expect("main should have statements");
        assert!(
            matches!(last, syn::Stmt::Expr(_, None)),
            "last statement should be a tail expression (no semicolon) for the return value. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_async_main_no_catch_unwind() {
        let source = r#"
async fn main() {
    do_stuff().await;
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "async main should NOT use catch_unwind. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_async_main_with_return_type() {
        let source = r#"
async fn main() -> ExitCode {
    do_stuff().await;
    ExitCode::SUCCESS
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        assert!(
            result.contains("piano_runtime::shutdown()"),
            "should inject shutdown. Got:\n{result}"
        );
        assert!(
            !result.contains("catch_unwind"),
            "async main should NOT use catch_unwind. Got:\n{result}"
        );
        // Tail expression is bound to __piano_result, shutdown runs,
        // then __piano_result is returned as the tail expression.
        assert!(
            result.contains("__piano_result"),
            "tail expression should be bound to __piano_result. Got:\n{result}"
        );
        let shutdown_pos = result.find("piano_runtime::shutdown()").unwrap();
        let return_pos = result.rfind("__piano_result").unwrap();
        assert!(
            shutdown_pos < return_pos,
            "shutdown should come before the return. Got:\n{result}"
        );
    }

    #[test]
    fn injects_set_runs_dir_in_async_main() {
        let source = r#"
async fn main() {
    do_stuff().await;
}
"#;
        let (result, _map) = inject_shutdown(source, Some("/project/target/piano/runs")).unwrap();
        assert!(
            result.contains("piano_runtime::set_runs_dir"),
            "should inject set_runs_dir for async main. Got:\n{result}"
        );
        // set_runs_dir should come before the user's code
        let set_pos = result.find("set_runs_dir").unwrap();
        let stuff_pos = result.find("do_stuff").unwrap();
        assert!(
            set_pos < stuff_pos,
            "set_runs_dir should come before user code. Got:\n{result}"
        );
    }

    #[test]
    fn injects_shutdown_preserving_inner_attrs_in_main() {
        let source = r#"
fn main() {
    #![allow(unused)]
    do_stuff();
}
"#;
        let (result, _map) = inject_shutdown(source, None).unwrap();
        // The rewritten source must parse successfully.
        syn::parse_str::<syn::File>(&result)
            .unwrap_or_else(|e| panic!("rewritten source should parse: {e}\n\n{result}"));
        assert!(
            result.contains("piano_runtime::init()"),
            "should inject init(). Got:\n{result}"
        );
        // Inner attr must come before injected code in the body.
        let attr_pos = result.find("#![allow(unused)]").unwrap();
        let init_pos = result.find("piano_runtime::init()").unwrap();
        assert!(
            attr_pos < init_pos,
            "inner attr must precede injected init. Got:\n{result}"
        );
    }
}
