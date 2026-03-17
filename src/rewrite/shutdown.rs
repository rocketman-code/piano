use ra_ap_syntax::ast::HasModuleItem;
use ra_ap_syntax::ast::HasName;
use ra_ap_syntax::{Edition, ast};

use crate::source_map::{SourceMap, StringInjector};

/// Build the lifecycle prefix that creates a file sink and a root `Ctx`.
///
/// When `cli_override` is true, the runs directory was specified via `--output-dir`
/// on the CLI and takes absolute precedence -- the generated code hardcodes the
/// path without checking `PIANO_RUNS_DIR`. When false, the generated code checks
/// `PIANO_RUNS_DIR` at runtime, falling back to `runs_dir` as the default.
fn build_lifecycle_prefix(runs_dir: &str, cpu_time: bool, cli_override: bool) -> String {
    let mut s = String::new();
    s.push_str("\n    use std::io::Write as _;");
    s.push_str("\n    let __piano_sink = {");
    if cli_override {
        // CLI --output-dir: hardcode the path, skip env var check.
        s.push_str(&format!(
            "\n        let __piano_dir = std::path::PathBuf::from({runs_dir:?});"
        ));
    } else {
        // Default: check PIANO_RUNS_DIR env var, fall back to baked-in default.
        s.push_str(&format!(
            "\n        let __piano_dir = match std::env::var(\"PIANO_RUNS_DIR\") {{\
             \n            Ok(dir) => std::path::PathBuf::from(dir),\
             \n            Err(_) => std::path::PathBuf::from({runs_dir:?}),\
             \n        }};"
        ));
    }
    s.push_str("\n        match std::fs::create_dir_all(&__piano_dir) {");
    s.push_str("\n            Ok(()) => {");
    s.push_str("\n                let __piano_ts = std::time::SystemTime::now()");
    s.push_str("\n                    .duration_since(std::time::UNIX_EPOCH)");
    s.push_str("\n                    .map(|d| d.as_millis())");
    s.push_str("\n                    .unwrap_or(0);");
    s.push_str("\n                let __piano_pid = std::process::id();");
    s.push_str("\n                let __piano_stem = format!(\"{}-{}\", __piano_ts, __piano_pid);");
    s.push_str("\n                let mut __piano_file = None;");
    s.push_str("\n                let mut __piano_warned = false;");
    s.push_str("\n                for __piano_suffix in 0u32..4 {");
    s.push_str("\n                    let __piano_path = if __piano_suffix == 0 {");
    s.push_str("\n                        __piano_dir.join(format!(\"{}.ndjson\", __piano_stem))");
    s.push_str("\n                    } else {");
    s.push_str("\n                        __piano_dir.join(format!(\"{}-{}.ndjson\", __piano_stem, __piano_suffix))");
    s.push_str("\n                    };");
    s.push_str("\n                    match std::fs::OpenOptions::new().write(true).create_new(true).open(&__piano_path) {");
    s.push_str("\n                        Ok(f) => { __piano_file = Some(f); break; }");
    s.push_str("\n                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,");
    s.push_str("\n                        Err(e) => {");
    s.push_str("\n                            let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output {}: {} -- no profiling data will be collected\", __piano_path.display(), e);");
    s.push_str("\n                            __piano_warned = true;");
    s.push_str("\n                            break;");
    s.push_str("\n                        }");
    s.push_str("\n                    }");
    s.push_str("\n                }");
    s.push_str("\n                if __piano_file.is_none() && !__piano_warned {");
    s.push_str("\n                    let _ = writeln!(std::io::stderr(), \"piano: warning: could not create profiling output (all suffixes exhausted) -- no profiling data will be collected\");");
    s.push_str("\n                }");
    s.push_str("\n                __piano_file.map(|f| std::sync::Arc::new(piano_runtime::file_sink::FileSink::new(f)))");
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

/// Inject profiling lifecycle code at the top of `fn main`.
///
/// Creates a file sink and root `Ctx` at the start of main's body.
/// The root `Ctx::drop` handles shutdown automatically -- no explicit
/// shutdown call or catch_unwind wrapping is needed.
pub fn inject_shutdown(
    source: &str,
    runs_dir: &str,
    cpu_time: bool,
    cli_override: bool,
) -> Result<(String, SourceMap), String> {
    let parse = ast::SourceFile::parse(source, Edition::Edition2024);
    let file = parse.tree();
    let mut injector = StringInjector::new();

    for item in file.items() {
        if let ast::Item::Fn(func) = item {
            let name = match func.name() {
                Some(n) => n,
                None => continue,
            };
            if name.text() != "main" {
                continue;
            }

            let body = match func.body() {
                Some(b) => b,
                None => continue,
            };
            let insert_pos = super::after_inner_attrs_in_block(source, &body.stmt_list().unwrap());

            injector.insert(insert_pos, build_lifecycle_prefix(runs_dir, cpu_time, cli_override));
            break;
        }
    }

    Ok(injector.apply(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_ctx_new_in_sync_main() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();
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
        assert!(
            result.contains("PIANO_RUNS_DIR"),
            "should check PIANO_RUNS_DIR env var at runtime. Got:\n{result}"
        );
    }

    #[test]
    fn injects_ctx_new_in_async_main() {
        let source = "async fn main() {\n    do_stuff().await;\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();
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
        let (result, _) = inject_shutdown(source, "/tmp/runs", true, false).unwrap();
        assert!(
            result.contains("Ctx::new(__piano_sink, true,"),
            "should pass true for cpu_time. Got:\n{result}"
        );
    }

    #[test]
    fn includes_runs_dir_in_path() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) =
            inject_shutdown(source, "/project/target/piano/runs", false, false).unwrap();
        assert!(
            result.contains("/project/target/piano/runs"),
            "should include runs_dir as fallback. Got:\n{result}"
        );
        assert!(
            result.contains("PIANO_RUNS_DIR"),
            "should check PIANO_RUNS_DIR env var at runtime. Got:\n{result}"
        );
    }

    #[test]
    fn no_op_when_no_main() {
        let source = "fn not_main() { }\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();
        assert_eq!(result, source, "no main = no changes");
    }

    #[test]
    fn handles_inner_attrs() {
        let source = "fn main() {\n    #![allow(unused)]\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();
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
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();
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

    #[test]
    fn retries_with_suffix_on_collision() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) = inject_shutdown(source, "/tmp/runs", false, false).unwrap();

        // Uses create_new(true) for atomic creation
        assert!(
            result.contains("create_new(true)"),
            "should use create_new(true). Got:\n{result}"
        );

        // Retry loop present
        assert!(
            result.contains("for __piano_suffix in 0u32..4"),
            "should have retry loop with 4 attempts. Got:\n{result}"
        );

        // Checks AlreadyExists to decide whether to retry
        assert!(
            result.contains("std::io::ErrorKind::AlreadyExists"),
            "should check AlreadyExists. Got:\n{result}"
        );

        // Suffix format: <stem>-<suffix>.ndjson
        assert!(
            result.contains(r#"format!("{}-{}.ndjson", __piano_stem, __piano_suffix)"#),
            "should retry with suffix format. Got:\n{result}"
        );

        // Exhaustion warning when all suffixes collide
        assert!(
            result.contains("all suffixes exhausted"),
            "should warn on exhaustion. Got:\n{result}"
        );
    }

    #[test]
    fn cli_override_hardcodes_path() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) =
            inject_shutdown(source, "/custom/output/dir", false, true).unwrap();
        assert!(
            result.contains("/custom/output/dir"),
            "should include the CLI-provided path. Got:\n{result}"
        );
        assert!(
            !result.contains("PIANO_RUNS_DIR"),
            "should NOT check PIANO_RUNS_DIR when CLI override is active. Got:\n{result}"
        );
    }

    #[test]
    fn no_cli_override_checks_env_var() {
        let source = "fn main() {\n    do_stuff();\n}\n";
        let (result, _) =
            inject_shutdown(source, "/default/runs/dir", false, false).unwrap();
        assert!(
            result.contains("PIANO_RUNS_DIR"),
            "should check PIANO_RUNS_DIR when no CLI override. Got:\n{result}"
        );
        assert!(
            result.contains("/default/runs/dir"),
            "should include default path as fallback. Got:\n{result}"
        );
    }
}
