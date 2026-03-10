//! RUSTC_WORKSPACE_WRAPPER mode: intercept rustc invocations to rewrite
//! source files and inject piano-runtime.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::error::{Error, io_context};
use crate::rewrite::{
    detect_allocator_kind, inject_global_allocator, inject_registrations, inject_shutdown,
    instrument_source,
};
use crate::source_map::SourceMap;

/// Configuration passed from the piano orchestrator to the wrapper via
/// a JSON file. The path is communicated through PIANO_WRAPPER_CONFIG.
#[derive(Debug, Serialize, Deserialize)]
pub struct WrapperConfig {
    /// Path to the pre-built libpiano_runtime.rlib
    pub runtime_rlib: PathBuf,
    /// Directory containing the rlib and transitive deps (for -L dependency=)
    pub runtime_deps_dir: PathBuf,
    /// Whether to instrument macro_rules! bodies (true when no --fn/--file/--mod given)
    pub instrument_macros: bool,
    /// Binary entry point that gets registrations, allocator, and shutdown injected
    pub entry_point: EntryPointConfig,
    /// Per-file target maps. Keys are source paths relative to workspace root.
    /// Values are maps from full function name to display name.
    pub targets: HashMap<PathBuf, HashMap<String, String>>,
    /// Per-file module prefixes for the MacroInstrumenter.
    pub module_prefixes: HashMap<PathBuf, String>,
}

/// Configuration for the binary entry point file.
#[derive(Debug, Serialize, Deserialize)]
pub struct EntryPointConfig {
    /// Source path relative to workspace root (e.g., "src/main.rs")
    pub source_path: PathBuf,
    /// All function display names (for registration injection)
    pub fn_names: Vec<String>,
    /// Directory for NDJSON output files
    pub runs_dir: PathBuf,
}

/// Environment variable name for the config file path.
pub const CONFIG_ENV: &str = "PIANO_WRAPPER_CONFIG";

/// Derive the source maps file path from the config file path.
pub fn source_maps_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("source_maps.json")
}

/// Read source maps written by the wrapper during instrumentation.
pub fn read_source_maps(config_path: &Path) -> HashMap<PathBuf, SourceMap> {
    let path = source_maps_path(config_path);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return HashMap::new();
    };
    serde_json::from_str(&content).unwrap_or_default()
}

/// Parsed fields from rustc's command-line arguments.
#[derive(Debug)]
pub struct ParsedRustcArgs {
    pub crate_name: Option<String>,
    pub crate_type: Option<String>,
    pub source_file: Option<String>,
    pub is_info_query: bool,
    pub has_print: bool,
}

impl ParsedRustcArgs {
    pub fn parse(args: &[String]) -> Self {
        let mut crate_name = None;
        let mut crate_type = None;
        let mut source_file = None;
        let mut is_info_query = false;
        let mut has_print = false;

        let mut prev = "";
        for arg in args {
            match prev {
                "--crate-name" => crate_name = Some(arg.clone()),
                "--crate-type" => crate_type = Some(arg.clone()),
                _ => {}
            }
            if arg == "-vV" {
                is_info_query = true;
            }
            if arg.starts_with("--print") {
                has_print = true;
            }
            // Source file: ends with .rs, is not a flag, is not "-" (stdin)
            if arg.ends_with(".rs") && !arg.starts_with('-') {
                source_file = Some(arg.clone());
            }
            prev = arg;
        }

        Self {
            crate_name,
            crate_type,
            source_file,
            is_info_query,
            has_print,
        }
    }

    /// Returns true if the wrapper should pass through to real rustc unchanged.
    pub fn should_skip(&self) -> bool {
        if self.is_info_query || self.has_print {
            return true;
        }
        if self.crate_name.as_deref() == Some("build_script_build") {
            return true;
        }
        if self.crate_name.as_deref() == Some("___") {
            return true;
        }
        if self.crate_type.as_deref() == Some("proc-macro") {
            return true;
        }
        false
    }
}

/// Guard that deletes a temp file on drop.
struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Wrapper entry point. Called when PIANO_WRAPPER_CONFIG is set.
///
/// argv layout: piano, real_rustc, rustc_args...
pub fn run_wrapper() -> i32 {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("piano wrapper: expected rustc path as first argument");
        return 1;
    }

    let real_rustc = &args[1];
    let rustc_args: Vec<String> = args[2..].to_vec();

    let parsed = ParsedRustcArgs::parse(&rustc_args);

    // Pass through non-instrumentable invocations
    if parsed.should_skip() {
        return exec_rustc(real_rustc, &rustc_args);
    }

    // Read config
    let config_path = match std::env::var(CONFIG_ENV) {
        Ok(p) => p,
        Err(_) => {
            eprintln!("piano wrapper: {CONFIG_ENV} not set");
            return 1;
        }
    };
    let config: WrapperConfig = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("piano wrapper: failed to read config: {e}");
            return 1;
        }
    };

    // Look up source file in config targets
    let Some(source_path) = &parsed.source_file else {
        // No source file in args -- pass through
        return exec_rustc(real_rustc, &rustc_args);
    };
    let source_key = PathBuf::from(source_path);

    let is_entry_point = config.entry_point.source_path == source_key;
    let has_direct_target = config.targets.contains_key(&source_key);
    // Check if any config target is a module of this crate (shares directory prefix)
    let source_parent = source_key.parent().unwrap_or(Path::new(""));
    let has_module_targets = config
        .targets
        .keys()
        .any(|p| p != &source_key && p.starts_with(source_parent));

    if !has_direct_target && !is_entry_point && !has_module_targets {
        // Not a target file, not the entry point, no module targets -- pass through
        return exec_rustc(real_rustc, &rustc_args);
    }

    // Rewrite the source file
    let config_p = Path::new(&config_path);
    match rewrite_and_compile(
        real_rustc,
        &rustc_args,
        source_path,
        &source_key,
        &config,
        config_p,
    ) {
        Ok(code) => code,
        Err(e) => {
            // Graceful degradation: compile original file with a warning
            eprintln!("piano wrapper: failed to instrument {source_path}: {e}");
            eprintln!("piano wrapper: compiling without instrumentation");
            exec_rustc(real_rustc, &rustc_args)
        }
    }
}

fn load_config(path: &str) -> Result<WrapperConfig, Error> {
    let content = std::fs::read_to_string(path)
        .map_err(io_context("read wrapper config", Path::new(path)))?;
    serde_json::from_str(&content)
        .map_err(|e| Error::BuildFailed(format!("failed to parse wrapper config: {e}")))
}

/// Write the source map for a file to the shared source maps JSON file.
/// The wrapper writes these during instrumentation; the orchestrator reads
/// them after a build failure to remap line numbers in error messages.
fn write_source_map(source_file: &str, map: &SourceMap, config_path: &Path) {
    let maps_path = source_maps_path(config_path);

    // Read existing maps (other wrapper invocations may have written theirs)
    let mut maps: HashMap<PathBuf, SourceMap> = std::fs::read_to_string(&maps_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    maps.insert(PathBuf::from(source_file), map.clone());

    if let Ok(json) = serde_json::to_string(&maps) {
        let _ = std::fs::write(&maps_path, json);
    }
}

fn rewrite_and_compile(
    real_rustc: &str,
    rustc_args: &[String],
    source_path: &str,
    source_key: &Path,
    config: &WrapperConfig,
    config_path: &Path,
) -> Result<i32, Error> {
    let source_p = Path::new(source_path);
    let source_dir = source_p.parent();
    let is_entry_point = config.entry_point.source_path == source_key;

    // Collect all module files that need instrumentation in this crate.
    // These are config target files that share the same directory prefix.
    let mut module_temps: Vec<TempFileGuard> = Vec::new();
    let mut all_macro_fn_names: Vec<String> = Vec::new();
    // Map from module stem to temp file name, for #[path] redirects.
    let mut path_redirects: HashMap<String, String> = HashMap::new();

    // Instrument non-root module files first
    for (target_path, target_map) in &config.targets {
        if target_path == source_key {
            continue; // Handle root file below
        }
        // Only rewrite files whose config path shares the source_key's parent directory prefix
        let target_parent = target_path.parent();
        let source_parent = source_key.parent();
        if target_parent != source_parent
            && !target_path.starts_with(source_parent.unwrap_or(Path::new("")))
        {
            continue;
        }

        let module_prefix = config
            .module_prefixes
            .get(target_path)
            .map(|s| s.as_str())
            .unwrap_or("");

        // Resolve actual filesystem path from source_path's parent + relative path
        let actual_path = if let Some(dir) = source_dir {
            let rel_from_root = target_path
                .strip_prefix(source_key.parent().unwrap_or(Path::new("")))
                .unwrap_or(target_path);
            dir.join(rel_from_root)
        } else {
            PathBuf::from(target_path)
        };

        let module_source = match std::fs::read_to_string(&actual_path) {
            Ok(s) => s,
            Err(_) => continue, // File not found, skip
        };

        let result = instrument_source(
            &module_source,
            target_map,
            config.instrument_macros,
            module_prefix,
        )
        .map_err(|e| {
            Error::BuildFailed(format!("rewrite failed for {}: {e}", target_path.display()))
        })?;
        all_macro_fn_names.extend(result.macro_fn_names);

        // Write temp file
        let stem = actual_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        let temp_name = format!(".{stem}.piano.rs");
        let temp_path = actual_path.with_file_name(&temp_name);
        std::fs::write(&temp_path, &result.source)
            .map_err(io_context("write temp file", &temp_path))?;
        module_temps.push(TempFileGuard(temp_path));

        // Record #[path] redirect: stem -> temp_name
        path_redirects.insert(stem.to_string(), temp_name);
    }

    // Instrument the crate root file
    let source =
        std::fs::read_to_string(source_path).map_err(io_context("read source", source_p))?;
    let root_target_map = config.targets.get(source_key);
    let module_prefix = config
        .module_prefixes
        .get(source_key)
        .map(|s| s.as_str())
        .unwrap_or("");
    let empty_targets = HashMap::new();
    let targets = root_target_map.unwrap_or(&empty_targets);
    let result = instrument_source(&source, targets, config.instrument_macros, module_prefix)
        .map_err(|e| Error::BuildFailed(format!("rewrite failed: {e}")))?;
    all_macro_fn_names.extend(result.macro_fn_names);

    let mut rewritten = result.source;
    let mut source_map = result.source_map;

    // Insert #[path] attributes for modules that have temp files
    if !path_redirects.is_empty() {
        rewritten = insert_mod_path_redirects(&rewritten, &path_redirects);
    }

    // Entry point: inject registrations, allocator, shutdown
    if is_entry_point {
        let mut fn_names = config.entry_point.fn_names.clone();
        fn_names.extend(all_macro_fn_names);

        let (r, reg_map) = inject_registrations(&rewritten, &fn_names)
            .map_err(|e| Error::BuildFailed(format!("registration injection failed: {e}")))?;
        source_map.merge(reg_map);
        rewritten = r;

        let alloc_kind = detect_allocator_kind(&rewritten)
            .map_err(|e| Error::BuildFailed(format!("allocator detection failed: {e}")))?;
        let (r, alloc_map) = inject_global_allocator(&rewritten, alloc_kind)
            .map_err(|e| Error::BuildFailed(format!("allocator injection failed: {e}")))?;
        source_map.merge(alloc_map);
        rewritten = r;

        let runs_dir_str = config.entry_point.runs_dir.to_string_lossy().to_string();
        let (r, shutdown_map) = inject_shutdown(&rewritten, Some(&runs_dir_str))
            .map_err(|e| Error::BuildFailed(format!("shutdown injection failed: {e}")))?;
        source_map.merge(shutdown_map);
        rewritten = r;
    }

    // Write temp file for the crate root
    let stem = source_p.file_stem().unwrap_or_default().to_string_lossy();
    let temp_name = format!(".{stem}.piano.rs");
    let temp_path = source_p.with_file_name(&temp_name);

    std::fs::write(&temp_path, &rewritten).map_err(io_context("write temp file", &temp_path))?;
    let _root_guard = TempFileGuard(temp_path.clone());

    // Build modified rustc args
    let mut new_args = rustc_args.to_vec();

    for arg in &mut new_args {
        if arg == source_path {
            *arg = temp_path.to_string_lossy().to_string();
        }
    }

    new_args.push("--extern".into());
    new_args.push(format!("piano_runtime={}", config.runtime_rlib.display()));
    new_args.push("-L".into());
    new_args.push(format!("dependency={}", config.runtime_deps_dir.display()));

    new_args.push("--remap-path-prefix".into());
    new_args.push(format!("{}={}", temp_path.display(), source_path));

    // Write source map to disk for the orchestrator to use in error remapping.
    // The orchestrator extracts rendered errors from Cargo's JSON output and
    // applies the source map there (the wrapper can't remap because Cargo uses
    // --message-format=json which wraps rustc errors in JSON).
    write_source_map(source_path, &source_map, config_path);

    let exit_code = exec_rustc(real_rustc, &new_args);

    // module_temps and _root_guard drop here, cleaning up all temp files
    drop(module_temps);

    Ok(exit_code)
}

/// Insert `#[path = "..."]` attributes before `mod` declarations that have
/// corresponding temp files. This redirects rustc to compile the instrumented
/// module file instead of the original.
///
/// Uses regex matching instead of line-based matching because instrument_source
/// emits single-line output via to_token_stream().to_string().
fn insert_mod_path_redirects(source: &str, redirects: &HashMap<String, String>) -> String {
    let mut result = source.to_string();
    for (mod_name, temp_name) in redirects {
        // Match: (pub|pub(crate)|pub(super)|pub(in path))? mod <name> ;
        // Handles original formatting and token-stream output (which adds
        // spaces around punctuation, e.g., `pub (crate) mod foo ;`).
        let pattern = format!(
            r"((?:pub(?:\s*\([^)]*\))?\s+)?mod\s+{}\s*;)",
            regex::escape(mod_name)
        );
        let re = regex::Regex::new(&pattern).unwrap();
        let attr = format!("#[path = \"{temp_name}\"] ");
        result = re.replace(&result, format!("{attr}$1")).to_string();
    }
    result
}

fn exec_rustc(rustc: &str, args: &[String]) -> i32 {
    let status = Command::new(rustc).args(args).status();

    match status {
        Ok(s) => s.code().unwrap_or(1),
        Err(e) => {
            eprintln!("piano wrapper: failed to exec rustc: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trip() {
        let config = WrapperConfig {
            runtime_rlib: PathBuf::from(
                "/target/piano/runtime-build/release/deps/libpiano_runtime-abc.rlib",
            ),
            runtime_deps_dir: PathBuf::from("/target/piano/runtime-build/release/deps/"),
            instrument_macros: true,
            entry_point: EntryPointConfig {
                source_path: PathBuf::from("src/main.rs"),
                fn_names: vec!["foo".into(), "bar::baz".into()],
                runs_dir: PathBuf::from("/target/piano/runs/"),
            },
            targets: HashMap::from([(
                PathBuf::from("src/main.rs"),
                HashMap::from([("crate::foo".into(), "foo".into())]),
            )]),
            module_prefixes: HashMap::from([(PathBuf::from("src/main.rs"), String::new())]),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: WrapperConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.runtime_rlib, config.runtime_rlib);
        assert_eq!(deserialized.entry_point.fn_names.len(), 2);
        assert!(
            deserialized
                .targets
                .contains_key(&PathBuf::from("src/main.rs"))
        );
    }

    #[test]
    fn parse_rustc_args_extracts_fields() {
        let args = vec![
            "--crate-name".into(),
            "mylib".into(),
            "--edition=2021".into(),
            "src/lib.rs".into(),
            "--crate-type".into(),
            "lib".into(),
            "--extern".into(),
            "serde=/path/to/serde.rlib".into(),
        ];
        let parsed = ParsedRustcArgs::parse(&args);
        assert_eq!(parsed.crate_name.as_deref(), Some("mylib"));
        assert_eq!(parsed.source_file.as_deref(), Some("src/lib.rs"));
        assert_eq!(parsed.crate_type.as_deref(), Some("lib"));
    }

    #[test]
    fn parse_rustc_args_detects_build_script() {
        let args = vec![
            "--crate-name".into(),
            "build_script_build".into(),
            "build.rs".into(),
            "--crate-type".into(),
            "bin".into(),
        ];
        let parsed = ParsedRustcArgs::parse(&args);
        assert!(parsed.should_skip());
    }

    #[test]
    fn parse_rustc_args_detects_proc_macro() {
        let args = vec![
            "--crate-name".into(),
            "my_derive".into(),
            "src/lib.rs".into(),
            "--crate-type".into(),
            "proc-macro".into(),
        ];
        let parsed = ParsedRustcArgs::parse(&args);
        assert!(parsed.should_skip());
    }

    #[test]
    fn parse_rustc_args_detects_info_query() {
        let args = vec!["-vV".into()];
        let parsed = ParsedRustcArgs::parse(&args);
        assert!(parsed.is_info_query);
    }

    #[test]
    fn parse_rustc_args_detects_probe() {
        let args = vec![
            "--crate-name".into(),
            "___".into(),
            "-".into(),
            "--print=file-names".into(),
        ];
        let parsed = ParsedRustcArgs::parse(&args);
        assert!(parsed.should_skip());
    }
}
