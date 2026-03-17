//! RUSTC_WORKSPACE_WRAPPER mode: intercept rustc invocations to rewrite
//! source files and inject piano-runtime.

use std::collections::{HashMap, HashSet};
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
    /// Per-file measured maps. Keys are source paths relative to workspace root.
    /// Values map function bare name to numeric ID (for guard injection).
    pub targets: HashMap<PathBuf, HashMap<String, u32>>,
    /// All instrumentable function names across the workspace.
    /// Functions in this set receive ctx parameter injection (pass-through).
    pub all_instrumentable: HashSet<String>,
    /// Per-file module prefixes for the MacroInstrumenter.
    pub module_prefixes: HashMap<PathBuf, String>,
    /// Global name -> ID map (includes macro-discovered names).
    /// Passed to instrument_source for macro instrumentation.
    pub macro_name_ids: HashMap<String, u32>,
}

/// Configuration for the binary entry point file.
#[derive(Debug, Serialize, Deserialize)]
pub struct EntryPointConfig {
    /// Source path relative to workspace root (e.g., "src/main.rs")
    pub source_path: PathBuf,
    /// The complete (id, display_name) name table for inject_registrations.
    pub name_table: Vec<(u32, String)>,
    /// Directory for NDJSON output files
    pub runs_dir: PathBuf,
    /// Whether to measure CPU time
    pub cpu_time: bool,
    /// Whether --output-dir was used (cli override)
    pub cli_override: bool,
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

/// Derive the concurrency output file path from the config file path.
pub fn concurrency_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("concurrency.ndjson")
}

/// Read concurrency entries written by wrapper processes during instrumentation.
/// Each line is a JSON object: {"func": "name", "pattern": "par_iter"}
pub fn read_concurrency(config_path: &Path) -> Vec<(String, String)> {
    let path = concurrency_path(config_path);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    content
        .lines()
        .filter_map(|line| {
            let entry: ConcurrencyEntry = serde_json::from_str(line).ok()?;
            Some((entry.func, entry.pattern))
        })
        .collect()
}

/// A single concurrency detection entry in the NDJSON file.
#[derive(Debug, Serialize, Deserialize)]
struct ConcurrencyEntry {
    func: String,
    pattern: String,
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
    let crate_name = parsed.crate_name.as_deref().unwrap_or("unknown");
    match rewrite_and_compile(
        real_rustc,
        &rustc_args,
        source_path,
        &source_key,
        &config,
        config_p,
        crate_name,
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

/// Write source maps in a single read-modify-write to minimize the race
/// window when concurrent wrapper processes access source_maps.json.
fn write_source_maps_batch(entries: &[(String, SourceMap)], config_path: &Path) {
    if entries.is_empty() {
        return;
    }
    let maps_path = source_maps_path(config_path);

    let mut maps: HashMap<PathBuf, SourceMap> = std::fs::read_to_string(&maps_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    for (source_file, map) in entries {
        maps.insert(PathBuf::from(source_file), map.clone());
    }

    if let Ok(json) = serde_json::to_string(&maps) {
        let _ = std::fs::write(&maps_path, json);
    }
}

/// Write concurrency entries to the shared NDJSON file using O_APPEND.
/// Each wrapper invocation accumulates entries in memory, then writes them
/// all in a single atomic append. Safe for concurrent wrapper processes.
fn write_concurrency(entries: &[(String, String)], config_path: &Path) {
    if entries.is_empty() {
        return;
    }
    let path = concurrency_path(config_path);
    let mut buf = String::new();
    for (func, pattern) in entries {
        let entry = ConcurrencyEntry {
            func: func.clone(),
            pattern: pattern.clone(),
        };
        // serde_json::to_string won't fail for simple string fields.
        if let Ok(json) = serde_json::to_string(&entry) {
            buf.push_str(&json);
            buf.push('\n');
        }
    }
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    else {
        return;
    };
    let _ = std::io::Write::write_all(&mut f, buf.as_bytes());
}

fn rewrite_and_compile(
    real_rustc: &str,
    rustc_args: &[String],
    source_path: &str,
    source_key: &Path,
    config: &WrapperConfig,
    config_path: &Path,
    crate_name: &str,
) -> Result<i32, Error> {
    let is_entry_point = config.entry_point.source_path == source_key;
    let source_parent = source_key.parent().unwrap_or(Path::new(""));

    // Phase 1: Instrument all target files, collecting (relative_path, content)
    let mut instrumented_files: Vec<(PathBuf, String)> = Vec::new();
    let mut all_concurrency: Vec<(String, String)> = Vec::new();
    let mut all_source_maps: Vec<(String, SourceMap)> = Vec::new();

    for (target_path, target_measured) in &config.targets {
        if target_path == source_key {
            continue; // Handle root file below
        }
        let target_parent = target_path.parent();
        if target_parent != Some(source_parent) && !target_path.starts_with(source_parent) {
            continue;
        }

        let module_prefix = config
            .module_prefixes
            .get(target_path)
            .map(|s| s.as_str())
            .unwrap_or("");

        let module_source = match std::fs::read_to_string(target_path.as_ref() as &Path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let result = instrument_source(
            &module_source,
            target_measured,
            &config.all_instrumentable,
            config.instrument_macros,
            module_prefix,
            &config.macro_name_ids,
        )
        .map_err(|e| {
            Error::BuildFailed(format!("rewrite failed for {}: {e}", target_path.display()))
        })?;
        all_concurrency.extend(result.concurrency);

        all_source_maps.push((
            target_path.to_string_lossy().into_owned(),
            result.source_map,
        ));
        instrumented_files.push((target_path.clone(), result.source));
    }

    // Instrument the crate root file
    let source = std::fs::read_to_string(source_path)
        .map_err(io_context("read source", Path::new(source_path)))?;
    let root_measured = config.targets.get(source_key);
    let module_prefix = config
        .module_prefixes
        .get(source_key)
        .map(|s| s.as_str())
        .unwrap_or("");
    let empty_measured = HashMap::new();
    let measured = root_measured.unwrap_or(&empty_measured);
    let result = instrument_source(
        &source,
        measured,
        &config.all_instrumentable,
        config.instrument_macros,
        module_prefix,
        &config.macro_name_ids,
    )
    .map_err(|e| Error::BuildFailed(format!("rewrite failed: {e}")))?;
    all_concurrency.extend(result.concurrency);

    let mut rewritten = result.source;
    let source_map = result.source_map;

    // Entry point: inject registrations, allocator, shutdown
    if is_entry_point {
        // Build the (u32, &str) slice from the pre-computed name table.
        let name_refs: Vec<(u32, &str)> = config
            .entry_point
            .name_table
            .iter()
            .map(|(id, name)| (*id, name.as_str()))
            .collect();

        let (r, reg_map) = inject_registrations(&rewritten, &name_refs)
            .map_err(|e| Error::BuildFailed(format!("registration injection failed: {e}")))?;
        let mut merged_map = source_map;
        merged_map.merge(reg_map);
        rewritten = r;

        let alloc_kind = detect_allocator_kind(&rewritten)
            .map_err(|e| Error::BuildFailed(format!("allocator detection failed: {e}")))?;
        let (r, alloc_map) = inject_global_allocator(&rewritten, alloc_kind)
            .map_err(|e| Error::BuildFailed(format!("allocator injection failed: {e}")))?;
        merged_map.merge(alloc_map);
        rewritten = r;

        let runs_dir_str = config.entry_point.runs_dir.to_string_lossy().to_string();
        let (r, shutdown_map) = inject_shutdown(
            &rewritten,
            &runs_dir_str,
            config.entry_point.cpu_time,
            config.entry_point.cli_override,
        )
        .map_err(|e| Error::BuildFailed(format!("shutdown injection failed: {e}")))?;
        merged_map.merge(shutdown_map);
        rewritten = r;

        all_source_maps.push((source_path.to_string(), merged_map));
    } else {
        all_source_maps.push((source_path.to_string(), source_map));
    }

    instrumented_files.push((source_key.to_path_buf(), rewritten));

    // Phase 2: Create staging overlay
    let ws_root =
        std::env::current_dir().map_err(io_context("get working directory", Path::new(".")))?;
    let staging_root = config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join(format!("staging-{crate_name}"));

    crate::staging::create_staging_overlay(&ws_root, &staging_root, &instrumented_files)?;
    let _staging_guard = crate::staging::StagingGuard(staging_root.clone());

    // Phase 3: Build modified rustc args
    let staging_source = staging_root.join(source_path);
    let mut new_args = rustc_args.to_vec();

    for arg in &mut new_args {
        if arg == source_path {
            *arg = staging_source.to_string_lossy().to_string();
        }
    }

    new_args.push("--extern".into());
    new_args.push(format!("piano_runtime={}", config.runtime_rlib.display()));
    new_args.push("-L".into());
    new_args.push(format!("dependency={}", config.runtime_deps_dir.display()));

    // Remap staging paths back to original workspace paths for debug symbols.
    // Trailing separator ensures the prefix match doesn't produce a leading '/'.
    let staging_prefix = format!("{}/", staging_root.display());
    new_args.push("--remap-path-prefix".into());
    new_args.push(format!("{staging_prefix}="));

    write_source_maps_batch(&all_source_maps, config_path);
    write_concurrency(&all_concurrency, config_path);

    let exit_code = exec_rustc(real_rustc, &new_args);

    // _staging_guard drops here, removing the staging directory

    Ok(exit_code)
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
                name_table: vec![(0, "foo".into()), (1, "bar::baz".into())],
                runs_dir: PathBuf::from("/target/piano/runs/"),
                cpu_time: false,
                cli_override: false,
            },
            targets: HashMap::from([(
                PathBuf::from("src/main.rs"),
                HashMap::from([("foo".into(), 0)]),
            )]),
            all_instrumentable: HashSet::from(["foo".into(), "bar".into()]),
            module_prefixes: HashMap::from([(PathBuf::from("src/main.rs"), String::new())]),
            macro_name_ids: HashMap::new(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: WrapperConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.runtime_rlib, config.runtime_rlib);
        assert_eq!(deserialized.entry_point.name_table.len(), 2);
        assert!(
            deserialized
                .targets
                .contains_key(&PathBuf::from("src/main.rs"))
        );
        assert_eq!(deserialized.all_instrumentable.len(), 2);
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
    fn concurrency_path_derives_from_config() {
        let config = Path::new("/target/piano/config.json");
        assert_eq!(
            super::concurrency_path(config),
            PathBuf::from("/target/piano/concurrency.ndjson"),
        );
    }

    #[test]
    fn read_concurrency_empty_when_missing() {
        let config = Path::new("/tmp/nonexistent/config.json");
        assert!(super::read_concurrency(config).is_empty());
    }

    #[test]
    fn read_concurrency_parses_ndjson() {
        let dir = std::env::temp_dir().join("piano-test-concurrency");
        let _ = std::fs::create_dir_all(&dir);
        let config = dir.join("config.json");
        let ndjson_path = dir.join("concurrency.ndjson");
        std::fs::write(
            &ndjson_path,
            "{\"func\":\"a\",\"pattern\":\"par_iter\"}\n{\"func\":\"b\",\"pattern\":\"rayon::scope\"}\n",
        )
        .unwrap();

        let result = super::read_concurrency(&config);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("a".to_string(), "par_iter".to_string()));
        assert_eq!(result[1], ("b".to_string(), "rayon::scope".to_string()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_concurrency_appends_ndjson() {
        let dir = std::env::temp_dir().join("piano-test-write-concurrency");
        let _ = std::fs::create_dir_all(&dir);
        let config = dir.join("config.json");
        let ndjson_path = super::concurrency_path(&config);
        let _ = std::fs::remove_file(&ndjson_path);

        // First write (simulates wrapper process 1)
        let entries1 = vec![("func_a".to_string(), "par_iter".to_string())];
        super::write_concurrency(&entries1, &config);

        // Second write (simulates wrapper process 2)
        let entries2 = vec![
            ("func_b".to_string(), "rayon::scope".to_string()),
            ("func_c".to_string(), "par_iter".to_string()),
        ];
        super::write_concurrency(&entries2, &config);

        // Both writes should be present
        let result = super::read_concurrency(&config);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "func_a");
        assert_eq!(result[1].0, "func_b");
        assert_eq!(result[2].0, "func_c");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_concurrency_skips_empty() {
        let dir = std::env::temp_dir().join("piano-test-write-empty");
        let _ = std::fs::create_dir_all(&dir);
        let config = dir.join("config.json");
        let ndjson_path = super::concurrency_path(&config);
        let _ = std::fs::remove_file(&ndjson_path);

        super::write_concurrency(&[], &config);
        assert!(
            !ndjson_path.exists(),
            "should not create file for empty entries"
        );

        let _ = std::fs::remove_dir_all(&dir);
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
