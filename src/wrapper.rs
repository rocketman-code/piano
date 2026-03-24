//! RUSTC_WORKSPACE_WRAPPER mode: intercept rustc invocations to
//! instrument source files and inject piano-runtime.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use std::io::Write as _;

use crate::error::{Error, io_context};
use crate::rewrite::{
    instrument_source,
    allocator::inject_global_allocator,
    registrations::inject_registrations,
    shutdown::inject_shutdown,
};
use crate::source_map::SourceMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct WrapperConfig {
    pub runtime_rlib: PathBuf,
    pub runtime_deps_dir: PathBuf,
    pub entry_point: EntryPointConfig,
    /// Per-file measured maps. Keys are source paths relative to workspace root.
    /// Values map function bare name to numeric ID (for guard injection).
    pub targets: HashMap<PathBuf, HashMap<String, u32>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryPointConfig {
    pub source_path: PathBuf,
    pub name_table: Vec<(u32, String)>,
    pub runs_dir: PathBuf,
    pub cpu_time: bool,
}

pub const CONFIG_ENV: &str = "PIANO_WRAPPER_CONFIG";

pub fn source_maps_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("source_maps.json")
}

pub fn read_source_maps(config_path: &Path) -> HashMap<PathBuf, SourceMap> {
    let path = source_maps_path(config_path);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return HashMap::new();
    };
    serde_json::from_str(&content).unwrap_or_default()
}

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
            if arg.ends_with(".rs") && !arg.starts_with('-') {
                source_file = Some(arg.clone());
            }
            prev = arg;
        }

        Self { crate_name, crate_type, source_file, is_info_query, has_print }
    }

    pub fn should_skip(&self) -> bool {
        if self.is_info_query || self.has_print { return true }
        if self.crate_name.as_deref() == Some("build_script_build") { return true }
        if self.crate_name.as_deref() == Some("___") { return true }
        if self.crate_type.as_deref() == Some("proc-macro") { return true }
        false
    }
}

/// Wrapper entry point. Called when PIANO_WRAPPER_CONFIG is set.
pub fn run_wrapper() -> i32 {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        let _ = writeln!(std::io::stderr(), "piano wrapper: expected rustc path as first argument");
        return 1;
    }

    let real_rustc = &args[1];
    let rustc_args: Vec<String> = args[2..].to_vec();
    let parsed = ParsedRustcArgs::parse(&rustc_args);

    if parsed.should_skip() {
        return exec_rustc(real_rustc, &rustc_args);
    }

    let config_path = match std::env::var(CONFIG_ENV) {
        Ok(p) => p,
        Err(_) => {
            let _ = writeln!(std::io::stderr(), "piano wrapper: {CONFIG_ENV} not set");
            return 1;
        }
    };
    let config: WrapperConfig = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "piano wrapper: failed to read config: {e}");
            return 1;
        }
    };

    let Some(source_path) = &parsed.source_file else {
        return exec_rustc(real_rustc, &rustc_args);
    };
    let source_key = PathBuf::from(source_path);

    let is_entry_point = config.entry_point.source_path == source_key;
    let has_target = config.targets.contains_key(&source_key);

    if !has_target && !is_entry_point {
        return exec_rustc(real_rustc, &rustc_args);
    }

    let crate_name = parsed.crate_name.as_deref().unwrap_or("unknown");
    match rewrite_and_compile(
        real_rustc, &rustc_args, source_path, &source_key, &config,
        Path::new(&config_path), crate_name,
    ) {
        Ok(code) => code,
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "piano wrapper: failed to instrument {source_path}: {e}");
            let _ = writeln!(std::io::stderr(), "piano wrapper: compiling without instrumentation");
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

    let source = std::fs::read_to_string(source_path)
        .map_err(io_context("read source", Path::new(source_path)))?;

    let empty_measured = HashMap::new();
    let measured = config.targets.get(source_key).unwrap_or(&empty_measured);

    // Phase 1: Inject guards
    let result = instrument_source(&source, measured)
        .map_err(|e| Error::BuildFailed(format!("rewrite failed: {e}")))?;

    let mut rewritten = result.source;
    let mut merged_map = result.source_map;

    // Phase 2: Entry point injections (name table, allocator, lifecycle)
    if is_entry_point {
        let name_refs: Vec<(u32, &str)> = config.entry_point.name_table
            .iter()
            .map(|(id, name)| (*id, name.as_str()))
            .collect();

        let (r, reg_map) = inject_registrations(&rewritten, &name_refs)
            .map_err(|e| Error::BuildFailed(format!("registration injection failed: {e}")))?;
        merged_map.merge(reg_map);
        rewritten = r;

        let (r, alloc_map) = inject_global_allocator(&rewritten)
            .map_err(|e| Error::BuildFailed(format!("allocator injection failed: {e}")))?;
        merged_map.merge(alloc_map);
        rewritten = r;

        let runs_dir_str = config.entry_point.runs_dir.to_string_lossy().to_string();
        let (r, shutdown_map) = inject_shutdown(&rewritten, &runs_dir_str, config.entry_point.cpu_time)
            .map_err(|e| Error::BuildFailed(format!("shutdown injection failed: {e}")))?;
        merged_map.merge(shutdown_map);
        rewritten = r;
    }

    // Phase 3: Write source maps
    write_source_maps(&[(source_path.to_string(), merged_map)], config_path);

    // Phase 4: Create staging overlay
    let ws_root = std::env::current_dir()
        .map_err(io_context("get working directory", Path::new(".")))?;
    let staging_root = config_path.parent().unwrap_or(Path::new("."))
        .join(format!("staging-{crate_name}"));

    let instrumented_files = vec![(source_key.to_path_buf(), rewritten)];
    crate::staging::create_staging_overlay(&ws_root, &staging_root, &instrumented_files)?;
    let _staging_guard = crate::staging::StagingGuard(staging_root.clone());

    // Phase 5: Build modified rustc args
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

    let staging_prefix = format!("{}/", staging_root.display());
    new_args.push("--remap-path-prefix".into());
    new_args.push(format!("{staging_prefix}="));

    Ok(exec_rustc(real_rustc, &new_args))
}

fn write_source_maps(entries: &[(String, SourceMap)], config_path: &Path) {
    if entries.is_empty() { return }
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

fn exec_rustc(rustc: &str, args: &[String]) -> i32 {
    let status = Command::new(rustc).args(args).status();
    match status {
        Ok(s) => s.code().unwrap_or(1),
        Err(e) => {
            let _ = writeln!(std::io::stderr(), "piano wrapper: failed to execute {rustc}: {e}");
            1
        }
    }
}
