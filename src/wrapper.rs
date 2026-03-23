//! RUSTC_WORKSPACE_WRAPPER mode: intercept rustc invocations to
//! instrument source files and inject piano-runtime.
//!
//! STUB: the rewriter is being rebuilt from scratch. This file retains
//! the config structs and utility functions used by the build pipeline.
//! The actual rewriting logic will be reimplemented against the new
//! runtime API (no parameter injection, guard-only instrumentation).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::source_map::SourceMap;

/// Configuration passed from the piano orchestrator to the wrapper via
/// a JSON file. The path is communicated through PIANO_WRAPPER_CONFIG.
#[derive(Debug, Serialize, Deserialize)]
pub struct WrapperConfig {
    pub runtime_rlib: PathBuf,
    pub runtime_deps_dir: PathBuf,
    pub instrument_macros: bool,
    pub entry_point: EntryPointConfig,
    pub targets: HashMap<PathBuf, HashMap<String, u32>>,
    pub all_instrumentable: HashSet<String>,
    pub module_prefixes: HashMap<PathBuf, String>,
    pub macro_name_ids: HashMap<String, u32>,
}

/// Configuration for the binary entry point file.
#[derive(Debug, Serialize, Deserialize)]
pub struct EntryPointConfig {
    pub source_path: PathBuf,
    pub name_table: Vec<(u32, String)>,
    pub runs_dir: PathBuf,
    pub cpu_time: bool,
    pub cli_override: bool,
}

/// Environment variable name for the config file path.
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

pub fn concurrency_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("concurrency.ndjson")
}

pub fn read_concurrency(config_path: &Path) -> Vec<(String, String)> {
    let path = concurrency_path(config_path);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    content
        .lines()
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).ok()?;
            let func = v.get("func")?.as_str()?.to_string();
            let pattern = v.get("pattern")?.as_str()?.to_string();
            Some((func, pattern))
        })
        .collect()
}

/// Wrapper entry point. Called when PIANO_WRAPPER_CONFIG is set.
///
/// STUB: rewriter is being rebuilt. Returns error exit code.
pub fn run_wrapper() -> i32 {
    eprintln!("piano: rewriter is being rebuilt. Instrumentation is temporarily unavailable.");
    1
}
