use anstyle::{Effects, Style};

pub(crate) mod diff;
pub(crate) mod format;
pub(crate) mod load;
pub(crate) mod tag;

#[cfg(test)]
pub(crate) mod test_util;

pub(super) const HEADER: Style = Style::new().bold();
pub(super) const DIM: Style = Style::new().effects(Effects::DIMMED);

/// Describes the file format a Run was loaded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunFormat {
    #[default]
    Json,
    Ndjson,
}

/// Whether an NDJSON run file was fully written or recovered from a crash.
///
/// Complete files have a trailer with the authoritative name table.
/// Recovered files are missing the trailer (process was killed before shutdown)
/// so the header name table is used instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCompleteness {
    /// File has header + measurements + trailer. Name table from trailer.
    Complete,
    /// File has header + measurements but no trailer (crashed/killed run).
    /// Name table from header.
    Recovered,
}

/// A single profiling run loaded from a JSON file written by piano-runtime.
#[derive(Debug, serde::Deserialize)]
pub struct Run {
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(alias = "timestamp")]
    pub timestamp_ms: u128,
    pub functions: Vec<FnEntry>,
    /// The file format this run was loaded from (not serialized).
    #[serde(skip)]
    pub source_format: RunFormat,
}

/// Timing data for one function within a profiling run.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct FnEntry {
    pub name: String,
    pub calls: u64,
    #[serde(default)]
    pub total_ms: Option<f64>,
    pub self_ms: f64,
    #[serde(default)]
    pub cpu_self_ms: Option<f64>,
    #[serde(default)]
    pub alloc_count: u64,
    #[serde(default)]
    pub alloc_bytes: u64,
    #[serde(default)]
    pub free_count: u64,
    #[serde(default)]
    pub free_bytes: u64,
}

/// Accumulated per-function counters (used during NDJSON aggregation).
#[derive(Default, Clone, Copy)]
pub(super) struct FnAgg {
    pub(super) calls: u64,
    pub(super) self_ns: u64,
    pub(super) inclusive_ns: u64,
    pub(super) alloc_count: u64,
    pub(super) alloc_bytes: u64,
    pub(super) free_count: u64,
    pub(super) free_bytes: u64,
    pub(super) cpu_self_ns: u64,
}

/// NDJSON header/trailer line.
///
/// Both header and trailer share the same structure: a "type" field
/// ("header" or "trailer") and a "names" map of name_id -> function name.
/// The header is written eagerly at startup; the trailer confirms clean shutdown.
#[derive(serde::Deserialize)]
pub(super) struct NdjsonNameTable {
    /// "header" or "trailer".
    #[serde(rename = "type")]
    pub(super) kind: String,
    /// Name table: string keys (name_id) mapped to function names.
    #[serde(default)]
    pub(super) names: std::collections::HashMap<String, String>,
}

/// NDJSON measurement line -- one per completed function invocation.
/// Used by --raw-spans mode.
#[derive(serde::Deserialize)]
pub(super) struct NdjsonMeasurement {
    pub(super) span_id: u64,
    pub(super) parent_span_id: u64,
    pub(super) name_id: u32,
    pub(super) start_ns: u64,
    pub(super) end_ns: u64,
    #[serde(default)]
    pub(super) thread_id: u64,
    #[serde(default)]
    pub(super) cpu_start_ns: u64,
    #[serde(default)]
    pub(super) cpu_end_ns: u64,
    pub(super) alloc_count: u64,
    pub(super) alloc_bytes: u64,
    #[serde(default)]
    pub(super) free_count: u64,
    #[serde(default)]
    pub(super) free_bytes: u64,
}

/// NDJSON aggregate line -- one per function (default mode).
/// Self-time is pre-computed by the runtime.
#[derive(serde::Deserialize)]
pub(super) struct NdjsonAggregate {
    #[serde(default)]
    pub(super) thread: u64,
    pub(super) name_id: u32,
    pub(super) calls: u64,
    pub(super) self_ns: u64,
    #[serde(default)]
    pub(super) inclusive_ns: u64,
    #[serde(default)]
    pub(super) cpu_self_ns: u64,
    #[serde(default)]
    pub(super) alloc_count: u64,
    #[serde(default)]
    pub(super) alloc_bytes: u64,
    #[serde(default)]
    pub(super) free_count: u64,
    #[serde(default)]
    pub(super) free_bytes: u64,
}

/// Format a SystemTime as a relative duration string ("N sec/min/hours/days ago").
pub fn relative_time(t: std::time::SystemTime) -> String {
    let elapsed = t.elapsed().unwrap_or_default();
    let secs = elapsed.as_secs();
    if secs < 60 {
        format!("{secs} sec ago")
    } else if secs < 3600 {
        format!("{} min ago", secs / 60)
    } else if secs < 86400 {
        let h = secs / 3600;
        let unit = if h == 1 { "hour" } else { "hours" };
        format!("{h} {unit} ago")
    } else {
        let d = secs / 86400;
        let unit = if d == 1 { "day" } else { "days" };
        format!("{d} {unit} ago")
    }
}

// Re-exports so external code can use `crate::report::load_run` etc.
pub use diff::{JsonDiffEntry, diff_runs, diff_runs_json};
pub use format::{JsonFnEntry, format_json, format_per_thread_tables, format_table};
pub use load::{
    find_latest_run_file, find_latest_run_file_since, find_ndjson_by_run_id, load_latest_run,
    load_latest_runs_per_thread, load_ndjson, load_ndjson_per_thread, load_run, load_run_by_id,
    load_two_latest_runs,
};
pub use tag::{load_tagged_run, resolve_tag, reverse_resolve_tag, save_tag};
