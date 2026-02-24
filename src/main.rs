use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};

use piano::build::{
    build_instrumented, find_bin_entry_point, find_workspace_root, inject_runtime_dependency,
    inject_runtime_path_dependency, prepare_staging,
};
use piano::error::Error;
use piano::report::{
    diff_runs, find_latest_run_file, format_frames_table, format_table, format_table_with_frames,
    load_latest_run, load_ndjson, load_run, load_tagged_run, save_tag,
};
use piano::resolve::{TargetSpec, resolve_targets};
use piano::rewrite::{
    inject_global_allocator, inject_registrations, inject_shutdown, instrument_source,
};

#[derive(Parser)]
#[command(
    name = "piano",
    about = "Automated instrumentation-based profiling for Rust",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Instrument and build the project. Profiles all functions by default;
    /// use --fn, --file, or --mod to narrow scope.
    Build {
        /// Instrument functions matching a substring (repeatable).
        #[arg(long = "fn", value_name = "PATTERN")]
        fn_patterns: Vec<String>,

        /// Instrument all functions in a file (repeatable).
        #[arg(long = "file", value_name = "PATH")]
        file_patterns: Vec<PathBuf>,

        /// Instrument all functions in a module (repeatable).
        #[arg(long = "mod", value_name = "NAME")]
        mod_patterns: Vec<String>,

        /// Project root (defaults to current directory).
        #[arg(long, default_value = ".")]
        project: PathBuf,

        /// Path to piano-runtime source (for development before publishing).
        #[arg(long)]
        runtime_path: Option<PathBuf>,

        /// Capture per-thread CPU time alongside wall time (Unix only).
        #[arg(long)]
        cpu_time: bool,
    },
    /// Show the latest profiling run (or a specific one).
    Report {
        /// Path to a specific run file. If omitted, shows the latest.
        run: Option<PathBuf>,

        /// Show all functions, including those with zero calls.
        #[arg(long)]
        all: bool,

        /// Show per-frame breakdown with spike detection.
        #[arg(long)]
        frames: bool,
    },
    /// Compare two profiling runs.
    Diff {
        /// First run file.
        a: PathBuf,
        /// Second run file.
        b: PathBuf,
    },
    /// Tag the latest run for easy reference.
    Tag {
        /// Tag name.
        name: String,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Error> {
    match cli.command {
        Commands::Build {
            fn_patterns,
            file_patterns,
            mod_patterns,
            project,
            runtime_path,
            cpu_time,
        } => cmd_build(
            fn_patterns,
            file_patterns,
            mod_patterns,
            project,
            runtime_path,
            cpu_time,
        ),
        Commands::Report { run, all, frames } => cmd_report(run, all, frames),
        Commands::Diff { a, b } => cmd_diff(a, b),
        Commands::Tag { name } => cmd_tag(name),
    }
}

fn cmd_build(
    fn_patterns: Vec<String>,
    file_patterns: Vec<PathBuf>,
    mod_patterns: Vec<String>,
    project: PathBuf,
    runtime_path: Option<PathBuf>,
    cpu_time: bool,
) -> Result<(), Error> {
    let project = std::fs::canonicalize(&project)?;

    // Build target specs from CLI args.
    let mut specs: Vec<TargetSpec> = Vec::new();
    for p in fn_patterns {
        specs.push(TargetSpec::Fn(p));
    }
    for p in file_patterns {
        specs.push(TargetSpec::File(p));
    }
    for m in mod_patterns {
        specs.push(TargetSpec::Mod(m));
    }

    // Resolve targets against the project source.
    let src_dir = project.join("src");
    let targets = resolve_targets(&src_dir, &specs)?;

    let total_fns: usize = targets.iter().map(|t| t.functions.len()).sum();
    eprintln!(
        "found {} function(s) across {} file(s)",
        total_fns,
        targets.len()
    );
    const INSTRUMENT_ALL_WARN_THRESHOLD: usize = 200;
    if specs.is_empty() && total_fns > INSTRUMENT_ALL_WARN_THRESHOLD {
        eprintln!(
            "warning: instrumenting {total_fns} functions may add overhead — \
             use --fn, --file, or --mod to narrow scope"
        );
    }
    for target in &targets {
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        eprintln!("  {}:", relative.display());
        for f in &target.functions {
            eprintln!("    {f}");
        }
    }

    // Detect workspace membership. If the project is a workspace member,
    // stage from the workspace root so inherited fields and cross-member
    // path dependencies resolve correctly.
    let workspace_root = find_workspace_root(&project);
    let (staging_root, member_subdir, package_name) = if let Some(ref ws_root) = workspace_root {
        let relative = project
            .strip_prefix(ws_root)
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .to_path_buf();
        // Read package name from the member's Cargo.toml.
        let member_toml = std::fs::read_to_string(project.join("Cargo.toml"))?;
        let doc: toml_edit::DocumentMut = member_toml
            .parse()
            .map_err(|e| Error::BuildFailed(format!("failed to parse member Cargo.toml: {e}")))?;
        let pkg_name = doc
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::BuildFailed("member Cargo.toml missing package.name".into()))?
            .to_string();
        (ws_root.clone(), Some(relative), Some(pkg_name))
    } else {
        (project.clone(), None, None)
    };

    // Prepare staging directory.
    let staging = tempfile::tempdir()?;
    prepare_staging(&staging_root, staging.path())?;

    // Determine the member directory within staging (workspace root for standalone).
    let member_staging = match &member_subdir {
        Some(sub) => staging.path().join(sub),
        None => staging.path().to_path_buf(),
    };

    // Inject piano-runtime dependency.
    let features: Vec<&str> = if cpu_time { vec!["cpu-time"] } else { vec![] };
    match runtime_path {
        Some(ref path) => {
            let abs_path = std::fs::canonicalize(path)?;
            inject_runtime_path_dependency(&member_staging, &abs_path, &features)?;
        }
        None => {
            inject_runtime_dependency(&member_staging, env!("PIANO_RUNTIME_VERSION"), &features)?;
        }
    }

    // Rewrite each target file in staging.
    let mut all_concurrency: Vec<(String, String)> = Vec::new();
    let mut all_async_skipped: Vec<String> = Vec::new();
    for target in &targets {
        let target_set: HashSet<String> = target.functions.iter().cloned().collect();
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        let staged_file = member_staging.join("src").join(relative);
        let source =
            std::fs::read_to_string(&staged_file).map_err(|source| Error::RunReadError {
                path: staged_file.clone(),
                source,
            })?;

        let result =
            instrument_source(&source, &target_set).map_err(|source| Error::ParseError {
                path: staged_file.clone(),
                source,
            })?;

        all_concurrency.extend(result.concurrency);
        all_async_skipped.extend(result.async_skipped);
        std::fs::write(&staged_file, result.source)?;
    }

    // Warn about async functions that were skipped.
    if !all_async_skipped.is_empty() {
        eprintln!(
            "warning: skipped {} async function(s) that cannot be instrumented:",
            all_async_skipped.len()
        );
        for name in &all_async_skipped {
            eprintln!("           {name}");
        }
        eprintln!("         Async functions may move across threads at .await points,");
        eprintln!("         breaking TLS-based profiling.");
        eprintln!("         See https://github.com/rocketman-code/piano/issues/53");
    }

    // Warn if parallel code was detected without --cpu-time.
    if !cpu_time && !all_concurrency.is_empty() {
        if all_concurrency.len() == 1 {
            let (func, pattern) = &all_concurrency[0];
            eprintln!("warning: {func} uses {pattern} but --cpu-time is not enabled.");
            eprintln!("         Wall time will include time blocked waiting for workers.");
            eprintln!("         Re-run with --cpu-time to separate CPU from wall time.");
        } else {
            eprintln!("warning: parallel code profiled without --cpu-time:");
            for (func, pattern) in &all_concurrency {
                eprintln!("           {func} ({pattern})");
            }
            eprintln!("         Wall time will include time blocked waiting for workers.");
            eprintln!("         Re-run with --cpu-time to separate CPU from wall time.");
        }
    }

    // Inject register calls into the binary entry point for all instrumented functions.
    let bin_entry = find_bin_entry_point(&member_staging)?;
    let main_file = member_staging.join(&bin_entry);
    {
        let all_fn_names: Vec<String> = targets
            .iter()
            .flat_map(|t| t.functions.iter().cloned())
            .collect();
        let main_source =
            std::fs::read_to_string(&main_file).map_err(|source| Error::RunReadError {
                path: main_file.clone(),
                source,
            })?;
        let rewritten = inject_registrations(&main_source, &all_fn_names).map_err(|source| {
            Error::ParseError {
                path: main_file.clone(),
                source,
            }
        })?;

        // Inject global allocator for allocation tracking.
        let existing = if rewritten.contains("#[global_allocator]") {
            Some("existing")
        } else {
            None
        };
        let rewritten =
            inject_global_allocator(&rewritten, existing).map_err(|source| Error::ParseError {
                path: main_file.clone(),
                source,
            })?;

        let rewritten = inject_shutdown(&rewritten).map_err(|source| Error::ParseError {
            path: main_file.clone(),
            source,
        })?;
        std::fs::write(&main_file, rewritten)?;
    }

    // Build the instrumented binary.
    let target_dir = project.join("target").join("piano");
    let binary = build_instrumented(staging.path(), &target_dir, package_name.as_deref())?;

    eprintln!("built: {}", binary.display());
    println!("{}", binary.display());

    Ok(())
}

fn cmd_report(run_path: Option<PathBuf>, show_all: bool, frames: bool) -> Result<(), Error> {
    // Resolve the run file path.
    let resolved_path = match &run_path {
        Some(p) if p.exists() => Some(p.clone()),
        Some(p) => {
            // Tag lookup — no direct file path, so no frame data available.
            let tag = p.to_string_lossy();
            let tags_dir = default_tags_dir()?;
            let runs_dir = default_runs_dir()?;
            let run = load_tagged_run(&tags_dir, &runs_dir, &tag)?;
            print!("{}", format_table(&run, show_all));
            return Ok(());
        }
        None => {
            // Find the latest run file.
            let dir = default_runs_dir()?;
            find_latest_run_file(&dir)?
        }
    };

    // If we have a .ndjson file, load frame data for richer output.
    if let Some(path) = &resolved_path
        && path.extension().and_then(|e| e.to_str()) == Some("ndjson")
    {
        let (_run, frame_data) = load_ndjson(path)?;
        if frames {
            print!("{}", format_frames_table(&frame_data));
        } else {
            print!("{}", format_table_with_frames(&frame_data, show_all));
        }
        return Ok(());
    }

    // Fall back to JSON table.
    let run = match resolved_path {
        Some(p) => load_run(&p)?,
        None => {
            let dir = default_runs_dir()?;
            load_latest_run(&dir)?
        }
    };
    print!("{}", format_table(&run, show_all));
    Ok(())
}

fn cmd_diff(a: PathBuf, b: PathBuf) -> Result<(), Error> {
    let run_a = resolve_run_arg(&a)?;
    let run_b = resolve_run_arg(&b)?;
    print!("{}", diff_runs(&run_a, &run_b));
    Ok(())
}

fn cmd_tag(name: String) -> Result<(), Error> {
    let runs_dir = default_runs_dir()?;
    let tags_dir = default_tags_dir()?;
    let latest = load_latest_run(&runs_dir)?;
    let run_id = latest
        .run_id
        .ok_or_else(|| Error::NoRuns(runs_dir.clone()))?;
    save_tag(&tags_dir, &name, &run_id)?;
    eprintln!("tagged '{name}' -> {run_id}");
    Ok(())
}

fn resolve_run_arg(arg: &Path) -> Result<piano::report::Run, Error> {
    if arg.exists() {
        return load_run(arg);
    }
    let tag = arg.to_string_lossy();
    let tags_dir = default_tags_dir()?;
    let runs_dir = default_runs_dir()?;
    load_tagged_run(&tags_dir, &runs_dir, &tag)
}

fn default_runs_dir() -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_RUNS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var_os("HOME").ok_or(Error::HomeNotFound)?;
    Ok(PathBuf::from(home).join(".piano").join("runs"))
}

fn default_tags_dir() -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_TAGS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var_os("HOME").ok_or(Error::HomeNotFound)?;
    Ok(PathBuf::from(home).join(".piano").join("tags"))
}
