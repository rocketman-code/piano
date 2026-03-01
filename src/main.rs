use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};

use piano::build::{
    build_instrumented, find_bin_entry_point, find_project_root, find_workspace_root,
    inject_runtime_dependency, inject_runtime_path_dependency, prepare_staging,
};
use piano::error::Error;
use piano::report::{
    diff_runs, find_latest_run_file, find_ndjson_by_run_id, format_frames_table, format_table,
    format_table_with_frames, load_latest_run, load_ndjson, load_run, load_run_by_id,
    load_tagged_run, load_two_latest_runs, relative_time, resolve_tag, reverse_resolve_tag,
    save_tag,
};
use piano::resolve::{ResolveResult, SkippedFunction, TargetSpec, resolve_targets};
use piano::rewrite::{
    inject_global_allocator, inject_registrations, inject_shutdown, instrument_source,
};

#[derive(Parser)]
#[command(
    name = "piano",
    about = "Automated instrumentation-based profiling for Rust",
    version,
    after_help = "Workflow: piano profile [OPTIONS] (or: piano build, piano run, piano report)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
struct BuildOpts {
    /// Instrument functions whose name contains PATTERN (repeatable).
    /// e.g. --fn parse matches parse, parse_line, MyStruct::try_parse.
    #[arg(long = "fn", value_name = "PATTERN")]
    fn_patterns: Vec<String>,

    /// Match --fn patterns exactly instead of by substring.
    #[arg(long, requires = "fn_patterns")]
    exact: bool,

    /// Instrument all functions in a file (repeatable).
    #[arg(long = "file", value_name = "PATH")]
    file_patterns: Vec<PathBuf>,

    /// Instrument all functions in a module (repeatable).
    #[arg(long = "mod", value_name = "NAME")]
    mod_patterns: Vec<String>,

    /// Project root (auto-detected from Cargo.toml).
    #[arg(long)]
    project: Option<PathBuf>,

    /// Path to piano-runtime source (for development before publishing).
    #[arg(long)]
    runtime_path: Option<PathBuf>,

    /// Capture per-thread CPU time alongside wall time (Unix only).
    #[arg(long)]
    cpu_time: bool,

    /// Show functions excluded from instrumentation and exit.
    #[arg(long)]
    list_skipped: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Instrument and build the project. Profiles all functions by default;
    /// use --fn, --file, or --mod to narrow scope.
    Build {
        #[command(flatten)]
        opts: BuildOpts,
    },
    /// Execute the last-built instrumented binary.
    /// Pass arguments to the binary after --.
    Run {
        /// Arguments to pass to the instrumented binary (after --).
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Build, execute, and report in one step.
    /// Pass arguments to the binary after --.
    Profile {
        #[command(flatten)]
        opts: BuildOpts,

        /// Show all functions, including those with zero calls.
        #[arg(long)]
        all: bool,

        /// Show per-frame breakdown with spike detection.
        #[arg(long)]
        frames: bool,

        /// Suppress warning when program exits with non-zero code.
        #[arg(long)]
        ignore_exit_code: bool,

        /// Arguments to pass to the instrumented binary (after --).
        #[arg(last = true)]
        args: Vec<String>,
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
        /// First run (path or tag; omit both to compare two most recent runs).
        a: Option<PathBuf>,
        /// Second run (path or tag).
        b: Option<PathBuf>,
    },
    /// Tag the latest run, or list existing tags (no args).
    Tag {
        /// Tag name. If omitted, lists all saved tags.
        name: Option<String>,
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
        Commands::Build { opts } => cmd_build(opts),
        Commands::Run { args } => cmd_run(args),
        Commands::Profile {
            opts,
            all,
            frames,
            ignore_exit_code,
            args,
        } => cmd_profile(opts, all, frames, ignore_exit_code, args),
        Commands::Report { run, all, frames } => cmd_report(run, all, frames),
        Commands::Diff { a, b } => cmd_diff(a, b),
        Commands::Tag { name } => cmd_tag(name),
    }
}

/// Deduplicate and sort skip reasons into a comma-separated string.
fn unique_skip_reasons(skipped: &[SkippedFunction]) -> String {
    skipped
        .iter()
        .map(|s| s.reason.to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build an instrumented binary and return (binary_path, runs_dir).
///
/// Returns `Ok(None)` when `--list-skipped` is used (early exit after printing).
fn build_project(opts: BuildOpts) -> Result<Option<(PathBuf, PathBuf)>, Error> {
    let BuildOpts {
        fn_patterns,
        exact,
        file_patterns,
        mod_patterns,
        project,
        runtime_path,
        cpu_time,
        list_skipped,
    } = opts;

    let project = match project {
        Some(p) => p,
        None => find_project_root(&std::env::current_dir()?)?,
    };

    if !project.exists() {
        return Err(Error::BuildFailed(format!(
            "project directory does not exist: {}",
            project.display()
        )));
    }
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
    if !src_dir.is_dir() {
        return Err(Error::BuildFailed(format!(
            "no src/ directory found in {} — is this a Rust project?",
            project.display()
        )));
    }
    let ResolveResult { targets, skipped } = resolve_targets(&src_dir, &specs, exact)?;

    if list_skipped {
        if skipped.is_empty() {
            eprintln!("no functions skipped");
        } else {
            for s in &skipped {
                println!("{}: {} ({})", s.path.display(), s.name, s.reason);
            }
        }
        return Ok(None);
    }

    if !specs.is_empty() && !skipped.is_empty() {
        eprintln!(
            "warning: {} function(s) skipped ({}) -- run piano build --list-skipped to see which",
            skipped.len(),
            unique_skip_reasons(&skipped)
        );
    }

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
    // Use a stable path so cargo can cache incremental builds across runs.
    // Dependencies compile once; only instrumented source files recompile.
    let staging = staging_root.join("target/piano/staging");
    std::fs::create_dir_all(&staging)?;
    prepare_staging(&staging_root, &staging)?;

    // Determine the member directory within staging (workspace root for standalone).
    let member_staging = match &member_subdir {
        Some(sub) => staging.join(sub),
        None => staging.clone(),
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
    let instrument_macros = specs.is_empty();
    let mut all_concurrency: Vec<(String, String)> = Vec::new();
    for target in &targets {
        let target_set: HashSet<String> = target.functions.iter().cloned().collect();
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        let staged_file = member_staging.join("src").join(relative);
        let display_path = PathBuf::from("src").join(relative);
        let source =
            std::fs::read_to_string(&staged_file).map_err(|source| Error::RunReadError {
                path: display_path.clone(),
                source,
            })?;

        let result =
            instrument_source(&source, &target_set, instrument_macros).map_err(|source| {
                Error::ParseError {
                    path: display_path,
                    source,
                }
            })?;

        all_concurrency.extend(result.concurrency);
        std::fs::write(&staged_file, result.source)?;
    }

    // Warn if parallel code was detected without --cpu-time.
    if !cpu_time && !all_concurrency.is_empty() {
        for (func, _pattern) in &all_concurrency {
            eprintln!(
                "warning: {func} spawns parallel work -- add --cpu-time to see computation time"
            );
        }
    }

    // Inject register calls into the binary entry point for all instrumented functions.
    let bin_entry = find_bin_entry_point(&member_staging)?;
    let main_file = member_staging.join(&bin_entry);
    let target_dir = project.join("target").join("piano");
    let runs_dir = target_dir.join("runs");
    std::fs::create_dir_all(&runs_dir)?;
    {
        let all_fn_names: Vec<String> = targets
            .iter()
            .flat_map(|t| t.functions.iter().cloned())
            .collect();
        let main_source =
            std::fs::read_to_string(&main_file).map_err(|source| Error::RunReadError {
                path: bin_entry.clone(),
                source,
            })?;
        let rewritten = inject_registrations(&main_source, &all_fn_names).map_err(|source| {
            Error::ParseError {
                path: bin_entry.clone(),
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
                path: bin_entry.clone(),
                source,
            })?;

        let runs_dir_str = runs_dir.to_string_lossy().to_string();
        let rewritten = inject_shutdown(&rewritten, Some(&runs_dir_str)).map_err(|source| {
            Error::ParseError {
                path: bin_entry.clone(),
                source,
            }
        })?;
        std::fs::write(&main_file, rewritten)?;
    }

    // Build the instrumented binary.
    let binary = build_instrumented(&staging, &target_dir, package_name.as_deref())?;

    Ok(Some((binary, runs_dir)))
}

fn cmd_build(opts: BuildOpts) -> Result<(), Error> {
    let Some((binary, _runs_dir)) = build_project(opts)? else {
        return Ok(());
    };
    let display_name = binary
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| binary.display().to_string());
    eprintln!("built: {display_name}");
    if !std::io::stdout().is_terminal() {
        println!("{}", binary.display());
    }

    Ok(())
}

fn find_latest_binary() -> Result<PathBuf, Error> {
    let project = find_project_root(&std::env::current_dir()?).map_err(|_| Error::NoBinary)?;
    let dir = project.join("target/piano/debug");
    if !dir.is_dir() {
        return Err(Error::NoBinary);
    }
    let mut best: Option<(PathBuf, std::time::SystemTime)> = None;
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Skip files with extensions (e.g. .d, .fingerprint) -- binaries have no extension on unix
        if path.extension().is_some() {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if entry.metadata()?.permissions().mode() & 0o111 == 0 {
                continue; // not executable
            }
        }
        let mtime = entry.metadata()?.modified()?;
        if best.as_ref().is_none_or(|(_, t)| mtime > *t) {
            best = Some((path, mtime));
        }
    }
    best.map(|(p, _)| p).ok_or(Error::NoBinary)
}

fn cmd_run(args: Vec<String>) -> Result<(), Error> {
    let binary = find_latest_binary()?;
    eprintln!("running: {}", binary.display());

    let status = std::process::Command::new(&binary)
        .args(&args)
        .status()
        .map_err(|e| Error::RunFailed(format!("failed to run {}: {e}", binary.display())))?;

    std::process::exit(status.code().unwrap_or(1));
}

fn cmd_profile(
    opts: BuildOpts,
    show_all: bool,
    frames: bool,
    ignore_exit_code: bool,
    args: Vec<String>,
) -> Result<(), Error> {
    let Some((binary, runs_dir)) = build_project(opts)? else {
        return Ok(());
    };
    let display_name = binary
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| binary.display().to_string());
    eprintln!("built: {display_name}");

    let status = std::process::Command::new(&binary)
        .args(&args)
        .status()
        .map_err(|e| Error::RunFailed(format!("failed to run {}: {e}", binary.display())))?;

    if !status.success() && !ignore_exit_code {
        if let Some(code) = status.code() {
            eprintln!(
                "warning: program exited with code {code} — profiling results may be incomplete"
            );
        } else {
            eprintln!(
                "warning: program terminated by signal — profiling results may be incomplete"
            );
        }
    }

    // Point cmd_report at the project's runs directory so it works even when
    // CWD differs from the --project path. Skip if already set — the user
    // or test harness may have overridden it, and the runtime's shutdown_to()
    // respects PIANO_RUNS_DIR too.
    if std::env::var_os("PIANO_RUNS_DIR").is_none() {
        // SAFETY: single-threaded CLI at this point — no concurrent env reads.
        unsafe { std::env::set_var("PIANO_RUNS_DIR", &runs_dir) };
    }

    eprintln!();
    let report_result = match cmd_report(None, show_all, frames) {
        Ok(()) => Ok(()),
        Err(Error::NoRuns) if !status.success() => {
            // Program failed and produced no data. The program's own error
            // output is the primary affordance (UX principle 6). Suppress
            // Piano's NoRuns to avoid cascading errors.
            Ok(())
        }
        Err(Error::NoRuns) => {
            // Program exited successfully but no data was written.
            // Something went wrong with the runtime's write -- give an
            // actionable message.
            Err(Error::NoDataWritten(runs_dir))
        }
        Err(e) => Err(e),
    };

    report_result?;

    if !status.success() && !ignore_exit_code {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}

fn cmd_report(run_path: Option<PathBuf>, show_all: bool, frames: bool) -> Result<(), Error> {
    // Resolve the run file path.
    let resolved_path = match &run_path {
        Some(p) if p.exists() => Some(p.clone()),
        Some(p) => {
            // Tag lookup — resolve to NDJSON file if available.
            let tag = p.to_string_lossy();
            let tags_dir = default_tags_dir()?;
            let runs_dir = default_runs_dir()?;
            let run_id = resolve_tag(&tags_dir, &tag)?;
            match find_ndjson_by_run_id(&runs_dir, &run_id)? {
                Some(ndjson_path) => Some(ndjson_path),
                None => {
                    // No NDJSON — fall back to basic JSON table.
                    let run = load_run_by_id(&runs_dir, &run_id).map_err(|e| match e {
                        Error::NoRuns => Error::RunNotFound {
                            tag: tag.to_string(),
                        },
                        other => other,
                    })?;
                    anstream::print!("{}", format_table(&run, show_all));
                    return Ok(());
                }
            }
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
            anstream::print!("{}", format_frames_table(&frame_data));
        } else {
            anstream::print!("{}", format_table_with_frames(&frame_data, show_all));
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
    anstream::print!("{}", format_table(&run, show_all));
    Ok(())
}

/// Derive a display label from a diff argument.
///
/// Tag names pass through as-is; file paths use the filename stem.
fn diff_label(arg: &Path) -> String {
    if arg.exists() {
        arg.file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| arg.to_string_lossy().into_owned())
    } else {
        arg.to_string_lossy().into_owned()
    }
}

fn cmd_diff(a: Option<PathBuf>, b: Option<PathBuf>) -> Result<(), Error> {
    match (a, b) {
        (Some(a), Some(b)) => {
            let label_a = diff_label(&a);
            let label_b = diff_label(&b);
            let run_a = resolve_run_arg(&a)?;
            let run_b = resolve_run_arg(&b)?;
            anstream::print!("{}", diff_runs(&run_a, &run_b, &label_a, &label_b));
        }
        (None, None) => {
            let runs_dir = default_runs_dir()?;
            let tags_dir = default_tags_dir().ok();
            let (previous, latest) = load_two_latest_runs(&runs_dir)?;

            let label_a = resolve_diff_label(&tags_dir, &previous, &runs_dir, "previous");
            let label_b = resolve_diff_label(&tags_dir, &latest, &runs_dir, "latest");
            eprintln!("comparing: {label_a} vs {label_b}");

            anstream::print!("{}", diff_runs(&previous, &latest, &label_a, &label_b));
        }
        _ => {
            return Err(Error::DiffArgCount);
        }
    }
    Ok(())
}

/// Determine a display label for an auto-selected diff run.
///
/// Uses the tag name if a tag points to this run, otherwise falls back
/// to a relative timestamp with a role prefix ("previous (2 min ago)").
fn resolve_diff_label(
    tags_dir: &Option<PathBuf>,
    run: &piano::report::Run,
    runs_dir: &Path,
    role: &str,
) -> String {
    // Try reverse-resolving a tag.
    if let (Some(tags), Some(run_id)) = (tags_dir, &run.run_id) {
        if let Some(tag) = reverse_resolve_tag(tags, run_id) {
            return tag;
        }
    }

    // Fall back to relative timestamp from file modified time.
    let stem = run.timestamp_ms.to_string();
    for ext in &["ndjson", "json"] {
        let path = runs_dir.join(format!("{stem}.{ext}"));
        if let Ok(meta) = std::fs::metadata(&path) {
            if let Ok(modified) = meta.modified() {
                let rel = relative_time(modified);
                return format!("{role} ({rel})");
            }
        }
    }

    // Last resort: use the raw timestamp with role prefix.
    format!("{role} ({stem})")
}

fn cmd_tag(name: Option<String>) -> Result<(), Error> {
    let Some(name) = name else {
        let tags_dir = match default_tags_dir() {
            Ok(dir) => dir,
            Err(Error::NoRuns) => {
                eprintln!("no tags saved");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let mut entries: Vec<String> = std::fs::read_dir(&tags_dir)
            .map_err(|source| Error::RunReadError {
                path: tags_dir.clone(),
                source,
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                if entry.file_type().ok()?.is_file() {
                    Some(entry.file_name().to_string_lossy().into_owned())
                } else {
                    None
                }
            })
            .collect();
        if entries.is_empty() {
            eprintln!("no tags saved");
            return Ok(());
        }
        entries.sort();
        for tag in &entries {
            println!("{tag}");
        }
        return Ok(());
    };

    let runs_dir = default_runs_dir()?;
    let tags_dir = default_tags_dir()?;
    let latest = load_latest_run(&runs_dir)?;
    let run_id = latest.run_id.ok_or(Error::NoRuns)?;
    save_tag(&tags_dir, &name, &run_id)?;
    eprintln!("tagged '{name}'");
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
    let project = find_project_root(&std::env::current_dir()?).map_err(|_| Error::NoRuns)?;
    let local = project.join("target/piano/runs");
    if local.is_dir() {
        return Ok(local);
    }
    Err(Error::NoRuns)
}

fn default_tags_dir() -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_TAGS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let project = find_project_root(&std::env::current_dir()?).map_err(|_| Error::NoRuns)?;
    let local = project.join("target/piano/tags");
    if local.is_dir() {
        return Ok(local);
    }
    // Auto-create tags dir if runs exist (tags live alongside runs)
    let runs_local = project.join("target/piano/runs");
    if runs_local.is_dir() {
        std::fs::create_dir_all(&local)?;
        return Ok(local);
    }
    Err(Error::NoRuns)
}
