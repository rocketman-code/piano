use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};

use piano::build::{
    build_instrumented, find_bin_entry_point, find_workspace_root, inject_runtime_dependency,
    inject_runtime_path_dependency, prepare_staging,
};
use piano::error::Error;
use piano::report::{
    diff_runs, find_latest_run_file, find_ndjson_by_run_id, format_frames_table, format_table,
    format_table_with_frames, load_latest_run, load_ndjson, load_run, load_run_by_id,
    load_tagged_run, resolve_tag, save_tag,
};
use piano::resolve::{TargetSpec, resolve_targets};
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

#[derive(Subcommand)]
enum Commands {
    /// Instrument and build the project. Profiles all functions by default;
    /// use --fn, --file, or --mod to narrow scope.
    Build {
        /// Instrument functions whose name contains PATTERN (repeatable).
        /// e.g. --fn parse matches parse, parse_line, MyStruct::try_parse.
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
        /// Instrument functions whose name contains PATTERN (repeatable).
        /// e.g. --fn parse matches parse, parse_line, MyStruct::try_parse.
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
        Commands::Run { args } => cmd_run(args),
        Commands::Profile {
            fn_patterns,
            file_patterns,
            mod_patterns,
            project,
            runtime_path,
            cpu_time,
            all,
            frames,
            ignore_exit_code,
            args,
        } => cmd_profile(
            fn_patterns,
            file_patterns,
            mod_patterns,
            project,
            runtime_path,
            cpu_time,
            all,
            frames,
            ignore_exit_code,
            args,
        ),
        Commands::Report { run, all, frames } => cmd_report(run, all, frames),
        Commands::Diff { a, b } => cmd_diff(a, b),
        Commands::Tag { name } => cmd_tag(name),
    }
}

/// Build an instrumented binary and return (binary_path, runs_dir).
fn build_project(
    fn_patterns: Vec<String>,
    file_patterns: Vec<PathBuf>,
    mod_patterns: Vec<String>,
    project: PathBuf,
    runtime_path: Option<PathBuf>,
    cpu_time: bool,
) -> Result<(PathBuf, PathBuf), Error> {
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
    let binary = build_instrumented(staging.path(), &target_dir, package_name.as_deref())?;

    Ok((binary, runs_dir))
}

fn cmd_build(
    fn_patterns: Vec<String>,
    file_patterns: Vec<PathBuf>,
    mod_patterns: Vec<String>,
    project: PathBuf,
    runtime_path: Option<PathBuf>,
    cpu_time: bool,
) -> Result<(), Error> {
    let (binary, _runs_dir) = build_project(
        fn_patterns,
        file_patterns,
        mod_patterns,
        project,
        runtime_path,
        cpu_time,
    )?;
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
    let dir = PathBuf::from("target/piano/debug");
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

#[allow(clippy::too_many_arguments)]
fn cmd_profile(
    fn_patterns: Vec<String>,
    file_patterns: Vec<PathBuf>,
    mod_patterns: Vec<String>,
    project: PathBuf,
    runtime_path: Option<PathBuf>,
    cpu_time: bool,
    show_all: bool,
    frames: bool,
    ignore_exit_code: bool,
    args: Vec<String>,
) -> Result<(), Error> {
    let (binary, runs_dir) = build_project(
        fn_patterns,
        file_patterns,
        mod_patterns,
        project,
        runtime_path,
        cpu_time,
    )?;
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
    match cmd_report(None, show_all, frames) {
        Ok(()) => Ok(()),
        Err(Error::NoRuns) if !status.success() && !ignore_exit_code => {
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
    }
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
                    let run = load_run_by_id(&runs_dir, &run_id)?;
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

fn cmd_diff(a: PathBuf, b: PathBuf) -> Result<(), Error> {
    let run_a = resolve_run_arg(&a)?;
    let run_b = resolve_run_arg(&b)?;
    anstream::print!("{}", diff_runs(&run_a, &run_b));
    Ok(())
}

fn cmd_tag(name: String) -> Result<(), Error> {
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
    let local = PathBuf::from("target/piano/runs");
    if local.is_dir() {
        return Ok(std::fs::canonicalize(local)?);
    }
    Err(Error::NoRuns)
}

fn default_tags_dir() -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_TAGS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let local = PathBuf::from("target/piano/tags");
    if local.is_dir() {
        return Ok(std::fs::canonicalize(local)?);
    }
    // Auto-create tags dir if runs exist (tags live alongside runs)
    let runs_local = PathBuf::from("target/piano/runs");
    if runs_local.is_dir() {
        std::fs::create_dir_all(&local)?;
        return Ok(std::fs::canonicalize(local)?);
    }
    Err(Error::NoRuns)
}
