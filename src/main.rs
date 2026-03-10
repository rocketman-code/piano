use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use clap::{Parser, Subcommand};

use piano::build::{
    build_instrumented, cargo_metadata, clean_stale_piano_files, find_bin_target,
    find_current_package, find_project_root, prebuild_runtime, prebuild_runtime_from_path,
};
use piano::error::{Error, io_context};
use piano::report::{
    diff_runs, diff_runs_json, find_latest_run_file, find_latest_run_file_since,
    find_ndjson_by_run_id, format_frames_json, format_frames_table, format_json,
    format_json_with_frames, format_per_thread_json, format_per_thread_tables,
    format_per_thread_tables_from_frames, format_table, format_table_with_frames, load_latest_run,
    load_latest_runs_per_thread, load_ndjson, load_run, load_run_by_id, load_tagged_run,
    load_two_latest_runs, relative_time, resolve_tag, reverse_resolve_tag, save_tag,
};
use piano::resolve::{
    ResolveResult, SkippedFunction, TargetSpec, module_prefix, qualify, resolve_targets,
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

    /// Build and profile a specific binary target (for projects with multiple
    /// [[bin]] entries). Matches cargo's --bin flag.
    #[arg(long)]
    bin: Option<String>,

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
        /// Stop profiling after N seconds (sends SIGTERM to the binary).
        #[arg(long, value_name = "SECONDS", value_parser = parse_duration_secs)]
        duration: Option<f64>,

        /// Grace period before escalating SIGTERM to SIGKILL (seconds).
        /// Set to 0 to disable escalation. Default: 10.
        #[arg(long, value_name = "SECONDS", default_value = "10", value_parser = parse_kill_timeout)]
        kill_timeout: f64,

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

        /// Show per-frame breakdown.
        #[arg(long)]
        frames: bool,

        /// Output structured JSON instead of a table.
        #[arg(long)]
        json: bool,

        /// Show per-thread breakdown instead of aggregated view.
        #[arg(long)]
        threads: bool,

        /// Suppress warning when program exits with non-zero code.
        #[arg(long)]
        ignore_exit_code: bool,

        /// Stop profiling after N seconds (sends SIGTERM to the binary).
        #[arg(long, value_name = "SECONDS", value_parser = parse_duration_secs)]
        duration: Option<f64>,

        /// Grace period before escalating SIGTERM to SIGKILL (seconds).
        /// Set to 0 to disable escalation. Default: 10.
        #[arg(long, value_name = "SECONDS", default_value = "10", value_parser = parse_kill_timeout)]
        kill_timeout: f64,

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

        /// Show per-frame breakdown.
        #[arg(long)]
        frames: bool,

        /// Output structured JSON instead of a table.
        #[arg(long)]
        json: bool,

        /// Show per-thread breakdown instead of aggregated view.
        #[arg(long)]
        threads: bool,
    },
    /// Compare two profiling runs.
    Diff {
        /// First run (path or tag; omit both to compare two most recent runs).
        a: Option<PathBuf>,
        /// Second run (path or tag).
        b: Option<PathBuf>,

        /// Output structured JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Tag the latest run, or list existing tags (no args).
    Tag {
        /// Tag name. If omitted, lists all saved tags.
        name: Option<String>,
    },
}

fn parse_duration_secs(s: &str) -> Result<f64, String> {
    let secs: f64 = s
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    if secs.is_nan() || secs.is_infinite() {
        return Err("invalid duration".to_string());
    }
    if secs < 0.0 {
        return Err("duration cannot be negative".to_string());
    }
    if secs == 0.0 {
        return Err("duration cannot be zero".to_string());
    }
    if secs > std::time::Duration::MAX.as_secs_f64() {
        return Err("duration is too large".to_string());
    }
    Ok(secs)
}

fn parse_kill_timeout(s: &str) -> Result<f64, String> {
    let secs: f64 = s
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    if secs.is_nan() || secs.is_infinite() {
        return Err("invalid timeout".to_string());
    }
    if secs < 0.0 {
        return Err("timeout cannot be negative".to_string());
    }
    Ok(secs)
}

fn main() {
    // Wrapper mode: when invoked as RUSTC_WORKSPACE_WRAPPER, Cargo passes
    // the real rustc path as argv[1]. Detect via PIANO_WRAPPER_CONFIG env var.
    if std::env::var_os(piano::wrapper::CONFIG_ENV).is_some() {
        std::process::exit(piano::wrapper::run_wrapper());
    }

    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Error> {
    let project_root = find_project_root(&std::env::current_dir()?).ok();
    match cli.command {
        Commands::Build { opts } => cmd_build(opts, &project_root),
        Commands::Run {
            duration,
            kill_timeout,
            args,
        } => cmd_run(duration, kill_timeout, args, &project_root),
        Commands::Profile {
            opts,
            all,
            frames,
            json,
            threads,
            ignore_exit_code,
            duration,
            kill_timeout,
            args,
        } => cmd_profile(
            opts,
            &project_root,
            all,
            frames,
            json,
            threads,
            ignore_exit_code,
            duration,
            kill_timeout,
            args,
        ),
        Commands::Report {
            run,
            all,
            frames,
            json,
            threads,
        } => cmd_report(run, all, frames, json, threads, &project_root),
        Commands::Diff { a, b, json } => cmd_diff(a, b, json, &project_root),
        Commands::Tag { name } => cmd_tag(name, &project_root),
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

/// Build an instrumented binary and return (binary_path, runs_dir, instrumented_fn_count).
///
/// Returns `Ok(None)` when `--list-skipped` is used (early exit after printing).
fn build_project(
    opts: BuildOpts,
    project_root: &Option<PathBuf>,
) -> Result<Option<(PathBuf, PathBuf, usize)>, Error> {
    let BuildOpts {
        fn_patterns,
        exact,
        file_patterns,
        mod_patterns,
        project,
        runtime_path,
        bin,
        cpu_time,
        list_skipped,
    } = opts;

    let project = match project {
        Some(p) => p,
        None => project_root.clone().ok_or_else(|| {
            Error::BuildFailed("could not find Cargo.toml in any parent directory".into())
        })?,
    };

    if !project.exists() {
        return Err(Error::BuildFailed(format!(
            "project directory does not exist: {}",
            project.display()
        )));
    }
    let project = std::fs::canonicalize(&project).map_err(io_context("canonicalize", &project))?;

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

    // Use cargo metadata for project discovery.
    let metadata = cargo_metadata(&project)?;
    let workspace_root = metadata.workspace_root.canonicalize().map_err(|e| {
        Error::BuildFailed(format!(
            "failed to canonicalize workspace root {}: {e}",
            metadata.workspace_root.display()
        ))
    })?;

    // Find the target package and binary.
    // If the user specified --bin, look for it. Otherwise find the current
    // package from the project directory.
    let (package_name, bin_src_path) = if bin.is_some() || metadata.packages.len() == 1 {
        let pkg_filter = if metadata.packages.len() == 1 {
            None
        } else {
            find_current_package(&metadata, &project).map(|p| p.name.as_str())
        };
        find_bin_target(&metadata, pkg_filter, bin.as_deref())?
    } else {
        // Multiple packages, no --bin: find the package matching project dir.
        let pkg = find_current_package(&metadata, &project).ok_or_else(|| {
            Error::BuildFailed(format!(
                "could not determine which package to build in workspace at {}",
                workspace_root.display()
            ))
        })?;
        find_bin_target(&metadata, Some(&pkg.name), None)?
    };

    // Derive source directory from the binary's src_path.
    // The src_path points to the entry point (e.g., /ws/crates/core/src/main.rs).
    // The package's source root is the parent of "src/" in the path, or the
    // manifest_path parent.
    let pkg = metadata
        .packages
        .iter()
        .find(|p| p.name == package_name)
        .expect("package must exist: find_bin_target returned it");
    let pkg_root = pkg
        .manifest_path
        .parent()
        .ok_or_else(|| Error::BuildFailed("package manifest has no parent directory".into()))?;
    let pkg_root = pkg_root.canonicalize().map_err(|e| {
        Error::BuildFailed(format!(
            "failed to canonicalize package root {}: {e}",
            pkg_root.display()
        ))
    })?;

    // Determine the source directory. Use src/ if it exists, otherwise use the
    // parent of the binary's src_path relative to the package root.
    let src_dir = if pkg_root.join("src").is_dir() {
        pkg_root.join("src")
    } else {
        // Derive from binary src_path: strip pkg_root prefix, take first component.
        let bin_rel = bin_src_path
            .canonicalize()
            .unwrap_or_else(|_| bin_src_path.clone());
        bin_rel.parent().unwrap_or(&pkg_root).to_path_buf()
    };

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
            eprintln!("    {}", f.minimal);
        }
    }

    let member_subdir = if pkg_root != workspace_root {
        Some(
            pkg_root
                .strip_prefix(&workspace_root)
                .map_err(|e| std::io::Error::other(e.to_string()))?
                .to_path_buf(),
        )
    } else {
        None
    };

    let instrument_macros = specs.is_empty();
    let src_rel = src_dir.strip_prefix(&pkg_root).unwrap_or(Path::new("src"));

    // Collect all functions with file prefix for cross-file disambiguation.
    let mut all_qualified: Vec<(PathBuf, piano::naming::QualifiedFunction)> = Vec::new();
    for target in &targets {
        let prefix = module_prefix(target.file.strip_prefix(&src_dir).unwrap_or(&target.file));
        for qf in &target.functions {
            all_qualified.push((
                target.file.clone(),
                piano::naming::QualifiedFunction::new(
                    &qualify(&prefix, &qf.minimal),
                    &qualify(&prefix, &qf.medium),
                    &qualify(&prefix, &qf.full),
                ),
            ));
        }
    }
    let just_fns: Vec<piano::naming::QualifiedFunction> =
        all_qualified.iter().map(|(_, qf)| qf.clone()).collect();
    let display_names = piano::naming::disambiguate(&just_fns);

    // Build per-file target maps: HashMap<full_name, display_name>
    let mut file_targets: HashMap<PathBuf, HashMap<String, String>> = HashMap::new();
    for ((file, qf), display) in all_qualified.iter().zip(display_names.iter()) {
        file_targets
            .entry(file.clone())
            .or_default()
            .insert(qf.full.clone(), display.clone());
    }

    // Pre-build piano-runtime with the user's toolchain.
    let target_dir = project.join("target").join("piano");
    let features: Vec<&str> = if cpu_time { vec!["cpu-time"] } else { vec![] };

    eprintln!("pre-building piano-runtime...");
    let runtime = match runtime_path {
        Some(ref path) => {
            let abs_path = std::fs::canonicalize(path).map_err(io_context("canonicalize", path))?;
            prebuild_runtime_from_path(&abs_path, &project, &target_dir, &features)?
        }
        None => prebuild_runtime(&project, &target_dir, &features)?,
    };

    // Clean stale temp files from previous crashed runs.
    clean_stale_piano_files(&src_dir)?;
    piano::staging::clean_stale_staging(&target_dir);

    let runs_dir = target_dir.join("runs");
    std::fs::create_dir_all(&runs_dir).map_err(io_context("create directory", &runs_dir))?;

    // Compute source paths relative to workspace root (matches what wrapper receives).
    let mut targets_relative: HashMap<PathBuf, HashMap<String, String>> = HashMap::new();
    let mut module_prefixes_relative: HashMap<PathBuf, String> = HashMap::new();

    for target in &targets {
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        let target_map = file_targets.get(&target.file).cloned().unwrap_or_default();

        // Path relative to workspace root for the config
        let ws_relative = if let Some(ref sub) = member_subdir {
            PathBuf::from(sub).join(src_rel).join(relative)
        } else {
            PathBuf::from(src_rel).join(relative)
        };

        targets_relative.insert(ws_relative.clone(), target_map);
        let prefix = module_prefix(relative);
        module_prefixes_relative.insert(ws_relative, prefix);
    }

    // Entry point path relative to workspace root
    let bin_src_canonical = bin_src_path.canonicalize().map_err(|e| {
        Error::BuildFailed(format!(
            "failed to canonicalize binary source path {}: {e}",
            bin_src_path.display()
        ))
    })?;
    let bin_entry_relative = bin_src_canonical
        .strip_prefix(&workspace_root)
        .map_err(|_| {
            Error::BuildFailed(format!(
                "binary source {} is outside workspace root {}",
                bin_src_canonical.display(),
                workspace_root.display()
            ))
        })?
        .to_path_buf();

    // Macro function names are discovered by the wrapper during instrumentation
    // and added to registrations there. The orchestrator only provides the
    // statically-resolved display names.
    let all_fn_names: Vec<String> = display_names;

    // Write wrapper config
    let config = piano::wrapper::WrapperConfig {
        runtime_rlib: runtime.rlib_path,
        runtime_deps_dir: runtime.deps_dir,
        instrument_macros,
        entry_point: piano::wrapper::EntryPointConfig {
            source_path: bin_entry_relative,
            fn_names: all_fn_names,
            runs_dir: runs_dir.clone(),
        },
        targets: targets_relative,
        module_prefixes: module_prefixes_relative,
    };

    let config_path = target_dir.join("config.json");
    let config_json = serde_json::to_string(&config)
        .map_err(|e| Error::BuildFailed(format!("failed to serialize wrapper config: {e}")))?;
    std::fs::write(&config_path, config_json)
        .map_err(io_context("write wrapper config", &config_path))?;

    // Clean stale concurrency data from previous builds.
    let _ = std::fs::remove_file(piano::wrapper::concurrency_path(&config_path));

    // Build with wrapper
    let pkg_arg = if member_subdir.is_some() {
        Some(package_name.as_str())
    } else {
        None
    };
    let binary = build_instrumented(
        &workspace_root,
        &target_dir,
        pkg_arg,
        bin.as_deref(),
        &config_path,
    )?;

    // Warn about concurrent functions when --cpu-time is not enabled.
    if !cpu_time {
        let concurrency = piano::wrapper::read_concurrency(&config_path);
        for (func, _pattern) in &concurrency {
            eprintln!(
                "warning: {func} spawns parallel work -- add --cpu-time to see computation time"
            );
        }
    }

    Ok(Some((binary, runs_dir, total_fns)))
}

fn cmd_build(opts: BuildOpts, project_root: &Option<PathBuf>) -> Result<(), Error> {
    let Some((binary, _runs_dir, _total_fns)) = build_project(opts, project_root)? else {
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

/// Returns true if the given file extension belongs to a binary executable.
/// On Windows, `.exe` is a binary extension. On Unix, binaries have no extension
/// so this always returns false.
fn is_binary_extension(ext: &std::ffi::OsStr) -> bool {
    cfg!(windows) && ext == "exe"
}

fn find_latest_binary(project_root: &Option<PathBuf>) -> Result<PathBuf, Error> {
    let project = project_root.as_ref().ok_or(Error::NoBinary)?;
    let dir = project.join("target/piano/release");
    if !dir.is_dir() {
        return Err(Error::NoBinary);
    }
    let mut best: Option<(PathBuf, std::time::SystemTime)> = None;
    for entry in std::fs::read_dir(&dir).map_err(io_context("read directory", &dir))? {
        let entry = entry.map_err(io_context("read directory entry", &dir))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Skip non-binary files by extension. On Unix, binaries have no extension.
        // On Windows, binaries have .exe extension -- allow those through.
        if let Some(ext) = path.extension() {
            if !is_binary_extension(ext) {
                continue;
            }
        }
        let meta = entry
            .metadata()
            .map_err(io_context("read metadata", &path))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o111 == 0 {
                continue; // not executable
            }
        }
        let mtime = meta
            .modified()
            .map_err(io_context("read modified time", &path))?;
        if best.as_ref().is_none_or(|(_, t)| mtime > *t) {
            best = Some((path, mtime));
        }
    }
    best.map(|(p, _)| p).ok_or(Error::NoBinary)
}

/// Why the child process stopped.
enum StopReason {
    /// Child exited on its own (normal exit or crash).
    Normal,
    /// The `--duration` timeout expired and we sent SIGTERM.
    Duration,
    /// User pressed Ctrl-C and we forwarded SIGTERM to the child.
    Interrupted,
    /// Child did not respond to SIGTERM within kill-timeout; escalated to SIGKILL.
    ForceKilled,
}

/// Result of running a child process, including why it stopped.
struct ChildOutcome {
    status: process::ExitStatus,
    stop_reason: StopReason,
}

static CHILD_PID: AtomicU32 = AtomicU32::new(0);
static DURATION_EXPIRED: AtomicBool = AtomicBool::new(false);
static SIGINT_RECEIVED: AtomicBool = AtomicBool::new(false);
static FORCE_KILLED: AtomicBool = AtomicBool::new(false);

/// Spawn a child process, optionally killing it after a timeout.
///
/// When `timeout` is `Some`, a background thread sleeps for the given duration
/// then sends SIGTERM to the child (Unix only; Windows is not yet supported). The existing
/// signal handler in the instrumented binary flushes profiling data on SIGTERM,
/// so this composes cleanly with signal recovery.
///
/// On Unix, a SIGINT handler forwards SIGTERM to the child so that Ctrl-C
/// triggers graceful shutdown instead of orphaning the child. The handler
/// uses `SA_RESETHAND` so a second Ctrl-C force-kills the parent.
fn run_child(
    binary: &Path,
    args: &[String],
    timeout: Option<Duration>,
    kill_timeout: Duration,
    suppress_stdout: bool,
    env: &[(&str, &str)],
) -> Result<ChildOutcome, Error> {
    // Reset flags from any previous invocation.
    DURATION_EXPIRED.store(false, Ordering::SeqCst);
    SIGINT_RECEIVED.store(false, Ordering::SeqCst);
    FORCE_KILLED.store(false, Ordering::SeqCst);

    let mut cmd = process::Command::new(binary);
    cmd.args(args);
    for &(key, val) in env {
        cmd.env(key, val);
    }
    if suppress_stdout {
        cmd.stdout(process::Stdio::null());
    }
    // Install SIGINT handler that forwards SIGTERM to the child.
    // Installed before spawn so no Ctrl-C gap can orphan the child.
    // The handler guards `pid == 0` so it is safe before the child exists.
    #[cfg(unix)]
    {
        extern "C" fn sigint_handler(_sig: i32) {
            SIGINT_RECEIVED.store(true, Ordering::SeqCst);
            let pid = CHILD_PID.load(Ordering::SeqCst);
            if pid != 0 {
                // SAFETY: sending a signal to a known PID is safe.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
        }

        unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = sigint_handler as *const () as usize;
            act.sa_flags = libc::SA_RESETHAND;
            libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
        }
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| Error::RunFailed(format!("failed to run {}: {e}", binary.display())))?;

    CHILD_PID.store(child.id(), Ordering::SeqCst);

    // Spawn a background thread to escalate SIGINT->SIGTERM to SIGKILL.
    if kill_timeout > Duration::ZERO {
        std::thread::spawn(move || {
            // Wait for SIGINT to be received, then start the grace period.
            loop {
                if SIGINT_RECEIVED.load(Ordering::SeqCst) {
                    break;
                }
                // If child exited before SIGINT, stop waiting.
                if CHILD_PID.load(Ordering::SeqCst) == 0 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            std::thread::sleep(kill_timeout);
            let pid = CHILD_PID.load(Ordering::SeqCst);
            if pid != 0 {
                FORCE_KILLED.store(true, Ordering::SeqCst);
                #[cfg(unix)]
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGKILL);
                }
            }
        });
    }

    if let Some(mut dur) = timeout {
        const MIN_DURATION: Duration = Duration::from_millis(1);
        if dur < MIN_DURATION {
            eprintln!(
                "warning: --duration {}s is below minimum resolution, using {}ms",
                dur.as_secs_f64(),
                MIN_DURATION.as_millis()
            );
            dur = MIN_DURATION;
        }
        eprintln!("will stop after {}s", dur.as_secs_f64());
        std::thread::spawn(move || {
            std::thread::sleep(dur);
            DURATION_EXPIRED.store(true, Ordering::SeqCst);
            #[cfg(unix)]
            {
                let pid = CHILD_PID.load(Ordering::SeqCst);
                if pid != 0 {
                    unsafe {
                        libc::kill(pid as libc::pid_t, libc::SIGTERM);
                    }
                }
            }
            // Escalate to SIGKILL if child doesn't exit within kill_timeout.
            if kill_timeout > Duration::ZERO {
                std::thread::sleep(kill_timeout);
                let pid = CHILD_PID.load(Ordering::SeqCst);
                if pid != 0 {
                    FORCE_KILLED.store(true, Ordering::SeqCst);
                    #[cfg(unix)]
                    unsafe {
                        libc::kill(pid as libc::pid_t, libc::SIGKILL);
                    }
                }
            }
        });
    }

    let status = child
        .wait()
        .map_err(|e| Error::RunFailed(format!("failed to wait for {}: {e}", binary.display())))?;

    let stop_reason = if FORCE_KILLED.load(Ordering::SeqCst) {
        StopReason::ForceKilled
    } else if DURATION_EXPIRED.load(Ordering::SeqCst) {
        StopReason::Duration
    } else if SIGINT_RECEIVED.load(Ordering::SeqCst) {
        StopReason::Interrupted
    } else {
        StopReason::Normal
    };

    CHILD_PID.store(0, Ordering::SeqCst);

    // Restore default SIGINT handler for the rest of the process lifetime.
    #[cfg(unix)]
    unsafe {
        let mut act: libc::sigaction = std::mem::zeroed();
        act.sa_sigaction = libc::SIG_DFL;
        libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
    }

    Ok(ChildOutcome {
        status,
        stop_reason,
    })
}

fn cmd_run(
    duration: Option<f64>,
    kill_timeout: f64,
    args: Vec<String>,
    project_root: &Option<PathBuf>,
) -> Result<(), Error> {
    let binary = find_latest_binary(project_root)?;
    eprintln!("running: {}", binary.display());
    eprintln!("--- program output ---");

    let timeout = duration.map(Duration::from_secs_f64);
    let kill_dur = Duration::from_secs_f64(kill_timeout);
    let run_env: Vec<(&str, &str)> = if duration.is_some() {
        vec![("PIANO_STREAM_FRAMES", "1")]
    } else {
        vec![]
    };
    let outcome = run_child(&binary, &args, timeout, kill_dur, false, &run_env)?;

    match outcome.stop_reason {
        StopReason::Duration | StopReason::ForceKilled => std::process::exit(0),
        StopReason::Interrupted => std::process::exit(130),
        StopReason::Normal => std::process::exit(outcome.status.code().unwrap_or(1)),
    }
}

#[allow(clippy::too_many_arguments)]
fn cmd_profile(
    opts: BuildOpts,
    project_root: &Option<PathBuf>,
    show_all: bool,
    frames: bool,
    json: bool,
    threads: bool,
    ignore_exit_code: bool,
    duration: Option<f64>,
    kill_timeout: f64,
    args: Vec<String>,
) -> Result<(), Error> {
    let Some((binary, runs_dir, total_fns)) = build_project(opts, project_root)? else {
        return Ok(());
    };
    let display_name = binary
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| binary.display().to_string());
    eprintln!("built: {display_name}");
    eprintln!("--- program output ---");

    let profile_start_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let child_env: Vec<(&str, &str)> = if frames || duration.is_some() {
        vec![("PIANO_STREAM_FRAMES", "1")]
    } else {
        vec![]
    };

    let timeout = duration.map(Duration::from_secs_f64);
    let kill_dur = Duration::from_secs_f64(kill_timeout);
    let outcome = run_child(&binary, &args, timeout, kill_dur, json, &child_env)?;
    let intentional_stop = matches!(
        outcome.stop_reason,
        StopReason::Duration | StopReason::Interrupted | StopReason::ForceKilled
    );

    if matches!(outcome.stop_reason, StopReason::ForceKilled) {
        eprintln!("warning: program did not respond to SIGTERM -- terminated after --kill-timeout");
    }

    if !outcome.status.success() && !ignore_exit_code && !intentional_stop {
        if let Some(code) = outcome.status.code() {
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

    // Guard against stale data: if no run file was written during THIS run,
    // don't let cmd_report pick up a file from a previous run.
    let effective_runs_dir = default_runs_dir(project_root)?;
    if find_latest_run_file_since(&effective_runs_dir, profile_start_ms)?.is_none() {
        if !outcome.status.success() && !ignore_exit_code && !intentional_stop {
            // Program failed, no data -- suppress (UX principle 6).
            return Ok(());
        }
        if total_fns == 0 {
            return Err(Error::NoFunctionsInstrumented);
        }
        return Err(Error::NoDataWritten(effective_runs_dir));
    }

    eprintln!("--- profiling report ---");
    let report_result = match cmd_report(None, show_all, frames, json, threads, project_root) {
        Ok(()) => Ok(()),
        Err(Error::NoRuns)
            if !outcome.status.success() && !ignore_exit_code && !intentional_stop =>
        {
            // Program failed and produced no data. The program's own error
            // output is the primary affordance (UX principle 6). Suppress
            // Piano's NoRuns to avoid cascading errors.
            //
            // When --ignore-exit-code is set, the user explicitly asked to
            // continue despite non-zero exit -- NoRuns should surface as
            // NoDataWritten so they know profiling failed.
            Ok(())
        }
        Err(Error::NoRuns) if total_fns == 0 => {
            // No functions were instrumented -- the binary ran but had
            // nothing to record.
            Err(Error::NoFunctionsInstrumented)
        }
        Err(Error::NoRuns) => {
            // Functions were instrumented but no data was written.
            // Something went wrong with the runtime's write -- give an
            // actionable message.
            Err(Error::NoDataWritten(runs_dir))
        }
        Err(e) => Err(e),
    };

    report_result?;

    if !outcome.status.success() && !ignore_exit_code && !intentional_stop {
        std::process::exit(outcome.status.code().unwrap_or(1));
    }

    Ok(())
}

fn cmd_report(
    run_path: Option<PathBuf>,
    show_all: bool,
    frames: bool,
    json: bool,
    threads: bool,
    project_root: &Option<PathBuf>,
) -> Result<(), Error> {
    // Resolve the run file path.
    let resolved_path = match &run_path {
        Some(p) if p.exists() => Some(p.clone()),
        Some(p) => {
            // Tag lookup — resolve to NDJSON file if available.
            let tag = p.to_string_lossy();
            let tags_dir = default_tags_dir(project_root)?;
            let runs_dir = default_runs_dir(project_root)?;
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
                    if frames {
                        eprintln!(
                            "warning: --frames requires NDJSON data; showing aggregated view"
                        );
                    }
                    if json {
                        println!("{}", format_json(&run, show_all));
                    } else {
                        anstream::print!("{}", format_table(&run, show_all));
                    }
                    return Ok(());
                }
            }
        }
        None => {
            // Find the latest run file.
            let dir = default_runs_dir(project_root)?;
            find_latest_run_file(&dir)?
        }
    };

    // If we have a .ndjson file, load frame data for richer output.
    if let Some(path) = &resolved_path
        && path.extension().and_then(|e| e.to_str()) == Some("ndjson")
    {
        let (_run, frame_data) = load_ndjson(path)?;

        let has_tid = frame_data
            .frames
            .iter()
            .any(|f| f.iter().any(|e| e.tid.is_some()));

        if threads && !has_tid {
            eprintln!(
                "warning: --threads requires per-thread data; this file predates thread tracking"
            );
            eprintln!("warning: showing aggregated view instead");
        }

        if threads && has_tid {
            if json {
                println!("{}", format_per_thread_json(&frame_data, show_all));
            } else {
                anstream::print!(
                    "{}",
                    format_per_thread_tables_from_frames(&frame_data, show_all)
                );
            }
        } else if json && frames {
            println!("{}", format_frames_json(&frame_data, show_all));
        } else if json {
            println!("{}", format_json_with_frames(&frame_data, show_all));
        } else if frames {
            anstream::print!("{}", format_frames_table(&frame_data));
        } else {
            anstream::print!("{}", format_table_with_frames(&frame_data, show_all));
        }
        return Ok(());
    }

    // Per-thread mode: load individual thread files without merging.
    if threads {
        let dir = default_runs_dir(project_root)?;
        let thread_runs = load_latest_runs_per_thread(&dir)?;
        anstream::print!("{}", format_per_thread_tables(&thread_runs, show_all));
        return Ok(());
    }

    // Fall back to JSON table.
    let run = match resolved_path {
        Some(p) => load_run(&p)?,
        None => {
            let dir = default_runs_dir(project_root)?;
            load_latest_run(&dir)?
        }
    };
    if json {
        println!("{}", format_json(&run, show_all));
    } else {
        anstream::print!("{}", format_table(&run, show_all));
    }
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

fn cmd_diff(
    a: Option<PathBuf>,
    b: Option<PathBuf>,
    json: bool,
    project_root: &Option<PathBuf>,
) -> Result<(), Error> {
    match (a, b) {
        (Some(a), Some(b)) => {
            let label_a = diff_label(&a);
            let label_b = diff_label(&b);
            let run_a = resolve_run_arg(&a, project_root)?;
            let run_b = resolve_run_arg(&b, project_root)?;
            if json {
                println!("{}", diff_runs_json(&run_a, &run_b));
            } else {
                anstream::print!("{}", diff_runs(&run_a, &run_b, &label_a, &label_b));
            }
        }
        (None, None) => {
            let runs_dir = default_runs_dir(project_root)?;
            let tags_dir = default_tags_dir(project_root).ok();
            let (previous, latest) = load_two_latest_runs(&runs_dir)?;

            if json {
                println!("{}", diff_runs_json(&previous, &latest));
            } else {
                let label_a = resolve_diff_label(&tags_dir, &previous, &runs_dir, "previous");
                let label_b = resolve_diff_label(&tags_dir, &latest, &runs_dir, "latest");
                eprintln!("comparing: {label_a} vs {label_b}");
                anstream::print!("{}", diff_runs(&previous, &latest, &label_a, &label_b));
            }
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

fn cmd_tag(name: Option<String>, project_root: &Option<PathBuf>) -> Result<(), Error> {
    let Some(name) = name else {
        let tags_dir = match default_tags_dir(project_root) {
            Ok(dir) => dir,
            Err(Error::NoRuns) => {
                eprintln!("no tags saved");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let mut entries: Vec<String> = std::fs::read_dir(&tags_dir)
            .map_err(|source| Error::TagReadError {
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

    let runs_dir = default_runs_dir(project_root)?;
    let tags_dir = default_tags_dir(project_root)?;
    let latest = load_latest_run(&runs_dir)?;
    let run_id = latest.run_id.ok_or(Error::NoRuns)?;
    save_tag(&tags_dir, &name, &run_id)?;
    eprintln!("tagged '{name}'");
    Ok(())
}

fn resolve_run_arg(
    arg: &Path,
    project_root: &Option<PathBuf>,
) -> Result<piano::report::Run, Error> {
    if arg.exists() {
        return load_run(arg);
    }
    let tag = arg.to_string_lossy();
    let tags_dir = default_tags_dir(project_root)?;
    let runs_dir = default_runs_dir(project_root)?;
    load_tagged_run(&tags_dir, &runs_dir, &tag)
}

fn default_runs_dir(project_root: &Option<PathBuf>) -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_RUNS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let project = project_root.as_ref().ok_or(Error::NoRuns)?;
    let local = project.join("target/piano/runs");
    if local.is_dir() {
        return Ok(local);
    }
    Err(Error::NoRuns)
}

fn default_tags_dir(project_root: &Option<PathBuf>) -> Result<PathBuf, Error> {
    if let Ok(dir) = std::env::var("PIANO_TAGS_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let project = project_root.as_ref().ok_or(Error::NoRuns)?;
    let local = project.join("target/piano/tags");
    if local.is_dir() {
        return Ok(local);
    }
    // Auto-create tags dir if runs exist (tags live alongside runs)
    let runs_local = project.join("target/piano/runs");
    if runs_local.is_dir() {
        std::fs::create_dir_all(&local).map_err(io_context("create directory", &local))?;
        return Ok(local);
    }
    Err(Error::NoRuns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_binary_extension_exe() {
        let ext = std::ffi::OsStr::new("exe");
        // On the current platform, the result depends on cfg!(windows).
        // On Windows: .exe is a binary extension -> true.
        // On Unix: no extension counts as binary, .exe is not -> false.
        assert_eq!(is_binary_extension(ext), cfg!(windows));
    }

    #[test]
    fn is_binary_extension_rejects_non_binary() {
        for name in &["d", "fingerprint", "rmeta", "rlib", "o", "so", "dylib"] {
            let ext = std::ffi::OsStr::new(name);
            assert!(
                !is_binary_extension(ext),
                "extension .{name} should not be treated as binary"
            );
        }
    }

    #[test]
    fn parse_duration_zero() {
        let err = parse_duration_secs("0").unwrap_err();
        assert_eq!(err, "duration cannot be zero");
    }

    #[test]
    fn parse_duration_negative() {
        let err = parse_duration_secs("-1").unwrap_err();
        assert_eq!(err, "duration cannot be negative");
    }

    #[test]
    fn parse_duration_nan() {
        let err = parse_duration_secs("nan").unwrap_err();
        assert_eq!(err, "invalid duration");
    }

    #[test]
    fn parse_duration_inf() {
        let err = parse_duration_secs("inf").unwrap_err();
        assert_eq!(err, "invalid duration");
    }

    #[test]
    fn parse_duration_neg_inf() {
        let err = parse_duration_secs("-inf").unwrap_err();
        assert_eq!(err, "invalid duration");
    }

    #[test]
    fn parse_duration_too_large() {
        let err = parse_duration_secs("1e300").unwrap_err();
        assert_eq!(err, "duration is too large");
    }

    #[test]
    fn parse_duration_negative_zero() {
        let err = parse_duration_secs("-0.0").unwrap_err();
        assert_eq!(err, "duration cannot be zero");
    }

    #[test]
    fn parse_duration_valid_fractional() {
        let secs = parse_duration_secs("0.5").unwrap();
        assert_eq!(secs, 0.5);
    }

    #[test]
    fn parse_duration_invalid_string() {
        assert!(parse_duration_secs("abc").is_err());
    }
}
