use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};

use piano::build::{
    build_instrumented, inject_runtime_dependency, inject_runtime_path_dependency, prepare_staging,
};
use piano::error::Error;
use piano::report::{
    diff_runs, format_table, load_latest_run, load_run, load_tagged_run, save_tag,
};
use piano::resolve::{TargetSpec, resolve_targets};
use piano::rewrite::{inject_registrations, instrument_source};

#[derive(Parser)]
#[command(
    name = "piano",
    about = "Automated instrumentation-based profiling for Rust"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Instrument matching functions and build the project.
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
    },
    /// Show the latest profiling run (or a specific one).
    Report {
        /// Path to a specific run file. If omitted, shows the latest.
        run: Option<PathBuf>,

        /// Show all functions, including those with zero calls.
        #[arg(long)]
        all: bool,
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
        } => cmd_build(
            fn_patterns,
            file_patterns,
            mod_patterns,
            project,
            runtime_path,
        ),
        Commands::Report { run, all } => cmd_report(run, all),
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

    if specs.is_empty() {
        return Err(Error::NoTargetsFound(
            "no targets specified (use --fn, --file, or --mod)".into(),
        ));
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
    for target in &targets {
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        eprintln!("  {}:", relative.display());
        for f in &target.functions {
            eprintln!("    {f}");
        }
    }

    // Prepare staging directory.
    let staging = tempfile::tempdir()?;
    prepare_staging(&project, staging.path())?;

    // Inject piano-runtime dependency.
    match runtime_path {
        Some(ref path) => {
            let abs_path = std::fs::canonicalize(path)?;
            inject_runtime_path_dependency(staging.path(), &abs_path)?;
        }
        None => {
            inject_runtime_dependency(staging.path(), env!("CARGO_PKG_VERSION"))?;
        }
    }

    // Rewrite each target file in staging.
    for target in &targets {
        let target_set: HashSet<String> = target.functions.iter().cloned().collect();
        let relative = target.file.strip_prefix(&src_dir).unwrap_or(&target.file);
        let staged_file = staging.path().join("src").join(relative);
        let source =
            std::fs::read_to_string(&staged_file).map_err(|source| Error::RunReadError {
                path: staged_file.clone(),
                source,
            })?;

        let rewritten =
            instrument_source(&source, &target_set).map_err(|source| Error::ParseError {
                path: staged_file.clone(),
                source,
            })?;

        std::fs::write(&staged_file, rewritten)?;
    }

    // Inject register calls into main.rs for all instrumented functions.
    let main_file = staging.path().join("src").join("main.rs");
    if main_file.exists() {
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
        std::fs::write(&main_file, rewritten)?;
    }

    // Build the instrumented binary.
    let target_dir = project.join("target").join("piano");
    let binary = build_instrumented(staging.path(), &target_dir)?;

    eprintln!("built: {}", binary.display());
    println!("{}", binary.display());

    Ok(())
}

fn cmd_report(run_path: Option<PathBuf>, show_all: bool) -> Result<(), Error> {
    let run = match run_path {
        Some(p) if p.exists() => load_run(&p)?,
        Some(p) => {
            let tag = p.to_string_lossy();
            let tags_dir = default_tags_dir()?;
            let runs_dir = default_runs_dir()?;
            load_tagged_run(&tags_dir, &runs_dir, &tag)?
        }
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
    eprintln!("tagged '{}' -> {}", name, run_id);
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
