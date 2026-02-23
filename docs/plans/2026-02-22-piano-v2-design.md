# Piano v2 Design

Revision of the original design addressing all issues found during pre-implementation review. The original design doc's principles remain unchanged; the internals are redesigned for correctness.

## Principles (unchanged)

1. Never touch user source files -- work on staging copies
2. User specifies what to instrument; tool handles the rest
3. Separate build from run from report (composable)
4. No sudo, no privileges, no kernel APIs
5. Rust-only, no YAGNI

## Architecture

Workspace with two published crates:

```
piano/
  Cargo.toml                # workspace root
  piano-runtime/            # zero-dep timing library (injected into user code)
    Cargo.toml
    src/
      lib.rs
      collector.rs
  src/                      # piano binary (orchestration CLI)
    main.rs
    lib.rs
    resolve.rs              # target resolution (--fn, --file, --mod)
    rewrite.rs              # AST rewriter (syn VisitMut)
    build.rs                # staging + cargo build
    report.rs               # load runs, format tables, diff
    error.rs                # thiserror enum
  tests/
    e2e.rs
```

## Layer 1: piano-runtime

Zero external dependencies. Gets added to user projects via crates.io, not path deps.

Core API:

- `enter(name: &str) -> Guard` -- pushes to thread-local stack, starts Instant timer
- `Guard::drop` -- records elapsed, attributes child time to parent (self-time calculation)
- `collect() -> Vec<FunctionRecord>` -- aggregates raw records, sorts by self_ms descending
- `flush()` -- writes JSON to `PIANO_RUNS_DIR` env var or `~/.piano/runs/<timestamp>.json`
- atexit registration -- automatic flush on normal exit and panic

Data flow:

```
enter("walk") -> Guard { start: Instant, name, depth }
  enter("resolve") -> Guard { start: Instant, name, depth }
  Guard::drop -> RawRecord { name, elapsed, is_child: true }
Guard::drop -> RawRecord { name, elapsed, children_ms accumulated }
```

Thread-local storage: `Vec<StackEntry>` (call stack) + `Vec<RawRecord>` (completed records). Zero contention across threads. Each thread flushes independently.

JSON output: hand-written `write!()` formatter. The schema is flat:

```json
{
  "timestamp": 1740000000,
  "functions": [
    {"name": "walk", "calls": 1, "total_ms": 12.5, "self_ms": 8.3},
    {"name": "resolve", "calls": 3, "total_ms": 4.2, "self_ms": 4.2}
  ]
}
```

No serde. No serde_json. No miniserde. Twenty lines of write!() calls.

## Layer 2: Target Resolution

`resolve.rs` -- finds which functions to instrument based on user-provided patterns.

Specs:
- `TargetSpec::Fn(pattern)` -- substring match against all function names
- `TargetSpec::File(path)` -- all functions in a specific file
- `TargetSpec::Mod(name)` -- all functions in a module (directory or file)

Uses `syn::visit::Visit` (read-only traversal) to walk the full AST:
- `visit_item_fn` -- top-level functions
- `visit_impl_item_fn` -- methods in impl blocks
- `visit_trait_item_fn` -- default methods in trait definitions

Returns `Vec<ResolvedTarget>` with file path + list of qualified function names (`Type::method` for impl blocks, `method` for top-level).

## Layer 3: AST Rewriter

`rewrite.rs` -- rewrites source files to inject timing guards.

`syn::visit_mut::VisitMut` implementation that handles:
- `visit_item_fn_mut` -- top-level functions
- `visit_impl_item_fn_mut` -- impl methods
- `visit_trait_item_fn_mut` -- trait default methods

Injects at position 0 of the function body:
```rust
let _piano_guard = piano_runtime::enter("Type::method");
```

Only rewrites functions present in the target list. Output via `prettyplease::unparse` for readable instrumented source.

## Layer 4: Build Orchestrator

`build.rs` -- prepares staging environment and builds instrumented binary.

Steps:
1. `prepare_staging(project_root, staging_dir)` -- copies project (respecting .gitignore via the `ignore` crate)
2. `inject_runtime_dependency(staging_dir)` -- uses `toml_edit` to add `piano-runtime = "<version>"` to `[dependencies]`
3. Overwrites staged source files with rewritten (instrumented) versions
4. `build_instrumented(staging_dir)` -- runs `cargo build --message-format=json` with `CARGO_TARGET_DIR` pointing to original target dir
5. Parses cargo JSON output to find the actual binary path (not a directory)

The `toml_edit` crate is mandatory. String replacement on structured formats is a correctness bug waiting to happen.

## Layer 5: Reporter

`report.rs` -- loads run data and presents results.

- `load_run(path) -> Run` -- parse JSON (hand-written parser matching the hand-written serializer)
- `format_table(run)` -- sorted by self_ms descending, formatted terminal table
- `diff_runs(a, b)` -- delta column with improvement/regression markers
- `latest_run(dir)` -- parses timestamps as integers, sorts numerically (not lexicographic)

## Layer 6: CLI

```
piano build --fn walk load          # instrument matching functions
piano build --file src/walker.rs    # all functions in file
piano build --mod walker            # all functions in module
piano report                        # latest run
piano report <run-id>               # specific run
piano diff <a> <b>                  # compare two runs
```

Implemented with clap derive. Each subcommand maps to a single function in main.rs.

## Error Handling

`thiserror` enum with specific, actionable variants:

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no functions match pattern '{0}'")]
    NoTargetsFound(String),
    #[error("failed to parse {}: {source}", path.display())]
    ParseError { path: PathBuf, source: syn::Error },
    #[error("build failed: {0}")]
    BuildFailed(String),
    #[error("no runs found in {}", .0.display())]
    NoRuns(PathBuf),
    #[error("failed to read run file {}: {source}", path.display())]
    RunReadError { path: PathBuf, source: io::Error },
    #[error("invalid run data in {}: {reason}", path.display())]
    InvalidRunData { path: PathBuf, reason: String },
    #[error("{0}")]
    Io(#[from] io::Error),
}
```

No `Box<dyn Error>`. Every error path produces a message the user can act on.

## Testing Strategy

- Runtime: unit tests for timing accuracy, self-time calculation, call counting, JSON validity
- Resolution: unit tests with synthetic .rs files in tempdir
- Rewriter: unit tests comparing input/output source strings
- Build: integration test with a real mini Rust project in tempdir
- E2E: full pipeline (create project -> instrument -> build -> run -> verify JSON)
- All tests use tempdir. Zero writes to real filesystem outside temp.

## Dependencies

piano (binary):
- clap (CLI)
- syn + quote (AST parsing/rewriting)
- prettyplease (formatted output)
- toml_edit (Cargo.toml manipulation)
- thiserror (error types)
- serde + serde_json (reading JSON run files in reporter only)
- ignore (respecting .gitignore during staging copy)

piano-runtime (injected into user code):
- (none)

## Key Decisions

| Decision | Rationale |
|---|---|
| Published crate, not path dep | Eliminates staging copy bugs entirely |
| Zero deps in runtime | User projects should not compile our dependencies |
| toml_edit over string replace | Structured formats require structured manipulation |
| thiserror over Box dyn Error | Users deserve actionable error messages |
| VisitMut for all fn types | Most Rust functions live in impl blocks |
| atexit for automatic flush | Transparent to the user -- instrument and go |
| timestamp_secs() not iso8601() | Names are contracts, not aspirations |
| Numeric sort for latest_run | Correctness over accidental behavior |
| --message-format=json | Get the actual binary path, not a directory |
