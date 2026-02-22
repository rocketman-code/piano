# cargo-profile Design

A cargo subcommand that automates instrumentation-based profiling for Rust. No sudo, no kernel APIs, no sampling -- pure source-level instrumentation via AST rewriting.

## Problem

Profiling Rust code is either privileged (perf, dtrace) or manual (hand-written timing code). Manual instrumentation is the most reliable approach -- no sampling noise, exact measurements -- but nobody does it because the ergonomics are terrible. You have to sprinkle timing code across dozens of functions, manage collection, then clean it all up.

cargo-profile automates the instrumentation step. The tool rewrites your code with timing, builds it, and gets out of the way. You run the binary however you want. The tool collects results and reports them.

## Core Principles

- Never touch user source files. Work on copies.
- User specifies what to instrument. The tool handles the rest.
- Separate build from run from report. Each step is composable.
- No sudo, no privileges, no platform-specific kernel APIs.
- Rust-only. No YAGNI.

## Architecture

Three layers plus a reporter:

### 1. Target Resolution

Takes user input (`--fn`, `--mod`, `--file`) and resolves it to a list of `(file_path, item_path)` pairs. Uses syn to parse source files and find matching functions. Supports glob-style matching so `--fn walk` catches `Walker::walk`, `walk_dir`, etc.

### 2. Rewriter

Takes resolved targets, copies the project's source directory to a staging area, parses target files with syn, and wraps matched function bodies with timing instrumentation. Writes modified files. Adds `cargo-profile-runtime` as a path dependency in the staged `Cargo.toml`.

Instrumentation is a single RAII guard inserted at the top of each function body:

```rust
fn walk(root: &Path) -> Vec<Module> {
    let _guard = cargo_profile_runtime::enter("walk");
    // original body unchanged
}
```

The guard's `Drop` impl records elapsed time. This handles all exit paths: early returns, panics, `?` operator.

### 3. Runtime + Collector

A tiny crate (no dependencies) injected as a path dependency. Provides:

- `enter(name) -> Guard` -- pushes onto a thread-local call stack, starts timer
- `Guard::drop` -- records duration, pops stack
- Thread-local `Vec` storage for zero-contention collection
- `atexit` handler that flushes collected data to JSON

Data captured per instrumented function call:
- Wall-clock duration (total time)
- Self-time (total minus time spent in instrumented children)
- Call count
- Caller-callee relationships (from the thread-local stack)

### 4. Reporter

Reads JSON result files and presents them. Independent of the other layers.

## Build Strategy

Goal: recompile only what changed, reuse all dependency caches.

1. Copy the project's `src/` directory to a staging dir (`/tmp/cargo-profile-<hash>/`)
2. Overwrite target files in the copy with instrumented versions
3. Copy `Cargo.toml`, add `cargo-profile-runtime` as a path dependency
4. Build with `CARGO_TARGET_DIR=<original>/target` to reuse all cached dependencies

Build overhead: copy source files (fast, typically a few MB) + recompile the local crate. Dependencies are fully cached. This is roughly equivalent to editing a file and running `cargo build`.

## CLI Interface

```bash
# Instrument and build
cargo profile build --fn "walk" "load"       # match function names
cargo profile build --mod walker cache        # instrument whole modules
cargo profile build --file src/walker.rs      # instrument all fns in a file

# User runs the binary however they want
./target/profiled/my-binary --my-args < my-input

# Report results
cargo profile report                          # latest run
cargo profile report <run-id>                 # specific run
cargo profile diff <a> <b>                    # compare two runs
```

## Output Format

Each run writes to `~/.cargo-profile/runs/<timestamp>.json`:

```json
{
  "timestamp": "2026-02-22T14:30:00Z",
  "duration_ms": 1523,
  "functions": [
    {
      "name": "walker::walk",
      "calls": 1,
      "total_ms": 850.2,
      "self_ms": 120.4
    },
    {
      "name": "cache::load",
      "calls": 3,
      "calls_from": {"walker::walk": 3},
      "total_ms": 729.8,
      "self_ms": 729.8
    }
  ]
}
```

Terminal report sorted by self-time:

```
Function           Calls   Self      Total
walker::walk       1       120.4ms   850.2ms
cache::load        3       729.8ms   729.8ms
```

## Tech Stack

- syn + quote: Rust source parsing and rewriting
- clap: CLI argument parsing
- serde + serde_json: result serialization
- std::time::Instant: timing (no external deps in runtime)

## Non-Goals

- Sampling-based profiling
- Kernel/OS-level profiling APIs
- Multi-language support
- GUI or web interface
- Automatic function discovery ("instrument everything")
