# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to pre-1.0 [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `piano profile` command: one-step build, execute, and report workflow with `--ignore-exit-code` and `--` argument passthrough
- `piano run` command: execute the last-built instrumented binary with `--` argument passthrough
- Async function instrumentation: async functions are now profiled instead of skipped; Guard detects thread migration and records wall time safely across `.await` points

## [0.5.1] - 2026-02-24

### Fixed

- `piano report` and `piano tag` no longer silently fall back to global `~/.piano/` storage; they error if no project-local runs exist

### Removed

- Global `~/.piano/runs/` and `~/.piano/tags/` fallback (all data is project-local under `target/piano/`)

## [0.5.0] - 2026-02-24

### Added

- CPU time profiling via `--cpu-time` flag (Linux + macOS, 64-bit)
- Instrument all functions by default when no `--fn`/`--file`/`--mod` specified
- Hidden-function footer in summary and frames reports
- Project-local run data: output written to `target/piano/runs/` instead of global `~/.piano/runs/`
- `shutdown_to(dir)` runtime API for directing output to a specific directory
- Panic-safe data capture: profiling data is collected even when the instrumented program panics
- Workflow note in `piano --help` output

### Fixed

- Unparseable source files are skipped with a warning instead of aborting
- Async functions skipped with a warning
- Cross-thread functions correctly merged into NDJSON report
- Symlinks followed when staging project files for instrumentation
- Bare stdout path suppressed in interactive terminals (no more duplicate path output)
- `--fn` help text clarified with substring matching example including qualified names
- Actionable error messages when project directory or `src/` is missing
- `piano report` and `piano tag` check project-local `target/piano/runs/` before falling back to global

### Changed

- Default behavior: `piano build` instruments all functions when no targeting flags are given
- Run data location: project-local `target/piano/runs/`

## [0.4.1] - 2026-02-23

### Fixed

- Zero-call registered functions now appear in NDJSON output (previously silently dropped)
- Dynamic column width in `--frames` table prevents long function names from being truncated
- piano-runtime MSRV lowered from 1.70 to 1.56 (supports older Rust projects)
- Flaky cross-thread timing test stabilized for CI (sleep margin increased from 50ms to 200ms)

### Changed

- MSRV for piano-runtime: 1.56 (was 1.70)

## [0.4.0] - 2026-02-23

### Added

- Per-frame allocation tracking with zero-distortion timing
- NDJSON output format with per-frame views and allocation diffs
- PianoAllocator AST injection for automatic allocation tracking
- Cross-thread instrumentation: per-thread Arc registry, fork/adopt/shutdown
- AST rewriter detects concurrency patterns (rayon par_iter, thread::spawn, rayon::scope) and injects fork/adopt/shutdown
- Workspace member support with inherited Cargo.toml fields
- Custom `[[bin]]` entry point resolution from Cargo.toml
- MSRV integration test verifying piano-runtime compiles on Rust 1.70
- GitHub Actions CI workflow (fmt, clippy, test, doc, msrv)
- Release checklist and versioning policy

### Fixed

- piano-runtime MSRV lowered from 1.88 to 1.70 (edition 2024 to 2021)
- TLS destructor crash in PianoAllocator on Rust 1.88
- Tail expression preserved in `fn main() -> T` (no longer drops return value)
- Projects with `-Dunsafe_code` in RUSTFLAGS no longer fail to build
- Binary entry point correctly resolved for non-standard `[[bin]]` paths

### Changed

- MSRV for piano-runtime: 1.70 (was 1.88)
- MSRV for piano CLI: 1.88 (pinned via rust-toolchain.toml)

## [0.3.0] - 2025-12-01

Initial tagged release.
