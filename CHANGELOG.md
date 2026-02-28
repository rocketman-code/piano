# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to pre-1.0 [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.8.1] - 2026-02-28

### Changed

- README updated to match 0.8.0 report output format (#204)
- CI workflow added to enforce version bump commit scope on release PRs (#201)
- Milestones now required for every release in release checklist (#204)

## [0.8.0] - 2026-02-28

### Added

- `--exact` flag for `piano build` and `piano profile`: exact-match mode for `--fn`, mirroring `cargo test -- --exact` convention (#120)
- `--list-skipped` flag for `piano build` and `piano profile`: list functions that Piano cannot instrument (const, unsafe, extern) with file paths and reasons; prints "no functions skipped" when all functions are instrumentable (#137, #186, #192)
- `piano tag` with no arguments lists all saved tags; `piano tag <name>` saves (#175)
- `piano diff` with no arguments compares the two most recent runs, showing "comparing: X vs Y" with tag names or relative timestamps (#125)
- User labels as diff column headers: tag names, filename stems, or relative timestamps instead of generic "Before"/"After" (#124)
- Auto-detect project root by walking up from cwd to find Cargo.toml, removing the need for `--project` in most cases (#135)
- Structural color in report tables: bold headers, dim separators, full NO_COLOR/CLICOLOR/CLICOLOR_FORCE/TERM=dumb support via anstream (#122)
- Recovery guidance in NoTargetsFound errors: shows similar function names via edit distance, or lists all available functions (#127)
- UX design principles documented in docs/standards/ux.md

### Fixed

- Diff table sorted by absolute self-time delta descending instead of alphabetically (#193)
- Duplicate error message on NoTargetsFound when all matched functions were skipped (#191)
- Missing tag produces "no run found for tag '...'" instead of leaking internal file paths (#198)
- Internal staging/tempdir paths removed from all user-facing error messages (#129)
- Cascading NoRuns error suppressed when profiled program exits non-zero (#139)
- `--fn` substring matching now checks qualified names (Type::method), not just bare names (#185)
- Stale tags produce RunNotFound with recovery guidance instead of generic NoRuns (#160)
- Non-printable characters in tag names displayed safely in error messages (#166)
- Redundant "invalid tag:" prefix removed from tag validation errors (#165)
- Run ID removed from tag confirmation message (#126)
- Concurrency warning simplified to one line per function (#138)
- Invalid tag error defines the valid character set instead of listing what's invalid (#128)
- App name shown in build output instead of internal binary path (#121)
- `--fn` help text harmonized between build and profile commands (#123)

### Changed

- Report columns reordered: Function | Self | Calls | Allocs | Alloc Bytes (self-time leads, Total removed from default view) (#180, #136)
- Runtime exits non-zero on profiling data write failure instead of silently discarding
- Runs directory pre-created before instrumented build to prevent write failures

## [0.7.0] - 2026-02-27

### Added

- Accurate async self-time: migration-safe Guard with phantom StackEntry tracks self-time correctly when async tasks migrate across threads (#94)
- Guard::check() injected after `.await` at any nesting depth (if, match, loop, etc.), not just top-level statements (#142)
- Function names preserved for migrated async guards; no more `<migrated>` bucket in reports (#116)
- Phantom StackEntry cleanup on intermediate threads during multi-hop async migration via deferred cleanup queue (#141)
- Instrument `fn` items inside `macro_rules!` definitions when profiling all functions (#143)

### Changed

- StackEntry uses packed `u64` identity field (cookie + name_id + depth) instead of separate `depth` and `cookie_low` fields
- Guard::check() uses recursive VisitMut-based injection instead of flat top-level iteration

## [0.6.0] - 2026-02-25

### Added

- `piano profile` command: one-step build, execute, and report workflow with `--ignore-exit-code` and `--` argument passthrough
- `piano run` command: execute the last-built instrumented binary with `--` argument passthrough
- Async function instrumentation: async functions are now profiled instead of skipped; Guard detects thread migration and records wall time safely across `.await` points
- Calibration harness for measuring instrumentation bias and overhead

### Fixed

- Allocation counter widened from u32 to u64, preventing silent truncation on high-allocation programs (#55)
- Synthetic frames created for companion-merged worker threads, preventing silent data loss (#57)
- Worker-thread percentiles show "-" instead of misleading aggregated values (#56)
- `flush()` writes to project-local `target/piano/runs/` instead of global directory (#84)
- `diff_runs` uses i128 for alloc_count delta, preventing wrap on extreme values (#101)
- `const`, `unsafe`, and `extern` functions skipped during instrumentation instead of causing compile errors (#102)
- Fork/adopt injection skipped for detached thread spawns (`std::thread::spawn`, `rayon::spawn`), preventing lifetime errors (#103)

### Changed

- Runtime timing uses hardware TSC counters (Apple Silicon CNTVCT, x86 RDTSC) instead of `Instant::now()`, reducing per-call overhead
- Timestamp capture reordered to minimize bias in self-time measurement

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
