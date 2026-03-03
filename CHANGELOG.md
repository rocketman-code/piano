# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to pre-1.0 [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.10.0] - 2026-03-03

Piano is now a true multi-threaded profiler -- worker thread data that was previously lost is captured correctly, profiling data survives process::exit() and crashes, and instrumented builds use release mode by default.

### Added

- Profiling data streams to disk as frames complete, so data collected before a crash or forced exit is not lost (#310)
- Profiling data recovered on `process::exit()` instead of being silently dropped (#303)

### Fixed

- Worker thread profiling data now collected from all threads instead of only the main thread (#309, #315)
- Instrumented builds use release mode by default, matching the performance characteristics of the program being profiled (#302)

### Removed

- Legacy JSON output format removed; NDJSON is now the only output format (#309)

## [0.9.3] - 2026-03-02

Fixes correctness bugs in async profiling, allocation tracking, and self-time precision -- upgrading is recommended for anyone profiling async or high-call-count programs.

### Added

- Signal handlers for SIGTERM and SIGINT recover profiling data on Unix instead of losing it (#257)
- `#[non_exhaustive]` on all public runtime structs for forward-compatible API evolution (#258)
- Internal injection API hidden from docs.rs to keep published documentation clean (#259)
- MIT LICENSE file (#248)

### Fixed

- Allocation counters no longer corrupted when async futures are cancelled mid-await (#250)
- Unbounded memory growth for high-call-count programs: per-invocation records replaced with in-flight aggregation, bounding memory to O(unique functions) instead of O(total calls) (#251)
- Floating-point precision loss in self-time computation eliminated by using integer nanoseconds for children time accumulation (#253)
- `select!`/`join!` and other macro invocations in async functions now treated as potential `.await` points for thread migration detection (#249)
- Migrated async guards capture post-migration CPU time instead of reporting zero (#269)
- Allocation tracking correctly scoped to condition expressions for `if`/`while`/`match` with `.await` in condition, so body allocations are not lost (#270)
- Non-block match arms (`Some(v) => process(v)`) now get allocation tracking when scrutinee contains `.await` (#292)
- NDJSON runs no longer approximate `total_ms` from `self_ms`; the field is set to 0.0 to honestly represent absent data (#254)
- `FrameFnSummary.calls` widened from `u32` to `u64` to prevent overflow for high-call-count frames (#286)
- IO errors now include file path and operation context instead of bare OS messages (#255)
- `flush()` reports write errors to stderr instead of silently discarding them (#256)
- `find_latest_binary` accepts `.exe` extension on Windows (#252)

## [0.9.2] - 2026-03-02

Fixes edge cases in allocator detection and TSC calibration that caused panics or silent zero-byte reports on certain hardware and project configurations.

### Fixed

- `#[cfg_attr(..., global_allocator)]` now detected by allocator analysis, fixing zero-byte reports for projects using conditional allocator attributes (#237)
- Multiple `#[cfg(...)]` attributes on the same allocator static correctly combined, so fallback negation works on all platforms (#238)
- Failed allocations (null pointer) no longer counted, matching the behavior of `realloc()` (#239)
- TSC calibration no longer panics when the hardware counter does not advance (#240)

## [0.9.1] - 2026-03-02

Fixes allocation tracking reporting zero bytes when the user's global allocator is behind a `#[cfg(...)]` gate -- a common pattern with tikv-jemallocator and mimalloc.

### Fixed

- Allocation tracking no longer reports zero when the user's `#[global_allocator]` is behind a `#[cfg(...)]` gate (#231)
  - Detection moved from string matching to syn AST walk
  - Cfg-gated allocators get a `#[cfg(not(...))]` fallback so every platform tracks allocations
  - Common pattern: `#[cfg(target_os = "linux")] #[global_allocator]` with tikv-jemallocator or mimalloc

## [0.9.0] - 2026-03-02

Piano is feature-complete. Every metric -- wall time, self time, CPU time, allocations -- is accurate across sync, threaded, and async execution models.

### Added

- Async-aware allocation tracking: allocation data survives thread migrations in async programs, so `tokio::spawn` and `select!` no longer silently drop alloc counts (#226, PR #225)

### Fixed

- Allocation data no longer silently lost when async tasks migrate threads
- Migrated async guards report real allocations instead of zeros
- Companion JSON merge validates run_id consistency, skipping mismatched files (#58)
- Child exit code propagated from `piano profile` -- non-zero exits no longer swallowed (#93)
- `extern "Rust" fn` inside `macro_rules!` templates now instrumented (#144)
- Stale files removed from staging directory on rebuild
- `tempfile` moved to dev-dependencies, shrinking the dependency tree for users

### Changed

- Staging directory uses a stable path (`target/piano/staging/`) for incremental compilation instead of a fresh tempdir per build

## [0.8.2] - 2026-02-28

Per-call instrumentation overhead cut from 129ns to 59ns on Apple Silicon -- profiling results are more representative of real performance.

### Changed

- Per-call instrumentation overhead reduced from 129ns to 59ns on Apple Silicon (#206)

## [0.8.1] - 2026-02-28

Documentation update. No runtime or CLI changes.

### Changed

- README updated to match 0.8.0 report output format (#204)

## [0.8.0] - 2026-02-28

Major UX overhaul: smarter defaults, better error messages, and less typing for common workflows.

### Added

- `--exact` flag for `piano build` and `piano profile`: exact-match mode for `--fn`, mirroring `cargo test -- --exact` (#120)
- `--list-skipped` flag: shows functions Piano cannot instrument (const, unsafe, extern) with file paths and reasons (#137, #186, #192)
- `piano tag` with no arguments lists all saved tags; `piano tag <name>` saves (#175)
- `piano diff` with no arguments compares the two most recent runs, showing tag names or relative timestamps (#125)
- User labels as diff column headers: tag names, filename stems, or relative timestamps instead of generic "Before"/"After" (#124)
- Auto-detect project root by walking up from cwd to find Cargo.toml, removing the need for `--project` in most cases (#135)
- Colored report tables with bold headers and dim separators; full NO_COLOR/CLICOLOR/CLICOLOR_FORCE/TERM=dumb support (#122)
- Recovery guidance on NoTargetsFound: shows similar function names via edit distance, or lists all available functions (#127)

### Fixed

- Diff table sorted by absolute self-time delta descending instead of alphabetically (#193)
- `--fn` substring matching now checks qualified names (`Type::method`), not just bare names (#185)
- Missing tag produces "no run found for tag '...'" instead of leaking internal file paths (#198)
- Duplicate error message when all matched functions were skipped (#191)
- Internal staging paths removed from all user-facing error messages (#129)
- Cascading NoRuns error suppressed when profiled program exits non-zero (#139)
- Stale tags produce a helpful error with recovery guidance instead of generic NoRuns (#160)
- Non-printable characters in tag names displayed safely in error messages (#166)
- Invalid tag error defines the valid character set instead of listing what is invalid (#128)
- App name shown in build output instead of internal binary path (#121)
- `--fn` help text harmonized between build and profile commands (#123)
- Redundant "invalid tag:" prefix removed from tag validation errors (#165)
- Run ID removed from tag confirmation message (#126)
- Concurrency warning simplified to one line per function (#138)

### Changed

- Report columns reordered: Function | Self | Calls | Allocs | Alloc Bytes -- self-time leads, Total removed from default view (#180, #136)
- Runtime exits non-zero on profiling data write failure instead of silently discarding

## [0.7.0] - 2026-02-27

Async profiling arrives. Piano now instruments async functions and tracks self-time accurately across thread migrations, so `tokio::spawn`, `select!`, and work-stealing runtimes produce correct results.

### Added

- Async self-time tracking: migration-safe Guard tracks self-time correctly when async tasks migrate across threads (#94)
- Guard checks injected after `.await` at any nesting depth (if, match, loop), not just top-level statements (#142)
- Function names preserved for migrated async guards -- no more `<migrated>` bucket in reports (#116)
- Phantom stack entry cleanup on intermediate threads during multi-hop migration (#141)
- Instrument `fn` items inside `macro_rules!` definitions when profiling all functions (#143)

### Changed

- Guard check injection uses recursive visitor instead of flat top-level iteration, catching `.await` inside nested expressions

## [0.6.0] - 2026-02-25

New `profile` and `run` commands, async function instrumentation, and hardware timestamp counters for lower overhead.

### Added

- `piano profile` command: one-step build, execute, and report workflow with `--ignore-exit-code` and `--` argument passthrough
- `piano run` command: execute the last-built instrumented binary with `--` argument passthrough
- Async function instrumentation: async functions are now profiled instead of skipped; Guard detects thread migration and records wall time safely across `.await` points
- Calibration harness for measuring instrumentation bias and overhead

### Fixed

- Allocation counter widened from u32 to u64, preventing silent truncation on high-allocation programs (#55)
- Worker-thread percentiles show "-" instead of misleading aggregated values (#56)
- Synthetic frames created for companion-merged worker threads, preventing silent data loss (#57)
- `flush()` writes to project-local `target/piano/runs/` instead of global directory (#84)
- `diff_runs` uses i128 for alloc_count delta, preventing wrap on extreme values (#101)
- `const`, `unsafe`, and `extern` functions skipped during instrumentation instead of causing compile errors (#102)
- Fork/adopt injection skipped for detached thread spawns (`std::thread::spawn`, `rayon::spawn`), preventing lifetime errors (#103)

### Changed

- Timing uses hardware TSC counters (Apple Silicon CNTVCT, x86 RDTSC) instead of `Instant::now()`, reducing per-call overhead
- Timestamp capture reordered to minimize bias in self-time measurement

## [0.5.1] - 2026-02-24

Removes global `~/.piano/` fallback -- all profiling data is now project-local under `target/piano/`.

### Fixed

- `piano report` and `piano tag` no longer silently fall back to global `~/.piano/` storage; they error if no project-local runs exist

### Removed

- Global `~/.piano/runs/` and `~/.piano/tags/` fallback (all data is project-local under `target/piano/`)

## [0.5.0] - 2026-02-24

CPU time profiling, instrument-everything default, panic-safe data capture, and project-local storage.

### Added

- CPU time profiling via `--cpu-time` flag (Linux + macOS, 64-bit)
- Instrument all functions by default when no `--fn`/`--file`/`--mod` specified
- Project-local run data: output written to `target/piano/runs/` instead of global `~/.piano/runs/`
- Panic-safe data capture: profiling data is collected even when the instrumented program panics
- Hidden-function footer in summary and frames reports

### Fixed

- Unparseable source files are skipped with a warning instead of aborting
- Async functions skipped with a warning instead of silently producing wrong data
- Cross-thread functions correctly merged into NDJSON report
- Symlinks followed when staging project files for instrumentation
- Bare stdout path suppressed in interactive terminals
- `--fn` help text clarified with substring matching example including qualified names
- Actionable error messages when project directory or `src/` is missing

### Changed

- Default behavior: `piano build` instruments all functions when no targeting flags are given
- Run data location: project-local `target/piano/runs/`

## [0.4.1] - 2026-02-23

Fixes silent data loss for zero-call functions, truncated function names in frames view, and broadens runtime compatibility down to Rust 1.56.

### Fixed

- Zero-call registered functions now appear in NDJSON output instead of being silently dropped
- Dynamic column width in `--frames` table prevents long function names from being truncated
- piano-runtime MSRV lowered from 1.70 to 1.56, supporting older Rust projects

## [0.4.0] - 2026-02-23

Per-frame allocation tracking, cross-thread instrumentation, NDJSON output, and workspace support.

### Added

- Per-frame allocation tracking with zero-distortion timing
- NDJSON output format with per-frame views and allocation diffs
- Automatic allocator injection: PianoAllocator wraps the user's global allocator via AST rewriting
- Cross-thread instrumentation with per-thread collection and fork/adopt for scoped concurrency
- Concurrency pattern detection (rayon par_iter, thread::spawn, rayon::scope) with automatic fork/adopt injection
- Workspace member support with inherited Cargo.toml fields
- Custom `[[bin]]` entry point resolution from Cargo.toml

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

[Unreleased]: https://github.com/rocketman-code/piano/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/rocketman-code/piano/compare/v0.9.3...v0.10.0
[0.9.3]: https://github.com/rocketman-code/piano/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/rocketman-code/piano/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/rocketman-code/piano/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/rocketman-code/piano/compare/v0.8.2...v0.9.0
[0.8.2]: https://github.com/rocketman-code/piano/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/rocketman-code/piano/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/rocketman-code/piano/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/rocketman-code/piano/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/rocketman-code/piano/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/rocketman-code/piano/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/rocketman-code/piano/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/rocketman-code/piano/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/rocketman-code/piano/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/rocketman-code/piano/releases/tag/v0.3.0
