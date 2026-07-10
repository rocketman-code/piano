# Code

## Rust Style

- Edition 2024 (CLI), Edition 2021 (piano-runtime, MSRV 1.59)
- MSRV: 1.88 (CLI), 1.59 (runtime -- broad compatibility for user projects)
- `cargo clippy --workspace --all-targets -- -D warnings` (strict, enforced in CI)
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` (documentation must compile cleanly, enforced in CI)
- Idiomatic Rust: prefer stdlib patterns (iterators, if-let chains, Result propagation with `?`) over manual indexing or `process::exit` (except at the CLI boundary in `main.rs`)

## Type Generation

Domain types are generated from the root spec set (`piano.carve` +
`piano-runtime.carve`, one module per file) via the carve binary.
Generated zones are committed, never build-time: `src/generated.rs`,
`src/invariants.rs`, and the workspace `derivation.snapshot` are
regenerated wholesale by `carve generate`; the `src/generated/<type>/ops.rs`
files are user-owned operation implementations written once. Freshness:
run `carve generate` at the workspace root and require a clean
`git diff` on the regenerated files (the CI job lands when carve is
publicly released; until then the check is run locally with the carve
binary built from its repo). Hand-written types that duplicate
spec-derived types are divergence risk -- the spec is the single source
of truth.

## Module Organization

A module should have one clear responsibility. When a developer opens a file, they should be able to state its purpose in one sentence without using "and."

- Recommended: <=2,000 lines per file (including inline tests)
- Hard limit: 3,000 lines -- never exceed without justification

When a file grows past 2,000 lines, look for natural responsibility seams to split. Cohesive files (single tightly-coupled concern) can stay above 2,000 if splitting would create more coupling than it resolves.

When splitting `foo.rs` into `foo/mod.rs` + child modules:
- Tests travel with their production code into submodules (inline `#[cfg(test)]` blocks)
- mod.rs holds shared types, re-exports, and the tightly coupled core
- Child modules declared as `pub(crate) mod` (not private `mod`) so sibling modules can access re-exports
- Items re-exported via lib.rs must be `pub` in the child module (not `pub(crate)`)

## Testing

Three layers, each derived from the spec:

1. Types (compiler-enforced): requires/ensures in piano.carve derive types
   via carve. The compiler rejects wrong dataflow. No tests needed.
2. Invariants (test-enforced): asserts clauses in piano.carve declare
   properties every output must satisfy. Tests check these predicates.
3. Relational postconditions (user-written): input-output relationships
   the spec can't express propositionally. User writes the test body;
   the spec mandates it exists.

Integration tests in `tests/`. Unit tests in source files (`#[cfg(test)]` modules) for parsing, formatting, and internal logic.

### When to Test

- ALWAYS test: relational postconditions (output = f(input)), spec-declared invariants (asserts), data transformations (NDJSON/JSON parsing, report formatting), AST rewriting correctness
- Test at boundaries: where runtime output gets parsed by the CLI, where user source gets rewritten
- Cross-validate against known-good outputs (sample_crossval pattern)

### When NOT to Test

- Dataflow (types guarantee it -- if carve derives the types, the compiler enforces the flow)
- Subjective design choices (report column widths, wording)
- Loud failures (code that crashes immediately and obviously)
- Trivial delegation (passing args to a well-tested dependency)

### Test Categories

- `e2e.rs` -- full pipeline: instrument, build, run, check output
- `sample_crossval.rs` -- validate report output against known-good samples
- `async_integration.rs` -- consolidated async tests (tokio, alloc, self-time, nested select, main return type)
- `threaded_integration.rs` -- consolidated threaded tests (rayon alloc tracking, cross-thread instrumentation)
- `minimal_integration.rs` -- consolidated minimal-dep tests (cfg-gated alloc, cpu time, custom bin path, exit in non-main, integration frames, strict lints, workspace member)
- `macro_rules.rs` -- macro_rules! template instrumentation
- `project_root.rs` -- project root detection
- `run_cmd.rs` -- `piano run` command pipeline
- `special_fns.rs` -- special function handling (const fn, unsafe fn, extern fn)
- `msrv_compat.rs` -- runtime compiles on Rust 1.59

## CI

### ci.yml (all PRs + push to main)

Seven jobs:

1. `fmt` (ubuntu-latest) -- `cargo fmt --check`
2. `clippy` (ubuntu-latest) -- `cargo clippy --workspace --all-targets -- -D warnings`
3. `test` (matrix: ubuntu-latest + macos-15) -- `cargo test --workspace` then `cargo test --workspace --features piano-runtime/cpu-time,piano-runtime/_test_internals`
4. `msrv` (ubuntu-latest) -- tests on Rust 1.88 (CLI MSRV) + installs 1.59 for runtime MSRV test
5. `doc` (ubuntu-latest) -- `cargo doc --workspace --no-deps` with `-D warnings`
6. `coverage` (ubuntu-latest) -- `cargo llvm-cov --workspace --features piano-runtime/cpu-time --lcov`, uploads to Codecov
7. `test-hygiene` (ubuntu-latest) -- rejects `std::env::set_var` / `remove_var` in `piano-runtime/src/` to prevent flaky test regressions

### release.yml (release/* PRs only)

1. `version-bump-scope` -- ensures `chore(cargo): bump version` commits only touch `Cargo.toml`, `piano-runtime/Cargo.toml`, and `Cargo.lock`

## QA Infrastructure

Two automated pillars supplement line coverage (CodeCov):

### Property Testing (proptest)
- `tests/proptest_rewrite.rs` generates random function signatures
- Checks: output parses, guards injected, async wrapped, non-targets untouched
- Runs as part of `cargo test --workspace`

### Allocation Oracles
- `piano-runtime/tests/alloc_oracle.rs` verifies exact allocation counts
- Fully deterministic, no CI flakiness
- Catches silent alloc data corruption/loss

### Accuracy Oracles
- `piano-runtime/tests/accuracy.rs` validates timing ratio accuracy
- Relaxed bounds (+-20%) catch catastrophic regression but tolerate CI noise
