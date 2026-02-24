# Code

## Rust Style

- Edition 2024 (CLI), Edition 2021 (piano-runtime, MSRV 1.56)
- MSRV: 1.88 (CLI), 1.56 (runtime -- broad compatibility for user projects)
- `cargo clippy --workspace --all-targets -- -D warnings` (strict, enforced in CI)
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` (documentation must compile cleanly, enforced in CI)
- Idiomatic Rust: prefer stdlib patterns (iterators, if-let chains, Result propagation with `?`) over manual indexing or `process::exit` (except at the CLI boundary in `main.rs`)

## Testing

Integration tests in `tests/`. No unit tests in source files currently.

### When to Test

- ALWAYS test: data transformations (NDJSON/JSON parsing, report formatting), AST rewriting correctness, cross-thread timing attribution, build pipeline (staging, dependency injection)
- Test at boundaries: where runtime output gets parsed by the CLI, where user source gets rewritten
- Cross-validate against known-good outputs (sample_crossval pattern)

### When NOT to Test

- Subjective design choices (report column widths, wording)
- Loud failures (code that crashes immediately and obviously)
- Trivial delegation (passing args to a well-tested dependency)

### Test Categories

- `e2e.rs` -- full pipeline: instrument, build, run, check output
- `sample_crossval.rs` -- validate report output against known-good samples
- `cross_thread.rs` -- multi-threaded instrumentation with fork/adopt
- `cpu_time.rs` -- CPU time feature pipeline
- `integration_frames.rs` -- per-frame NDJSON output
- `alloc_threaded.rs` -- allocation tracking across threads
- `msrv_compat.rs` -- runtime compiles on Rust 1.56
- `strict_lints.rs` -- runtime compiles with strict warnings
- `workspace_member.rs` -- workspace member instrumentation
- `custom_bin_path.rs` -- non-default binary entry points

## CI

Five jobs, all on ubuntu-latest:

1. `fmt` -- `cargo fmt --check`
2. `clippy` -- `cargo clippy --workspace --all-targets -- -D warnings`
3. `test` -- `cargo test --workspace`
4. `msrv` -- tests on Rust 1.88 (CLI MSRV) + installs 1.56 for runtime MSRV test
5. `doc` -- `cargo doc --workspace --no-deps` with `-D warnings`

Triggers: push to main, all pull requests.
