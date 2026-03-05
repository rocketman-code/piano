# Contributing to Piano

Thanks for your interest in contributing. This guide covers the essentials to get started.

## Prerequisites

- Rust 1.88+ (pinned in `rust-toolchain.toml`, installed automatically by rustup)
- Git

## Build

```bash
cargo build                  # debug build
cargo build --release        # release build
```

## Test

```bash
cargo test --workspace       # all tests
cargo test e2e               # single test file
cargo test cross_thread      # single test by name
```

## Lint and format

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

All three are enforced in CI.

## Commit format

[Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): lowercase imperative description
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `style`, `perf`

Examples:

```
feat(rewrite): detect parallel iterator patterns
fix(report): merge cross-thread functions into NDJSON report
test(integration): add cpu_time pipeline test
```

## Pull requests

- All changes go through PRs, no exceptions
- Branch naming: `type/slug` (e.g., `feat/namespace-packages`, `fix/cache-invalidation`)
- One `type(scope)` per PR
- Merge strategy: rebase merge only (linear history)
- Before requesting review, confirm all tests and lints pass locally

PR checklist:

- `cargo fmt --check` passes
- `cargo clippy --workspace --all-targets -- -D warnings` passes
- `cargo test --workspace` passes

## Project standards

Detailed standards for code style, testing, issues, releases, and UX are in [docs/standards/](docs/standards/).
