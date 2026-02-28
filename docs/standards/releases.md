# Releases

## Versioning

Pre-1.0 semver (0.x.y):
- Minor bump (0.x.0) for new features
- Patch bump (0.x.1) for bug fixes
- Breaking changes allowed in minor bumps (pre-1.0 semver) but treated as a last resort
- 1.0 = "the CLI interface is stable, your scripts won't break"

Both `piano` and `piano-runtime` share the same version number. Bump them together.

## Compatibility Surface (breaking = version bump)

- CLI flags and subcommands (renaming/removing breaks scripts)
- NDJSON format version and field names (tooling may parse these)
- JSON run file schema (tags reference saved runs by path)
- `piano-runtime` public API (`enter`, `init`, `flush`, `register`, `reset`, `fork`, `adopt`, `PianoAllocator`, `Guard`, `AdoptGuard`, `SpanContext`)
- Environment variables (`PIANO_RUNS_DIR`, `PIANO_TAGS_DIR`) -- scripts and CI may rely on these

## NOT Compatibility Concerns

- Human-readable report formatting -- column widths, spacing, wording
- Internal lib API in the `piano` crate -- not intended for external use
- Run file directory layout under `target/piano/` -- implementation detail
- Performance -- faster is never breaking, slower is a regression but not a semver event

## Release Cadence

Batch related changes, release when there's a meaningful set of user-facing changes. No version bump per PR.

## Release Checklist

1. All milestone issues closed
2. `cargo test --workspace` passes
3. `cargo clippy --workspace --all-targets -- -D warnings` clean
4. `cargo doc --workspace --no-deps` builds without warnings
5. Update `CHANGELOG.md`
6. Bump version in both `Cargo.toml` and `piano-runtime/Cargo.toml`
7. Run `cargo generate-lockfile --ignore-rust-version` (without this flag, cargo constrains all workspace deps to the lowest member MSRV, downgrading shared dependencies like clap)
8. Commit: `chore(cargo): bump version to 0.x.y` -- this commit may only touch `Cargo.toml`, `piano-runtime/Cargo.toml`, and `Cargo.lock` (CI enforces this on `release/*` PRs)
9. Open a PR (`release/v0.x.y` branch) -- main has branch protection, even version bumps go through a PR
10. After merge, tag the merge commit: `git tag v0.x.y` and `git push --tags`
11. Publish `piano-runtime` first: `cargo publish -p piano-runtime`
12. Wait for crates.io to index, then publish `piano`: `cargo publish -p piano`
13. Close the milestone on GitHub

Publishing order matters: `piano build` injects `piano-runtime` as a dependency into user projects, so the runtime crate must be available on crates.io before the CLI crate references it.

## Git Tags

Format: `v0.x.y` (e.g., `v0.1.0`, `v0.2.0`). Tag the version bump commit.

## Changelog

`CHANGELOG.md` in the repo root, following [Keep a Changelog](https://keepachangelog.com/) format.
Each release maps to a closed GitHub milestone.
Update the changelog as part of the release commit, before tagging.
