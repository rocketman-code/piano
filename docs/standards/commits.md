# Commits

## Format

[Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): lowercase imperative description
```

## Types

- `feat` -- new feature
- `fix` -- bug fix
- `refactor` -- code restructuring (no behavior change)
- `test` -- tests only (no production code)
- `docs` -- documentation
- `chore` -- maintenance (deps, tooling, config)
- `style` -- formatting (no logic change)
- `perf` -- performance improvement

## Scope

The affected area in lowercase. Examples: `report`, `rewrite`, `build`, `resolve`, `runtime`, `cli`, `ci`, `cargo`.

## Granularity

Many tightly scoped commits, one logical concern per commit. Never batch multiple concerns into a monolith.

## Fix + Tests in Same Commit

Implementation and its tests go in the same commit. Never split a fix from the test that validates it. Standalone test additions (new edge cases, coverage improvements unrelated to a specific change) can be separate `test(scope)` commits.

## Examples

```
feat(rewrite): detect parallel iterator patterns in instrumented code
fix(report): merge cross-thread functions into NDJSON report
test(integration): add cpu_time pipeline test
refactor(build): extract workspace member detection
chore(cargo): bump version to 0.4.1
docs(standards): add code standards
style(error): apply consistent quote style
```
