# Pull Requests

## All Changes Go Through PRs

No exceptions, no size threshold. Even a one-line typo fix gets a PR. This gives:
- CI gate (tests, clippy, fmt run automatically)
- Linkable paper trail
- Accountability for every change on main

## Branch Naming

`type/slug` where type matches conventional commit types:

```
feat/namespace-packages
fix/cache-invalidation
refactor/build-pipeline
docs/contributing
test/cross-thread-edge-cases
chore/bump-msrv
```

## PR Scoping

One `type(scope)` per PR. If you can't describe the PR with a single conventional commit prefix, split it into multiple PRs. A PR can contain multiple commits, but they should all serve the same `type(scope)`.

## Push Early

Always push feature branches to origin after the first commit. An unpushed branch is an unrecoverable branch.

```
git push -u origin feat/my-feature
gh pr create --draft
```

## Merge Strategy

Rebase merge only. Linear history, individual commits preserved on main. No squash merge (destroys granular commit history). No merge commits (non-linear history).

How to merge:
- CLI: `gh pr merge <number> --rebase` (always pass `--rebase` explicitly)
- UI: GitHub's "Rebase and merge" button

Never use `gh pr merge` without `--rebase` -- the default creates a merge commit.

## PR Iteration

During review: add new commits on top. Never amend or force-push during review -- it destroys review context and reviewers can't see what changed between reviews. Fixer commits are fine during iteration -- they get cleaned up before merge.

## Pre-Merge Cleanup

Only required when the branch has iteration noise (fixer commits, review responses, partial work). If the branch already has clean, logically scoped commits, skip the cleanup and merge directly.

When cleanup is needed:

1. Soft reset to the base branch: `git reset --soft $(git merge-base HEAD main)`
2. Re-commit by logical concern -- tightly scoped, one concern per commit
3. Force push the clean history to the feature branch
4. Then merge

Force pushing to feature branches is only allowed during this cleanup step, never during active review.

## Branch Protection (enforced on GitHub)

- PRs required to merge into main
- CI must pass before merge
- No force pushes to main
- Linear history required

## PR Content

Summary + test plan. Checklist items:
- `cargo fmt --check` passes
- `cargo clippy --workspace --all-targets -- -D warnings` passes
- `cargo test --workspace` passes
