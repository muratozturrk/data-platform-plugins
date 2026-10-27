---
name: git-workflow-standards
description: Git standard: branch naming, Conventional Commits, PR size and template, review rules, CODEOWNERS, semantic versioning and tagging, release notes, monorepo conventions, protected branches. Use when setting up a repo, writing commits or PRs, or configuring CI gates.
---

# Git Workflow Standards

## Branches
- Trunk-based: `main` protected; short-lived branches `{type}/{ticket-or-short-desc}` with types `feat|fix|chore|docs|refactor|test|ci|hotfix|release`; kebab-case; e.g. `feat/dp-142-tenant-controller`.
- Branch lifetime ≤ 3 days; rebase on main before merge; squash-merge with the PR title as the commit.

## Commits (Conventional Commits 1.0)
```
type(scope): short imperative summary ≤ 72 chars

Body: what and why, not how. Wrap at 72.

BREAKING CHANGE: description      # or type! for breaking
Refs: DP-142
```
Types: `feat fix docs style refactor perf test build ci chore revert`. Scope = component/plugin (`control-plane`, `sdk`, `data-governance`). Breaking changes bump major. Enforced by commitlint; release notes and versions generated (release-please / semantic-release).

## Pull requests
- Template: What / Why / How tested / Risk / Rollback / Screenshots-or-logs / Checklist (lint, tests, docs, ADR?, naming reviewed).
- ≤ 400 changed lines or split; one concern per PR; draft until CI green.
- Two approvals for `platform.core`/`sdk`/manifest schemas (CODEOWNERS), one elsewhere; author never self-approves; reviewers respond within 1 business day.
- Required checks: pre-commit, type check, tests, fitness functions, naming check, docs build.

## Versioning and tags
- SemVer `vMAJOR.MINOR.PATCH` per released unit (control plane, SDK, each plugin, manifest schema `apiVersion`); tags signed; `CHANGELOG.md` per unit.
- Pre-releases `v1.4.0-rc.1`; no `latest` tags in deployments.

## Repository hygiene
- `.gitignore`, `.gitattributes` (LF, linguist), `.editorconfig`, `CODEOWNERS`, `SECURITY.md`, `LICENSE`, `CONTRIBUTING.md` in every repo.
- No secrets ever (gitleaks in pre-commit and CI); no binaries > 1 MB (LFS); generated artifacts not committed.
- Monorepo: `plugins/`, `packages/`, `apps/`, `infra/`, `docs/`; path-filtered CI; per-package versioning.

## Anti-patterns
- Long-lived feature branches and merge commits from main
- "fix", "wip", "updates" commit messages
- Force-push to shared branches
- Release by hand without tag or changelog

Sources: Conventional Commits 1.0.0 (https://www.conventionalcommits.org); Semantic Versioning 2.0.0 (https://semver.org); Chris Beams, "How to Write a Git Commit Message"; Google Engineering Practices — Code Review (https://google.github.io/eng-practices/review/); trunkbaseddevelopment.com; Keep a Changelog; GitHub CODEOWNERS docs.
