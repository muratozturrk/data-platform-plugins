---
name: architecture-fitness-functions
description: Turn platform architecture rules (no domain nouns in core, tenant context everywhere, core never imports plugins, versioned public surface, no hard-coded environment) into automated checks that run in CI. Use when introducing an architecture rule, reviewing a PR for structural drift, or setting up architectural governance for the platform.
---

# Architecture Fitness Functions

*Building Evolutionary Architectures* (2nd ed.) defines a fitness function as any mechanism that provides an objective integrity assessment of some architectural characteristic, and shows how to use them to automate architectural governance. For a platform that must stay generic across customers, fitness functions are the only governance that survives deadlines.

## When to Use This Skill

- A new architecture rule is agreed ("core must not know about tenants' domains")
- Reviewers keep catching the same class of problem
- Setting up CI gates for the platform repository
- Measuring structural health over time (trend, not just pass/fail)

## Core Concepts

### Categories
- **Atomic vs holistic**: one characteristic (import rules) vs interaction of several (performance under tenant isolation)
- **Triggered vs continuous**: CI check vs production monitor (e.g. cross-tenant access attempts = 0)
- **Static vs dynamic**: code analysis vs runtime measurement
- **Intentional vs emergent**: defined up front vs discovered after an incident; record both as rules

### The platform's rule set → checks
| Rule | Fitness function | Tooling hint |
|---|---|---|
| Core never imports plugins or provider SDKs | Dependency rule | import-linter (Python), ArchUnit (JVM), dependency-cruiser (TS) |
| No domain/customer nouns in core | Forbidden-token scan on core paths with an allow-list | custom script in CI; token list is configuration |
| No hard-coded env values | Regex scan for hostnames, bucket URIs, regions, abs paths, IPs outside `config/` and provider plugins | semgrep/custom |
| Tenant context on all storage/secret/queue access | Call-site check: provider functions must receive `ctx` | semgrep rule / typed signatures + mypy strict |
| Public surface versioned | Diff OpenAPI/JSON Schema/SPI against last release; breaking change requires major bump + changelog entry | oasdiff, json-schema-diff, custom |
| Manifests validate against schema | All example and test manifests validate | CI step |
| Every SPI implementation passes contract tests | Run `platform.sdk.testing` suites against all registered plugins | pytest parametrised over entry points |
| Stranger-installability | Quickstart executed in a clean container from docs | CI job, scheduled |
| Cross-tenant isolation | Integration test: two tenants, all cross reads fail; production metric `cross_tenant_denied` alerts on any allowed | tests + monitor |
| Core size stays thin (TVP) | Core LOC / public-surface growth tracked; alert on step changes | metrics job |

## Patterns

### Rule → check → owner → trend
Every rule is recorded as: statement, rationale, fitness function, severity (block / warn), owner, and a dashboard trend. Warn-level checks graduate to block after a grace period.

### Fail with a fix
Checks print *what*, *where*, *why it matters for productization* and *how to fix* (config key, plugin, SPI). A failing check without a fix hint is itself a defect.

### Ratchets
For rules with existing violations, record the baseline count and fail only if it grows; drive it down over releases.

### Architecture decision records
Each rule links to an ADR; when a rule is relaxed, the ADR is superseded, never silently deleted.

## Anti-patterns
- Rules that live only in a wiki or in reviewers' heads
- Checks so noisy they are routinely skipped
- Fitness functions that test tools instead of the characteristic
- No owner, no trend, no removal process for obsolete rules

See `references/sources.md`.
