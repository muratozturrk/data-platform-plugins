---
name: api-versioning-sdk
description: Define and version the platform's public surface (REST/gRPC API, CLI, SDK, manifest schemas, plugin SPI) with a deprecation policy so customers and plugin authors can depend on it. Use when exposing any new capability externally, changing a contract, or designing the SDK/CLI.
---

# Public Surface: API, CLI, SDK and Versioning

A product is defined by what it promises not to break. For a data platform the public surface is wider than the REST API: it includes the CLI, the SDK, every manifest schema and every plugin SPI.

## When to Use This Skill

- Adding or changing an endpoint, CLI command, SDK method, manifest field or SPI method
- Designing the SDK that customers and plugin authors use
- Writing the deprecation and support policy
- Generating docs and clients from contracts

## Core Concepts

### The five public contracts
1. **Control-plane API** (REST/gRPC): tenants, projects, pipelines, runs, policies, usage
2. **CLI**: thin client over the API plus local dev commands (`validate`, `render`, `run --local`)
3. **SDK** (`platform.sdk`): the only module plugin authors and customers import; everything else is private
4. **Manifest schemas**: `apiVersion` per kind
5. **Plugin SPI**: one version per plugin type

Each has its own semantic version and changelog; the platform release notes list all five.

### Compatibility rules
- Additive = minor; breaking = major; fixes = patch
- Server supports current and previous major API version concurrently
- SDK supports the server's current and previous major
- SPI: core loads N and N-1
- Deprecation: announce → warn at use (logs, headers, CLI) → remove no sooner than one major release and a published minimum window (e.g. 6 months)

### Contract-first
OpenAPI / protobuf / JSON Schema are the source of truth; server stubs, SDK clients, CLI help and docs are generated from them. Contract tests run in CI against the spec.

## Patterns

### URL and header versioning
`/v1/tenants/{tenant}/pipelines`; tenant in the path or in a validated token claim, never in a query string. Deprecation headers (`Deprecation`, `Sunset`) on old versions.

### SDK layering
```
platform.sdk            # stable public: client, models, plugin base classes, testing utilities
platform.sdk.testing    # contract test suites for SPI implementers
platform._internal      # private; import-linter rule forbids external use
```

### Error contract
Structured errors: `code`, `message`, `hint`, `docs_url`, `tenant`, `request_id`. Codes are versioned and listed in the docs; messages may change, codes may not.

### Idempotency and pagination
Idempotency keys on mutating calls (pipeline submits, onboarding); cursor pagination everywhere; stable sort keys.

### Documentation as contract
Every public element has a reference page generated from the contract and a "since / deprecated / removed" line. Quickstart and upgrade guides are tested in CI (doctest-style or scripted).

## Anti-patterns
- Customers importing internal modules because the SDK lacked something
- Breaking manifest changes without a migrator
- CLI with logic that the API does not have (two sources of truth)
- Silent removal of fields, flags or endpoints

See `references/sources.md`.
