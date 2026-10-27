---
name: config-driven-design
description: Design platform behaviour as declarative, schema-validated manifests (sources, pipelines, quality rules, access policies, schedules, SLAs) instead of code, so the same engine serves any customer. Use when adding any behaviour that varies per tenant, project, environment or customer.
---

# Config-Driven (Declarative) Design

## When to Use This Skill

- A new pipeline type, connector option, quality rule, policy or schedule is requested
- Someone proposes an `if tenant == ...` branch or a customer-specific module
- Designing the manifest format, its schema and its versioning
- Deciding what is config, what is code, and what is a plugin

## Core Concepts

### Three buckets
| Bucket | Contains | Owned by |
|---|---|---|
| Core (code) | Engine: parsing, validation, scheduling, execution, SPI loading, tenant context, telemetry | Platform team |
| Plugins (code) | Connectors, transforms, sinks, validators, secret/catalog/notification adapters | Platform team or third parties |
| Configuration (data) | Everything that varies per tenant/project/env: sources, pipelines, rules, policies, schedules, limits | Users / customers |

Rule of thumb: if two customers would want it different, it is configuration. If two customers would want different *behaviour*, it is a plugin selected by configuration.

### Manifest + schema
Every manifest type has a JSON Schema owned by the core, versioned (`apiVersion: platform.example/v1`), validated at load time with precise error messages. The schema *is* the product contract; breaking it is a major version.

### Layered configuration
`defaults (core) ← plugin defaults ← tenant ← project ← environment ← runtime overrides`. Later layers override earlier ones; the merged, resolved config is logged (with secrets redacted) for every run.

### Registries, not hard-coding
Sources, connections and credentials are referenced by id from a registry manifest; pipelines never embed hosts, buckets or secrets. Environment differences live only in environment layers.

## Patterns

### Pipeline manifest (example shape)
```yaml
apiVersion: platform.example/v1
kind: Pipeline
metadata:
  name: landing-to-silver
  tenant: ${TENANT}          # injected by context, never literal
  owner: team-a
spec:
  source: { ref: registry/sources/erp-postgres }
  loadStrategy: { type: incremental, key: updated_at }
  transforms:
    - plugin: sql-transform@v1
      config: { file: silver/accounts.sql }
  quality:
    - plugin: expectations@v1
      config: { suite: silver/accounts.yaml, onFailure: block }
  sink: { ref: registry/sinks/lakehouse-silver }
  schedule: { cron: "0 * * * *", timezone: ${TENANT_TZ} }
  sla: { freshnessMinutes: 90 }
```
Every `plugin:` value is resolved through the SPI registry; every `ref:` through the tenant's registry; every `${...}` through the context resolver.

### Validation pipeline
1. Schema validation (structure, types, enums)
2. Reference resolution (do all `ref:` and `plugin:` exist for this tenant?)
3. Policy checks (does this tenant's plan allow this plugin / schedule / size?)
4. Dry-run compile (render the execution plan without side effects)
Fail fast with the manifest path, line and a fix hint.

### Schema evolution
- Additive changes (new optional field) → minor version
- Renames/removals → major version with an automatic migrator (`platform manifest migrate v1 v2`)
- Keep a `deprecated` annotation on fields with a removal date; emit warnings at load

### Escape hatch without leaking
Allow `plugin: custom-python@v1` with a path to user code so advanced users are not blocked, but run it under the same SPI contract, tenant context and resource limits as any plugin.

## Anti-patterns
- Config files that are actually code (Jinja-heavy YAML, logic in templates)
- Validation only at runtime, deep in the job
- Secrets or environment values inline in pipeline manifests
- A "misc" or "custom" free-form section that everyone abuses

See `references/sources.md`.
