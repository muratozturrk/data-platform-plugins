---
name: api-naming
description: Naming for the platform's public surface: REST paths, query parameters, JSON fields, enums, error codes, event names, CLI commands/flags, SDK methods and OpenAPI operationIds. Use when adding or reviewing any public API, CLI or SDK element.
---

# API, CLI and SDK Naming

## When to Use This Skill
- Adding an endpoint, field, enum value, error code, CLI command or SDK method
- Reviewing an OpenAPI/protobuf change

## Core Concepts

### REST (converged practice: Stripe, GitHub, Google AIP, Microsoft, Zalando)
- Paths: lowercase kebab-case, **plural nouns for collections**, no verbs, no trailing slash, no file extensions: `/v1/tenants/{tenant_id}/workspaces/{workspace_id}/pipelines/{pipeline_id}`.
- Version as the first path segment (`/v1`); sunset via `Deprecation`/`Sunset` headers.
- Nesting ≤ 2 levels; beyond that use filters (`/v1/runs?pipeline_id=…`).
- Non-CRUD actions as sub-resources or custom methods, consistently: `/pipelines/{id}/runs` (POST = start), `/runs/{id}:cancel` (AIP-136 style) — pick one and apply everywhere.
- Query params snake_case; standard names `page_size`, `page_token`, `order_by`, `filter`, `fields`.
- Resource ids are opaque strings named `{entity}_id`; never expose database integers as the only id.

### JSON fields
- `snake_case` (matches Python/SQL/dbt and the data assets; one casing across the platform, no translation layer), timestamps RFC 3339 UTC ending in `_at`, durations with unit `_ms/_seconds`, money as string + `_currency`, booleans `is_/has_`.
- Same name as the column/manifest field for the same concept (`tenant_id`, `created_at`).
- Enums `UPPER_SNAKE` strings (`RUNNING`, `FAILED`), never integers.
- Pagination envelope `{items, next_page_token}`; errors `{code, message, hint, docs_url, request_id}`.

### Error codes
`{domain}.{condition}` stable strings: `pipeline.not_found`, `manifest.schema_invalid`, `tenant.quota_exceeded`; messages may change, codes may not.

### Events (webhooks / platform event bus)
`{entity}.{past_tense_verb}` (`run.completed`, `pipeline.published`), versioned payload schema; same vocabulary as Kafka domain events.

### OpenAPI / SDK
- `operationId` = `{verb}{Entity}` (`listPipelines`, `createRun`); SDK methods snake_case in Python (`client.pipelines.list()`), camelCase in TypeScript — generated from the same spec.
- Tags = resource names.

### CLI
`platform {noun} {verb}` (`platform pipeline validate`, `platform tenant create`); flags `--kebab-case`, env vars `PLATFORM_{UPPER_SNAKE}`; global flags `--tenant`, `--workspace`, `--output json|table`.

### gRPC/protobuf (if used)
Services `{Entity}Service`, RPCs `{Verb}{Entity}`, messages `{Verb}{Entity}Request/Response`, fields snake_case (proto convention).

## Anti-patterns
- `/getPipeline`, `/pipeline/list`, `/Pipelines`
- Mixing camelCase and snake_case across endpoints
- `id` fields that mean different things in different payloads
- Boolean query flags without `is_` where the field exists elsewhere as `is_`
- Renaming a public field without an alias and a major version
