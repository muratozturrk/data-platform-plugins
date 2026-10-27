---
name: plugin-architecture
description: Structure the platform as a minimal core plus plugins behind a versioned service-provider interface (microkernel architecture) so connectors, transforms, validators and adapters can be added or customised without touching the core. Use when designing extension points, a new plugin type, plugin discovery, isolation or the SPI contract.
---

# Plugin (Microkernel) Architecture

Richards & Ford describe the microkernel style as "a natural fit for product-based applications": a core system containing only the minimal functionality to run, plus independent plug-in components that add features without impacting the core. Eclipse, Jenkins, Airflow providers, dbt adapters and Kafka Connect all work this way. A data platform meant to be sold as a product should too.

## When to Use This Skill

- Deciding whether something belongs in core or in a plugin
- Designing a new plugin type (connector, transform, sink, validator, secret provider, catalog adapter, notifier, auth provider, storage provider)
- Defining the SPI contract, versioning and compatibility
- Designing plugin discovery, packaging, isolation and permissions

## Core Concepts

### Core responsibilities (and nothing else)
Manifest parsing and validation, tenant context, SPI registry and loading, execution planning and orchestration hand-off, telemetry, error contract, lifecycle hooks. The core knows *interfaces*, never implementations.

### Plugin types for a data platform
| Type | Contract (minimal) |
|---|---|
| Source connector | `discover()`, `read(partition, cursor) -> records`, `schema()` |
| Sink connector | `write(batch)`, `commit()`, `schema_capabilities()` |
| Transform | `apply(frame, config) -> frame`, `output_schema(input_schema)` |
| Validator | `validate(frame, rules) -> result` |
| Secret provider | `get(ref, tenant) -> secret` |
| Catalog adapter | `register(asset)`, `lineage(edge)` |
| Notifier | `send(event, channel_config)` |
| Storage/compute provider | `resolve(path, tenant)`, `submit(job)` |

### SPI design rules
- One interface per plugin type, versioned independently (`SourceConnector v1`)
- Plugins declare `name`, `version`, `spi_version`, `capabilities`, and a JSON Schema for their own config
- Core calls plugins only through the SPI; plugins never import core internals, only the public `platform.sdk`
- Backward compatibility: the core supports N and N-1 SPI versions; deprecation is announced one major release ahead

### Discovery
Python: entry points (`[project.entry-points."platform.plugins"]`) or a plugin directory scanned at start-up; JVM: ServiceLoader. Record which plugins were loaded, their versions and their source (built-in / tenant-provided) in run metadata.

### Isolation and trust
- Built-in plugins: in-process
- Tenant-provided plugins: separate process/container, resource limits, allow-listed capabilities (network, filesystem, secrets scope), signed packages
- All plugins run with the tenant context and may only access that tenant's resources

## Patterns

### Default plugin preserves current behaviour
When extracting logic from core, first implement it as the *default* plugin so nothing changes for existing users, then let tenants override by configuration.

### Contract test suite
A single test suite runs against every implementation of an SPI (built-in and third-party). Passing it is the definition of "compatible". Ship it in the SDK so plugin authors run it locally.

### Capability negotiation
Plugins declare capabilities (`supports_incremental`, `supports_schema_evolution`, `supports_pushdown`). The core plans around them instead of assuming.

### Plugin scaffold as a golden path
`platform plugin new --type source --name foo` generates package layout, config schema, contract tests, docs stub and CI. Reduces the cost of doing it right.

## Anti-patterns
- "Plugin" that reaches into core private modules
- Core with `if plugin.name == "x"` special cases
- Un-versioned SPI; silent breaking changes
- Plugins that read environment variables directly instead of receiving resolved config
- A single giant "utils" plugin

See `references/sources.md`.
