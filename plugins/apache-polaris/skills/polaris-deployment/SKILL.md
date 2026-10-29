---
name: polaris-deployment
description: "Run and operate Apache Polaris OSS (1.8.0) - container images and ports (8181 API, 8182 health/metrics), in-memory vs relational-jdbc persistence on PostgreSQL, root principal bootstrap (POLARIS_BOOTSTRAP_CREDENTIALS vs polaris-admin-tool bootstrap, polaris_schema), storage integration (FILE vs S3/GCS/Azure, S3-compatible endpoints with STS role vending, uid 10000 file permissions), the first catalog/principal/role setup, and operations: Prometheus metrics with tags, OpenTelemetry traces, MDC/JSON logs, and the startup production-readiness checks. Use when deploying Polaris with Docker Compose, moving from a quickstart to a durable setup, debugging catalogs lost on restart, 'Unsupported storage type: FILE', 'Failed to create file', 'no schema has been selected to create in', a Prometheus target down on 8282, missing JSON logs, or 'Production readiness checks failed!'."
---

# Polaris Deployment

Getting Apache Polaris OSS running and keeping it operable: which image and ports, where the
metadata lives, how the first root principal comes to exist, how catalogs reach object storage,
and how to observe the server once it runs. Everything here is measured on Polaris 1.8.0 (plus
an older 1.1.0-incubating-SNAPSHOT image many quickstarts still use). Tuning knobs (feature
flags, realms, rate limits, token broker) and day-2 management with the `polaris` CLI are in the
`polaris-configuration-and-cli` skill; Iceberg itself is in the apache-iceberg plugin
(`iceberg-table-format`, `iceberg-catalogs`).

## When to Use This Skill

- Writing a Docker Compose (or similar) deployment of Polaris.
- Catalogs, principals or roles disappear after a restart.
- Moving from a quickstart (in-memory, FILE storage) to PostgreSQL and object storage.
- Bootstrapping the root principal without leaving the secret in the server environment.
- Connecting Polaris to S3 or an S3-compatible store with STS so it can vend credentials.
- Setting up the first catalog, principal and roles on an empty server.
- Wiring Polaris into Prometheus, an OpenTelemetry collector and a log pipeline.
- Reading what Polaris itself reports as not production-ready.

## Decision Table

| Need | Technique / config | Reference |
|---|---|---|
| State that survives restarts | `polaris.persistence.type=relational-jdbc` + `quarkus.datasource.*` | [containers-and-persistence.md](references/containers-and-persistence.md) |
| Create the root principal once | `apache/polaris-admin-tool bootstrap --realm --credential` | [containers-and-persistence.md](references/containers-and-persistence.md) |
| Quick local demo | In-memory + `POLARIS_BOOTSTRAP_CREDENTIALS` + FILE storage flags | [containers-and-persistence.md](references/containers-and-persistence.md) |
| Object storage with vended keys | `storageConfigInfo` S3 + `roleArn` (+ `stsEndpoint`, `endpoint`, `pathStyleAccess`) | [storage-and-first-catalog.md](references/storage-and-first-catalog.md) |
| Make a fresh server usable | Token, catalog, catalog role, principal, principal role, assignments, grant | [storage-and-first-catalog.md](references/storage-and-first-catalog.md) |
| New catalog or new namespace? | Catalog for hard boundaries / environments / storage; namespace for grouping | [storage-and-first-catalog.md](references/storage-and-first-catalog.md) |
| Metrics, traces, logs | `:8182/q/metrics` + tags, OTLP, MDC + JSON logs | [observability-and-readiness.md](references/observability-and-readiness.md) |
| Production gap list | Startup readiness checks | [observability-and-readiness.md](references/observability-and-readiness.md) |

## Containers and Persistence

**What it is.** `apache/polaris` serves the APIs on 8181 and health/metrics on 8182
(`/q/health/ready` is the right healthcheck). The default metastore is in memory.

```yaml
  polaris-bootstrap:
    image: apache/polaris-admin-tool:latest
    command: ["bootstrap", "--realm=POLARIS", "--credential=POLARIS,root,s3cr3t"]
    environment: &durable_env
      polaris.persistence.type: relational-jdbc
      quarkus.datasource.db-kind: postgresql
      quarkus.datasource.jdbc.url: jdbc:postgresql://postgres:5432/polaris
      quarkus.datasource.username: polaris
      quarkus.datasource.password: polaris
  polaris:
    image: apache/polaris:latest
    environment: {<<: *durable_env, polaris.realm-context.realms: POLARIS}
    depends_on: {polaris-bootstrap: {condition: service_completed_successfully}}
```

**Gotchas (measured)**

- In-memory: a catalog created before a restart was gone after it (ready again in 11 s);
  relational-jdbc kept it (13 s).
- The admin tool failed on a fresh database with `no schema has been selected to create in`
  until `CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;` ran.
- With admin-tool bootstrap the server needs no `POLARIS_BOOTSTRAP_CREDENTIALS`: the root principal
  lives in the database and the secret stays out of the server environment.
- EclipseLink persistence no longer exists in 1.8.0; use relational-jdbc.

## Storage Integration and the First Catalog

**What it is.** Each catalog's `storageConfigInfo` says where data may live and how Polaris gets
credentials. Polaris writes `metadata.json` with its own keys; engines get table-scoped vended keys.

```json
{"storageType": "S3", "allowedLocations": ["s3://warehouse/sales/"], "region": "us-east-1",
 "endpoint": "http://seaweedfs:8333", "pathStyleAccess": true,
 "roleArn": "arn:aws:iam::000000000000:role/lake-vendor", "stsEndpoint": "http://seaweedfs:8333"}
```

**Gotchas (measured)**

- FILE storage on 1.8.0 defaults: 400 `Unsupported storage type: FILE`. It needs
  `ALLOW_INSECURE_STORAGE_TYPES=true` and FILE in `SUPPORTED_CATALOG_STORAGE_TYPES` (labs only).
- Polaris runs as uid 10000; on a root-owned shared directory the first write fails with
  `Failed to create file` until the table's `metadata/` and `data/` directories are writable.
- A placeholder role ARN is rejected by 1.1.0-SNAPSHOT (400 `Invalid role ARN format`) but
  accepted by 1.8.0 (201), which only fails later at vending time.
- `stsUnavailable: true` means metadata only: engines asking for vended credentials get 400.
- `CATALOG_MANAGE_PRIVILEGES` and `TABLE_DELETE` do not exist (400); use `CATALOG_MANAGE_ACCESS`, `TABLE_DROP`.

## Observability and Readiness

**What it is.** Quarkus metrics, OpenTelemetry traces and logging, plus Polaris' own startup
checks against production rules.

```yaml
quarkus.otel.sdk.disabled: "false"
quarkus.otel.exporter.otlp.traces.endpoint: http://otel-collector:4317
polaris.metrics.tags.environment: prod
polaris.log.mdc.environment: prod
quarkus.log.console.json.enabled: "true"
```

**Gotchas (measured)**

- Metrics are on `:8182/q/metrics`; `:8282` (older docs) refuses connections.
- The request ID is read from `X-Request-ID`, not `Polaris-Request-Id`.
- `quarkus.log.console.json=true` (older name) writes no JSON; `...json.enabled=true` does, with
  `mdc` = requestId, realmId and custom keys. Custom MDC keys do not show in the plain format.
- Startup logs `Production readiness checks failed!` with each offending option (in-memory
  persistence, `require-header=false`, insecure storage). `polaris.readiness.ignore-severe-issues=true`
  only lets the server start anyway.

## Run the Examples

Two stacks under `examples/`:

| Stack | Proves | Tests |
|---|---|---|
| [quickstart-and-persistence](examples/quickstart-and-persistence/README.md) | Quickstart image, in-memory vs PostgreSQL, admin tool, FILE vs S3 catalogs, first-user setup | 13 |
| [observability](examples/observability/README.md) | Metrics port and tags, Prometheus, OTLP spans, log formats, readiness checks | 8 |

```bash
cd examples/quickstart-and-persistence
docker compose -p polaris-deploy up -d --build
docker compose -p polaris-deploy exec runner python -m pytest -v
docker compose -p polaris-deploy down -v

cd ../observability
docker compose -p polaris-ops up -d --build
docker compose -p polaris-ops exec runner python -m pytest -v
docker compose -p polaris-ops down -v
```

Keep the project names: the runners find containers by the compose project label. On Git Bash
for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Shipping the default in-memory metastore | Every restart wipes catalogs, principals, grants | relational-jdbc + admin-tool bootstrap |
| Root secret in `POLARIS_BOOTSTRAP_CREDENTIALS` on a durable server | Visible in the container environment | Bootstrap once with the admin tool |
| FILE storage beyond a laptop | Insecure type, no vending, permission trouble | S3 / GCS / Azure with STS roles |
| Trusting a 201 on catalog create as proof the ARN works | 1.8.0 validates the ARN only at vending | Smoke-test a vended table load |
| Publishing port 8182 | Exposes `/q/metrics` and health internals | Keep it on the internal network |
| Leaving `polaris.readiness.ignore-severe-issues=true` in production | Hides the server's own warnings | Fix each named option |
| Copying older property names (`:8282`, `quarkus.log.console.json`, `Polaris-Request-Id`) | Silently ignored on 1.8.0 | Use the 1.8.0 names above |

## References

- [references/containers-and-persistence.md](references/containers-and-persistence.md): images, ports, persistence types, admin-tool bootstrap, quickstart vs production.
- [references/storage-and-first-catalog.md](references/storage-and-first-catalog.md): storage types, S3-compatible + STS, FILE permissions, first-user sequence, catalog design.
- [references/observability-and-readiness.md](references/observability-and-readiness.md): metrics, traces, logs, readiness checks, production checklist.
- Related skills: `polaris-configuration-and-cli` (flags, realms, rate limiting, token broker, CLI), `polaris-catalog-federation` (S3/STS example stack, external catalogs), `polaris-catalog-fundamentals`.
