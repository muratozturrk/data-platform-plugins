# Containers, Persistence and Bootstrap

## Concept

A Polaris OSS deployment is one Quarkus service plus a metastore. Three decisions make it
usable beyond a laptop:

1. **Image and ports**: `apache/polaris` serves the REST APIs on **8181** (`/api/catalog/...`,
   `/api/management/v1/...`) and the management endpoints on **8182** (`/q/health/ready`,
   `/q/metrics`). Keep 8182 internal.
2. **Persistence**: the image defaults to an in-memory metastore. Every restart wipes catalogs,
   principals and roles. `relational-jdbc` (PostgreSQL) is the durable option.
3. **Bootstrap**: the first identity is the realm's root principal. In-memory setups create it at
   every start from `POLARIS_BOOTSTRAP_CREDENTIALS`; durable setups create it once in the
   database with `apache/polaris-admin-tool`.

Managed offerings (for example Snowflake Open Catalog, Dremio Catalog) remove these decisions;
this reference is about running Polaris yourself.

## Configuration Reference (Polaris 1.8.0)

| Setting | Example | Meaning |
|---|---|---|
| `POLARIS_BOOTSTRAP_CREDENTIALS` | `POLARIS,root,s3cr3t` (`;`-separated per realm) | Root principal per realm at startup |
| `polaris.realm-context.realms` | `POLARIS` | Allowed realms, first is the default |
| `polaris.persistence.type` | `relational-jdbc` | Durable metastore (runtime defaults also list `in-memory`, `in-memory-atomic`, `nosql` (beta)) |
| `quarkus.datasource.db-kind` | `postgresql` | JDBC driver |
| `quarkus.datasource.jdbc.url` | `jdbc:postgresql://postgres:5432/polaris` | One concrete database (no `{realm}` placeholder) |
| `quarkus.datasource.username` / `.password` | `polaris` | DB login |
| `quarkus.otel.sdk.disabled` | `"true"` | Turn tracing off where no collector exists |
| `polaris.readiness.ignore-severe-issues` | `"true"` | Start despite failed production-readiness checks (labs only) |
| `JAVA_OPTS_APPEND` | `-Xms128m -Xmx384m` | Heap limits for small hosts |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | | Polaris' own S3 access for writing metadata |

The EclipseLink metastore of pre-1.0 releases is gone: the string `eclipse-link` does not occur
in the 1.8.0 image's jars. Use relational-jdbc.

## Worked Example: Durable Polaris with the Admin Tool

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment: {POSTGRES_USER: polaris, POSTGRES_PASSWORD: polaris, POSTGRES_DB: polaris}
    volumes:
      - ./config/postgres-init.sql:/docker-entrypoint-initdb.d/01-polaris-schema.sql:ro
    healthcheck: {test: ["CMD-SHELL", "pg_isready -U polaris"], interval: 3s, retries: 30}

  polaris-bootstrap:                       # one-off: schema + root principal for realm POLARIS
    image: apache/polaris-admin-tool:latest
    command: ["bootstrap", "--realm=POLARIS", "--credential=POLARIS,root,s3cr3t"]
    environment: &durable_env
      polaris.persistence.type: relational-jdbc
      quarkus.datasource.db-kind: postgresql
      quarkus.datasource.jdbc.url: jdbc:postgresql://postgres:5432/polaris
      quarkus.datasource.username: polaris
      quarkus.datasource.password: polaris
    depends_on: {postgres: {condition: service_healthy}}
    restart: "no"

  polaris:
    image: apache/polaris:latest
    environment:
      <<: *durable_env
      polaris.realm-context.realms: POLARIS      # no POLARIS_BOOTSTRAP_CREDENTIALS needed
    depends_on: {polaris-bootstrap: {condition: service_completed_successfully}}
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8182/q/health/ready >/dev/null"]
```

`config/postgres-init.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;
```

The admin tool also takes short flags (`bootstrap -r my_realm -c my_realm,client-id,my-secret`).
Bootstrapping more realms and the `{realm}` JDBC-URL trap: `polaris-configuration-and-cli` skill.

## Measured Behavior

| Check | Result |
|---|---|
| In-memory server: create catalog, restart container | Catalog gone; root works again (re-bootstrapped from env); ready after 11 s |
| relational-jdbc server: create catalog, restart | Catalog still there; ready after 13 s |
| Durable server environment | No `POLARIS_BOOTSTRAP_CREDENTIALS`; root token works (principal lives in PostgreSQL) |
| Admin tool on a fresh database without the schema | Fails: `no schema has been selected to create in` |
| Quickstart image `apache/polaris:1.1.0-incubating-SNAPSHOT` with `POLARIS,root_user,my_secret_id` | Healthy, token issued |

## Quickstart vs Production

| Setup | Persistence | Storage | Use for |
|---|---|---|---|
| Quickstart compose (Polaris + engine sharing a directory) | In-memory | FILE (insecure types enabled) | Learning, demos |
| Polaris + PostgreSQL + admin tool | relational-jdbc | S3 / GCS / Azure with vending | Anything that must survive a restart |
| Managed Polaris | Provider-managed | Provider-managed | Production without operating Polaris |

Basic compose commands: `docker compose up -d`, `stop`, `down`, `down --volumes` (also deletes the
named volumes, which for PostgreSQL means the metastore).

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| All catalogs and principals gone after restart | Default in-memory metastore | `polaris.persistence.type=relational-jdbc` + datasource |
| Admin tool: `no schema has been selected to create in` | Fresh database without `polaris_schema` | Create the schema first (init SQL) |
| Root secret visible in `docker inspect` | `POLARIS_BOOTSTRAP_CREDENTIALS` on the server | Bootstrap once with the admin tool |
| Health green but nothing works | Healthcheck only covers readiness | Also smoke-test a token request (`/api/catalog/v1/oauth/tokens`) |
| Container killed on small hosts | Default heap too large | `JAVA_OPTS_APPEND` with explicit `-Xmx` |
| Startup log: `Production readiness checks failed!` | In-memory store, insecure storage, header not required | Fix each named option; see [observability-and-readiness.md](observability-and-readiness.md) |

## Related

- [storage-and-first-catalog.md](storage-and-first-catalog.md): storage types, STS, the first user.
- [observability-and-readiness.md](observability-and-readiness.md): health, metrics, readiness checks.
- `polaris-configuration-and-cli` skill: realms, feature flags, token broker, rate limiting, CLI.
