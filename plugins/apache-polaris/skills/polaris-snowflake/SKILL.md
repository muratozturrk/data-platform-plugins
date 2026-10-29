---
name: polaris-snowflake
description: "Let Snowflake query Apache Polaris (or Snowflake Open Catalog, hosted Polaris) tables: external volume + catalog integration (CATALOG_SOURCE = POLARIS, REST_CONFIG, REST_AUTHENTICATION with OAUTH_CLIENT_ID/SECRET and OAUTH_ALLOWED_SCOPES), CREATE ICEBERG TABLE ... CATALOG_TABLE_NAME as a pointer, ALTER ICEBERG TABLE ... REFRESH / AUTO_REFRESH, discovery functions, externally managed vs Snowflake-managed Iceberg tables and CATALOG_SYNC; on the Polaris side a dedicated read-only snowflake_service principal (polaris CLI, role assignments, TABLE_LIST / TABLE_READ_DATA / NAMESPACE_LIST), valid scopes, and production settings for an internet-facing Polaris (token broker, token lifetime, dev-mode root credentials, rotation flag). Use when wiring Snowflake to Polaris, debugging 400 invalid_scope / 401 / 403 / missing rows in a Snowflake Iceberg table on Polaris, or hardening a Polaris that Snowflake must reach."
---

# Snowflake on Apache Polaris

Snowflake reads Polaris tables through two Snowflake objects. An **external volume** says where the
files are and which cloud role Snowflake uses to read them. A **catalog integration** holds the
Polaris REST endpoint, the OAuth client credentials of a Polaris principal, and the allowed scopes.
A `CREATE ICEBERG TABLE ... CATALOG = <integration>` then registers a pointer to a Polaris table.
No data is copied. Snowflake Open Catalog is Polaris hosted by Snowflake: the integration is the
same, the URI is a Snowflake domain, and principals are called service connections.

**What is verified here and what is not.** Snowflake cannot run locally, so the tests execute no
Snowflake SQL. Everything on the Polaris side was tested on Polaris 1.8.0: the principal, its
roles and privileges, the OAuth scopes, the REST calls an integration makes, read-only vended
credentials, refresh semantics with an Iceberg REST client as stand-in, and production settings.
The Snowflake statements below are documented syntax, marked *not run*. Check them against
current Snowflake documentation.

## When to Use This Skill

- Connecting Snowflake to a self-hosted Polaris or to Snowflake Open Catalog
- Creating the Polaris principal, roles and grants that a Snowflake integration uses
- Snowflake errors: table not found at registration, 401/403, missing new rows, DML refused
- Choosing between externally managed (Polaris-backed), Snowflake-managed Iceberg, and native tables
- Hardening a Polaris endpoint that Snowflake reaches over the internet

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Snowflake reads Polaris tables | External volume + catalog integration + `CREATE ICEBERG TABLE` | [catalog-integration](references/catalog-integration.md) |
| See commits from other engines | `ALTER ICEBERG TABLE ... REFRESH` or `AUTO_REFRESH = TRUE` | [catalog-integration](references/catalog-integration.md) |
| Snowflake must write and share | Snowflake-managed Iceberg table + `CATALOG_SYNC` to Open Catalog | [catalog-integration](references/catalog-integration.md) |
| Least-privilege Polaris principal | CLI: principal, principal role, catalog role, grants, **two assignments** | [service-principal](references/service-principal.md) |
| Namespace discovery from Snowflake | Add `NAMESPACE_LIST` | [service-principal](references/service-principal.md) |
| Correct `OAUTH_ALLOWED_SCOPES` | `PRINCIPAL_ROLE:ALL` or `PRINCIPAL_ROLE:<role>`, no spaces | [service-principal](references/service-principal.md) |
| Production Polaris for Snowflake | Durable persistence, token broker, token lifetime, no dev-mode root | [production-settings](references/production-settings.md) |

## Connecting (Snowflake side, not run)

```sql
CREATE EXTERNAL VOLUME my_iceberg_ext_volume
  STORAGE_LOCATIONS = ((NAME = 'lake' STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<bucket>/<path>/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<acct>:role/<snowflake-read-role>'));

CREATE CATALOG INTEGRATION polaris_catalog_int
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  REST_CONFIG = (CATALOG_URI = 'https://<polaris-host>/api/catalog' CATALOG_NAME = 'prod_catalog')
  REST_AUTHENTICATION = (TYPE = OAUTH
    OAUTH_CLIENT_ID = '<clientId>' OAUTH_CLIENT_SECRET = '<clientSecret>'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:snowflake_reader'));

CREATE OR REPLACE ICEBERG TABLE events_external
  CATALOG = 'POLARIS_CATALOG_INT' EXTERNAL_VOLUME = 'MY_ICEBERG_EXT_VOLUME'
  CATALOG_TABLE_NAME = 'db.events';
ALTER ICEBERG TABLE events_external REFRESH;
```

Gotchas:
- Snowflake cannot reach `localhost:8181`. Polaris needs a public HTTPS endpoint (allowlist
  `SYSTEM$ALLOWLIST()` ranges, automated) or private connectivity. Use a tunnel for experiments.
- Omit `CATALOG_NAMESPACE` and put the full path in `CATALOG_TABLE_NAME`. With a namespace set,
  Snowflake prefixes table names with it.
- External volumes do not support S3 bucket names with dots, and the storage must be in
  Snowflake's region.
- Externally managed tables are read-only in Snowflake (`Cannot perform DML on an externally
  managed Iceberg table`). Row-level delete files may not be supported, so compact merge-on-read
  tables before exposing them, and check current release notes.

## The Snowflake Principal (Polaris side, tested)

```bash
polaris principals create snowflake_service                       # prints clientId / clientSecret
polaris principal-roles create snowflake_reader
polaris catalog-roles create --catalog prod_catalog catalog_reader
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_LIST
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_READ_DATA
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader NAMESPACE_LIST
polaris principal-roles grant --principal snowflake_service snowflake_reader
polaris catalog-roles grant --catalog prod_catalog --principal-role snowflake_reader catalog_reader
```

(Add `--host/--port/--client-id/--client-secret` for the admin connection.)

Measured on 1.8.0:
- Several privileges in one `grant` line exit with code 2 (`invalid choice`). Grant one privilege
  per command.
- Without the last two assignments: a token for `PRINCIPAL_ROLE:snowflake_reader` is issued (200)
  and then rejected on first use (401). With `PRINCIPAL_ROLE:ALL`, listing tables returns 403.
- `PRINCIPAL_ROLE: ALL` with a space returns 400 `invalid_scope`.
- With `TABLE_LIST` + `TABLE_READ_DATA` alone: list tables works, load table works, and 3 rows are
  read with vended credentials. List namespaces returns 403, so add `NAMESPACE_LIST` for
  `SYSTEM$LIST_NAMESPACES_FROM_CATALOG`. An append failed at the S3 upload, because the vended
  keys are read-only.

## Refresh Semantics

A registered table points at the metadata loaded at creation or at the last refresh. With an
Iceberg REST client in the same role: 3 rows before another engine appended, still 3 after, 5
after `refresh()`, and 3 again at the first snapshot. Snowflake's `REFRESH` and `AUTO_REFRESH`
are documented to behave the same way. That was not run in Snowflake.

## Two RBAC Layers

Snowflake RBAC decides who may use the integration and the table objects. Polaris RBAC decides
what the integration's principal can reach. Neither layer enforces the other's rules. Snowflake
masking or row-access policies do not protect the files from Spark, Dremio or Trino reading the
same Polaris table. Revoke the Polaris principal to cut Snowflake off at once.

## Production Settings (tested on 1.8.0)

- A server started without `POLARIS_BOOTSTRAP_CREDENTIALS` logs
  `realm: POLARIS root principal credentials: <id>:<secret>`, and those credentials work.
- Token broker: the default `rsa-key-pair` signs RS256 with `expires_in` 3600.
  `polaris.authentication.token-broker.type=symmetric-key` plus
  `...max-token-generation=PT30M` gave HS256 and 1800.
- `polaris.features."ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING"=true` blocked
  nothing on 1.8.0 with in-memory persistence. Rotate on a schedule instead.
- Older YAML keys (`metaStoreManager`, `tokenBroker`, `featureConfiguration`) belong to the pre-1.0
  server. Polaris 1.x uses Quarkus properties.

## Run the Examples

`examples/snowflake-standin/` simulates Snowflake with REST calls and PyIceberg against three
Polaris 1.8.0 servers:

```bash
cd examples/snowflake-standin
docker compose -p polaris-snowflake up -d --build
docker compose -p polaris-snowflake exec runner python -m pytest -v
docker compose -p polaris-snowflake down -v
```

Keep the project name `polaris-snowflake` (a test finds a container by that label). On Git Bash
for Windows, prefix `exec` with `MSYS_NO_PATHCONV=1`. See
[examples/snowflake-standin/README.md](examples/snowflake-standin/README.md).

## Anti-Patterns

- Root or a shared engine principal in `OAUTH_CLIENT_ID`. Use one principal per external service.
- Creating roles without assigning them, then debugging 401/403 in Snowflake.
- Expecting Spark's commits to show up in Snowflake without a refresh.
- Writing to the same table from Snowflake and other engines without a clear owner. Snowflake
  writes need a Snowflake-managed Iceberg table.
- Exposing a dev-mode Polaris (in-memory state, root credentials in logs) to Snowflake.
- Relying on Snowflake masking policies to protect lakehouse files.

## References

- [references/catalog-integration.md](references/catalog-integration.md): Snowflake objects, registration, refresh, discovery, table kinds, Open Catalog
- [references/service-principal.md](references/service-principal.md): Polaris principal and privileges for Snowflake, scopes, measured results
- [references/production-settings.md](references/production-settings.md): properties for an exposed Polaris
- [examples/snowflake-standin/README.md](examples/snowflake-standin/README.md): runnable Polaris-side tests
