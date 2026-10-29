---
name: polaris-dremio
description: "Use Dremio as an engine on Apache Polaris: the Iceberg REST Catalog source (endpoint /api/catalog, warehouse = Polaris catalog name, scope, credential client_id:client_secret, Use vended credentials vs fs.s3a static keys) and the Snowflake Open Catalog source; the Dremio OSS limitation (no REST/Open Catalog source in OSS 26.0.5, use Enterprise 26+ or registerTable); Source.Namespace.Table naming, federated joins, Iceberg metadata functions (table_files, table_history, table_snapshot, table_manifests, table_partitions, clustering_information), AT SNAPSHOT / AT TIMESTAMP, CREATE TABLE, CTAS, COPY INTO (FILES, REGEX, FILE_FORMAT options, ON_ERROR, copy_errors), OPTIMIZE TABLE (BIN_PACK, FOR PARTITIONS, REWRITE MANIFESTS), VACUUM TABLE EXPIRE SNAPSHOTS, SHOW TBLPROPERTIES, and Polaris RBAC behind the source principal. Use when attaching Polaris to Dremio, writing Dremio SQL on Polaris tables, loading files with COPY INTO, compacting from Dremio, or handing Dremio-written Iceberg tables to Polaris."
---

# Dremio on Apache Polaris

Dremio attaches Polaris as an **Iceberg REST Catalog** source, or as the dedicated **Snowflake
Open Catalog** source for hosted Polaris. From then on, Polaris tables are addressed as
`<Source>.<Namespace>.<Table>`, can be joined with any other Dremio source, and can be read,
written and maintained in SQL. Polaris authorizes every call with the principal in the source's
`credential` and, with vending on, hands Dremio table-scoped storage keys.

**Edition matters.** Both source types need Dremio Enterprise 26+ (or Dremio Cloud). Dremio OSS
26.0.5 rejects both, even with the REST plugin's support options switched on. So the tests run
Dremio SQL on OSS against a Nessie source named `lake` (in place of `PolarisCatalog`) on the same
S3 bucket. They then register Dremio's output in Polaris with `registerTable` and read it back
through Polaris. Results marked "on OSS" may differ on Enterprise REST sources.

Generic Iceberg maintenance and metadata concepts are in the `iceberg-maintenance` and
`iceberg-table-format` skills of the apache-iceberg plugin.

## When to Use This Skill

- Adding Polaris (OSS or Open Catalog) as a Dremio source and choosing vended vs static storage access
- Planning a Dremio OSS deployment that must share tables with Polaris
- Writing Dremio SQL on Polaris tables: queries, joins, metadata functions, time travel
- Loading CSV/JSON/Parquet files with `COPY INTO`, or building tables with CTAS
- Compacting and expiring from Dremio (`OPTIMIZE`, `VACUUM`)

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Attach Polaris OSS | Iceberg REST Catalog source: URI, `warehouse`, `scope`, `credential`, vending on | [connecting](references/connecting.md) |
| Attach Open Catalog | Snowflake Open Catalog source (HTTPS only) | [connecting](references/connecting.md) |
| Dremio OSS must share with Polaris | Write in another catalog, then Polaris `registerTable` | [connecting](references/connecting.md) |
| Query and federate | `SELECT ... FROM Source.ns.table JOIN other_source...` | [queries-and-metadata](references/queries-and-metadata.md) |
| Inspect layout and history | `TABLE(table_files('<t>'))` and friends | [queries-and-metadata](references/queries-and-metadata.md) |
| Load files | `COPY INTO t FROM '@src/path/' FILE_FORMAT 'csv' (...)` | [ingestion-and-maintenance](references/ingestion-and-maintenance.md) |
| Compact | `OPTIMIZE TABLE t REWRITE DATA USING BIN_PACK [FOR PARTITIONS ...]` | [ingestion-and-maintenance](references/ingestion-and-maintenance.md) |
| Expire snapshots | `VACUUM TABLE t EXPIRE SNAPSHOTS older_than '...' retain_last N` | [ingestion-and-maintenance](references/ingestion-and-maintenance.md) |

## Connecting

The Iceberg REST Catalog source for Polaris OSS (UI fields as a source config):

```python
{
  "restEndpointUri": "http://polaris:8181/api/catalog",
  "propertyList": [{"name": "warehouse", "value": "my_catalog"},         # Polaris catalog name
                   {"name": "scope", "value": "PRINCIPAL_ROLE:ALL"}],
  "secretPropertyList": [{"name": "credential", "value": "<client_id>:<client_secret>"}],
}   # + "Use vended credentials" on; or, with it off, fs.s3a.access.key / fs.s3a.secret.key
    #   and fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Gotchas:
- Keep **Use vended credentials** on when Polaris has an STS role. Static `fs.s3a` keys bypass
  Polaris's table-level storage scoping, so Dremio can reach whatever the keys allow. Some
  documentation advises turning vending off for Polaris OSS. Do that only if the catalog cannot vend.
- Both storage modes worked against Polaris 1.8.0 with an Iceberg REST client (3 rows written and
  read each way). Only static mode needed keys in the engine. Without the vending request, the
  load-table response carried no storage keys.
- On OSS 26.0.5, `POST /api/v3/catalog` with type `RESTCATALOG` or `SNOWFLAKEOPENCATALOG` returned
  400 `An invalid value was found: ...`, even after
  `ALTER SYSTEM SET "plugins.restcatalog.enabled" = true` and `"plugins.restcatalog.mutable.enabled" = true`.
- The Snowflake Open Catalog source requires HTTPS. A local HTTP Polaris works only with the generic source.

## RBAC Interplay

Dremio sees exactly what the source principal may see in Polaris. Dremio's own privileges on the
source and its datasets come on top of that and do not widen it. Use one principal per Dremio
source: a reader principal (`TABLE_LIST`, `TABLE_READ_DATA`, `NAMESPACE_LIST`) for
analytics-only sources, and a writer with `CATALOG_MANAGE_CONTENT` only where Dremio runs CTAS,
COPY INTO or OPTIMIZE. A narrow `scope` (`PRINCIPAL_ROLE:<role>`) pins the source to one role.
Other engines that read the same Polaris table bypass Dremio's row and column policies.

## Queries and Metadata (measured on OSS)

- Names must be fully qualified. `sales.orders` without the source failed with `Object 'sales.orders' not found`.
- A join of an Iceberg table with a CSV in an S3 source returned all 60 orders.
- After 12 INSERTs into 4 daily partitions: `table_files` 48 rows, `table_history` 13,
  `table_snapshot` 13 (`CREATE TABLE` commits an empty snapshot), `table_manifests` 12,
  `table_partitions` 4. `clustering_information` errors on unclustered tables.
- `AT SNAPSHOT '<create id>'` returned 0 rows, the first insert 5, `AT TIMESTAMP` of that commit 5, and now 60.

## Writing and Loading (measured on OSS)

- CTAS works (7 rows). An empty `CREATE TABLE` writes a `00000-...metadata.json` and a manifest list with no data.
- `COPY INTO` with a folder of CSV or JSON works (3 rows each).
- `REGEX '.*_2023-12-.*\\.csv'` loads 0 rows, because SQL strings do not unescape `\\`. Use
  `'.*_2023-12-.*\.csv'`, which loaded 2 files.
- Options follow the format: `FILE_FORMAT 'csv' (FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'))`.
  `FILES (...) (FILE_FORMAT 'csv', ...)` is a syntax error.
- The transformation form `FROM (SELECT ... t.$2 ... FROM '@src/file.csv' t)` is a syntax error
  on OSS 26.0.5. Load into a staging table, then `INSERT ... SELECT`.
- `sys.copy_errors_history.table_name` is source-qualified (`lake.hr.employees`).
  `TABLE(copy_errors('<table>', '<job_id>'))` gives the file, line and column of each rejected row.
- Registering Dremio's table in Polaris (`POST .../namespaces/{ns}/register`) worked only with
  the catalog property `polaris.config.allow.unstructured.table.location=true`. With it off, the
  result was 403 `Invalid locations`, and polaris also refuses overlapping catalog locations.

## Maintenance (measured on OSS)

- `OPTIMIZE ... FOR PARTITIONS order_date >= '2024-01-01'` took 24 files to 2. `BIN_PACK (...,
  MIN_INPUT_FILES = 10)` took the other 24 to 2. A default `OPTIMIZE` rewrote no data but cut
  manifests from 14 to 1. Rows stayed at 60.
- `VACUUM TABLE ... EXPIRE SNAPSHOTS` (all forms) returned `not supported for this source` on the
  Nessie source. It is untested on an Enterprise REST source. On young tables, the defaults
  (older than 5 days, keep 1) expire nothing.
- `SHOW TBLPROPERTIES` returned no rows on OSS, although the metadata held 6 properties. Read
  properties from the table metadata instead.
- Reflections (Dremio's query-accelerating materializations) are configured per source, refresh
  policy included. They were not exercised in these tests.

## Run the Examples

`examples/dremio/` runs Dremio OSS 26.0.5, Nessie 0.76.6, Polaris 1.8.0 and SeaweedFS:

```bash
cd examples/dremio
docker compose -p polaris-dremio up -d --build
docker compose -p polaris-dremio exec runner python -m pytest -v
docker compose -p polaris-dremio down -v
```

On Git Bash for Windows, prefix `exec` with `MSYS_NO_PATHCONV=1`. Dremio needs about 4 GB of RAM
and 1 to 2 minutes to start. See [examples/dremio/README.md](examples/dremio/README.md).

## Anti-Patterns

- Planning a Polaris integration on Dremio OSS. Check the edition first.
- Static S3 keys in the source when Polaris can vend.
- Root credentials in the source's `credential`.
- Two-part names (`ns.table`). Dremio has no default catalog.
- Double backslashes in `COPY INTO ... REGEX`.
- Trusting `VACUUM` defaults to free storage on fresh tables.
- Filtering `sys.copy_errors_history` on an unqualified table name.

## References

- [references/connecting.md](references/connecting.md): source settings, storage modes, editions, registerTable path, RBAC
- [references/queries-and-metadata.md](references/queries-and-metadata.md): naming, federation, metadata functions, time travel
- [references/ingestion-and-maintenance.md](references/ingestion-and-maintenance.md): CTAS, COPY INTO, copy errors, OPTIMIZE, VACUUM, table properties
- [examples/dremio/README.md](examples/dremio/README.md): runnable tests
