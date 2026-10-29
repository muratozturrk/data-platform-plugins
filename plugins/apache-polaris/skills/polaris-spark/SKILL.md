---
name: polaris-spark
description: "Connect Apache Spark to Apache Polaris and work on Polaris-managed Iceberg tables: SparkCatalog + RESTCatalog properties (uri, warehouse = Polaris catalog name, credential clientId:clientSecret, scope PRINCIPAL_ROLE:..., token-refresh-enabled), the X-Iceberg-Access-Delegation vended-credentials header with S3FileIO, FILE vs S3 storage, several Polaris catalogs in one session, DataFrameWriterV2 and SparkSQL DDL/DML, time travel, Structured Streaming reads and writes, maintenance procedures via CALL <catalog>.system.*, and how Polaris RBAC (principal roles, catalog roles, privileges) shows up as Spark errors. Use when writing a SparkSession config for Polaris, debugging SdkClientException / 401 / 403 / CANNOT_SAFELY_CAST / 'Cannot process overwrite snapshot' on a Polaris catalog, or running Spark pipelines and maintenance on Polaris tables."
---

# Spark on Apache Polaris

Spark talks to Polaris through Iceberg's REST client. It needs no Polaris-specific code: one
`SparkCatalog` per Polaris catalog, backed by `org.apache.iceberg.rest.RESTCatalog`, configured
with the Polaris REST URI, the catalog name as `warehouse`, and a principal's OAuth client
credentials. Polaris checks every load and commit against the RBAC of the principal behind
`credential`. With credential vending, Polaris also hands Spark storage keys scoped to one table.

The facts below were measured on Spark 3.5.5, iceberg-spark-runtime 1.5.2 and hadoop-aws 3.4.0,
against Polaris 1.8.0 (S3 + STS vending) and a 1.1.0-incubating-SNAPSHOT server (FILE storage).
Generic Iceberg-on-Spark topics (DDL, metadata tables, procedures) live in the apache-iceberg
plugin: see the `iceberg-table-format` and `iceberg-maintenance` skills there.

## When to Use This Skill

- Writing `spark.sql.catalog.<name>.*` settings for a Polaris catalog (notebook, job, spark-defaults)
- Spark on S3/ADLS/GCS behind Polaris without static storage keys (credential vending)
- Several Polaris catalogs, or Polaris next to another catalog, in one Spark session
- Errors on a Polaris catalog: `SdkClientException`, 401/403, `Failed to create file`,
  `CANNOT_SAFELY_CAST`, `Cannot process overwrite snapshot`, `Cannot find snapshot`
- Streaming into or out of Polaris tables, and compacting the small files that streaming leaves

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Minimal Spark session on Polaris | `SparkCatalog` + `catalog-impl RESTCatalog`, `uri`, `warehouse`, `credential`, `scope` | [connection](references/connection.md) |
| No storage keys in Spark | `header.X-Iceberg-Access-Delegation=vended-credentials` + `io-impl S3FileIO` | [connection](references/connection.md) |
| Add a second Polaris catalog at runtime | `spark.conf.set("spark.sql.catalog.<n>...", ...)` | [connection](references/connection.md) |
| Narrow a job to one role | `scope=PRINCIPAL_ROLE:<role>` instead of `ALL` | [connection](references/connection.md) |
| Create, upsert, delete from Python | `writeTo().partitionedBy().create()`, MERGE via temp view | [dataframe-and-sql](references/dataframe-and-sql.md) |
| SQL lifecycle and time travel | `CREATE TABLE ... USING iceberg`, `VERSION AS OF`, `TIMESTAMP AS OF` | [dataframe-and-sql](references/dataframe-and-sql.md) |
| Stream into / out of a Polaris table | `writeStream.format("iceberg").toTable()`, `readStream.format("iceberg")` | [streaming-and-maintenance](references/streaming-and-maintenance.md) |
| Fix small files from streaming | `CALL polaris.system.rewrite_data_files / expire_snapshots / rewrite_manifests` | [streaming-and-maintenance](references/streaming-and-maintenance.md) |

## Connecting

The minimal configuration below ran unchanged against Polaris:

```python
conf = (pyspark.SparkConf()
  .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                              "org.apache.hadoop:hadoop-aws:3.4.0")
  .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .set("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
  .set("spark.sql.catalog.polaris.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
  .set("spark.sql.catalog.polaris.uri", "http://polaris:8181/api/catalog")
  .set("spark.sql.catalog.polaris.warehouse", "polariscatalog")          # Polaris catalog name
  .set("spark.sql.catalog.polaris.credential", os.environ["POLARIS_CREDENTIAL"])  # id:secret
  .set("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
  .set("spark.sql.catalog.polaris.token-refresh-enabled", "true"))
```

When the catalog stores its data on object storage, add these for vending:

```python
c = "spark.sql.catalog.lake"
spark.conf.set(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
spark.conf.set(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set(f"{c}.s3.endpoint", "http://seaweedfs:8333")   # non-AWS S3 only
spark.conf.set(f"{c}.s3.path-style-access", "true")           # non-AWS S3 only
spark.conf.set(f"{c}.client.region", "us-east-1")
```

Gotchas:
- If Spark has no keys and sends no header, the first S3 write fails with `SdkClientException`,
  even though Polaris authorized the call. With the header, 2 rows were written and read with no
  keys in the Spark container.
- With static keys in Spark instead of vending, Spark can reach everything those keys allow, and
  the catalog's RBAC no longer protects the files.
- A FILE catalog only works if Polaris and Spark share the directory and both can write to it.
  Otherwise you get `Failed to create file: file:/data/...`.
- The 1.1.0-incubating-SNAPSHOT server dropped `endpoint`, `stsEndpoint` and `pathStyleAccess`
  from an S3 storage config, so it called real AWS STS (403). Polaris 1.8.0 keeps all three. Use
  1.8.0 or later for vending on MinIO, SeaweedFS and similar stores.
- `hadoop-aws:3.4.0` brings the AWS SDK v2 bundle (2.23.19) that S3FileIO needs. No extra jar was
  needed, but the first download is 571 MB.
- Read `credential` from the environment or a secret store. Never paste it into a notebook.

## RBAC Seen From Spark

Spark acts as whatever principal is behind `credential`, limited by `scope`. Typical symptoms:

| Spark symptom | Polaris cause |
|---|---|
| Token request fails with 400 `invalid_scope` | The scope string is malformed, e.g. `PRINCIPAL_ROLE: ALL` with a space |
| Token issued, first call 401 | The narrow scope names a principal role the principal does not hold |
| `ForbiddenException` / 403 on SHOW TABLES or load | No catalog role with `TABLE_LIST` / `TABLE_READ_DATA` reaches the principal role |
| Write or `CALL ... system.*` refused | The catalog role lacks write privileges. `CATALOG_MANAGE_CONTENT` was enough for create, DML and all three procedures |
| S3 upload fails although the table loaded | Vended keys are read-only for a principal with read privileges only |

Grant a catalog role to a principal role, and that principal role to the principal. Missing
either assignment gives the 401/403 pattern above. The `polaris-access-control` skill covers
the privilege model.

## Several Catalogs in One Session

Each Polaris catalog is its own Spark catalog with its own `warehouse` (and its own `credential`
if needed). You can add catalogs after the session starts with `spark.conf.set(...)` and use them
right away (`CREATE NAMESPACE IF NOT EXISTS lake.db`). The S3 tests register `s3cat_vended` and
`s3cat_plain` this way next to the FILE catalog `polaris`. Tables are addressed as
`<spark-catalog>.<namespace>.<table>`, and procedures as `CALL <spark-catalog>.system.<proc>(table => '<ns>.<table>')`.

## DataFrame and SQL

- `writeTo("polaris.db.events").partitionedBy("event_date").create()` infers the schema from the
  data, so Python date strings become a STRING column. Cast with `to_date` before `create()`.
- An INSERT of `'2023-01-01'` into a DATE column fails with
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`. Use `DATE '2023-01-01'`. WHERE predicates still
  cast string literals.
- Time travel needs real values from `<table>.snapshots`. Made-up snapshot ids or timestamps
  fail with `Cannot find snapshot with ID` or `Cannot find a snapshot older than`.
- With copy-on-write (the Iceberg 1.5 default), the snapshot operations were `append`, `overwrite`
  (MERGE/UPDATE), `delete` (a partition-aligned DELETE, metadata only) and `append`.

## Streaming and Maintenance

- On a table with MERGE/DELETE history, `readStream.format("iceberg")` fails with
  `Cannot process overwrite snapshot`. `streaming-skip-overwrite-snapshots` and
  `streaming-skip-delete-snapshots` make it run, but rows inserted by MERGE are never emitted, and
  deleted rows that were emitted earlier stay downstream.
- `toTable()` on a missing table creates it unpartitioned. Run `CREATE TABLE ... PARTITIONED BY` first.
- 25 s at 10 rows/s with a 2 s trigger on 16 cores produced 240 rows in 192 files.
  `rewrite_data_files` turned them into 1 file, `expire_snapshots(retain_last => 1)` deleted 192
  files, and `rewrite_manifests` cut 13 manifests to 1.
- Bound every query (`processAllAvailable()`, `availableNow`, or a timeout plus `stop()`), because
  `awaitTermination()` never returns in a job or test.

## Run the Examples

`examples/spark/` holds pytest suites (`connecting/`, `dataframe/`, `sparksql/`, `streaming/`,
`maintenance/`) with Polaris (FILE and S3+STS), Spark 3.5.5 and SeaweedFS:

```bash
cd examples/spark
docker compose -p polaris-spark up -d --build
docker compose -p polaris-spark exec spark python3 -m pytest -v
docker compose -p polaris-spark down -v
```

On Git Bash for Windows, prefix the `exec` line with `MSYS_NO_PATHCONV=1`. The stack must be fresh:
`bootstrap.py` is not idempotent and Polaris state is in memory. See
[examples/spark/README.md](examples/spark/README.md).

## Anti-Patterns

- Root credentials in `credential` for jobs. Give each job its own principal and a narrow scope.
- Static S3 keys in Spark while Polaris could vend. This bypasses table-level storage scoping.
- Relying on inferred types for dates and timestamps in `writeTo().create()`.
- Treating an Iceberg streaming read with skip options as a change feed.
- `expire_snapshots` ahead of the slowest streaming consumer's checkpoint.
- One Spark catalog name pointing at different Polaris catalogs in different jobs. Keep
  catalog name to `warehouse` mapping stable.

## References

- [references/connection.md](references/connection.md): catalog properties, OAuth flow, vending, FILE vs S3, multi-catalog, error table
- [references/dataframe-and-sql.md](references/dataframe-and-sql.md): DataFrameWriterV2, SQL DML, metadata tables, time travel pitfalls
- [references/streaming-and-maintenance.md](references/streaming-and-maintenance.md): streaming read/write options, fanout, small files, procedures
- [examples/spark/README.md](examples/spark/README.md): runnable tests
