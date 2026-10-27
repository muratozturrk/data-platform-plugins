# First Lakehouse: Spark Ingestion, Dremio Queries, BI over a Semantic Layer

Scope: a complete lakehouse on one machine (operational source, object storage, ingestion engine,
catalog, query engine, BI) and the mechanics that make it work: shared catalog, shared files,
snapshot history, metadata pruning, curated views over Arrow Flight.

## Problem
A retail chain's sales transactions (product, category, amount, date, store, age group, campaign)
live in PostgreSQL. Analysts need SQL and dashboards over them without a warehouse copy, and the team
wants to see every lakehouse layer working together before designing each one separately.

## Architecture

| Layer | Tool | Port / protocol | Role |
|---|---|---|---|
| Operational DB | PostgreSQL 16 | 5432, JDBC | Source of truth, copied once |
| Object storage | S3 API (MinIO, SeaweedFS, AWS S3) | 9000 / 8333, HTTP | Parquet data + Iceberg metadata |
| Ingestion | Spark 3.5 + Iceberg 1.8 | JDBC in, S3FileIO out | Writes tables, commits to the catalog |
| Catalog | Nessie (`/api/v2`) | 19120, REST | Maps table name -> current `metadata.json` |
| Query engine | Dremio OSS | 9047 REST/UI, 31010 JDBC, 32010 Arrow Flight | SQL, views, time travel |
| BI | Superset | 8088; `dremio+flight://` | Charts and dashboards |

Spark and Dremio never talk to each other: both point at the same catalog and the same bucket. Spark
commits a new metadata location to Nessie; Dremio looks the table up there and reads the same files.

## Spark configuration (Nessie catalog, S3FileIO)

```python
c = "spark.sql.catalog.nessie"
spark = (SparkSession.builder
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config(c, "org.apache.iceberg.spark.SparkCatalog")
  .config(f"{c}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
  .config(f"{c}.uri", "http://nessie:19120/api/v2")       # v1 also works; v2 is current
  .config(f"{c}.ref", "main")
  .config(f"{c}.authentication.type", "NONE")              # local only
  .config(f"{c}.warehouse", "s3://warehouse/")
  .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  .config(f"{c}.s3.endpoint", "http://seaweedfs:8333")     # container DNS name
  .config(f"{c}.s3.path-style-access", "true")
  .config(f"{c}.cache-enabled", "false")                   # see other engines' commits at once
  .config("spark.sql.defaultCatalog", "nessie")
  .getOrCreate())
```

Bake JDBC drivers into the image (no Ivy download per session); on one Compose network every service
resolves by name, so never hardcode container IPs.

## Load patterns

```python
def full_reload(spark, ns, table="fashion_sales"):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    spark.read.jdbc(JDBC_URL, table, properties=PROPS) \
        .writeTo(f"{ns}.{table}").tableProperty("format-version", "2").createOrReplace()

def incremental_append(spark, target):
    hw = spark.table(target).agg(F.coalesce(F.max("id"), F.lit(0))).first()[0]
    new = spark.read.jdbc(JDBC_URL, f"(SELECT * FROM fashion_sales WHERE id > {int(hw)}) AS delta",
                          properties=PROPS)             # predicate runs inside PostgreSQL
    n = new.count()
    if n:
        new.writeTo(target).append()
    return n
```

| Condition | Pattern | Why |
|---|---|---|
| Small table, or rows updated in place | `createOrReplace()` full reload | One atomic snapshot; second reload keeps the row count and adds exactly one snapshot |
| Append-only source with increasing key | High-water mark append | Only new rows cross the wire; 2 new rows -> `append` snapshot with `added-records=2`, rerun appends 0 |
| Updates and deletes must be captured | CDC + `MERGE` | High-water marks miss updates and deletes |

JDBC type mapping (Spark 3.5.5, Iceberg 1.8.1): `VARCHAR(n)` -> `string` (length lost),
`DECIMAL(10,2)` -> `decimal(10,2)`, `SERIAL` -> `int`, `DATE` -> `date`.

Files after the first write under `s3://warehouse/<namespace>/<table>_<uuid>/`: `data/*.parquet`,
`metadata/*.metadata.json`, `metadata/snap-*.avro` (manifest list), `metadata/*-m0.avro` (manifest).

## Nessie specifics

- Entries: `GET /api/v2/trees/main/entries` lists `ICEBERG_TABLE`, `NAMESPACE`, `ICEBERG_VIEW`.
- `GET /trees/main/contents/<ns.table>` returns `snapshotId` (= latest Spark snapshot) and
  `metadataLocation`; `/trees/main/history` has one commit per catalog change naming the table.
- Tables created through `NessieCatalog` get `gc.enabled=false` and
  `write.metadata.delete-after-commit.enabled=false`; the table also carries `nessie.commit.id`
  (64-char hash). `DROP TABLE ... PURGE` fails with "Cannot purge table: GC is disabled" because other
  branches may share the files. Drop without PURGE and clean storage with Nessie GC.
- In-memory Nessie loses all state on restart: fine for a laptop, never for shared use.

## Dremio on the same catalog

Nessie source settings: endpoint `http://nessie:19120/api/v2`, auth None, storage provider AWS, root
path `warehouse`, access key/secret, "Encrypt connection" off locally, and connection properties
`fs.s3a.path.style.access=true`, `fs.s3a.endpoint=<host:port>`, `dremio.s3.compat=true`.

```sql
SELECT category, count(*) n, sum(sales_amount) revenue FROM nessie.sales.fashion_sales GROUP BY category;
SELECT * FROM TABLE(table_snapshot('nessie.sales.fashion_sales')) ORDER BY committed_at;
SELECT count(*) FROM nessie.sales.fashion_sales AT SNAPSHOT '6695...';   -- id as string literal
SELECT count(*) FROM nessie.sales.fashion_sales AT TIMESTAMP '2024-06-01 00:00:00';
```

- Per-category counts and revenue are identical in Spark and Dremio; the bucket's object count is
  unchanged after Dremio queries (no copy).
- A Spark commit is visible in Dremio's next query and in `table_snapshot` without a metadata refresh;
  snapshot ids match Spark's `<table>.snapshots`.
- After one Spark insert, the table has 3,001 rows and `AT SNAPSHOT '<previous id>'` returns 3,000.
- Dremio's REST API rejects a source metadata policy with `datasetRefreshAfterMs` below 60,000 ms.

## Metadata pruning

Iceberg manifests store per-file min/max per column. The engine skips files whose range cannot match.

| Layout of the same 3,000 rows (6 files) | Files read for `sales_amount = 35` |
|---|---|
| No filter | 6 of 6 |
| `repartitionByRange(6, "sales_amount").sortWithinPartitions("sales_amount")` | 1 of 6 |
| `repartition(6)` (random) | 6 of 6 |

The same decision from Spark metadata:
`SELECT count_if(readable_metrics.sales_amount.lower_bound <= 35 AND readable_metrics.sales_amount.upper_bound >= 35) FROM t.data_files`
returns 1 candidate. Pruning needs clustering: sort or partition by the common filter column at write
time. In Dremio, measure it from the job profile (`/apiv2/profiles/<job_id>.json`, records of the
`SPLIT_GEN_MANIFEST_SCAN` operator = files left after pruning); `sys.jobs_recent.rows_scanned` counts
rows after reader-level filtering and shows 0 for both layouts.

## BI over a semantic layer

A chart is a dimension plus a metric; the BI tool generates
`SELECT <dim>, <metric> FROM <dataset> GROUP BY <dim>`. Define shared metrics once in views.

```sql
CREATE OR REPLACE VIEW nessie.sales.sales_by_region AS
SELECT store_location AS region, count(*) AS orders, sum(sales_amount) AS revenue,
       round(avg(sales_amount), 2) AS avg_order_value
FROM nessie.sales.fashion_sales GROUP BY store_location;
```

Python clients use `pyarrow.flight`: `FlightClient("grpc+tcp://dremio:32010")`,
`authenticate_basic_token(user, password)`, `get_flight_info(FlightDescriptor.for_command(sql))`,
then `do_get(ticket).read_all()` returns a `pyarrow.Table`.

- Superset URI: `dremio+flight://USER:PASSWORD@dremio:32010/?UseEncryption=false` (set
  `UseEncryption=true` outside a laptop). Run `superset init` once.
- `SUM` over `DECIMAL(10,2)` returns `decimal128(38, 2)` over Flight; results equal PostgreSQL's own
  aggregates exactly (Decimal equality) for "Sales by Day" (92 days) and "Sales by Category" (6 slices).
- `CREATE VIEW` on a Nessie source stores the view in Nessie as `ICEBERG_VIEW`, visible to other
  Nessie-aware engines. Spark's `SHOW TABLES` does not list them, so drop views before dropping the
  namespace.
- Validate dashboards against aggregates computed by the source system, not constants.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot purge table: GC is disabled` | Nessie sets `gc.enabled=false` | `DROP TABLE` without PURGE; Nessie GC for files |
| Dremio parse error `Encountered ...` on `AT SNAPSHOT 123` | Snapshot id must be quoted | `AT SNAPSHOT '123'` |
| Filter reads every file | Values not clustered; every file spans the full range | Sort/partition by the filter column, then compact |
| Can't drop namespace after creating views | Views are `ICEBERG_VIEW` entries invisible to `SHOW TABLES` | `DROP VIEW` each view first |
| Dremio grabs most host RAM | Defaults size from host memory | `DREMIO_MAX_HEAP_MEMORY_SIZE_MB`, `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` |
