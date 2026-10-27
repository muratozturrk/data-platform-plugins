# Lakehouse Basics: File Format vs Table Format, Plain Parquet vs Iceberg

## Problem
Data architecture moved from OLTP databases to warehouses to lakes, and each step fixed one
problem while creating another. Warehouses are fast but costly, closed and copy-heavy; plain
lakes are cheap and open but unreliable ("data swamps"). A lakehouse puts an open table format
(Iceberg) on low-cost object storage so many engines share one copy with warehouse guarantees.

## Generations

| Generation | Examples | Strength | Catch |
|---|---|---|---|
| OLTP database | PostgreSQL, MySQL, Oracle, SQL Server | Row layout: point lookups, small updates | Analytic scans slow the application; not built for long history |
| Enterprise DW | Teradata, Netezza, Exadata | Columnar OLAP, governance | Coupled storage/compute, sized for peak, rigid |
| Cloud DW | Snowflake, BigQuery, Redshift | Separate storage/compute, pay per use | Load before query, proprietary format, one copy per warehouse |
| Hadoop/cloud data lake | HDFS/S3 + Hive, Spark, Impala | Cheap, open, any data | No schema enforcement, no transactions, coordination outside storage |
| Lakehouse | Iceberg + any engine | One open copy, ACID, evolution, many engines | Catalog and version alignment become platform work |

Separating storage from compute (cloud DW) is not separating data from the engine: the data
still lives in the vendor's format.

## Row vs column layout (measured)
200,000 orders, 9 columns, 3 wide text columns.

| Access pattern | Row layout (CSV + key index) | Column layout (Parquet) |
|---|---|---|
| Revenue per country (2 of 9 columns) | 48,016,061 B read = whole file | 1,120,101 B of an 11,256,725 B file (42.9x less) |
| Point lookup of one full record | 256 B via key index | 3,658,939 B (whole row group of every column decoded) |

Match layout to access pattern: columnar for scans, row stores for point access.

## Layers

| Layer | Responsibility | Examples |
|---|---|---|
| File format | How records are laid out inside one file | Parquet, ORC, Avro, CSV |
| Table format | Which files form the table, schema, partitions, versions | Iceberg, Delta Lake, Hudi, Paimon |
| Catalog | Where the current table metadata is; commit coordination | Iceberg REST, Polaris, Nessie, Glue, Lakekeeper, Gravitino, HMS (legacy) |

Iceberg does not change how data is stored: data files are ordinary Parquet. pyarrow reads them
with no Iceberg library; `record_count` equals the Parquet row count and the manifest bounds equal
the footer min/max. Iceberg changes how files are found and committed.

## Why plain lakes fail

| Incident | Plain Parquet directory | Iceberg |
|---|---|---|
| Sink dies mid-batch (2 of 4 files land) | Reader counts 6,000 rows from a 4,000 + partial batch | 2 stray files on S3 are invisible; 4,000 rows; no new snapshot |
| Nightly overwrite hits a bad record | Old data deleted before write; path unreadable | Previous snapshot current; later good overwrite still time-travels to the old one |
| Upstream changes `amount` to string, adds `currency` | Write succeeds, readers fail at query time | Append rejected; `ADD COLUMN` evolves explicitly |
| Column rename | Renamed column NULL in every row | Metadata-only, same files, sums unchanged |
| Filter on timestamp, layout by `order_date=` dirs | 31 LIST calls, all 30 files | 2 metadata files, 3 data files planned |

On S3 the plain-lake picture is worse than on HDFS: object stores have no atomic rename for
commit protocols. Hive Metastore added a table abstraction but became a bottleneck at hundreds of
thousands to millions of partitions and still pruned by listing.

## Hive limitations and the Iceberg answer

| Hive | Iceberg |
|---|---|
| Partition metadata in an RDBMS | Metadata files on storage: metadata.json, manifest list, manifests |
| Schema tied to file layout | Columns tracked by id |
| Pruning by directory listing and footers | Pruning during planning from manifest stats |
| Users must filter on the partition column | Hidden partitioning via transforms |
| Coordination external / best effort | Snapshot-based ACID with optimistic concurrency |

## One table, many engines
```python
# Python service (PyIceberg) and Spark share the REST catalog and the same files
tbl = catalog.load_table("db.orders")
tbl.append(arrow_batch)                                   # new snapshot, visible to Spark
con = tbl.scan(selected_fields=("country", "amount", "status")).to_duckdb(table_name="orders")
con.execute("SELECT country, round(sum(amount), 2) FROM orders GROUP BY country").fetchall()
```

- Spark and DuckDB return identical revenue for all countries; PyIceberg's planned files equal
  Spark's `data_files`; reading from both engines adds zero objects to the bucket.
- A PyIceberg append is the next Spark query's latest snapshot (with `cache-enabled=false`);
  a Spark `UPDATE` is visible to PyIceberg after `load_table`.
- Spark `TIMESTAMP` is Iceberg `timestamptz`: Arrow data from Python must be
  `timestamp[us, tz=UTC]`, or the append fails with a schema mismatch.
- PyIceberg against S3-compatible storage needs `s3.endpoint`, `s3.path-style-access` and
  credentials set explicitly on the client.
- Engines in different runtimes (JVM Spark, Python PyIceberg/DuckDB) share nothing but the REST
  catalog and object storage, and list the same snapshot ids.

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog("lake", **{
    "type": "rest", "uri": "http://rest:8181",
    "s3.endpoint": "http://seaweedfs:8333", "s3.path-style-access": "true",
    "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
    "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"], "s3.region": "us-east-1",
})
```

## Five lakehouse layers

| Layer | Role | Examples |
|---|---|---|
| Storage | Data + metadata files | S3, ADLS, GCS, HDFS, MinIO, Ceph |
| Ingestion | Batch/streaming writes | Spark, Flink, Kafka Connect, Fivetran |
| Catalog | Table -> metadata pointer, access control | REST, Polaris, Nessie, Glue, Lakekeeper |
| Federation | Join lake data with other sources without copying | Dremio, Trino (DuckDB locally) |
| Consumption | BI, notebooks, AI/ML, data APIs | Tableau, Power BI, Superset, Jupyter |

## Table formats compared

| Aspect | Iceberg | Others |
|---|---|---|
| Core | ACID, schema evolution, time travel | Same baseline in Delta Lake, Hudi, Paimon |
| Partitioning | Hidden partitioning, partition evolution without rewrite | Mostly static schemes |
| Ecosystem | Spark, Flink, Trino, Dremio, Snowflake, BigQuery, DuckDB; ASF project | Hudi streaming focus; Delta UniForm for interop |

## When to use / not

| Situation | Choice |
|---|---|
| Raw landing zone, write once, single writer | Plain files can be enough |
| Several writers, retries, overwrites, schema changes | Table format |
| Several engines or teams on the same data | Iceberg + REST catalog |
| Sub-second BI dashboards | Lakehouse plus curated aggregates / materialized views (still needed) |
| Existing Delta estate | Evaluate UniForm before migrating |
| Bad data modeling | Not fixed by any table format |

## Gotchas
- "Sales is this table in my catalog", never "sales is this folder".
- A lakehouse reduces copies and pipelines; it does not remove curated datasets.
- Pin catalog and client Iceberg versions together; mismatches reject requests.
- `DROP TABLE` without `PURGE` leaves files; `register_table` on the saved metadata.json restores
  the table (2,000 rows back).
