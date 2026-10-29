# Querying Polaris Tables and Iceberg Metadata from Dremio

## Concept

A Dremio source maps to one Polaris catalog. Namespaces appear as folders and tables as
datasets. Dremio has no default catalog, so every table is addressed as
`<Source>.<Namespace>.<Table>`. Because Dremio is a federation engine, a Polaris table can be
joined with files, databases and other catalogs in the same SQL. Iceberg metadata is exposed
through table functions rather than Spark's `<table>.<metadata>` suffix.

All results below were measured on Dremio OSS 26.0.5 with a Nessie source `lake` in place of a
Polaris source. The SQL is the same for an Iceberg REST source on Enterprise. Only the source
name changes.

## SQL reference

| Purpose | SQL |
|---|---|
| Read | `SELECT * FROM PolarisCatalog.sales.customers LIMIT 10` |
| Federated join | `... JOIN TABLE(<s3src>.<bucket>.<folder>."file.csv"(type => 'text', fieldDelimiter => ',', extractHeader => true)) c ON ...` |
| Data files | `SELECT * FROM TABLE(table_files('PolarisCatalog.sales.orders'))` |
| Commit history | `TABLE(table_history('...'))` |
| Snapshots | `TABLE(table_snapshot('...'))` |
| Manifests | `TABLE(table_manifests('...'))` |
| Partitions | `TABLE(table_partitions('...'))` |
| Clustering | `TABLE(clustering_information('...'))` (clustered tables only) |
| Time travel | `SELECT ... FROM t AT SNAPSHOT '<snapshot_id>'` / `AT TIMESTAMP '<ts>'` |

## Metadata function columns (measured)

| Function | Columns |
|---|---|
| `table_files` | One row per data file: `file_path`, `file_size_in_bytes`, `record_count`, `partition`, bounds, counts |
| `table_history` | `made_current_at`, `snapshot_id`, `parent_id`, `is_current_ancestor` |
| `table_snapshot` | `committed_at`, `snapshot_id`, `parent_id`, `operation`, `manifest_list`, `summary` |
| `table_manifests` | `path`, `length`, added/existing/deleted file counts, `partition_summaries` |
| `table_partitions` | `partition`, `record_count`, `file_count`, `spec_id` |
| `clustering_information` | Clustering keys and depth |

| Question | Function |
|---|---|
| Too many small files? | `table_files` (count, sizes) |
| What changed and when? | `table_history`, `table_snapshot` |
| Metadata bloat? | `table_manifests` |
| Partition skew or pruning? | `table_partitions` |
| Clustering health? | `clustering_information` |

## Worked example: trickle-ingested orders

```sql
CREATE TABLE IF NOT EXISTS lake.sales.orders
  (order_id BIGINT, customer_id INT, order_date DATE, total_amount DOUBLE)
  PARTITION BY (order_date);
-- 12 x INSERT INTO lake.sales.orders VALUES (5 rows each), spread over 4 dates

SELECT c.segment, COUNT(*) AS orders
FROM lake.sales.orders o
JOIN TABLE(landing.landing.lookups."customers.csv"(type => 'text', fieldDelimiter => ',',
                                                   extractHeader => true)) c
  ON o.customer_id = CAST(c.customer_id AS INT)
GROUP BY c.segment ORDER BY c.segment;               -- gold 26, silver 34

SELECT COUNT(*) FROM TABLE(table_files('lake.sales.orders'));      -- 48
SELECT snapshot_id, committed_at FROM TABLE(table_snapshot('lake.sales.orders'))
ORDER BY committed_at;                                             -- 13 rows
SELECT COUNT(*) FROM lake.sales.orders AT SNAPSHOT '<first id>';    -- 0 (CREATE TABLE)
SELECT COUNT(*) FROM lake.sales.orders AT SNAPSHOT '<second id>';   -- 5
SELECT COUNT(*) FROM lake.sales.orders AT TIMESTAMP '<second committed_at>';  -- 5
```

Measured counts:

| Function | Rows |
|---|---|
| `table_files` | 48 (12 commits x 4 partitions) |
| `table_history` | 13 |
| `table_snapshot` | 13 (includes the empty `CREATE TABLE` snapshot) |
| `table_manifests` | 12 |
| `table_partitions` | 4 |
| `clustering_information` | error `Error while applying rule ClusteringInfoPrule` (table not clustered) |

The small-file state is what [ingestion-and-maintenance.md](ingestion-and-maintenance.md) then compacts.

## Polaris-specific notes

- What appears under the source is what the source principal may list. Missing namespaces usually
  mean a missing `NAMESPACE_LIST`, and missing tables a missing `TABLE_LIST`, in Polaris.
- Metadata functions load the table, so they need the same `TABLE_READ_DATA` as a SELECT.
- Commits by other engines (Spark, Trino) through Polaris show up in `table_snapshot` and
  `table_history` like Dremio's own commits. Time travel works to any snapshot still in the
  metadata.
- Dremio may cache metadata per source. If a commit from another engine is not visible, refresh
  the dataset's metadata. This was not measured here.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `Object 'sales.orders' not found` | Two-part name, and Dremio has no default catalog | `Source.ns.table` |
| `Error while applying rule ClusteringInfoPrule` | `clustering_information` on an unclustered table | Use only for tables with clustering keys |
| `AT SNAPSHOT` of the first snapshot returns 0 rows | `CREATE TABLE` commits an empty snapshot | Use the next snapshot |
| Join with a CSV returns nothing | Type mismatch (CSV columns are text) | `CAST(c.col AS INT)` |
| Namespace or table missing in the source tree | Source principal lacks list privileges in Polaris | Grant `NAMESPACE_LIST` / `TABLE_LIST` |

## Related

- [connecting.md](connecting.md), [ingestion-and-maintenance.md](ingestion-and-maintenance.md)
- The `iceberg-table-format` skill of the apache-iceberg plugin covers snapshots and metadata tables in general
