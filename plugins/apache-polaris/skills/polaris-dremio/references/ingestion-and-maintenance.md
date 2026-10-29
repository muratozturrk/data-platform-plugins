# CTAS, COPY INTO and Maintenance from Dremio

## Concept

Dremio can create and fill Iceberg tables in SQL, with no Spark job. `CREATE TABLE`, CTAS and
`COPY INTO` load CSV/JSON/Parquet files from any Dremio file source. On an Enterprise REST source
the commits go to Polaris, and every engine on the catalog sees the result. Dremio also compacts
(`OPTIMIZE TABLE`) and expires snapshots (`VACUUM TABLE`). Results below were measured on Dremio
OSS 26.0.5 with a Nessie source `lake`. File paths use an S3 source `landing` (`@landing/landing/...`).

## SQL reference

| Need | Statement |
|---|---|
| Empty table | `CREATE TABLE Src.ns.t (col TYPE, ...)` |
| Derived table | `CREATE TABLE Src.ns.t AS SELECT ...` |
| Load a folder | `COPY INTO Src.ns.t FROM '@files/path/' FILE_FORMAT 'csv'` |
| Load a subset | `... FILES ('a.csv', 'b.csv')` or `... REGEX '<java regex>'` |
| Format options | `FILE_FORMAT 'csv' (FIELD_DELIMITER '\|', NULL_IF ('NA'), ON_ERROR 'continue')` |
| `ON_ERROR` | `'abort'`, `'continue'` (skip bad rows), `'skip_file'` |
| Rejected rows | `sys.copy_errors_history`, then `TABLE(copy_errors('<table>', '<job_id>'))` |
| Compact partitions | `OPTIMIZE TABLE t REWRITE DATA USING BIN_PACK FOR PARTITIONS <predicate>` |
| Compact with thresholds | `... BIN_PACK (TARGET_FILE_SIZE_MB = n, MIN_FILE_SIZE_MB = n, MAX_FILE_SIZE_MB = n, MIN_INPUT_FILES = n)` |
| Full compaction | `OPTIMIZE TABLE t` |
| Manifests only | `OPTIMIZE TABLE t REWRITE MANIFESTS` |
| Expire snapshots | `VACUUM TABLE t EXPIRE SNAPSHOTS [older_than '<ts>'] [retain_last N]` (defaults: older than 5 days, keep at least 1) |
| Properties | `ALTER TABLE t SET TBLPROPERTIES ('k' = 'v')`, `SHOW TBLPROPERTIES t` |

## Worked example: landing files

```sql
COPY INTO lake.sales.my_table   FROM '@landing/landing/path/to/files/' FILE_FORMAT 'csv';    -- 3 rows
COPY INTO lake.logs.web_events  FROM '@landing/landing/logs/2024-01/' FILE_FORMAT 'json';    -- 3 rows
COPY INTO lake.sales.daily_orders FROM '@landing/landing/orders/2023/'
  REGEX '.*_2023-12-.*\.csv' FILE_FORMAT 'csv';                                          -- 2 files
COPY INTO lake.hr.employees FROM '@landing/landing/hr_data/' FILES ('new_hires.csv')
  FILE_FORMAT 'csv' (FIELD_DELIMITER '|', ON_ERROR 'continue', NULL_IF ('NA'));          -- 2 ok, 1 rejected

SELECT * FROM sys.copy_errors_history WHERE table_name = 'lake.hr.employees';
SELECT * FROM TABLE(copy_errors('lake.hr.employees', '<job_id>'));   -- file, line 4, emp_id, cast error

CREATE TABLE lake.sales.high_value_orders AS
SELECT customer_id, COUNT(*) AS order_count, MAX(total_amount) AS max_order
FROM lake.sales.orders WHERE total_amount > 1000 GROUP BY customer_id;                  -- 7 rows
```

On OSS the CTAS table was then registered in Polaris and read through it with vended credentials:
7 rows, same columns. See [connecting.md](connecting.md).

## COPY INTO results (as commonly written vs. working form)

| Form | Result | Working form |
|---|---|---|
| Folder CSV / folder JSON | 3 rows each | as written |
| `REGEX '.*_2023-12-.*\\.csv'` | ran, 0 rows | `REGEX '.*_2023-12-.*\.csv'`, because SQL strings do not unescape backslashes |
| `FILES ('new_hires.csv') (FILE_FORMAT 'csv', FIELD_DELIMITER '\|', ...)` | syntax error | `FILES (...) FILE_FORMAT 'csv' (FIELD_DELIMITER '\|', ...)` |
| `COPY INTO t(cols) FROM (SELECT ... t.$2 ... FROM '@src/f.csv' t)` | syntax error on OSS 26.0.5 | Staging table + `INSERT ... SELECT` |
| `sys.copy_errors_history WHERE table_name = 'hr.employees'` | 0 rows | `'lake.hr.employees'` (source-qualified) |

An empty `CREATE TABLE` wrote one `00000-...metadata.json` and one manifest list, with no data files.

## Worked example: compacting the 48-file table

```sql
OPTIMIZE TABLE lake.sales.orders REWRITE DATA USING BIN_PACK FOR PARTITIONS order_date >= '2024-01-01';
OPTIMIZE TABLE lake.sales.orders REWRITE DATA USING BIN_PACK (
  TARGET_FILE_SIZE_MB = 256, MIN_FILE_SIZE_MB = 100, MAX_FILE_SIZE_MB = 1000, MIN_INPUT_FILES = 10);
OPTIMIZE TABLE lake.sales.orders;
OPTIMIZE TABLE lake.sales.orders REWRITE MANIFESTS;
```

| After | Data files | Manifests | Rows |
|---|---|---|---|
| start (12 INSERTs, 4 partitions) | 48 | 12 | 60 |
| `FOR PARTITIONS` (2 partitions) | 26 | 13 | 60 |
| `BIN_PACK (... MIN_INPUT_FILES = 10)` | 4 | 14 | 60 |
| `OPTIMIZE TABLE` (defaults) | 4 | 1 | 60 |
| `REWRITE MANIFESTS` | 4 | 1 | 60 |

Counts come from `table_files` and `table_manifests`, so you can script the same before/after
check from Dremio.

## VACUUM and properties

- All three forms (`older_than ... retain_last 20`, `retain_last 100`, bare `EXPIRE SNAPSHOTS`)
  returned `VACUUM TABLE command is not supported for this source` on the Nessie source of OSS
  26.0.5. They were not tested on an Enterprise REST source.
- Even where VACUUM works, the defaults (older than 5 days) expire nothing on a young table.
  Pass `older_than` and `retain_last` for an immediate effect.
- `SHOW TBLPROPERTIES` returned no rows, before and after
  `ALTER TABLE ... SET TBLPROPERTIES ('commit.retry.num-retries' = '3')`. The table's metadata.json
  held 6 properties: that one, plus Dremio's `gc.enabled=false`,
  `write.metadata.delete-after-commit.enabled=false`, target sizes and `zstd`. Read properties
  from the metadata (PyIceberg, Spark `SHOW TBLPROPERTIES`, Polaris load-table response).
- `gc.enabled=false` on Dremio-written tables means other engines' `expire_snapshots` will refuse
  to delete files until you change it. Check before scheduling Spark maintenance on them.

## RBAC notes

CTAS, COPY INTO and OPTIMIZE commit through the source principal, which needs write privileges
(e.g. `CATALOG_MANAGE_CONTENT`) and write-capable vended keys. Keep a read-only principal for
analytics sources and a separate writer source for ingestion and maintenance.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| COPY INTO ran but loaded 0 rows | Double backslash in REGEX | One backslash |
| Syntax error near `(FILE_FORMAT` | Options placed before the format | `FILE_FORMAT 'csv' (options)` |
| Syntax error in transformation COPY | Not supported on OSS 26.0.5 | Staging table, then `INSERT ... SELECT` |
| No rows in `copy_errors_history` | Unqualified `table_name` | Source-qualified name |
| VACUUM not supported | Source type / edition | Expire from Spark (`expire_snapshots`) or on an Enterprise REST source |
| `SHOW TBLPROPERTIES` empty | OSS behavior observed | Read the metadata |

## Related

- [connecting.md](connecting.md), [queries-and-metadata.md](queries-and-metadata.md)
- The `iceberg-maintenance` skill of the apache-iceberg plugin covers compaction strategies and retention
