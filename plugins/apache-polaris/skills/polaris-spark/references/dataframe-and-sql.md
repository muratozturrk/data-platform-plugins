# DataFrame API and SparkSQL on Polaris Tables

## Concept

Once the catalog is configured, Polaris tables behave like any Iceberg table in Spark.
DataFrameWriterV2 (`df.writeTo(...)`) and SQL create, change and query them. Every commit goes
through Polaris, so any other engine on the same catalog sees it at once. Only the identifiers are
Polaris-specific: `<spark-catalog>.<namespace>.<table>`. Iceberg semantics in depth (snapshots,
copy-on-write vs merge-on-read, metadata tables) are in the `iceberg-table-format` skill of the
apache-iceberg plugin.

## API reference

| Operation | DataFrame API | SQL |
|---|---|---|
| Create | `df.writeTo(t).partitionedBy("c").tableProperty(k, v).create()` | `CREATE TABLE t (...) USING iceberg PARTITIONED BY (c) TBLPROPERTIES (...)` |
| Append | `df.writeTo(t).append()` | `INSERT INTO t VALUES ...` / `INSERT INTO t SELECT ...` |
| Upsert | temp view + `spark.sql("MERGE INTO ...")` | `MERGE INTO t USING src ON ... WHEN MATCHED ... WHEN NOT MATCHED ...` |
| Update / delete | `spark.sql(...)` | `UPDATE t SET ... WHERE ...`, `DELETE FROM t WHERE ...` |
| Read | `spark.table(t).filter(...)` | `SELECT ...` |
| History | `spark.table(f"{t}.snapshots")`, `.history`, `.files`, `.partitions`, `.manifests` | `SELECT * FROM t.snapshots` |
| Time travel | `spark.read.option("snapshot-id", ...)` | `SELECT ... FROM t VERSION AS OF <id>` / `TIMESTAMP AS OF '<ts>'` |

## Worked example (measured snapshot sequence)

```python
from pyspark.sql.functions import col, lit
columns = ["id", "name", "event_date"]
df = spark.createDataFrame([(1, "Alice", "2023-01-01"), (2, "Bob", "2023-01-02"),
                            (3, "Charlie", "2023-01-03")], columns)
df.writeTo("polaris.db.events").partitionedBy("event_date") \
  .tableProperty("write.format.default", "parquet").create()          # snapshot 1: append

spark.createDataFrame([(1, "Alice Updated", "2023-01-01"), (4, "David", "2023-01-04")],
                      columns).createOrReplaceTempView("updates")
spark.sql("""MERGE INTO polaris.db.events t USING updates u ON t.id = u.id
             WHEN MATCHED THEN UPDATE SET t.name = u.name
             WHEN NOT MATCHED THEN INSERT (id, name, event_date) VALUES (u.id, u.name, u.event_date)""")
                                                                         # snapshot 2: overwrite
spark.sql("DELETE FROM polaris.db.events WHERE event_date < '2023-01-02'")  # snapshot 3: delete
spark.createDataFrame([(5, "Eve", "2023-01-05"), (6, "Frank", "2023-01-06")],
                      columns).writeTo("polaris.db.events").append()     # snapshot 4: append
```

Measured results:
- schema `id bigint, name string, event_date string`
- `filter(col("event_date") > lit("2023-01-01"))` returned 2 rows
- after MERGE: 4 rows, id 1 = `Alice Updated`
- after DELETE: ids 2, 3, 4
- after append: ids 2 to 6
- `.snapshots` operations `append, overwrite, delete, append`, with 5 data files

The DELETE matched a whole partition, so it was a metadata-only `delete` snapshot.

SQL version of the same lifecycle on `polaris.db.sql_events` (`event_date DATE`): the operations
were `append, overwrite, delete, overwrite`. Time travel with the first snapshot's `snapshot_id`
and `committed_at` returned ids 1, 2, 3 in both forms:

```sql
SELECT committed_at, snapshot_id FROM polaris.db.sql_events.snapshots ORDER BY committed_at;
SELECT id FROM polaris.db.sql_events VERSION AS OF <snapshot_id>;
SELECT id FROM polaris.db.sql_events TIMESTAMP AS OF '<committed_at>';
```

## Type handling

- `writeTo().create()` takes its schema from the DataFrame. Python strings stay STRING. Use
  `to_date(col("event_date"))` (or an explicit schema) if the column is a date.
- A comparison on a STRING date column is a string comparison. It gives the right answer only for
  ISO-formatted values.
- Iceberg's write check refuses implicit STRING to DATE casts. This fails:
  `INSERT INTO t VALUES (1, 'Alice', '2023-01-01')` →
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST ... "STRING" to "DATE"`.
  This works: `VALUES (1, 'Alice', DATE '2023-01-01')`. The same applies to a MERGE source built
  from literals (`'2023-01-01' AS event_date`).
- Predicates are different: `WHERE event_date > '2023-01-01'` on a DATE column works, because
  Spark casts the literal in a comparison.

## RBAC notes

- DDL and DML need write privileges on the catalog role that the session's principal role
  holds. `CATALOG_MANAGE_CONTENT` covered create, MERGE/UPDATE/DELETE, append and metadata
  tables in the tests.
- With read-only privileges, reads and metadata tables work, but writes fail. With vending, the
  failure is at the storage upload, because the vended keys are read-only.
- Polaris authorizes each table load, so the metadata tables (`.snapshots`, `.files`) need the
  same read privilege as the table.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `TABLE_OR_VIEW_ALREADY_EXISTS` | Name already created by an earlier step or job | Use another name, `CREATE TABLE IF NOT EXISTS`, or `DROP TABLE` first |
| `CANNOT_SAFELY_CAST "STRING" to "DATE"` | String literals written into a DATE column | `DATE '...'` literals or `to_date()` |
| Date column turned out STRING | Schema inferred from Python strings | Explicit schema or cast before `create()` |
| `Cannot find snapshot with ID ...` | Made-up or expired snapshot id | Take ids from `<t>.snapshots` |
| `Cannot find a snapshot older than ...` | Timestamp before the first snapshot | Use a `committed_at` at or after the first commit |
| MERGE "update" inserts a row again | The target row was deleted earlier, so the NOT MATCHED branch ran | Check the table state before relying on MATCHED |
| MERGE/UPDATE without the extensions | `IcebergSparkSessionExtensions` not set | Set `spark.sql.extensions` |

## Related

- [connection.md](connection.md), [streaming-and-maintenance.md](streaming-and-maintenance.md)
- The `iceberg-table-format` skill of the apache-iceberg plugin covers DDL, partitioning, row-level operations and metadata tables in general
