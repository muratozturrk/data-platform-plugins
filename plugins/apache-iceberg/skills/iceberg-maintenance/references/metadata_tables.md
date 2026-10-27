# Iceberg metadata tables in depth

Scope: the 16 Spark metadata tables (Iceberg 1.8.1), their schemas, semantics, time travel and pitfalls.

## Problem
Metadata tables are read-only SQL views over `metadata.json`, manifest lists and manifests. They answer
audit, storage and maintenance questions without listing storage, but their semantics are subtle:
`files` includes delete files, `all_*` tables repeat files, `history` ignores branches, and ids of
expired snapshots become NULLs or vanish from inner joins.

## Catalog of tables

| Group | Tables | Scope |
|---|---|---|
| History | `history`, `snapshots`, `metadata_log_entries`, `refs` | Table level |
| Manifests / partitions | `manifests`, `all_manifests`, `partitions` | Current / all reachable |
| Files (current snapshot) | `files`, `data_files`, `delete_files`, `entries`, `position_deletes` | Current |
| Files (all reachable snapshots) | `all_files`, `all_data_files`, `all_delete_files`, `all_entries` | Every snapshot reachable from a ref |

Syntax: Spark `SELECT * FROM db.tbl.snapshots`; Dremio `TABLE(table_snapshot('db.tbl'))`; PyIceberg
`catalog.load_table("db.tbl").inspect.snapshots()` (PyArrow). PyIceberg 0.9 offers 11 tables (no
`all_files`, `all_data_files`, `all_delete_files`, `all_entries`, `position_deletes`) and returns the same
row counts as Spark.

## Key schemas

| Table | Columns |
|---|---|
| `history` | `made_current_at`, `snapshot_id`, `parent_id` (may be expired), `is_current_ancestor` (false after rollback) |
| `snapshots` | `committed_at`, `snapshot_id`, `parent_id`, `operation` (`append`/`overwrite`/`delete`/`replace`), `manifest_list`, `summary` map |
| `metadata_log_entries` | `timestamp`, `file`, `latest_snapshot_id`, `latest_schema_id`, `latest_sequence_number` |
| `refs` | `name` (not `ref_name`), `type` (`BRANCH`/`TAG`), `snapshot_id`, `max_reference_age_in_ms`, `min_snapshots_to_keep`, `max_snapshot_age_in_ms` |
| `manifests` | `content` (0 data, 1 delete), `path`, `length`, `partition_spec_id`, `added_snapshot_id`, `added/existing/deleted_data_files_count`, `added/existing/deleted_delete_files_count`, `partition_summaries`; `all_manifests` adds `reference_snapshot_id` |
| `partitions` | `partition`, `spec_id`, `record_count`, `file_count`, `total_data_file_size_in_bytes`, `position_delete_record_count/_file_count`, `equality_delete_record_count/_file_count`, `last_updated_at`, `last_updated_snapshot_id` |
| `files` family | `content` (0/1/2), `file_path`, `file_format` (`PARQUET`, `AVRO`, `ORC`, `PUFFIN`), `spec_id`, `partition`, `record_count`, `file_size_in_bytes`, `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts`, `lower_bounds`, `upper_bounds` (binary), `key_metadata`, `split_offsets`, `equality_ids`, `sort_order_id`, `referenced_data_file`, `content_offset`, `content_size_in_bytes`, `readable_metrics` (typed bounds per column name) |
| `entries` / `all_entries` | `status` (0 existing, 1 added, 2 deleted, relative to the row's `snapshot_id`), `snapshot_id`, `sequence_number`, `file_sequence_number`, `data_file` struct, `readable_metrics` |
| `position_deletes` | `file_path`, `pos`, `row` (NULL when written by Spark), `partition`, `spec_id`, `delete_file_path`; v3 adds `content_offset`, `content_size_in_bytes` |

The `partition` struct is the union of every spec's fields: after `ADD PARTITION FIELD months(order_date)`
it is `struct<region:string, order_date_month:int>`, older files show NULL for the new field, and the
month is stored as months since 1970-01 (661 = February 2025). Key partitions by `(partition, spec_id)`.

## Time travel on metadata tables

| Tables | `VERSION AS OF <id or tag>` |
|---|---|
| `files`, `data_files`, `delete_files`, `entries`, `manifests`, `partitions`, `position_deletes` | Layout at that snapshot |
| `history`, `snapshots`, `refs`, `metadata_log_entries` | Accepted, ignored |
| `all_*` | Rejected: `Cannot select snapshot in table: ALL_MANIFESTS` |

- A non-main branch name (`VERSION AS OF 'dev'`) fails on 1.8.1 with the misleading
  `Cannot find source column for partition field`; use the branch head's `snapshot_id` from `refs`.
- An expired snapshot id fails with `Cannot find snapshot with ID ...`.

## Semantics that break naive queries (measured on a 6-snapshot history, 2 expired)

| Fact | Consequence | Correct query |
|---|---|---|
| `history` = main's snapshot log (3 rows); `snapshots` = all refs (4 rows incl. a `dev` commit) | Audits built on `history JOIN snapshots` lose branch commits | `snapshots LEFT JOIN history LEFT JOIN refs` |
| `metadata_log_entries` = 11 files for 6 snapshots; tag, spec change, branch, expire also write `metadata.json` | `latest_snapshot_id` repeats or is NULL (3 rows) once its snapshot expired | Read `operation` as "state at this file" |
| `files` = 6 data + 2 delete files | Small-file query returns 8 instead of 6 | `WHERE content = 0` or `data_files` |
| `all_data_files` = 15 rows for 8 distinct files | Counts and sums inflated | `count(DISTINCT file_path)` |
| `all_entries JOIN snapshots` returns 7 of 18 | Entries of expired snapshots dropped | `LEFT JOIN` |
| v3: 2 DVs in 1 Puffin file, both rows report the whole file size | `COUNT(*)` = 2 files, `SUM(size)` doubled | `count(DISTINCT file_path)` |
| `all_manifests` = 14 rows for 8 manifest files | `reference_snapshot_id` is the referencing, not the creating snapshot | Creator = `added_snapshot_id` |
| `partitions.record_count` ignores position deletes (EU 20 vs 17 live) | Not a live row count | Query the table |
| `last_updated_at` NULL when the last-changing snapshot expired | "Stale partition" checks misfire | Treat NULL as unknown |
| `summary['total-records']` stays 50 after a 3-row MOR delete | Growth charts ignore deletes | Combine with delete counters |
| `LIKE '%parquet'` on `file_path` | Keeps v2 delete files, drops v3 Puffin DVs | Filter on `content` |

## Useful queries

```sql
-- Change audit across all refs
SELECT s.committed_at, s.snapshot_id, s.operation, h.snapshot_id IS NOT NULL AS in_main_history,
       r.refs, s.summary['spark.app.id'] AS app_id, s.summary['added-records'] AS added
FROM db.t.snapshots s
LEFT JOIN db.t.history h ON s.snapshot_id = h.snapshot_id
LEFT JOIN (SELECT snapshot_id, sort_array(collect_list(name)) AS refs
           FROM db.t.refs GROUP BY snapshot_id) r ON s.snapshot_id = r.snapshot_id
ORDER BY s.committed_at;

-- Rollbacks: abandoned snapshots stay until expired
SELECT * FROM db.t.history WHERE NOT is_current_ancestor;

-- Lifecycle of one file (added by an expired snapshot -> committed_at NULL)
SELECT e.snapshot_id, e.status, e.sequence_number, s.committed_at, s.operation
FROM db.t.all_entries e LEFT JOIN db.t.snapshots s ON e.snapshot_id = s.snapshot_id
WHERE e.data_file.file_path = 's3://.../data/region=APAC/00000-...parquet'
ORDER BY e.sequence_number, e.status;

-- Partition skew
SELECT partition, spec_id, record_count, file_count,
       position_delete_file_count + equality_delete_file_count AS delete_files,
       round(record_count / avg(record_count) OVER (), 2) AS skew_factor
FROM db.t.partitions ORDER BY record_count DESC;
```

`position_deletes.row` is NULL for Spark-written deletes. To learn which rows were deleted, read the
data file at `pos` (PyIceberg `table.io.new_input(path)` + PyArrow); positions resolve to the deleted ids.

## Gotchas
- Never name a table after a metadata table (`db.files` resolves to the `files` metadata table of `db`).
- Add job ids to snapshots with `snapshot-property.<key>` write options; `snapshots` has no user column.
- `history` has no operation or engine column; join `snapshots`.
- Export `snapshots` regularly for long-term audits: expiration removes rows from all four history tables.
- Delete manifests report counts in `*_delete_files_count`; data-only checks see them as empty.
- Freeze metadata schemas in a test (`DESCRIBE db.t.<name>`) so dashboards fail loudly after upgrades.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Branch commit missing from audit | Built on `history` | Start from `snapshots` |
| More files than exist | `all_*` duplicates, v3 vectors | `DISTINCT file_path` |
| Lineage rows disappear | Inner join on expired snapshot ids | `LEFT JOIN` |
| `Cannot find source column for partition field` | Branch name in `VERSION AS OF` | Use snapshot id from `refs` |
| Same region shown twice in `partitions` | Partition evolution: one row per spec | Group by `(partition, spec_id)` |
