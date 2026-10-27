# Snapshot expiry, orphan files, retention and GDPR erasure

Scope: storage footprint and history length: `expire_snapshots`, `remove_orphan_files`, retention
table properties, tags/branches as retention anchors, and erasure that actually removes bytes.

## Problem
Time travel keeps every replaced or deleted data file alive. Every commit rewrites `metadata.json`
with the full snapshot list, so without expiration it can grow to hundreds of MB and stall ingestion
even when data files are well compacted. `DELETE` is logical: the rows stay on storage in old
snapshots, MOR base files and tagged states. Snapshot expiration is the only operation that
physically deletes data files and trims the snapshot list.

## Mechanisms

| Mechanism | Removes | Controlled by |
|---|---|---|
| `system.expire_snapshots` | Old snapshots + files referenced only by them | `older_than`, `retain_last`, `snapshot_ids`, or table properties |
| Tags / branches | Nothing: they pin snapshots | `RETAIN n DAYS`, `WITH SNAPSHOT RETENTION`, `history.expire.max-ref-age-ms` |
| `system.remove_orphan_files` | Files under the table location that no snapshot references | `older_than` (>= 24 h ago), `location`, `dry_run`, `file_list_view` |
| `system.rewrite_position_delete_files` | Dangling position deletes | `rewrite-all` |
| `write.metadata.delete-after-commit.enabled` | Old `metadata.json` files beyond the cap | `write.metadata.previous-versions-max` |

## Table properties

| Property | Default | Meaning |
|---|---|---|
| `history.expire.max-snapshot-age-ms` | 432000000 (5 days) | Default age limit when `expire_snapshots` gets no `older_than` |
| `history.expire.min-snapshots-to-keep` | 1 | Default `retain_last` |
| `history.expire.max-ref-age-ms` | never | Age after which non-main refs are removed by expiration |
| `write.metadata.delete-after-commit.enabled` | false | Delete the oldest tracked `metadata.json` after each commit |
| `write.metadata.previous-versions-max` | 100 | `metadata.json` files kept in the metadata log (`metadata_log_entries`) |

The 100-entry cap applies to the metadata log, not to `history`: with `previous-versions-max=3`
and 6 appends, `history` has 6 rows, `snapshots` 6, `metadata_log_entries` 4. `history` shrinks only
through snapshot expiration.

```sql
ALTER TABLE ns.events SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms'   = '604800000',   -- 7 days
  'history.expire.min-snapshots-to-keep' = '10',
  'history.expire.max-ref-age-ms'        = '7776000000',  -- 90 days
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '50');

CALL lake.system.expire_snapshots(table => 'ns.events');   -- applies the table's own policy
CALL lake.system.expire_snapshots(table => 'ns.events',
  older_than => TIMESTAMP '2025-01-05 00:00:00', retain_last => 10);
CALL lake.system.expire_snapshots(table => 'ns.events', snapshot_ids => array(123, 456));
```

Result columns: `deleted_data_files_count`, `deleted_position_delete_files_count`,
`deleted_equality_delete_files_count`, `deleted_manifest_files_count`, `deleted_manifest_lists_count`.

Measured: 10 append snapshots, `retain_last => 3` -> 3 remain, 7 manifest lists deleted, 0 data files
deleted (every file still live). With `min-snapshots-to-keep=2` and a 1 ms max age, a no-argument call
leaves 2 snapshots. After a COW `DELETE`, the old rows stay visible via `VERSION AS OF` and on storage
until expiration; afterwards storage is clean and time travel to that snapshot fails.

## Tags and branches

```sql
ALTER TABLE ns.events CREATE TAG `q4_audit` AS OF VERSION 8765 RETAIN 365 DAYS;
ALTER TABLE ns.events REPLACE BRANCH dev AS OF VERSION 4321 WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
ALTER TABLE ns.events DROP TAG IF EXISTS `month_end_close`;
```

- A tagged snapshot survives `retain_last => 1` and stays queryable (`VERSION AS OF 'q4_audit'`).
- `RETAIN 7 DAYS` sets `refs.max_reference_age_in_ms` = 604,800,000; NULL means never.
- `REPLACE BRANCH ... WITH SNAPSHOT RETENTION` **without** `AS OF VERSION` also moves the branch to
  main's head, so the branch's own commit loses protection. Always pass `AS OF VERSION <id>`.
- Tag after major compactions so preserved states point to optimized layouts.

## Orphan files
Leftovers of failed or aborted writes that no snapshot references.

```sql
CALL lake.system.remove_orphan_files(table => 'ns.events',
  older_than => TIMESTAMP '2025-01-09 00:00:00', dry_run => true);
-- Large S3 tables: pass a listing (e.g. from S3 Inventory) instead of a recursive list
CALL lake.system.remove_orphan_files(table => 'ns.events',
  older_than => TIMESTAMP '2025-01-09 00:00:00', file_list_view => 'orphan_candidates');
```

- `file_list_view` is a view with `file_path STRING, last_modified TIMESTAMP`.
- `older_than` less than 24 hours ago is rejected ("less than 24 hours"): younger files may belong to
  an in-flight write. An injected aborted-task file aged 5 days is the only file removed; row count unchanged.
- Orphan cleanup is not an erasure tool: expiration is what deletes data files of old snapshots.

## GDPR / CCPA erasure that removes bytes

1. Find tags (and branches) whose snapshot still contains the subject; report them, drop only after approval.
2. `DELETE FROM t WHERE user_id = 42` (COW rewrites files, MOR writes delete files).
3. `rewrite_data_files(delete-file-threshold=1, min-input-files=1)` + `rewrite_position_delete_files(rewrite-all)`.
4. `expire_snapshots(older_than => now, retain_last => 1)`: deletes the old files.
5. `remove_orphan_files(older_than => now - 3 days)` for stray files of failed writes.
6. Evidence: list every Parquet object under `<location>/data/`, read the subject column, require zero hits.

```python
for uri, _ in list_objects(table_location(spark, table), "data/"):
    if not uri.endswith(".parquet"):
        continue                                     # Puffin deletion vectors
    pf = pq.ParquetFile(io.BytesIO(read_object(uri)))
    if column in pf.schema_arrow.names and value in pf.read(columns=[column]).column(column).to_pylist():
        hits.append(uri)
```

Both COW and MOR tables end compliant with no residual files, other users' data unchanged and 1 snapshot
left. With a `month_end_close` tag the job reports non-compliance and residual files; after the approved
tag drop it is compliant.

## Retention by workload

| Workload | Approach | Reason |
|---|---|---|
| Batch / rewrite-heavy | Expire daily | History grows slowly |
| CDC / thousands of commits per day | Time window (e.g. 2 h) + tags for key states | `retain_last` covers minutes only |
| Audit / backfill states | Tag the snapshot | Precise protection |
| Regulated personal data | Short-lived refs, `history.expire.max-ref-age-ms` | Refs block erasure |

## Gotchas
- No query may run longer than the retention window of its starting snapshot.
- Expiration alone does not shrink a MOR table: compaction must rewrite base files first.
- COW frees replaced files as soon as referencing snapshots expire; MOR only when no retained snapshot
  references the base file or its deletes.
- Expiring before compaction keeps pre-compaction files alive one cycle longer.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Deleted user still on storage after expiry | Tag/branch pins old snapshot; MOR deletes never materialized | Review refs; compact with `delete-file-threshold=1`; expire again |
| `deleted_data_files_count = 0` | Append-only history: all files still live | Expected |
| `metadata.json` keeps growing | No expiration; huge snapshot list | Schedule `expire_snapshots`; set retention properties |
| Old `*.metadata.json` pile up | `delete-after-commit` off | Enable it, set `previous-versions-max` |
| Orphan procedure error "less than 24 hours" | Safety guard | Use `older_than` >= 24 h (3 days is common) |
| Time travel "Cannot find snapshot" | Snapshot expired | Tag states needed for audits before expiry |
