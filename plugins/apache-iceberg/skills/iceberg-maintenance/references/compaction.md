# Compaction: rewrite_data_files, rewrite_position_delete_files, rewrite_manifests

Scope: data-file and metadata compaction with Iceberg's Spark procedures.

## Problem
Normal writes degrade layout: streaming/micro-batch commits leave many small files and one thin
manifest per commit; MOR updates add delete files every read must merge; random arrival order spreads
each key over every file so min/max pruning stops working. Compaction is the one operation that
reverses all of these. It does **not** shrink `metadata.json` or free storage: it adds a snapshot, and
the replaced files stay on storage until `expire_snapshots` removes the snapshots that reference them.

## Procedures

| Procedure | Rewrites | Key arguments / options |
|---|---|---|
| `system.rewrite_data_files` | Data files (+ applies deletes) | `table`, `strategy` (`binpack` default, `sort`), `sort_order` (`'col ASC NULLS LAST'` or `'zorder(a, b)'`), `where`, `options` map |
| `system.rewrite_position_delete_files` | Position delete files | `table`, `where`, `options` (`rewrite-all`, size thresholds); drops dangling deletes |
| `system.rewrite_manifests` | Manifests only (metadata) | `table`, `use_caching`, `spec_id` |

Main `rewrite_data_files` options:

| Option | Default | Use |
|---|---|---|
| `target-file-size-bytes` | `write.target-file-size-bytes` (512 MB) | Output size goal; guides, does not enforce exactly. 128-256 MB is common on memory-limited clusters |
| `min-file-size-bytes` / `max-file-size-bytes` | 75% / 180% of target | Files outside the range are candidates. Narrow range = thorough, long jobs; wide = cheap outlier-only jobs |
| `min-input-files` | 5 | Skip file groups with fewer files (not worth a commit) |
| `rewrite-all` | false | Rewrite every file regardless of size (needed to re-sort an already "big enough" layout) |
| `delete-file-threshold` | unlimited | Rewrite a data file once it has >= N delete files (`1` = materialize all MOR deletes) |
| `remove-dangling-deletes` | false | Drop delete files that no longer apply to any live data file |
| `partial-progress.enabled` / `partial-progress.max-commits` | false / 10 | Commit file groups independently so a late failure keeps finished work |
| `max-concurrent-file-group-rewrites` | 5 | Parallel file groups |
| `max-file-group-size-bytes` | 100 GB | Upper bound of one rewrite unit |
| `rewrite-job-order` | `none` | `bytes-asc`, `bytes-desc`, `files-asc`, `files-desc` |

Result columns: `rewritten_data_files_count`, `added_data_files_count`, `rewritten_bytes_count`,
`failed_data_files_count`, `removed_delete_files_count`.

## Mechanics
1. Enumerate eligible files (size thresholds, `where` filter, delete-file threshold).
2. Group them per partition into file groups (`max-file-group-size-bytes`).
3. Skip groups below `min-input-files`.
4. Rewrite groups in parallel (bin-pack or sort/z-order).
5. Commit per group (partial progress) or once at the end.
6. Old files remain referenced by older snapshots until expiration.

## Strategy selection

| Condition | Choice | Reason |
|---|---|---|
| Many small files, no dominant filter | `binpack` | Cheapest; fixes file count only |
| Frequent filters on one column | `sort` by that column | Restores min/max file skipping |
| Filters on 2-4 columns | `sort` with `zorder(a, b)` | Multi-dimensional pruning; measure cost first |
| Hot partitions, tight SLA window | `where` on closed partitions | Rewrite only what is needed, no writer conflicts |
| MOR table with deletes | `delete-file-threshold` + `rewrite_position_delete_files` | Restores read performance |
| Many thin manifests | `rewrite_manifests` | Metadata-only, fast |

Measured on 3 daily partitions x 8 micro-batches x 4 files (96 files): binpack -> 3 files (one per
day), content fingerprint unchanged. Sort by `user_id` (64 KiB target, `rewrite-all`) drops a lookup
for one user from >90% of files to at most 3 files (one per day) and under 50%. A clustered rewrite
of the same rows (8 files, range-partitioned by `user_id`) leaves the key in at most 2 of 8 files.

## SQL

```sql
-- Sort compaction of closed partitions only (double quotes: Spark SQL has no '' escape)
CALL lake.system.rewrite_data_files(
  table => 'ns.events', strategy => 'sort', sort_order => 'user_id ASC NULLS LAST',
  where => "event_ts < TIMESTAMP '2025-01-12 00:00:00'",
  options => map('target-file-size-bytes', '134217728', 'min-input-files', '5',
                 'partial-progress.enabled', 'true', 'max-concurrent-file-group-rewrites', '4',
                 'rewrite-job-order', 'bytes-asc'));

-- Z-order on two filter columns
CALL lake.system.rewrite_data_files(table => 'db.sales', strategy => 'sort',
  sort_order => 'zorder(customer_id, product_id)');

-- MOR cleanup: materialize deletes, then purge dangling delete files
CALL lake.system.rewrite_data_files(table => 'ns.events',
  options => map('delete-file-threshold', '1', 'min-input-files', '1'));
CALL lake.system.rewrite_position_delete_files(table => 'ns.events',
  options => map('rewrite-all', 'true'));

-- Metadata compaction
CALL lake.system.rewrite_manifests(table => 'ns.events');
```

Policy as data (see `examples/table_maintenance/compaction/compaction.py`):

```python
policy = CompactionPolicy(strategy="sort", sort_order="zorder(customer_id, product_id)",
                          where="region = 'us-west'", delete_file_threshold=3,
                          target_file_size_bytes=256 * MiB)
spark.sql(rewrite_data_files_sql("db.sales", policy))
```

## MOR deletes and delete files
- COW rewrites affected files on every UPDATE/DELETE/MERGE (expensive writes, clean reads); MOR writes
  position/equality delete files (cheap writes, reads merge). Inflection point: when an operation
  changes more than ~10% of rows in the affected files, COW is cheaper overall.
- Same sparse refund MERGE: COW has `deleted-data-files > 0` and `added-delete-files = 0`; MOR has
  `deleted-data-files = 0`, `added-position-deletes` = rows changed and a smaller `added-files-size`.
- v2 with `write.delete.granularity=partition`: live delete files grow 1, 2, 3, 4 over four commits.
  With `file` granularity (default since 1.8) each commit replaces the data file's previous delete
  file, so live deletes stay bounded. v3 deletion vectors: at most one DV per data file.
- After `rewrite_data_files` applies deletes, the old position delete files are still tracked as
  dangling. On a 3-MERGE MOR table, `rewrite_position_delete_files(rewrite-all)` purged all of them,
  leaving 3 data files and 0 live delete files.

## Manifest merging properties

| Property | Default | Effect |
|---|---|---|
| `commit.manifest-merge.enabled` | true | Appends merge small manifests on commit |
| `commit.manifest.min-count-to-merge` | 100 | Merge only once this many manifests exist (20 commits leave >= 20 manifests) |
| `commit.manifest.target-size-bytes` | 8 MB | Target manifest size for merging and `rewrite_manifests` |

On 24 micro-batch commits, `rewrite_manifests` rewrites all >= 24 manifests into fewer ones, raises
files per manifest and leaves the data file set unchanged.

## Gotchas
- Compact **closed** partitions (yesterday and older). Rewriting a partition the stream appends to
  causes commit conflicts. Scoped by `event_ts < today`, the open day keeps its identical file set.
- Match frequency to write cadence; every 5 minutes only makes sense for commits every few seconds.
- Partition evolution and new sort orders apply to new data only; compaction applies them to old data.
- Z-order and clustering are workload dependent: verify gains in query plans, not in file sizes.
- Always fingerprint (`count`, `sum`, row-hash sum) before/after in tests: compaction never changes content.
- A table named `manifests`/`files`/`snapshots` collides with metadata-table name resolution.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `CommitFailedException` during compaction | Writer still appends to the partition | `where` on closed partitions; retry; partial progress |
| Nothing rewritten | Groups below `min-input-files`, files inside size range | Lower `min-input-files`, `rewrite-all` |
| Delete files remain after compaction | Dangling position deletes still tracked | `rewrite_position_delete_files` or `remove-dangling-deletes` |
| Storage grows after compaction | Old files pinned by snapshots | `expire_snapshots`; check tags/branches |
| Parse error in `where` | `''` escape is not valid Spark SQL | Wrap the filter in double quotes |
| S3 connection pool timeout | Many files opened in parallel | Raise `http-client.apache.max-connections` |
