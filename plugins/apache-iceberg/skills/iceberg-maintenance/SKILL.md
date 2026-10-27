---
name: iceberg-maintenance
description: "Maintain Apache Iceberg tables with Spark procedures and metadata tables: diagnose small files, delete-file buildup, poor colocation and metadata sprawl; compact with rewrite_data_files (binpack, sort, zorder, where, delete-file-threshold), rewrite_position_delete_files and rewrite_manifests; control storage with expire_snapshots (older_than, retain_last), remove_orphan_files, history.expire.* and write.metadata.* properties, tags/branches as retention anchors and GDPR erasure verified on storage; query snapshots, history, refs, manifests, partitions, files, entries, position_deletes and all_* tables; build health gauges and a metadata-driven maintenance planner. Use when an Iceberg table gets slow to plan or scan, storage or metadata.json keeps growing, MOR deletes pile up, deleted data must be physically erased, or you write SQL over Iceberg metadata tables."
---

# Iceberg Table Maintenance

An Iceberg table degrades with normal use. Streaming commits create small files, thin manifests and
a long snapshot list, and row-level changes create delete files. Compaction fixes the layout.
Snapshot expiration is the only thing that physically frees old files and keeps `metadata.json`
small. The metadata tables measure all of it, so maintenance can be driven by thresholds instead of
a fixed schedule. Everything here uses Spark SQL procedures (`CALL <catalog>.system.<proc>`) on Iceberg
1.8.x. The table properties are standard table properties, so any engine that writes Iceberg honours them.

## When to Use This Skill

- Query planning or scans get slower, file counts explode, or a point lookup opens almost every file.
- A MOR table (v2 delete files, v3 deletion vectors) gets slower to read after many UPDATE/DELETE/MERGE.
- You need to choose a compaction strategy (binpack, sort, z-order) and its options or scope.
- Storage, `metadata.json` size or snapshot count keep growing. You need to set up retention or orphan cleanup.
- A GDPR/CCPA erasure request must remove the bytes, with evidence for an audit.
- You are writing SQL over `snapshots`, `history`, `refs`, `manifests`, `partitions`, `files`, `entries`,
  `position_deletes` or `all_*` tables, or building health dashboards and a maintenance scheduler.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Merge small files, nothing else | `rewrite_data_files` `binpack` + `target-file-size-bytes` | [compaction.md](references/compaction.md) |
| Restore file skipping for 1 or 2-4 filter columns | `strategy => 'sort'`, `sort_order` / `zorder(a, b)` | [compaction.md](references/compaction.md) |
| Compact without conflicts with a streaming writer | `where` on closed partitions, partial progress | [compaction.md](references/compaction.md) |
| MOR deletes slow every read | `delete-file-threshold` + `rewrite_position_delete_files` | [compaction.md](references/compaction.md) |
| Many thin manifests | `rewrite_manifests`, `commit.manifest.*` properties | [compaction.md](references/compaction.md) |
| Bound history and free replaced files | `expire_snapshots` (`older_than`, `retain_last`), `history.expire.*` | [snapshot_expiry_orphans_retention.md](references/snapshot_expiry_orphans_retention.md) |
| Keep audit or backfill states | Tags / branches with `RETAIN`, `max-ref-age-ms` | [snapshot_expiry_orphans_retention.md](references/snapshot_expiry_orphans_retention.md) |
| Remove files of failed writes | `remove_orphan_files` (>= 24 h, `file_list_view`) | [snapshot_expiry_orphans_retention.md](references/snapshot_expiry_orphans_retention.md) |
| Physically erase a data subject | Delete, compact, expire, orphan cleanup, then a storage scan | [snapshot_expiry_orphans_retention.md](references/snapshot_expiry_orphans_retention.md) |
| Correct SQL over metadata tables | Semantics of each table, `DISTINCT file_path`, `LEFT JOIN` | [metadata_tables.md](references/metadata_tables.md) |
| Diagnose layout and decide what to run | Gauges + pure planner rules, fixed execution order | [table_health_monitoring.md](references/table_health_monitoring.md) |

## Topics

### Suboptimal data files
Small files, poor colocation, metadata sprawl and the MOR read penalty all come from normal writes.
- **Mechanic**: each commit writes a new `metadata.json`, a manifest list and usually a new manifest.
  Appends merge manifests only after `commit.manifest.min-count-to-merge` (100) manifests exist.
  20 micro-batch commits leave 80 files, all small, plus 20 snapshots and at least 20 manifests.
- **Diagnose** from metadata only. Use `files WHERE content = 0` for the small-file ratio, `manifests` for
  files per manifest, and `readable_metrics.<col>.lower_bound/upper_bound` for the files a lookup must
  open. Snapshot summaries (`added-delete-files`, `deleted-data-files`, `added-dvs`) show what a commit did.
- **Gotchas**: with v2 and `write.delete.granularity=partition`, delete files stack up (1, 2, 3, 4 per
  commit). The `file` default replaces them, and v3 keeps at most one DV per data file. COW is cheaper
  overall when more than about 10% of the rows in the affected files change.
- Ref: [references/table_health_monitoring.md](references/table_health_monitoring.md) · Code: [examples/table_maintenance/](examples/table_maintenance/) (`suboptimal_files/`)

### Compaction
`rewrite_data_files` merges small files, applies deletes and optionally re-sorts rows. `rewrite_manifests`
consolidates metadata.
- **Mechanic**: eligible files (size range, `where`, delete threshold) go into file groups per partition.
  Groups below `min-input-files` are skipped. The rest are rewritten in parallel and committed per group
  when `partial-progress.enabled` is on.
- **Measured**: binpack turns 96 files into 3 (one per day). A sort by `user_id` cuts the files a lookup
  opens from more than 90% to at most one per day. After 3 MOR MERGEs, compaction plus
  `rewrite_position_delete_files` leaves 0 live delete files.
- **Gotchas**: after deletes are applied, the old delete files stay as dangling deletes, so purge them.
  Use double quotes around `where` filters, because Spark SQL has no `''` escape. Compact closed
  partitions only. Compaction does not free storage. Expiration does.
- Ref: [references/compaction.md](references/compaction.md) · Code: [examples/table_maintenance/](examples/table_maintenance/) (`compaction/`)

### Storage footprint and retention
- **Mechanic**: `expire_snapshots` removes snapshots, plus the files that only those snapshots reference.
  Without arguments it applies `history.expire.max-snapshot-age-ms` (5 days) and
  `history.expire.min-snapshots-to-keep` (1). Tags and branches are never expired by age unless
  `history.expire.max-ref-age-ms` or the ref's own `RETAIN` says so.
- `write.metadata.delete-after-commit.enabled=true` with `write.metadata.previous-versions-max` (100)
  bounds old `metadata.json` files. That cap applies to `metadata_log_entries`, not to `history`.
- `remove_orphan_files` rejects `older_than` values less than 24 hours ago. On large S3 tables, feed it an
  inventory through `file_list_view`. It is not an erasure tool.
- **GDPR erasure**: report blocking tags, then `DELETE`, then compact with `delete-file-threshold=1` and
  purge deletes, then `expire_snapshots(older_than => now, retain_last => 1)`, then remove orphans. Finally,
  scan every Parquet object under `data/` for the subject. This works for both COW and MOR tables. A tag
  blocks compliance until someone approves dropping it.
- Ref: [references/snapshot_expiry_orphans_retention.md](references/snapshot_expiry_orphans_retention.md) · Code: [examples/table_maintenance/](examples/table_maintenance/) (`retention_gdpr/`)

### Metadata tables in depth
There are 16 tables: current-state tables (`files`, `data_files`, `delete_files`, `entries`, `manifests`,
`partitions`, `position_deletes`) and `all_*` tables (every snapshot still reachable from a ref), plus
`history`, `snapshots`, `metadata_log_entries` and `refs`.
- **Mechanic**: current-state tables support `VERSION AS OF <id|tag>`. Table-level views ignore it, and
  `all_*` tables reject it. A non-main branch name fails on 1.8.1, so use its snapshot id from `refs`.
- **Gotchas**: `files` includes delete files. `all_data_files` repeats files (15 rows for 8 files). v3
  DVs share one Puffin file. Use `count(DISTINCT file_path)` in all of these cases. `history` is main
  only. Ids of expired snapshots become NULL or drop out of inner joins, so use `LEFT JOIN`.
  `partitions.record_count` ignores deletes.
- Ref: [references/metadata_tables.md](references/metadata_tables.md) · Code: [examples/metadata_tables/](examples/metadata_tables/)

### Health monitoring and maintenance planner
- **Mechanic**: `collect_health()` reads `files`, `manifests`, `snapshots` and `refs` into a `TableHealth`.
  `plan_maintenance()` is a pure function that returns `compact_partition`, `rewrite_manifests`,
  `expire_snapshots` and `review_tag` actions. `execute()` runs them in a fixed order: compact, then
  manifests, then expire, then tag review.
- **Gauges**: small data-file ratio (> 10% below 128 MB), live and reachable delete files, snapshots
  per 24 h, files per manifest, metadata file count, tag age.
- **Gotchas**: skip the open partition. Never auto-drop refs. Reachable deletes that stay above 0 after
  expiration point at tags or branches. Judge the benefit from query plans, not from file sizes.
- Ref: [references/table_health_monitoring.md](references/table_health_monitoring.md) · Code: [examples/table_maintenance/](examples/table_maintenance/) (`maintenance_planner/`), [examples/metadata_tables/](examples/metadata_tables/) (`health_monitoring/`)

## Cross-cutting Best Practices

- Run maintenance in this order: `rewrite_data_files`, then `rewrite_position_delete_files`, then
  `rewrite_manifests`, then `expire_snapshots`, then `remove_orphan_files`. Expiring first keeps old
  files one cycle longer.
- Scope compaction to closed partitions and match its cadence to the write rate. Use partial progress
  for long jobs.
- Declare retention as table properties so that one scheduler job (`expire_snapshots(table)`) serves
  every table. For CDC tables, use time windows plus tags, not `retain_last` alone.
- Keep the retention window longer than your longest query and your time-travel SLA.
- Keep refs short-lived on regulated data. Flag old tags for review and never drop them silently.
- Prove compaction changes nothing, using `count`, `sum` and a row-hash fingerprint. Prove erasure on
  storage, not through a query.
- In metadata SQL: filter on `content`, count `DISTINCT file_path`, use `LEFT JOIN` on snapshot ids,
  and key partitions by `(partition, spec_id)`.
- Never name a table `files`, `manifests`, `snapshots`, and so on, because the name collides with the
  metadata-table syntax.
- Match the REST catalog version to the Iceberg runtime version, and raise the S3 HTTP pool size
  (`http-client.apache.max-connections`) for maintenance jobs.
