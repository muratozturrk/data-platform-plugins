# Table health monitoring and the maintenance planner

Scope: diagnosing suboptimal data files from metadata tables, health gauges, and a rule-based planner
that decides compaction, manifest rewrite, expiration and ref review per table.

## Problem
Four layout problems build up even in a well-designed table:

| Problem | Cause | Symptom / metric |
|---|---|---|
| Small files | Streaming / micro-batch commits | High data-file count, small-file ratio near 1.0 |
| Poor colocation | Partitioning / sort order do not match filters; random arrival | A point lookup must open >90% of files |
| Metadata sprawl | One snapshot + manifest per commit | Many thin manifests, long snapshot list, many `metadata.json` versions |
| MOR read penalty | Delete files accumulate | Delete files per data file rising |

The cost of small files is the fixed per-file cost (object store calls, task scheduling, one metadata
entry per file), not bandwidth. Metadata growth never goes away by itself. 20 micro-batch commits of
2,000 rows with 4 files each produce 80 data files (100% below 32 MiB), 20 snapshots, >= 20 manifests
(below `commit.manifest.min-count-to-merge`) and >= 20 `metadata.json` versions.

A fixed nightly schedule wastes compute on healthy tables and reacts too late on hot ones. Metadata
tables let maintenance run on measured thresholds.

## Diagnosis from metadata only (no data scan)

```sql
-- Small files and delete files (data files only for the ratio)
SELECT count_if(content = 0)                                              AS data_files,
       count_if(content = 0 AND file_size_in_bytes < 33554432)            AS small_files,
       count_if(content = 1) AS pos_delete_files, count_if(content = 2) AS eq_delete_files
FROM ns.events.files;

-- Thin manifests (data manifests only; delete manifests use *_delete_files_count)
SELECT count(*) AS manifests,
       sum(added_data_files_count + existing_data_files_count) / count(*) AS files_per_manifest
FROM ns.events.manifests WHERE content = 0;

-- Colocation: files an engine must open for WHERE user_id = 42
SELECT count_if(readable_metrics.user_id.lower_bound <= 42
                AND readable_metrics.user_id.upper_bound >= 42) AS touched, count(*) AS total
FROM ns.events.data_files;

-- Per-partition health for a days(event_ts) table
SELECT partition.event_ts_day AS day, count_if(content = 0) AS data_files,
       count_if(content = 0 AND file_size_in_bytes < 33554432) AS small_files,
       count_if(content > 0) AS delete_files
FROM ns.events.files GROUP BY partition.event_ts_day;
```

Snapshot summaries record what a commit physically did: `added-data-files`, `deleted-data-files`,
`added-files-size`, `removed-files-size`, `added-delete-files`, `added-position-deletes`, `added-dvs`,
`removed-delete-files`. Use them to compare COW and MOR writes.

## Gauges

| Gauge | Source | Trigger | Caveat |
|---|---|---|---|
| Small data-file ratio | `files WHERE content = 0` | > 10% below 128 MB | Without `content = 0` delete files count as small (8/8 instead of 6/6) |
| Live delete files | `files`, `count(DISTINCT file_path)` where `content > 0` | > 0 on read-heavy tables | v3: count files, not vectors |
| Reachable delete files | `all_delete_files`, `DISTINCT file_path` | Still > 0 after expiration | Tags/branches keep them |
| Snapshots in 24 h | `snapshots.committed_at` | Above expected commit rate | Includes branch commits |
| Data manifests / files per manifest | `manifests` | Many manifests, few files each | Delete manifests separately |
| Metadata files | `metadata_log_entries` | Growing while data is stable | Capped by `previous-versions-max` |
| Tags / branches | `refs` | Old tags | Retention anchors; never auto-drop |

Small-file trigger restricted to data files (`examples/metadata_tables/common/metaquery.py`):

```python
r = spark.sql(f"""SELECT COUNT(*) AS total_files,
       SUM(CASE WHEN file_size_in_bytes < {small_bytes} THEN 1 ELSE 0 END) AS small_files
    FROM {table}.files WHERE content = 0""").first()
ratio = (r.small_files or 0) / r.total_files if r.total_files else 0.0
trigger = ratio > 0.10
```

Report **live and reachable** deletes side by side: live deletes cost read time (fix: compaction),
reachable ones cost storage (fix: expiration, then ref review). After compaction +
`rewrite_position_delete_files` + `expire_snapshots(retain_last => 1)` a table showed 0 live but 2
reachable delete files and 3 snapshots, because tag `audit_s4` and branch `dev` pinned them. After
dropping both refs and expiring again: 0 / 0, 1 snapshot, row count intact.

## Planner rules

```python
for p in health.partitions:
    if p.day >= current_day:
        continue                                   # open partition: writer still active
    if p.small_files >= t.min_small_files_per_partition or \
       (p.delete_files / p.data_files if p.data_files else 0) > t.max_delete_files_per_data_file:
        actions.append(Action("compact_partition", ..., {"where": day_filter, "with_deletes": p.delete_files > 0}))
if health.manifests >= t.min_manifests_to_rewrite and health.avg_files_per_manifest < t.min_avg_files_per_manifest:
    actions.append(Action("rewrite_manifests", ...))
if health.oldest_snapshot_at < now - t.snapshot_retention and health.snapshots > t.min_snapshots_to_keep:
    actions.append(Action("expire_snapshots", ..., {"older_than": cutoff, "retain_last": t.min_snapshots_to_keep}))
for name, committed_at in health.tag_snapshot_at.items():
    if now - committed_at > t.max_tag_age:
        actions.append(Action("review_tag", ...))  # flag, never drop
```

Default thresholds: 10 small files per partition, 0.2 delete files per data file, >= 10 manifests with
< 20 files each, 7-day snapshot retention keeping 10 snapshots, 90-day tag age, 128 MiB target.

Execution order is fixed: **data compaction -> manifest rewrite -> expiration -> tag review**. Expiring
first keeps pre-compaction files alive one cycle longer. End to end on 3 daily partitions with refunds
and a tag: the plan has 2 partition compactions, a manifest rewrite, an expiration and a tag review;
afterwards each closed day has 1 file and 0 deletes, the open day is untouched, only the tagged
pre-cutoff snapshot remains, content is unchanged and a second run plans no compaction.

## Approaches

| Approach | Use when | Trade-off |
|---|---|---|
| Fixed schedule for all tables | Few tables, uniform workloads | Wastes compute, too slow for hot tables |
| Metadata-driven planner | Many tables with different write patterns | Thresholds per table class |
| Managed optimizer (Dremio, AWS Glue, Apache Amoro) | Want it as a service | Less control over rules and timing |

Keep the planner pure (no Spark) so rules are unit-testable and run in a cheap scheduler task
(Airflow sensor/branch); keep SQL in one library returning plain Python values for tests, dashboards
(`as_metrics()` gauges for Prometheus/CloudWatch/Datadog) and the scheduler.

## Gotchas
- Monitor trends, not single values. Warning signs: delete artifacts that only grow, snapshot counts
  growing faster than expected, metadata growing while data volume is stable. Healthy tables show a
  sawtooth between maintenance runs.
- File sizes only show convergence to the target; whether queries benefit is visible in query plans.
- At small scale even compacted files are "small": use relative thresholds or file counts.
- Snapshots created by a run's own compactions survive until the next run.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| 100% small files on a MOR table | Delete files counted | `content = 0` |
| Counts only grow | Schedule slower than write rate | Raise cadence; check failures |
| Reachable deletes stay > 0 | Refs pin snapshots | Review tags/branches, then expire |
| Planner compacts today's partition | Missing open-partition guard | Skip `day >= current_day` |
| Expiration frees nothing after compaction | Ran before compaction | Fixed order: compact, manifests, expire |
