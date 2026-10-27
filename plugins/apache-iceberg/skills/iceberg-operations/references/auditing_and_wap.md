# Auditing and Write-Audit-Publish

Scope: snapshot audit trail, commit attribution, schema history, refs governance, OpenLineage, and WAP
with branches (`write.wap.enabled`, `spark.wap.branch`, `fast_forward`, `cherrypick_snapshot`).

## Problem
Many jobs write to shared tables. When a row count drops, you have to find the commit, the files and
the job behind it. Regulations (GDPR, CCPA) require proof that deletions, access limits and retention were
applied. And consumers of `main` should never see a batch that fails quality checks.

## Audit sources
| Source | Answers |
|---|---|
| `snapshots` (`summary`, `parent_id`, `operation`) | What changed, how many records, which commit |
| `files` / `manifests` with `VERSION AS OF` | The exact files a commit added or removed |
| `metadata.json` files (`metadata_log_entries.file`) | Schema and property changes |
| `refs` | Branches, tags and their retention |
| Catalog audit logs (Polaris, Nessie, Glue + CloudTrail) | Who called which metadata API |
| OpenLineage | Which job read and wrote which table version |

## Change tracking and attribution
Put identity into the snapshot at write time. The summary travels with the table to every engine and survives catalog moves:

```python
(df.writeTo("lake.sales.orders")
   .option("snapshot-property.audit.job-id", "orders_backfill")
   .option("snapshot-property.audit.run-id", run_id)
   .overwritePartitions())
```

```sql
SELECT snapshot_id, parent_id, committed_at, operation,
       cast(coalesce(summary['added-records'],   '0') AS BIGINT) AS added,
       cast(coalesce(summary['deleted-records'], '0') AS BIGINT) AS deleted,
       summary['audit.job-id'] AS job_id, summary['audit.run-id'] AS run_id
FROM lake.sales.orders.snapshots ORDER BY committed_at;
```

A lossy commit is an `overwrite` with `deleted > added`. In a history of append x3, overwrite and append, the
query finds exactly one lossy commit: deleted 1,000, added 960, job `orders_backfill`. A manual
`INSERT` without the write option shows `job_id = NULL`, which makes unattributed commits easy to flag.

**Drill down to files**: compare the `files` table at the parent snapshot with the same table at the child snapshot.

```sql
SELECT file_path FROM lake.sales.orders.files VERSION AS OF <parent_id>;   -- before
SELECT file_path FROM lake.sales.orders.files VERSION AS OF <snapshot_id>; -- after
-- removed = before - after, added = after - before
```

For the lossy backfill, the removed set equals exactly the files of that day's partition, and every added file is
under `order_ts_day=2025-03-01`.

**Schema history.** A schema-only change (`ADD COLUMN`) creates a new `metadata.json` but **no snapshot**.
`metadata_log_entries.latest_schema_id` reports the latest *snapshot's* schema (it still shows 0 after the change),
so read the schema history from the `metadata.json` files themselves:

```python
for r in spark.sql("SELECT timestamp, file FROM lake.sales.orders.metadata_log_entries ORDER BY timestamp").collect():
    meta = json.loads(read_object(r.file))         # current-schema-id, schemas[], current-snapshot-id
```

**Commit-rate baseline**: count commits per operation over 30 days (`count_if(operation = 'append')`, ...)
and alert when the counts deviate.

## Metadata table facts (Iceberg 1.8.1)
| Assumption | Reality |
|---|---|
| `snapshots.user_id` | Does not exist. The columns are exactly `committed_at, snapshot_id, parent_id, operation, manifest_list, summary` |
| `refs.ref_name` | The column is `name` (plus `type`, `snapshot_id`, `max_reference_age_in_ms`, ...) |
| `metadata_log_entries.operation` | Does not exist (AnalysisException). Its `file` column holds `metadata.json` paths, never data files |
| `committed_at` gives a strict order | It is writer-provided. Order audits by the `parent_id` lineage |

## Branches and tags
| Ref | Moves with commits | Typical use | Retention |
|---|---|---|---|
| `main` | yes | Production lineage | Never expires |
| Dev / audit branch | yes | QA, WAP, experiments | `RETAIN n DAYS` -> `max_reference_age_in_ms` (7 days = 604,800,000) |
| Tag | no | Release, month-end, audited restore point | Pins its snapshot until dropped or aged out |

```sql
ALTER TABLE lake.sales.orders CREATE BRANCH dev RETAIN 7 DAYS;
ALTER TABLE lake.sales.orders CREATE TAG release_2025_03;
SELECT name, type, snapshot_id, max_reference_age_in_ms FROM lake.sales.orders.refs;
```

Tags are read-only. `INSERT INTO t.tag_x` fails with `Cannot write to table at a specific snapshot`. Long-lived
tags keep data alive and block erasure, so let short-lived branches age out. Catalog branches (Nessie) move all
tables of a namespace together, which gives multi-table isolation.

## Write-Audit-Publish
```python
spark.sql("ALTER TABLE lake.sales.orders SET TBLPROPERTIES ('write.wap.enabled'='true')")
spark.sql("ALTER TABLE lake.sales.orders CREATE BRANCH IF NOT EXISTS `audit_r1` RETAIN 7 DAYS")
spark.conf.set("spark.wap.branch", "audit_r1")
try:
    batch.writeTo("lake.sales.orders").option("snapshot-property.audit.run-id", "r1").append()  # lands on the branch
finally:
    spark.conf.unset("spark.wap.branch")               # otherwise later writes AND reads use the branch
failures = quality_checks(spark, "lake.sales.orders.branch_audit_r1")   # nulls, amounts, duplicates
```

```sql
-- reject: the batch was never visible on main
ALTER TABLE lake.sales.orders DROP BRANCH `audit_r1`;
-- publish: main is still an ancestor of the branch
CALL lake.system.fast_forward('sales.orders', 'main', 'audit_r1');
-- publish fallback: main moved during the audit (fast_forward raises "... not an ancestor ...")
CALL lake.system.cherrypick_snapshot('sales.orders', <audit_r1 head snapshot id>);
-- restore point for disaster recovery, then clean up
ALTER TABLE lake.sales.orders CREATE TAG `audited_r1` AS OF VERSION <main head> RETAIN 30 DAYS;
ALTER TABLE lake.sales.orders DROP BRANCH `audit_r1`;
```

Measured outcomes:
- Good batch: `main` count stays at 1,000 while the batch is staged. After publishing it is 1,500, the method is
  `fast_forward`, refs = `main` + `audited_r1`, and the summary carries the run id.
- Bad batch: the failures are "10 orders without customer_id" and "5 duplicate order_id values". The `main`
  snapshot and count (1,000) are unchanged, and only `main` remains in refs.
- Concurrent writer on `main`: `fast_forward` is rejected, `cherrypick_snapshot` publishes, and the result is
  1,000 + 200 + 300 = 1,500 rows.

Run the checks on the **branch ref**, not on the batch DataFrame. Readers of the branch see exactly what `main` will
look like after publishing (duplicates against existing rows included).

## Lineage (openlineage-python 1.27)
Put the Iceberg snapshot id into the standard `DatasetVersionDatasetFacet`, and the OpenLineage `runId` into the
snapshot summary. Lineage and commits then join on ids, not timestamps.

```python
from openlineage.client.event_v2 import Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import dataset_version_dataset, source_code_location_job
facets = {"version": dataset_version_dataset.DatasetVersionDatasetFacet(datasetVersion=str(snapshot_id))}
job = Job(namespace="datalake-engine", name="daily_etl_job", facets={
    "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(type="git", url=repo_url)})
```

`from openlineage.client.facet import SourceCodeLocationRunFacet` raises `ImportError`. Source code location is a
*job* facet in `facet_v2`.

## Retention and secure deletion order
| Step | Procedure | Note |
|---|---|---|
| 1 | `DELETE` / `UPDATE` | COW rewrites files. MOR writes delete files or deletion vectors |
| 2 | `rewrite_data_files` | MOR only: materializes deletes into new base files |
| 3 | `expire_snapshots` | The main mechanism that physically frees files |
| 4 | `remove_orphan_files` | Corrective, normally a no-op, an expensive listing. Always after expiration |

Metadata proves only *logical* deletion. To prove physical deletion, run expiration (with no tag or branch pinning
the old snapshots) and check storage.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `user_id` / `ref_name` column not found | The metadata table schema differs | Use summary keys / `name` |
| Schema change missing from the snapshot history | Schema-only commits create no snapshot | Read the `metadata.json` files |
| Writes go to `main` despite WAP | `write.wap.enabled` not set, or `spark.wap.branch` set on another session | Set the property on the table and the conf on the writing session |
| Later queries read the audit branch | `spark.wap.branch` left set | `spark.conf.unset` in `finally` |
| `fast_forward` fails with "not an ancestor" | `main` moved during the audit | `cherrypick_snapshot` of the staged snapshot (works for appends) |
| Erased rows still on storage | A tag or branch pins old snapshots | Drop or age out the refs, then expire |
