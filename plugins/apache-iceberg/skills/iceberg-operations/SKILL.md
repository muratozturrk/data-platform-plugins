---
name: iceberg-operations
description: "Operating Apache Iceberg lakehouses in production: policy-driven maintenance orchestration with Airflow DAGs (metadata-gated compaction/expiration, per-table policies, Pushgateway metrics, alerting), auditing (snapshot summaries, snapshot-property job attribution, file diffs, schema history, refs, OpenLineage), Write-Audit-Publish with branches (write.wap.enabled, spark.wap.branch, fast_forward, cherrypick_snapshot, audited tags), and disaster recovery (catalog export + register_table, rollback_to_snapshot and purge by snapshot_ids, validation against storage, rewrite_table_path cross-region replication, replication lag, split-brain, RPO/RTO). Use when scheduling Iceberg maintenance, building audit trails, gating publishes with WAP, or planning and drilling recovery."
---

# Iceberg Operations

Iceberg gives you the procedures and the metadata, but it does not run them for you. This skill covers
the operational layer around a table. **Orchestration** turns maintenance procedures into idempotent,
observable, per-table workflows that run only where metadata says they are needed. **Auditing** makes the
table its own ledger: snapshot summaries, file diffs, refs, lineage, and Write-Audit-Publish so that bad data
never reaches `main`. **Disaster recovery** protects the three layers (catalog, storage, table state), and each of them fails on its own.

All facts below were measured on Spark 3.5.5 + Iceberg 1.8.1 + Airflow 2.10.5 with a REST catalog.

## When to Use This Skill

- Scheduling compaction, delete-file rewrites and snapshot expiration across many tables with different write patterns.
- Writing Airflow DAGs for Iceberg maintenance: branching, trigger rules, idempotency, metrics and failure alerts.
- Explaining a row-count change: finding the commit, the job and the exact files behind it.
- Attributing commits to pipelines, tracking schema changes, governing branches and tags.
- Publishing only validated batches (WAP with branches, fast-forward or cherry-pick).
- Backing up the catalog, re-registering tables, rolling back bad writes, replicating to another region.
- Defining RPO/RTO, recovery procedures and drills, or validating that a restored table is actually readable.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Maintain hundreds of tables without wasting compute | Per-table policy file + metadata-gated branch DAG | [maintenance_orchestration.md](references/maintenance_orchestration.md) |
| Skip maintenance on healthy tables | `files`/`snapshots` metrics -> pure `plan()` -> `BranchPythonOperator` / `ShortCircuitOperator` | [maintenance_orchestration.md](references/maintenance_orchestration.md) |
| Prove maintenance worked | Re-measure after, push gauges (Pushgateway), alert in `on_failure_callback` | [maintenance_orchestration.md](references/maintenance_orchestration.md) |
| Find the commit that dropped rows | `snapshots.summary` change log + `files VERSION AS OF` diff | [auditing_and_wap.md](references/auditing_and_wap.md) |
| Know which job wrote a commit | `snapshot-property.<key>` write option | [auditing_and_wap.md](references/auditing_and_wap.md) |
| Track schema changes | Read the `metadata.json` files (schema changes create no snapshot) | [auditing_and_wap.md](references/auditing_and_wap.md) |
| Keep bad data from consumers | WAP: branch + `spark.wap.branch`, checks on branch, `fast_forward` / `cherrypick_snapshot`, tag | [auditing_and_wap.md](references/auditing_and_wap.md) |
| Join lineage to commits | OpenLineage `DatasetVersionDatasetFacet` = snapshot id, run id in summary | [auditing_and_wap.md](references/auditing_and_wap.md) |
| Survive catalog loss | Scheduled REST export of name -> `metadata-location`, `register_table` | [disaster_recovery.md](references/disaster_recovery.md) |
| Undo a bad write | Newest `audited_*` tag -> `rollback_to_snapshot` -> expire by `snapshot_ids` -> validate | [disaster_recovery.md](references/disaster_recovery.md) |
| Usable copy in another region | `rewrite_table_path` + copy plan + `register_table`, track replication lag | [disaster_recovery.md](references/disaster_recovery.md) |
| Prove a restore is readable | HEAD every manifest list, manifest and data file | [disaster_recovery.md](references/disaster_recovery.md) |

## Maintenance Orchestration

Flow: policy file -> `collect_metrics` (metadata tables only) -> `choose_actions` (branch) -> `compact` ->
`expire` -> `publish_metrics` (re-measure), with one TaskGroup per table and `max_active_runs=1`.
- **Mechanic**: `rewrite_data_files` + `rewrite_position_delete_files`, then `expire_snapshots(older_than, retain_last)`.
  Gates are file-count and snapshot-count/age limits from the per-table policy. On a CDC table this took 48 data files
  to 1 and 12 snapshots to 3, with the fingerprint unchanged. A second run plans nothing.
- **Gotchas**: a `PythonOperator` returning `False` does **not** gate anything (the next task still succeeds).
  Use `ShortCircuitOperator` or a branch. Give branch joins `none_failed_min_one_success`, and give `expire` after
  `compact` `none_failed`. A size-based "small file" count never reaches 0 on small tables, so gate on count limits. A
  missing table must fail only its own group.
- **Airflow**: use `schedule=`, not `schedule_interval=`. For Airflow 3, import from `airflow.sdk` and
  `airflow.providers.standard.operators.*`.
- Ref: [references/maintenance_orchestration.md](references/maintenance_orchestration.md) · Code: [examples/lakehouse_operations/](examples/lakehouse_operations/) (`maintenance_orchestration/`, `dags/iceberg_maintenance.py`)

## Auditing

Every commit is a snapshot with `operation`, `parent_id` and a `summary` map (`added-records`, `deleted-records`, custom keys).
- **Mechanic**: put job and run ids in the summary with `.option("snapshot-property.audit.job-id", ...)`. A lossy
  commit is an `overwrite` with `deleted > added`. Compare `files VERSION AS OF parent` with `files VERSION AS OF child`
  to list the files it touched. A commit with no job id is a manual or misconfigured write.
- **Gotchas**: `snapshots` has no `user_id` column (it has exactly six columns), and `refs` uses `name`, not
  `ref_name`. `metadata_log_entries` has no `operation` column and lists only `metadata.json` paths. `ADD COLUMN` creates no
  snapshot, and `latest_schema_id` lags behind it. `committed_at` is writer-provided, so order by lineage instead. Tags are read-only.
- Ref: [references/auditing_and_wap.md](references/auditing_and_wap.md) · Code: [examples/lakehouse_operations/](examples/lakehouse_operations/) (`auditing_and_wap/audit.py`, `lineage.py`)

## Write-Audit-Publish

Write the batch to an audit branch, check the branch, then publish to `main` or drop the branch.
- **Mechanic**: set `write.wap.enabled=true` on the table, `CREATE BRANCH ... RETAIN 7 DAYS`, and
  `spark.wap.branch=<branch>` on the session. Run the checks on `table.branch_<b>`. Publish with
  `CALL system.fast_forward('t', 'main', '<b>')`. If `main` moved (the error says "not an ancestor"), use
  `cherrypick_snapshot('t', <staged id>)`. Then `CREATE TAG audited_<run> AS OF VERSION <id> RETAIN 30 DAYS`.
- **Measured**: a good batch stays invisible while staged (1,000 rows on `main`), then shows 1,500 after the fast-forward. A
  bad batch (10 null customer ids, 5 duplicate ids) leaves `main` untouched. When a concurrent writer moved `main`,
  cherry-pick published 1,000 + 200 + 300 rows.
- **Gotchas**: unset `spark.wap.branch` in `finally`, because it also redirects later reads and writes. Every published snapshot gets
  a tag, which becomes the restore point for recovery. Long-lived tags block erasure.
- Ref: [references/auditing_and_wap.md](references/auditing_and_wap.md) · Code: [examples/lakehouse_operations/](examples/lakehouse_operations/) (`auditing_and_wap/wap.py`)

## Disaster Recovery

Three layers fail on their own: the catalog (mapping), the storage (files) and the table state (pointer).
- **Catalog**: export name -> `metadata-location` through the REST API on a schedule, store it in the DR bucket, and restore missing
  tables with `CALL system.register_table(table => ..., metadata_file => ...)`. This is idempotent, and history and tags are kept.
  No `set_current_snapshot` is needed afterwards. With no backup, the highest-numbered `metadata.json` is the last resort.
- **Table state**: `rollback_to_snapshot` is metadata-only. Newer snapshots and their files stay, and so does the schema.
  Purge the abandoned snapshots with `expire_snapshots(snapshot_ids => array(...))`, because an `older_than` cutoff never
  reaches them. Then validate with HEAD requests, since `count(*)` succeeds from manifest stats even with a missing file.
- **Region**: a bucket copy keeps absolute `s3://primary/...` paths. `rewrite_table_path(table, source_prefix,
  target_prefix, staging_location)` returns `latest_version` and a `file_list_location` CSV. Copy the files, then register.
  It needs Hadoop S3A on the classpath. Snapshot ids are preserved, so lag = primary ids missing in the replica.
- **Split-brain**: registering the same metadata writable twice let the secondary's expiration delete all of the primary's data files.
- Ref: [references/disaster_recovery.md](references/disaster_recovery.md) · Code: [examples/lakehouse_operations/](examples/lakehouse_operations/) (`disaster_recovery/`, `dags/iceberg_recovery.py`)

## Cross-cutting Best Practices

- Gate every destructive or expensive action on fresh metadata, and make every task idempotent.
- Keep the policy (thresholds, retention) in one place: a policy file, or table properties so every engine sees it.
- Order maintenance as compact -> expire -> (rarely) remove orphan files, and never expire ahead of replication lag.
- Write identity (job, run, OpenLineage run id) into the snapshot summary at commit time.
- Publish through WAP and tag what passed. Use rollback targets based on data quality, not on time.
- Back up the catalog mapping often (per the table's RPO), and keep exports in another region.
- Validate restores against storage, test them with the exact engine versions, and drill quarterly (partial and full).
- Restrict delete permissions to the maintenance service, and enable object versioning or object lock.
