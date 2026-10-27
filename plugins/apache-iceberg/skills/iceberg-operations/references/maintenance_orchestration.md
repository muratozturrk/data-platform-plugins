# Maintenance Orchestration

Scope: scheduling, gating and monitoring Iceberg maintenance procedures across many tables.

## Problem
Iceberg ships `rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots` and
`remove_orphan_files`, but it has no scheduler. If you run them by hand or with cron, they run too often
(wasted compute) or too rarely (silent degradation: more small files, longer planning, growing metadata).
One schedule for every table stops working as the table count grows. A CDC table may need compaction
several times an hour, while a monthly report table needs it once, after its load. And knowing that a
job *ran* tells you little. You also need to know whether it was needed, whether it worked, and whether it failed.

## Tool fit

| Tool | Fit |
|---|---|
| Apache Airflow | Multistep DAGs with dependencies, retries, alerting and branching. The common choice |
| dbt | Maintenance as `run-operation` macros right after the model that caused it. No full scheduling |
| Argo Workflows, Prefect, Dagster | Container-native. Keeps maintenance resources separate from ingestion and queries |
| Flink (in-pipeline) | Compaction and snapshot management inside the streaming job, so there are fewer writer/maintenance conflicts |
| cron / manual | Fine as a starting point. Does not scale to hundreds of tables |

## Mechanics: policy -> metrics -> plan -> act -> re-measure

```
table_policies.json --> DAG iceberg_table_maintenance (@daily, max_active_runs=1)
  per table TaskGroup:
    collect_metrics (files + snapshots metadata tables, no data scan)
      -> choose_actions (BranchPythonOperator)
           -> compact  (rewrite_data_files + rewrite_position_delete_files)
           -> expire   (expire_snapshots older_than + retain_last)   trigger_rule=none_failed
           -> nothing_to_do
      -> publish_metrics (re-measure, push to Pushgateway)  trigger_rule=none_failed_min_one_success
  on_failure_callback -> push iceberg_maintenance_failure=1
```

The metrics query reads only metadata:

```sql
SELECT count_if(content = 0)                                  AS data_files,
       count_if(content = 0 AND file_size_in_bytes < 20971520) AS small_files,   -- 20 MB
       count_if(content > 0)                                  AS delete_files,
       coalesce(sum(file_size_in_bytes), 0)                   AS total_bytes
FROM lake.sales.orders_cdc.files;

SELECT count(*) AS snapshots,
       (unix_timestamp(current_timestamp()) - unix_timestamp(min(committed_at))) / 3600.0 AS oldest_snapshot_age_hours
FROM lake.sales.orders_cdc.snapshots;
```

The pure planning rule is the only place where policy meets metrics, so you can unit test it without Spark:

```python
def plan(metrics, policy):
    actions = []
    if metrics["small_files"] > policy.max_small_files or metrics["delete_files"] > policy.max_delete_files:
        actions.append("compact")                         # compaction first, then expiration
    if (metrics["snapshots"] > policy.retain_last
            and (metrics["oldest_snapshot_age_hours"] > policy.max_snapshot_age_hours
                 or metrics["snapshots"] > policy.max_snapshots)):
        actions.append("expire")                          # never at or below retain_last
    return actions
```

The procedures:

```sql
CALL lake.system.rewrite_data_files(table => 'sales.orders_cdc', options => map(
  'target-file-size-bytes', '134217728', 'min-input-files', '2',
  'delete-file-threshold', '1', 'partial-progress.enabled', 'true'));
CALL lake.system.rewrite_position_delete_files(table => 'sales.orders_cdc', options => map('rewrite-all', 'true'));
CALL lake.system.expire_snapshots(table => 'sales.orders_cdc',
  older_than => TIMESTAMP '2025-03-01 00:00:00', retain_last => 50);
```

Per-table policy file (unspecified keys fall back to the `TablePolicy` defaults):

```json
{"sales.orders_cdc":     {"max_small_files": 500,  "max_delete_files": 20,  "retain_last": 50, "max_snapshot_age_hours": 24},
 "sales.monthly_report": {"max_small_files": 2000, "max_delete_files": 200, "retain_last": 12, "max_snapshot_age_hours": 2160}}
```

With the same metrics (800 small files, 120 snapshots, the oldest 30 h old), the CDC policy plans `compact` +
`expire` and the report policy plans nothing.

## Workflow qualities

| Quality | Meaning | How to get it |
|---|---|---|
| Idempotent | A rerun has the same effect as one run | Gate every action on fresh metrics. A second run plans nothing (`nothing_to_do` for both tables, identical files and current snapshot) |
| Transparent | One purpose per step, documented inputs and outputs | Policy file in, metrics dict through XCom, procedure results out |
| Observable | Success, failure, duration and side effects are visible | Metrics before and after, `maintenance_success` / `maintenance_failure` gauges, `files_removed_by_maintenance` |
| Isolated | One broken table does not block the others | One TaskGroup per table. A missing table fails its own `collect_metrics`, its `publish_metrics` is `upstream_failed`, and the other groups succeed (the DAG run is marked `failed`) |

Measured effect on a CDC table fed by 12 micro-batches: 48 data files became 1, 12 snapshots became 3
(`retain_last=3`), and the row/sum/hash fingerprint stayed the same. The Pushgateway held `iceberg_data_files=1`,
`iceberg_files_removed_by_maintenance=47` and `iceberg_maintenance_success=1`. The healthy table had
`compact` and `expire` skipped and got no new snapshot.

## Decision table

| Condition | Choice | Reason |
|---|---|---|
| Few tables, uniform load | Fixed schedule | The procedures are incremental and exit early when there is little to do |
| Many tables, different write patterns | Metadata-gated, per-table policies | Compute goes where fragmentation actually exists |
| Transformations already in dbt | `dbt run-operation` after the model | Cleanup stays next to the job that causes it |
| High-throughput streaming tables | Maintenance inside the Flink job | Avoids commit conflicts between the writer and external compaction |
| Retention must be visible to every engine | Table properties (`history.expire.max-snapshot-age-ms`, `history.expire.min-snapshots-to-keep`, `write.target-file-size-bytes`) | The policy lives with the table, and the orchestrator reads it |

## Gotchas

- **A `PythonOperator` return value gates nothing.** A check that returns `False` still leaves the
  next task in state `success`. Use `ShortCircuitOperator` (the next task becomes `skipped`) or `BranchPythonOperator`.
- **Branch joins need trigger rules.** Under the default `all_success`, a skipped branch also skips the
  join. Use `none_failed_min_one_success` on the join, and `none_failed` on `expire` when it runs after `compact`.
- **A fixed small-file size threshold never reaches zero.** On small tables, the single fully
  compacted file is still under 20 MB and counts as "small". Gate on a file *count* limit
  (e.g. more than 500), not on `small_files == 0`.
- **`max_active_runs=1`.** Two maintenance runs on the same table conflict with each other.
- **Order: compact -> expire -> (rarely) remove_orphan_files.** Compacting before expiring saves
  work but is not needed for correctness. Orphan cleanup is corrective, does an expensive listing, and should not run as a routine step.
- **Alert on patterns, not single values.** Examples: delete files that never drop after compaction, or
  planning time that rises together with snapshot count. If planning time rises while snapshot count stays flat, look at the engine configuration.
- **Monitor the orchestrator itself.** A skipped or failing DAG is a table health problem. A typical failure in
  `rewrite_data_files` is `OSError: [Errno 24] Too many open files`. Raise
  `http-client.apache.max-connections` (default 50) for procedures that open many files.
- **Airflow versions.** `schedule=` replaced `schedule_interval=` (2.4+). In Airflow 3, import `DAG`/`task`/`TaskGroup`
  from `airflow.sdk` and the operators from `airflow.providers.standard.operators.*`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Compaction runs although the check said "no" | The gate is a plain `PythonOperator` | `ShortCircuitOperator` or branch |
| `publish_metrics` skipped every time a branch is skipped | Default `all_success` trigger rule | `none_failed_min_one_success` |
| `expire` never runs when `compact` is skipped | `expire` sits downstream of `compact` with `all_success` | `trigger_rule=none_failed` |
| A table is compacted on every run | Gate is `small_files > 0` with a size threshold above the output size | Use a count limit relative to policy |
| Expiration removes nothing | `snapshots <= retain_last`, or a tag/branch pins the snapshots | Check `refs`, adjust `retain_last` |
| One missing table stops all maintenance | Tables share one task chain | One TaskGroup per table, plus a failure callback |
