# Lakehouse operations examples

One local lakehouse covers three test suites: maintenance orchestration, auditing with
Write-Audit-Publish, and disaster recovery drills. The stack is Spark with Airflow running
in-process, an Iceberg REST catalog, SeaweedFS (S3 API, buckets `warehouse` = primary region and
`warehouse-dr` = DR region) and a Prometheus Pushgateway. All 29 tests pass on Spark 3.5.5, Iceberg 1.8.1
and Airflow 2.10.5, and the full run takes about 3 minutes.

| File | Shows |
|---|---|
| `common/lakehouse.py` | SparkSession for the REST catalog with `S3FileIO`, plus Hadoop S3A for `rewrite_table_path`. Also S3 helpers and a `drop_table` fallback through REST `DELETE` |
| `common/sales.py` | Deterministic sales-order generator (`days(order_ts)` partitioning), micro-batch appends and a table fingerprint |
| `conftest.py`, `pytest.ini` | A session-scoped Spark fixture and one namespace per test module |
| `maintenance_orchestration/maintenance.py` | `TablePolicy`, `collect_metrics` (from the `files` and `snapshots` metadata tables), a pure `plan()`, `compact` (`rewrite_data_files` + `rewrite_position_delete_files`), `expire` and `push_metrics` |
| `maintenance_orchestration/table_policies.json` | Central per-table policy file (CDC table vs monthly report table) |
| `dags/iceberg_maintenance.py` | One TaskGroup per policy entry: `collect_metrics -> choose_actions (branch) -> compact -> expire -> publish_metrics`. Uses `max_active_runs=1` and a failure-alert callback |
| `maintenance_orchestration/test_orchestration.py` | Compaction takes the table from 48 files to 1 and expiration from 12 snapshots to 3. Healthy tables are skipped, metrics reach the Pushgateway, a broken entry fails alone, a second run is idempotent, and a `PythonOperator` return value does not gate the next task |
| `auditing_and_wap/audit.py` | `snapshot-property.*` job attribution, `change_log`, `files_changed` (a `files` diff between parent and child), `schema_history` (read from the `metadata.json` files) and a commit baseline |
| `auditing_and_wap/wap.py` | WAP: `write.wap.enabled` + `spark.wap.branch`, quality checks on the branch, then `fast_forward` (or `cherrypick_snapshot` as a fallback), an `audited_*` tag, and the branch is dropped |
| `auditing_and_wap/lineage.py` | OpenLineage `event_v2`/`facet_v2` events with the snapshot id in `DatasetVersionDatasetFacet` |
| `auditing_and_wap/test_auditing.py` | Finds the lossy overwrite and drills down to its files. Also covers unattributed commits, schema-only changes, refs retention, read-only tags, the three WAP outcomes and the lineage join |
| `disaster_recovery/catalog_backup.py` | REST catalog export (name -> `metadata-location`), backups saved in the DR bucket, `register_table` restore, and a fallback to the latest `metadata.json` |
| `disaster_recovery/recovery.py` | `last_good_snapshot` (newest `audited_*` tag), `rollback_to_snapshot`, purging by `snapshot_ids`, and `validate_snapshot` (HEAD on every referenced file) |
| `disaster_recovery/replication.py` | Naive bucket copy vs `rewrite_table_path` + copy plan, and `replication_lag` |
| `dags/iceberg_recovery.py` | Manually triggered recovery DAG `find_last_good -> rollback -> purge_abandoned -> validate -> notify` |
| `disaster_recovery/test_disaster_recovery.py` | Drills for catalog loss, re-registration without a backup, split-brain, rollback/purge, the recovery DAG, a lost data file and region loss |
| `docker-compose.yml`, `Dockerfile`, `config/s3.json` | The stack. Airflow 2.10.5 is installed with its constraints file, and the hadoop-aws 3.3.4 and AWS SDK bundle jars are added |

## Prerequisites

- Docker with Compose v2. Plan for about 6 GB of free RAM.
- Image versions: `tabulario/spark-iceberg` (Spark 3.5.5, Iceberg 1.8.1), `apache/iceberg-rest-fixture:1.8.1`,
  `chrislusf/seaweedfs`, `prom/pushgateway:v1.10.0`. Airflow 2.10.5, openlineage-python and prometheus_client
  are installed into the Spark image.
- `config/s3.json` and the compose defaults (`admin` / `password`) are **local-only dev credentials**.
  Change both together, or export `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` before `up` and edit `s3.json` to match.

## How to run

```bash
docker compose -p iceberg-lakehouse-operations up -d --build
docker compose -p iceberg-lakehouse-operations exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-lakehouse-operations down -v
```

On Git Bash for Windows, prefix `exec` with `MSYS_NO_PATHCONV=1`. No ports are published, so use
`docker compose exec` for everything. Select one suite with
`python -m pytest -v maintenance_orchestration` (or `auditing_and_wap`, `disaster_recovery`).

## Fixes

- Folders were renamed to topic names, and every import, `pytest.ini` testpaths entry and DAG import was updated to match.
- The Spark app names and the compose project name are now generic (`lakehouse-tests`, `lakehouse-operations`,
  `iceberg-lakehouse-operations`).
- Test names were made generic: `test_python_operator_return_value_does_not_gate_downstream`,
  `test_naive_audit_queries_do_not_resolve_on_iceberg_1_8_1` and `test_expire_cutoff_does_not_remove_the_abandoned_snapshot`.
  The throwaway DAG is now `gate_check`.
- `docker-compose.yml`: hardcoded `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` were replaced with
  `${AWS_ACCESS_KEY_ID:-admin}` / `${AWS_SECRET_ACCESS_KEY:-password}`.
- `common/lakehouse.py`: the boto3 credential defaults are marked `# local dev default`.
- Comments and docstrings that pointed at external text were reworded. Code behavior is unchanged.
- I checked the Iceberg procedure names and arguments against Iceberg 1.8.1 and needed no change:
  `register_table(table, metadata_file)`, `rollback_to_snapshot(table, snapshot_id)`,
  `fast_forward(table, branch, to)`, `cherrypick_snapshot(table, snapshot_id)`,
  `expire_snapshots(..., snapshot_ids => array(...))` and `rewrite_table_path(table, source_prefix, target_prefix, staging_location)`
  (outputs `latest_version`, `file_list_location`).
- The Airflow code is valid for the pinned 2.10.5 and already uses `schedule=`, not `schedule_interval=`. For
  **Airflow 3** you would also need to: import `DAG` from `airflow.sdk`; import operators from
  `airflow.providers.standard.operators.{python,empty}`; import `TaskGroup` from `airflow.sdk`; and in
  `test_orchestration.py`, replace `dag.schedule_interval` (removed in 3.x) with `dag.timetable.summary`.
