"""Merger -- daily devices increment MERGEd into PostgreSQL.

  wait_for_increment  (FileSensor on the day's CSV)
        |
  merge_devices       (ONE SQL task, ONE transaction:
        |              CREATE TEMPORARY TABLE changed_devices (LIKE dp.devices)
        |              COPY the CSV into it, dedup per key, MERGE with soft deletes)
  check_no_duplicate_keys   (SQLCheckOperator: the key really is unique in the target)

Why it is idempotent: the MERGE matches on the immutable (type, version) key, so a
retry or a re-run of the same day updates rows instead of inserting them again. The
temp table disappears with the session, so a failed attempt leaves nothing behind.
Known limit: backfilling an older day starts from the latest table state;
see the Stateful Merger when consumers need point-in-time consistency.

Requirements: apache-airflow>=2.6, apache-airflow-providers-common-sql,
apache-airflow-providers-postgres; Airflow connection `dp_postgres` (credentials stay
in the connection / secrets backend, never in this file). COPY ... FROM '<path>' reads
on the database server: the landing directory must be mounted there (or switch to
COPY FROM STDIN via PostgresHook.copy_expert).
Local test: airflow dags test devices_merger 2024-10-07
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor

CONN_ID = "dp_postgres"
LANDING_FILE = "/data_to_load/devices/{{ ds_nodash }}/dataset.csv"

# The whole string runs as a single execute() in one transaction (autocommit=False):
# either the MERGE is committed with all its changes or nothing is.
MERGE_SQL = """
CREATE TEMPORARY TABLE changed_devices (LIKE dp.devices);
ALTER TABLE changed_devices ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT false,
                            ADD COLUMN change_ts TIMESTAMP NOT NULL;

COPY changed_devices (type, full_name, version, is_deleted, change_ts)
  FROM '__LANDING_FILE__' CSV DELIMITER ';' HEADER;

-- MERGE refuses two source rows for one target row: keep the latest change per key.
DELETE FROM changed_devices c
 USING changed_devices newer
 WHERE c.type = newer.type AND c.version = newer.version
   AND (c.change_ts, c.ctid) < (newer.change_ts, newer.ctid);

MERGE INTO dp.devices AS d
USING changed_devices AS c_d
   ON c_d.type = d.type AND c_d.version = d.version
 WHEN MATCHED AND c_d.is_deleted THEN
      DELETE
 WHEN MATCHED AND NOT c_d.is_deleted THEN
      UPDATE SET full_name = c_d.full_name
 WHEN NOT MATCHED AND NOT c_d.is_deleted THEN       -- guard: never insert already-deleted rows
      INSERT (type, full_name, version) VALUES (c_d.type, c_d.full_name, c_d.version);
"""

CHECK_DUPLICATES_SQL = """
SELECT COUNT(*) = 0 FROM (
  SELECT type, version FROM dp.devices GROUP BY type, version HAVING COUNT(*) > 1
) dup
"""

with DAG(
    dag_id="devices_merger",
    start_date=datetime(2024, 10, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,          # two MERGEs on the same keys must not interleave
    default_args={
        "owner": "data-engineering",
        "retries": 3,           # safe: the MERGE is idempotent
        "retry_delay": timedelta(minutes=5),
    },
    tags=["idempotency", "merger"],
) as dag:
    wait_for_increment = FileSensor(
        task_id="wait_for_increment",
        filepath=LANDING_FILE,
        poke_interval=120,
        timeout=6 * 3600,
        mode="reschedule",
    )

    merge_devices = SQLExecuteQueryOperator(
        task_id="merge_devices",
        conn_id=CONN_ID,
        sql=MERGE_SQL.replace("__LANDING_FILE__", LANDING_FILE),  # Jinja renders {{ ds_nodash }}
        autocommit=False,
        split_statements=False,
    )

    check_no_duplicate_keys = SQLCheckOperator(
        task_id="check_no_duplicate_keys", conn_id=CONN_ID, sql=CHECK_DUPLICATES_SQL)

    wait_for_increment >> merge_devices >> check_no_duplicate_keys
