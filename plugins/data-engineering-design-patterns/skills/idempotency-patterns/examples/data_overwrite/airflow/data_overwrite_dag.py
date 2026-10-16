"""
Data Overwrite -- daily partition overwrite orchestrated by Airflow.

Each scheduled run replaces ONLY the partition of its own logical date
(`{{ ds }}`). Running the same date twice -- retry, clear, backfill -- produces
the same partition, so retries are enabled: overwrite is what makes them safe.

Two writers, same idempotency granularity (one day):
  1. Lakehouse (object store, no TRUNCATE/DROP metadata layer):
     spark_data_overwrite.py --dates {{ ds }}  -> Delta `replaceWhere`
     on `_ingestion_date`, committed atomically as one new table version.
  2. PostgreSQL serving table: no INSERT OVERWRITE, so DELETE + INSERT ... SELECT
     in ONE transaction (SQLExecuteQueryOperator runs the statement list on one
     connection and commits once at the end; a failure rolls everything back).

BigQuery equivalent (load job with write disposition WRITE_TRUNCATE, i.e.
`bq load --replace=true`), scoped to one partition with the `$YYYYMMDD` decorator:

    BigQueryInsertJobOperator(task_id="load_bq", configuration={"load": {
        "sourceUris": ["gs://visits/{{ ds_nodash }}/*.json"],
        "destinationTable": {"projectId": "p", "datasetId": "dp",
                             "tableId": "visits${{ ds_nodash }}"},
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "writeDisposition": "WRITE_TRUNCATE"}})

Databricks / Spark SQL / Snowflake can do the PostgreSQL step in one statement:
    INSERT OVERWRITE dp.visits PARTITION (visit_date = '{{ ds }}')
    SELECT ... FROM dp.visits_raw WHERE visit_date = '{{ ds }}';
INSERT OVERWRITE replaces the whole table or partition; DELETE + INSERT lets you
choose the rows to remove with any predicate.

Requirements: apache-airflow 2.x, apache-airflow-providers-apache-spark,
apache-airflow-providers-common-sql, apache-airflow-providers-postgres.
Connections: spark_default, target_dwh (PostgreSQL; password lives in the
Airflow connection / secrets backend, never in this file).

Self-test of the SQL builder (no Airflow needed):  python data_overwrite_dag.py
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta

SPARK_APP = "/opt/jobs/data_overwrite/spark_data_overwrite.py"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
PG_CONN_ID = "target_dwh"
TARGET_TABLE = "dp.visits"
SOURCE_TABLE = "dp.visits_raw"
PARTITION_COL = "visit_date"
# Explicit column list: INSERT ... SELECT needs no VALUES list,
# but naming the columns keeps the statement stable when the source gains columns.
COLUMNS = ("visit_id", "user_id", "page", "visit_time", "visit_date")

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\{\{ ds \}\}$")


def build_overwrite_sql(target: str, source: str, partition_col: str,
                        columns: tuple[str, ...], ds: str) -> list[str]:
    """DELETE + INSERT for one partition; both run in the same transaction.

    Identifiers are validated because they are interpolated, not bound.
    `ds` is either a literal date or the `{{ ds }}` template.
    """
    for ident in (target, source, partition_col, *columns):
        if not _IDENT.match(ident):
            raise ValueError(f"unsafe identifier: {ident!r}")
    if not _DATE.match(ds):
        raise ValueError(f"unexpected partition value: {ds!r}")
    if partition_col not in columns:
        raise ValueError("partition column must be part of the inserted columns")
    cols = ", ".join(columns)
    return [
        f"DELETE FROM {target} WHERE {partition_col} = '{ds}'",
        f"INSERT INTO {target} ({cols}) "
        f"SELECT {cols} FROM {source} WHERE {partition_col} = '{ds}'",
    ]


def check_partition_not_empty(ds: str) -> int:
    """Fails the run when the overwrite left an empty partition.

    An empty source turns DELETE + INSERT into a plain DELETE; we let the
    transaction commit (the source really is empty) but fail loudly so
    someone looks at it instead of shipping an empty day.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    rows = hook.get_first(
        f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE {PARTITION_COL} = %s",
        parameters=(ds,),
    )[0]
    if rows == 0:
        raise ValueError(f"{TARGET_TABLE} partition {ds} is empty after overwrite")
    print(f"{TARGET_TABLE} {PARTITION_COL}={ds}: {rows} rows")
    return rows


def _build_dag():
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    default_args = {
        "owner": "data-engineering",
        # Retrying is safe: each attempt replaces the same partition.
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="data_overwrite_visits_daily",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=True,              # backfills are just more overwrites
        max_active_runs=4,         # different dates never touch the same partition
        default_args=default_args,
        tags=["idempotency", "data-overwrite"],
    ) as dag:
        overwrite_lake_partition = SparkSubmitOperator(
            task_id="overwrite_lake_partition",
            application=SPARK_APP,
            application_args=["--dates", "{{ ds }}", "--reason", "daily_run"],
            conn_id="spark_default",
            packages=DELTA_PACKAGE,
        )

        overwrite_pg_partition = SQLExecuteQueryOperator(
            task_id="overwrite_pg_partition",
            conn_id=PG_CONN_ID,
            sql=build_overwrite_sql(TARGET_TABLE, SOURCE_TABLE, PARTITION_COL,
                                    COLUMNS, "{{ ds }}"),
            autocommit=False,      # one transaction for DELETE + INSERT
        )

        validate_pg_partition = PythonOperator(
            task_id="validate_pg_partition",
            python_callable=check_partition_not_empty,
            op_kwargs={"ds": "{{ ds }}"},
        )

        overwrite_lake_partition >> overwrite_pg_partition >> validate_pg_partition
    return dag


def _self_check() -> None:
    stmts = build_overwrite_sql("dp.visits", "dp.visits_raw", "visit_date",
                                COLUMNS, "2024-11-20")
    assert stmts[0] == "DELETE FROM dp.visits WHERE visit_date = '2024-11-20'"
    assert stmts[1].startswith("INSERT INTO dp.visits (visit_id")
    assert "WHERE visit_date = '2024-11-20'" in stmts[1]
    # Same input -> same statements: the task is deterministic per logical date.
    assert stmts == build_overwrite_sql("dp.visits", "dp.visits_raw",
                                        "visit_date", COLUMNS, "2024-11-20")
    for bad in [("dp.visits; DROP TABLE x", "2024-11-20"),
                ("dp.visits", "2024-11-20' OR '1'='1")]:
        try:
            build_overwrite_sql(bad[0], "dp.visits_raw", "visit_date", COLUMNS, bad[1])
        except ValueError:
            continue
        raise AssertionError(f"accepted unsafe input {bad}")
    print("data_overwrite_dag self-check OK")


if __name__ == "__main__":
    _self_check()
else:
    try:
        dag = _build_dag()
    except ImportError:  # imported outside Airflow (e.g. docs tooling)
        dag = None
