"""
Transactional Writer -- several loads, one PostgreSQL transaction (Airflow).

The task loads two device files. For each file it creates a temporary table
LIKE dp.devices, COPYs the file into it and MERGEs it into dp.devices. All of
it runs on ONE connection with autocommit off and a single COMMIT at the end.
If the second file is broken (e.g. `dataset_too_long_type.csv` with values
too long for a column), the exception triggers ROLLBACK and the first MERGE is
discarded too: consumers see both files or neither, never a partial load.

Notes:
- Readers must not use READ UNCOMMITTED (dirty reads). PostgreSQL treats it as
  READ COMMITTED, so this is safe here; other engines differ.
- The transaction covers one attempt only.
  A retry after a successful COMMIT runs a new transaction; it stays correct
  because MERGE on (type, version) is itself replay-safe (Merger pattern).
- COPY ... FROM STDIN (psycopg2 copy_expert) streams the file from the Airflow
  worker; a server-side `COPY ... FROM '/path'` needs the file on the
  database host and the pg_read_server_files role.
- MERGE needs PostgreSQL 15+.

Requirements: apache-airflow 2.x, apache-airflow-providers-postgres.
Connection `dp_postgres` holds host/credentials (never in this file).
Self-test (no Airflow needed):  python transactional_writer_dag.py
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta

PG_CONN_ID = "dp_postgres"
TARGET_TABLE = "dp.devices"
INPUT_FILES = [
    "/data_to_load/{{ ds_nodash }}/dataset_1.csv",
    "/data_to_load/{{ ds_nodash }}/dataset_2.csv",
]

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")


def statements_for_file(index: int, target: str = TARGET_TABLE) -> tuple[str, str, str]:
    """(create temp table, COPY, MERGE) for the index-th file of the batch."""
    if not _IDENT.match(target):
        raise ValueError(f"unsafe identifier {target!r}")
    tmp = f"changed_devices_file{index}"
    create = f"CREATE TEMPORARY TABLE {tmp} (LIKE {target}) ON COMMIT DROP"
    copy = f"COPY {tmp} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)"
    merge = (
        f"MERGE INTO {target} AS d USING {tmp} AS c_d "
        "ON c_d.type = d.type AND c_d.version = d.version "
        "WHEN MATCHED THEN UPDATE SET full_name = c_d.full_name "
        "WHEN NOT MATCHED THEN INSERT (type, full_name, version) "
        "VALUES (c_d.type, c_d.full_name, c_d.version)"
    )
    return create, copy, merge


def load_files_in_one_transaction(files: list[str]) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn = PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()
    conn.autocommit = False            # BEGIN is implicit in psycopg2
    try:
        with conn.cursor() as cur:
            for index, path in enumerate(files, start=1):
                create, copy, merge = statements_for_file(index)
                cur.execute(create)
                with open(path, encoding="utf-8") as f:
                    cur.copy_expert(copy, f)
                cur.execute(merge)
                print(f"{path}: merged {cur.rowcount} rows (not visible yet)")
        conn.commit()                  # the only moment readers see changes
        print(f"committed {len(files)} file(s) into {TARGET_TABLE}")
    except Exception:
        conn.rollback()                # every MERGE of this attempt is discarded
        raise
    finally:
        conn.close()


def check_devices_consistency() -> None:
    """Post-commit check: the MERGE key must stay unique and the table non-empty."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    total, dupes = hook.get_first(
        f"SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT (type, version)) FROM {TARGET_TABLE}"
    )
    if total == 0:
        raise ValueError(f"{TARGET_TABLE} is empty after the load")
    if dupes:
        raise ValueError(f"{TARGET_TABLE} has {dupes} duplicate (type, version) keys")
    print(f"{TARGET_TABLE}: {total} rows, keys unique")


def _build_dag():
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="transactional_writer_devices",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,     # concurrent runs would only fight over row locks
        default_args={"owner": "data-engineering", "retries": 2,
                      "retry_delay": timedelta(minutes=5)},
        tags=["idempotency", "transactional-writer"],
    ) as dag:
        load = PythonOperator(task_id="load_files_in_one_transaction",
                              python_callable=load_files_in_one_transaction,
                              op_kwargs={"files": INPUT_FILES})
        check = PythonOperator(task_id="check_devices_consistency",
                               python_callable=check_devices_consistency)
        load >> check
    return dag


def _self_check() -> None:
    c1, p1, m1 = statements_for_file(1)
    c2, _, m2 = statements_for_file(2)
    assert "changed_devices_file1 (LIKE dp.devices)" in c1 and "ON COMMIT DROP" in c1
    assert p1.startswith("COPY changed_devices_file1 FROM STDIN")
    # Regression guard: the second MERGE must read changed_devices_file2, not _file1.
    assert "USING changed_devices_file2" in m2 and "USING changed_devices_file1" in m1
    assert c1 != c2
    try:
        statements_for_file(1, "dp.devices; DROP TABLE x")
    except ValueError:
        pass
    else:
        raise AssertionError("unsafe identifier accepted")
    print("transactional_writer_dag self-check OK")


if __name__ == "__main__":
    _self_check()
else:
    try:
        dag = _build_dag()
    except ImportError:
        dag = None
