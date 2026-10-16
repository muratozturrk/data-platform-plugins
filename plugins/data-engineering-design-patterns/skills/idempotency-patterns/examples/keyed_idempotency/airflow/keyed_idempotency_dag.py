"""
Keyed Idempotency for data containers and relational tables, orchestrated by Airflow.

1. Data containers: the key is the NAME of the output object.
   It is derived only from the logical date, never from now() or the run id:
   the run for 2024-11-20 always writes `visits_20_11_2024.csv` (and a table or
   partition would be `visits_20_11_2024` / `dt=2024-11-20`). A retry or a
   backfill rewrites the same file instead of adding a new one. The file is
   written to a temporary name and renamed in one step, so readers never see a
   half-written file.

2. Relational table: a key-value store
   overwrites a row with the same key; PostgreSQL raises a duplicate-key error
   instead. The write therefore becomes INSERT ... ON CONFLICT DO UPDATE on
   PRIMARY KEY(session_id, user_id). The session key uses the user's FIRST
   ingestion time of the day, ordered ascending, so late rows
   with an earlier event time do not change it.

Tables (PostgreSQL 13+):
    dp.visits(visit_id TEXT, user_id BIGINT, page TEXT, visit_time TIMESTAMPTZ,
                ingestion_time TIMESTAMPTZ DEFAULT now())
    dp.sessions(session_id TEXT, user_id BIGINT, pages TEXT[],
                  ingestion_time TIMESTAMPTZ, PRIMARY KEY(session_id, user_id))

Requirements: apache-airflow 2.x, apache-airflow-providers-postgres.
Connection `dp_postgres` holds host/credentials (never in this file).
Env: VISITS_EXPORT_DIR (default /data/exports/visits).
Self-test (no Airflow needed):  python keyed_idempotency_dag.py
"""
from __future__ import annotations

import csv
import os
from datetime import date, datetime, timedelta

PG_CONN_ID = "dp_postgres"
EXPORT_DIR = os.environ.get("VISITS_EXPORT_DIR", "/data/exports/visits")

# Daily sessions: one per user and day, keyed by the first ingestion time.
UPSERT_SESSIONS_SQL = """
INSERT INTO dp.sessions (session_id, user_id, pages, ingestion_time)
SELECT md5(user_id::text || '|' || first_ingestion::text) AS session_id,
       user_id, pages, first_ingestion
FROM (
  SELECT user_id,
         MIN(ingestion_time) AS first_ingestion,
         ARRAY_AGG(page ORDER BY ingestion_time ASC, visit_time ASC) AS pages
  FROM dp.visits
  WHERE ingestion_time >= %(start)s AND ingestion_time < %(end)s
  GROUP BY user_id
) s
ON CONFLICT (session_id, user_id)
DO UPDATE SET pages = EXCLUDED.pages, ingestion_time = EXCLUDED.ingestion_time
"""


def output_name(logical_date: date) -> str:
    """Container key from the logical date only (e.g. 20/11/2024 -> 20_11_2024)."""
    return f"visits_{logical_date.strftime('%d_%m_%Y')}.csv"


def day_bounds(logical_date: date) -> dict:
    start = datetime(logical_date.year, logical_date.month, logical_date.day)
    return {"start": start, "end": start + timedelta(days=1)}


def write_atomically(path: str, header: list[str], rows) -> int:
    """Write to <path>.tmp then os.replace: same name every run, never partial."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    count = 0
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
    os.replace(tmp, path)   # atomic on POSIX and Windows for the same volume
    return count


def export_daily_visits(ds: str) -> str:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logical = date.fromisoformat(ds)
    path = os.path.join(EXPORT_DIR, output_name(logical))
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with hook.get_conn() as conn, conn.cursor(name="visits_export") as cur:
        # Server-side cursor streams the day without loading it into memory.
        cur.execute(
            "SELECT visit_id, user_id, page, visit_time, ingestion_time "
            "FROM dp.visits WHERE ingestion_time >= %(start)s "
            "AND ingestion_time < %(end)s ORDER BY ingestion_time, visit_id",
            day_bounds(logical),
        )
        count = write_atomically(
            path, ["visit_id", "user_id", "page", "visit_time", "ingestion_time"], cur)
    print(f"wrote {count} visits to {path}")
    return path


def upsert_daily_sessions(ds: str) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(UPSERT_SESSIONS_SQL, parameters=day_bounds(date.fromisoformat(ds)))


def check_no_duplicate_sessions(ds: str) -> None:
    """One session per user and day; a second key would mean a mutable key input."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    dupes = hook.get_first(
        "SELECT COUNT(*) FROM (SELECT user_id FROM dp.sessions "
        "WHERE ingestion_time >= %(start)s AND ingestion_time < %(end)s "
        "GROUP BY user_id HAVING COUNT(*) > 1) d",
        parameters=day_bounds(date.fromisoformat(ds)),
    )[0]
    if dupes:
        raise ValueError(f"{dupes} users have more than one session on {ds}")


def _build_dag():
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="keyed_idempotency_visits",
        start_date=datetime(2024, 11, 1),
        schedule="@daily",
        catchup=True,
        default_args={"owner": "data-engineering", "retries": 3,
                      "retry_delay": timedelta(minutes=5)},
        tags=["idempotency", "keyed-idempotency"],
    ) as dag:
        export = PythonOperator(task_id="export_daily_visits",
                                python_callable=export_daily_visits,
                                op_kwargs={"ds": "{{ ds }}"})
        upsert = PythonOperator(task_id="upsert_daily_sessions",
                                python_callable=upsert_daily_sessions,
                                op_kwargs={"ds": "{{ ds }}"})
        check = PythonOperator(task_id="check_no_duplicate_sessions",
                               python_callable=check_no_duplicate_sessions,
                               op_kwargs={"ds": "{{ ds }}"})
        export >> upsert >> check
    return dag


def _self_check() -> None:
    import tempfile

    assert output_name(date(2024, 11, 20)) == "visits_20_11_2024.csv"
    assert output_name(date(2024, 11, 21)) == "visits_21_11_2024.csv"
    b = day_bounds(date(2024, 12, 31))
    assert b["end"] == datetime(2025, 1, 1)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, output_name(date(2024, 11, 20)))
        write_atomically(path, ["a"], [[1], [2]])
        write_atomically(path, ["a"], [[1], [2]])        # replay
        assert os.listdir(d) == [os.path.basename(path)]  # one file, no tmp
        with open(path, encoding="utf-8") as f:
            assert f.read().splitlines() == ["a", "1", "2"]
    assert "ON CONFLICT (session_id, user_id)" in UPSERT_SESSIONS_SQL
    print("keyed_idempotency_dag self-check OK")


if __name__ == "__main__":
    _self_check()
else:
    try:
        dag = _build_dag()
    except ImportError:
        dag = None
