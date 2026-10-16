"""Dataset Materializer - Airflow 2.x TaskFlow + PostgreSQL.

Two flavours driven by one DAG:
  * `refresh_windowed_visits` - full refresh of a materialized view over the
    last 3 weekly tables (REFRESH ... CONCURRENTLY keeps it readable).
  * `merge_visits_counter`    - incremental materialization: aggregate only rows
    inserted in this data interval and MERGE them into a counter table
    (PostgreSQL 15+). The interval bounds make reruns deterministic... but
    MERGE with `count + visits` is not idempotent - a rerun double-counts, so
    the DAG first records the processed interval and skips it if already done.

Run: put in dags/; connection `dp_postgres` via env. Requires the Postgres provider.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"


@dag(schedule="@hourly", start_date=datetime(2024, 11, 1), catchup=False, max_active_runs=1,
     tags=["dp", "storage", "dataset-materializer"])
def dataset_materializer():

    @task
    def create_objects():
        PostgresHook(postgres_conn_id=CONN_ID).run("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS dp.windowed_visits AS
            SELECT visit_id, user_id, page, event_time FROM dp.visits_week_1
            UNION ALL SELECT visit_id, user_id, page, event_time FROM dp.visits_week_2
            UNION ALL SELECT visit_id, user_id, page, event_time FROM dp.visits_week_3
        WITH NO DATA;
        CREATE UNIQUE INDEX IF NOT EXISTS windowed_visits_pk ON dp.windowed_visits (visit_id, event_time);
        CREATE TABLE IF NOT EXISTS dp.visits_counter (user_id TEXT PRIMARY KEY, count BIGINT NOT NULL);
        CREATE TABLE IF NOT EXISTS dp.visits_counter_runs (interval_start TIMESTAMP PRIMARY KEY);""")

    @task
    def refresh_windowed_visits():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        populated = hook.get_first(
            "SELECT ispopulated FROM pg_matviews WHERE schemaname='dp' AND matviewname='windowed_visits'")[0]
        # CONCURRENTLY needs a populated view + unique index; first refresh must be blocking.
        hook.run("REFRESH MATERIALIZED VIEW " + ("CONCURRENTLY " if populated else "")
                 + "dp.windowed_visits WITH DATA")

    @task
    def merge_visits_counter(data_interval_start=None, data_interval_end=None):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        with conn, conn.cursor() as cur:  # one transaction: MERGE + run marker
            cur.execute("INSERT INTO dp.visits_counter_runs VALUES (%s) ON CONFLICT DO NOTHING",
                        (data_interval_start,))
            if cur.rowcount == 0:
                raise AirflowSkipException("interval already merged")
            cur.execute("""
            MERGE INTO dp.visits_counter AS target
            USING (SELECT user_id, COUNT(*) AS visits FROM dp.visits
                   WHERE insertion_time >= %(start)s AND insertion_time < %(end)s
                   GROUP BY user_id) AS input
            ON target.user_id = input.user_id
            WHEN MATCHED THEN UPDATE SET count = target.count + input.visits
            WHEN NOT MATCHED THEN INSERT (user_id, count) VALUES (input.user_id, input.visits)""",
                        {"start": data_interval_start, "end": data_interval_end})

    created = create_objects()
    created >> [refresh_windowed_visits(), merge_visits_counter()]


dataset_materializer()
