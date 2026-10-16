"""Horizontal Partitioner - Airflow 2.x TaskFlow + PostgreSQL range partitions.

Daily DAG that (1) makes sure the parent table is range-partitioned by
event_time, (2) creates the partition for the run's data interval, and
(3) loads the day into it. Rerunning a day = truncating one partition
(cheap metadata op, see Fast Metadata Cleaner).

Run: drop into dags/, create Airflow connection `dp_postgres`
(AIRFLOW_CONN_DP_POSTGRES=postgresql://user:${PGPASSWORD}@host:5432/dp).
Requires apache-airflow-providers-postgres.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"

PARENT_DDL = """
CREATE TABLE IF NOT EXISTS dp.visits_all (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id TEXT NOT NULL,
    page VARCHAR(20) NULL,
    PRIMARY KEY (visit_id, event_time)
) PARTITION BY RANGE (event_time);
"""


@dag(schedule="@daily", start_date=datetime(2024, 11, 24), catchup=True, max_active_runs=1,
     tags=["dp", "storage", "horizontal-partitioner"])
def horizontal_partitioner():

    @task
    def create_parent_table():
        PostgresHook(postgres_conn_id=CONN_ID).run(PARENT_DDL)

    @task
    def create_partition(data_interval_start=None, data_interval_end=None, ds_nodash=None):
        # Upper bound is exclusive in PostgreSQL range partitions.
        PostgresHook(postgres_conn_id=CONN_ID).run(f"""
            CREATE TABLE IF NOT EXISTS dp.visits_all_{ds_nodash}
            PARTITION OF dp.visits_all
            FOR VALUES FROM ('{data_interval_start}') TO ('{data_interval_end}');
            TRUNCATE dp.visits_all_{ds_nodash};""")

    @task
    def load_day(data_interval_start=None, data_interval_end=None):
        PostgresHook(postgres_conn_id=CONN_ID).run("""
            INSERT INTO dp.visits_all (visit_id, event_time, user_id, page)
            SELECT visit_id, event_time, user_id, page FROM dp.visits_raw
            WHERE event_time >= %(start)s AND event_time < %(end)s""",
            parameters={"start": data_interval_start, "end": data_interval_end})

    create_parent_table() >> create_partition() >> load_day()


horizontal_partitioner()
