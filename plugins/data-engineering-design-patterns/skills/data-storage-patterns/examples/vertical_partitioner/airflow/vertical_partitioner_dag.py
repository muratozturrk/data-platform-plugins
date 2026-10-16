"""Vertical Partitioner - Airflow 2.x TaskFlow + PostgreSQL.

From the JSONB `dp.visits_all` table, a daily DAG splits each row into the
technical-context table (immutable, INSERT INTO ... SELECT DISTINCT) and the
user-context table. Both writes run in parallel; a final view recombines them
for consumers who need the full row.

Run: put in dags/, define connection `dp_postgres` via env
(AIRFLOW_CONN_DP_POSTGRES). Requires apache-airflow-providers-postgres.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"


def run(sql: str, **params) -> None:
    PostgresHook(postgres_conn_id=CONN_ID).run(sql, parameters=params or None)


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "vertical-partitioner"])
def vertical_partitioner():

    @task
    def create_tables():
        run("""
        CREATE TABLE IF NOT EXISTS dp.technical (
            visit_id CHAR(36) PRIMARY KEY, browser TEXT, browser_version TEXT, device_type TEXT);
        CREATE TABLE IF NOT EXISTS dp.user_context (
            visit_id CHAR(36) PRIMARY KEY, user_id TEXT, ip TEXT, login TEXT);""")

    @task
    def write_technical(ds=None):
        run("""
        INSERT INTO dp.technical (visit_id, browser, browser_version, device_type)
        SELECT DISTINCT ON (visit_id) visit_id, context->'technical'->>'browser',
               context->'technical'->>'browser_version', context->'technical'->>'device_type'
        FROM dp.visits_all WHERE event_time::date = %(ds)s
        ON CONFLICT (visit_id) DO NOTHING""", ds=ds)

    @task
    def write_user_context(ds=None):
        run("""
        INSERT INTO dp.user_context (visit_id, user_id, ip, login)
        SELECT DISTINCT ON (visit_id) visit_id, user_id,
               context->'user'->>'ip', context->'user'->>'login'
        FROM dp.visits_all WHERE event_time::date = %(ds)s
        ON CONFLICT (visit_id) DO NOTHING""", ds=ds)

    @task
    def expose_full_view():
        run("""
        CREATE OR REPLACE VIEW dp.visits_full AS
        SELECT v.visit_id, v.event_time, v.page, u.user_id, u.ip, u.login,
               t.browser, t.browser_version, t.device_type
        FROM dp.visits_events v
        LEFT JOIN dp.user_context u USING (visit_id)
        LEFT JOIN dp.technical t USING (visit_id)""")

    created = create_tables()
    created >> [write_technical(), write_user_context()] >> expose_full_view()


vertical_partitioner()
