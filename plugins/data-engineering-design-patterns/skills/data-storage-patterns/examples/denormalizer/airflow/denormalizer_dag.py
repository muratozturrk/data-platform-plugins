"""Denormalizer - Airflow 2.x: build a One Big Table on top of a normalized model.

Combines both data representation patterns: normalized
tables stay the consistent source of truth, and after they are refreshed
(Local Sequencer: wait for the upstream normalizer DAG via a dataset), this DAG
rebuilds a read-optimized `visits_obt` with CTAS into a new table and an atomic
rename swap, so readers never see a half-built table.

Run: put in dags/; connection `dp_postgres` via env. Requires the Postgres provider.
The upstream DAG should declare `outlets=[NORMALIZED_VISITS]` on its last task.
"""
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"
NORMALIZED_VISITS = Dataset("postgres://dp/visits_normalized")

OBT_SQL = """
DROP TABLE IF EXISTS dp.visits_obt_next;
CREATE TABLE dp.visits_obt_next AS
SELECT v.visit_id, v.event_time, v.page,
       c.name AS page_category,
       u.id AS user_id, u.login AS user_login,
       d.id AS device_id, d.full_name AS device_full_name,
       b.name AS browser_name, b.version AS browser_version
FROM dp.visits v
LEFT JOIN dp.pages p ON p.id = v.pages_id
LEFT JOIN dp.categories c ON c.id = p.page_categories_id
LEFT JOIN dp.users u ON u.id = v.users_id
LEFT JOIN dp.visits_context vc ON vc.visit_id = v.visit_id
LEFT JOIN dp.devices d ON d.id = vc.devices_id
LEFT JOIN dp.browsers b ON b.id = vc.browsers_id;
CREATE INDEX ON dp.visits_obt_next (event_time);
"""

# hook.run (autocommit=False) executes this in one transaction -> atomic swap for readers.
SWAP_SQL = """
ALTER TABLE IF EXISTS dp.visits_obt RENAME TO visits_obt_old;
ALTER TABLE dp.visits_obt_next RENAME TO visits_obt;
DROP TABLE IF EXISTS dp.visits_obt_old;
"""


@dag(schedule=[NORMALIZED_VISITS], start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "denormalizer"])
def denormalizer():

    @task
    def build_one_big_table():
        PostgresHook(postgres_conn_id=CONN_ID).run(OBT_SQL)

    @task
    def check_row_count():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        obt = hook.get_first("SELECT COUNT(*) FROM dp.visits_obt_next")[0]
        src = hook.get_first("SELECT COUNT(*) FROM dp.visits")[0]
        if obt != src:  # LEFT joins must not duplicate or drop facts
            raise ValueError(f"OBT has {obt} rows, visits has {src}: check join keys for fan-out")

    @task
    def swap_tables():
        PostgresHook(postgres_conn_id=CONN_ID).run(SWAP_SQL)

    build_one_big_table() >> check_row_count() >> swap_tables()


denormalizer()
