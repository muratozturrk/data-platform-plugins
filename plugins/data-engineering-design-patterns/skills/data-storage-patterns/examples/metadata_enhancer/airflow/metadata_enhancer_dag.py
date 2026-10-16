"""Metadata Enhancer - Airflow 2.x: keep database statistics fresh after loads.

PostgreSQL auto-analyze only fires after a threshold of modified rows
(autovacuum_analyze_threshold + scale_factor * reltuples). Small daily loads
into a big table may never cross it, so the planner works with stale stats.
This DAG loads the day, checks `n_mod_since_analyze`, and runs ANALYZE when
the table drifted - plus extended statistics for correlated columns.

Run: put in dags/; connection `dp_postgres` from env (AIRFLOW_CONN_DP_POSTGRES).
Requires apache-airflow-providers-postgres.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dp_postgres"
TABLE = "visits_all"
MAX_STALE_RATIO = 0.02  # re-analyze if > 2% of rows changed since last ANALYZE


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["dp", "storage", "metadata-enhancer"])
def metadata_enhancer():

    @task
    def load_day(ds=None):
        PostgresHook(postgres_conn_id=CONN_ID).run(
            f"INSERT INTO dp.{TABLE} SELECT * FROM dp.visits_staging WHERE event_time::date = %(ds)s",
            parameters={"ds": ds})

    @task.branch
    def stats_are_stale() -> str:
        row = PostgresHook(postgres_conn_id=CONN_ID).get_first("""
            SELECT n_mod_since_analyze, GREATEST(n_live_tup, 1), last_analyze, last_autoanalyze
            FROM pg_stat_user_tables WHERE schemaname = 'dp' AND relname = %s""", parameters=(TABLE,))
        modified, live, *_ = row
        print(f"modified since analyze={modified}, live={live}, last={row[2:]}")
        return "refresh_statistics" if modified / live > MAX_STALE_RATIO else "statistics_fresh"

    @task
    def refresh_statistics():
        PostgresHook(postgres_conn_id=CONN_ID).run(f"""
            CREATE STATISTICS IF NOT EXISTS dp.{TABLE}_user_page (dependencies, ndistinct)
                ON user_id, page FROM dp.{TABLE};
            ANALYZE dp.{TABLE};""")

    @task
    def statistics_fresh():
        print("planner statistics up to date")

    load_day() >> stats_are_stale() >> [refresh_statistics(), statistics_fresh()]


metadata_enhancer()
