"""Full Loader for a database without native versioning (PostgreSQL).

Single data exposition abstraction: consumers only query the ``devices`` VIEW. Each run
loads the full dataset into a new technical table ``devices_<version>`` and, only when it is complete and
validated, repoints the view. A failed run leaves the view on the previous table, and the last N tables are
kept so a rollback is a single ``CREATE OR REPLACE VIEW``.

  retrieve_dataset -> create_versioned_table -> load_versioned_table (COPY)
      -> validate_versioned_table -> expose_new_version (view switch) -> drop_old_versions

Version = the immutable logical date (``ds_nodash``) plus the try number, so retries and backfills never
reuse a half-loaded table. Airflow 2.4+, apache-airflow-providers-postgres (connection ``dwh``).
"""
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dwh"
VIEW = "devices"
TEMPLATE_TABLE = "devices_template"      # DDL owner: columns and types live in one place
KEEP_VERSIONS = 3
INPUT_DIR = Variable.get("devices_input_dir", default_var="/data_to_load")
MIN_ROWS = 1
MAX_SHRINK = 0.5


def _version(context) -> str:
    return f"{context['ds_nodash']}_{context['ti'].try_number}"


@dag(
    dag_id="full_loader_devices_versioned_view",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "full-load"],
)
def full_loader():

    @task
    def retrieve_dataset(ds=None) -> str:
        path = os.path.join(INPUT_DIR, ds, "dataset.csv")
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise FileNotFoundError(f"Provider export missing or empty: {path}")
        return path

    @task
    def create_versioned_table(**context) -> str:
        table = f"devices_{_version(context)}"
        PostgresHook(postgres_conn_id=CONN_ID).run(
            f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (LIKE {TEMPLATE_TABLE} INCLUDING ALL);")
        return table

    @task
    def load_versioned_table(table: str, path: str) -> str:
        # COPY devices_${version} FROM ... CSV DELIMITER ';' HEADER
        PostgresHook(postgres_conn_id=CONN_ID).copy_expert(
            f"COPY {table} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)", path)
        return table

    @task
    def validate_versioned_table(table: str) -> str:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        new_count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        exists = hook.get_first("SELECT to_regclass(%s) IS NOT NULL", parameters=(VIEW,))[0]
        old_count = hook.get_first(f"SELECT COUNT(*) FROM {VIEW}")[0] if exists else None
        if new_count < MIN_ROWS or (old_count and new_count < old_count * (1 - MAX_SHRINK)):
            raise ValueError(f"{table}: {new_count} rows vs {old_count} currently exposed; not publishing")
        return table

    @task
    def expose_new_version(table: str) -> str:
        # The switch is a single catalog operation, consumers see old or new, never partial
        PostgresHook(postgres_conn_id=CONN_ID).run(f"CREATE OR REPLACE VIEW {VIEW} AS SELECT * FROM {table}")
        return table

    @task
    def drop_old_versions(current: str) -> list:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        tables = [r[0] for r in hook.get_records(
            "SELECT tablename FROM pg_tables WHERE tablename ~ '^devices_[0-9]{8}_[0-9]+$' "
            "ORDER BY tablename DESC")]
        to_drop = [t for t in tables[KEEP_VERSIONS:] if t != current]
        for t in to_drop:
            hook.run(f"DROP TABLE IF EXISTS {t}")
        return to_drop

    path = retrieve_dataset()
    table = create_versioned_table()
    loaded = load_versioned_table(table, path)
    drop_old_versions(expose_new_version(validate_versioned_table(loaded)))


full_loader()
