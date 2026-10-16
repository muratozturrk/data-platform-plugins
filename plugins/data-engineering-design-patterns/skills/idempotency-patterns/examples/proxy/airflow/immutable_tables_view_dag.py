"""Proxy -- write-once internal tables exposed through a passthrough view.

Problem: a batch job regenerates the full devices dataset each run and used to
overwrite it. Legal now requires every past version to be kept, while consumers
still read one stable name.

Solution (view-based approach):
  load_data_to_internal_table -> validate -> freeze (remove write permissions)
      -> refresh_view (CREATE OR REPLACE VIEW dp.devices) -> remove read access
         from the previous internal table (optional)

Why the suffix uses dag_run.start_date and not the logical date:
a rerun of the same interval must NOT rewrite the dataset produced earlier. With
the run start time every attempt lands in a new table; the old one stays intact
and the view simply moves. Idempotency comes from immutability, not from
cleaning.

Immutability must be enforced by the data store, not only by this DAG:
- the loader role may only CREATE tables; it owns no old table, so it cannot
  DROP/DELETE them (the user doing the manipulation can only create tables);
- the freeze step transfers ownership to FROZEN_OWNER_ROLE through a separate,
  privileged connection (PG_ADMIN_CONN_ID) and revokes INSERT/UPDATE/DELETE/TRUNCATE.
  If the loader were a member of the owner role it could undo all of that.

Run:   Airflow 2.x + apache-airflow-providers-postgres, connections `pg_dwh` (loader)
       and optionally `pg_dwh_admin` (ownership transfer).
       airflow dags test proxy_immutable_devices 2024-01-08
Check: python immutable_tables_view_dag.py   (self-check, no Airflow needed)
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

CONN_ID = os.environ.get("PROXY_PG_CONN_ID", "pg_dwh")
ADMIN_CONN_ID = os.environ.get("PROXY_PG_ADMIN_CONN_ID", "")          # empty = skip owner transfer
FROZEN_OWNER_ROLE = os.environ.get("PROXY_FROZEN_OWNER_ROLE", "devices_archive_owner")
LOADER_ROLE = os.environ.get("PROXY_LOADER_ROLE", "devices_loader")
READER_ROLE = os.environ.get("PROXY_READER_ROLE", "devices_reader")
INPUT_FILE = os.environ.get("PROXY_INPUT_FILE", "/data_to_load/devices.csv")
SCHEMA = "dp"
VIEW = f"{SCHEMA}.devices"
TEMPLATE = f"{SCHEMA}.devices_template"
REVOKE_PREVIOUS_READ = os.environ.get("PROXY_REVOKE_PREVIOUS_READ", "true") == "true"
MIN_ROWS = int(os.environ.get("PROXY_MIN_ROWS", "1"))
INTERNAL_RE = re.compile(r"^dp\.devices_internal_\d{8}_\d{6}$")


# --------------------------------------------------------------------------- pure helpers
def devices_table_name_for(start_date: datetime) -> str:
    """Unique write-once table per run attempt."""
    return f"{SCHEMA}.devices_internal_{start_date.strftime('%Y%m%d_%H%M%S')}"


def freeze_statements(table: str, loader: str = LOADER_ROLE, reader: str = READER_ROLE) -> list[str]:
    """Make `table` read-only for everybody except its (new) owner."""
    if not INTERNAL_RE.match(table):
        raise ValueError(f"refusing to change grants on unexpected table {table!r}")
    return [
        f"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON {table} FROM PUBLIC",
        f"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON {table} FROM {loader}",
        f"GRANT SELECT ON {table} TO {loader}",   # needed to (re)build the view
        f"GRANT SELECT ON {table} TO {reader}",
    ]


def view_sql(table: str) -> str:
    if not INTERNAL_RE.match(table):
        raise ValueError(f"view must point to an internal table, got {table!r}")
    return f"CREATE OR REPLACE VIEW {VIEW} AS SELECT * FROM {table}"


def _self_check() -> None:
    t = devices_table_name_for(datetime(2024, 1, 8, 3, 15, 0))
    assert t == "dp.devices_internal_20240108_031500"
    rerun = devices_table_name_for(datetime(2024, 1, 9, 7, 0, 1))
    assert rerun != t  # rerun of the same interval never targets the same table
    stmts = freeze_statements(t)
    assert any("FROM PUBLIC" in s for s in stmts) and stmts[-1].endswith(READER_ROLE)
    assert view_sql(t).endswith(t)
    for bad in ("dp.devices", "dp.devices_internal_x; DROP TABLE y"):
        try:
            view_sql(bad)
            raise AssertionError(bad)
        except ValueError:
            pass
    print("immutable_tables_view_dag self-check OK")


# --------------------------------------------------------------------------- DAG
def _build_dag():
    import pendulum
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    def get_devices_table_name() -> str:
        dag_run = get_current_context()["dag_run"]
        return devices_table_name_for(dag_run.start_date)

    def hook(conn_id: str = CONN_ID) -> PostgresHook:
        return PostgresHook(postgres_conn_id=conn_id)

    @dag(
        dag_id="proxy_immutable_devices",
        schedule="@daily",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,  # two concurrent runs would race on the view swap
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=["idempotency", "proxy"],
    )
    def proxy_immutable_devices():
        @task
        def load_data_to_internal_table() -> str:
            table = get_devices_table_name()
            h = hook()
            # Plain CREATE (no IF NOT EXISTS): an existing name means something is wrong.
            # A task retry after a partial failure gets a new try but the same dag_run, so
            # drop only a table this very run created and never published.
            published = h.get_first(
                "SELECT 1 FROM information_schema.view_table_usage WHERE view_schema = %s "
                "AND view_name = 'devices' AND table_schema || '.' || table_name = %s",
                parameters=(SCHEMA, table))
            if published:
                raise RuntimeError(f"{table} is already published; refusing to rewrite it")
            h.run(f"DROP TABLE IF EXISTS {table}", autocommit=True)
            h.run(f"CREATE TABLE {table} (LIKE {TEMPLATE} INCLUDING ALL)", autocommit=True)
            h.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER)", INPUT_FILE)
            log.info("Loaded %s into %s", INPUT_FILE, table)
            return table

        @task
        def validate(table: str) -> str:
            rows = hook().get_first(f"SELECT COUNT(*) FROM {table}")[0]
            if rows < MIN_ROWS:
                raise ValueError(f"{table} has {rows} rows (< {MIN_ROWS}); not publishing it")
            log.info("%s validated with %d rows", table, rows)
            return table

        @task
        def freeze(table: str) -> str:
            if ADMIN_CONN_ID:
                hook(ADMIN_CONN_ID).run(f"ALTER TABLE {table} OWNER TO {FROZEN_OWNER_ROLE}",
                                        autocommit=True)
                hook(ADMIN_CONN_ID).run(freeze_statements(table), autocommit=True)
            else:
                log.warning("No admin connection: loader still owns %s and could alter it", table)
                hook().run(freeze_statements(table), autocommit=True)
            return table

        @task
        def refresh_view(table: str) -> str | None:
            h = hook()
            previous = h.get_first(
                "SELECT table_schema || '.' || table_name FROM information_schema.view_table_usage "
                "WHERE view_schema = %s AND view_name = 'devices'", parameters=(SCHEMA,))
            # Separate step from the load: if the swap fails, nothing is reprocessed.
            h.run([view_sql(table), f"GRANT SELECT ON {VIEW} TO {READER_ROLE}"], autocommit=True)
            log.info("%s now exposes %s (previous: %s)", VIEW, table, previous)
            return previous[0] if previous and previous[0] != table else None

        @task
        def remove_read_access_from_previous(previous: str | None) -> None:
            if not (REVOKE_PREVIOUS_READ and previous and INTERNAL_RE.match(previous)):
                log.info("Nothing to revoke (previous=%s)", previous)
                return
            # The version is kept for legal, but consumers must go through the proxy.
            conn = ADMIN_CONN_ID or CONN_ID
            hook(conn).run(f"REVOKE SELECT ON {previous} FROM {READER_ROLE}", autocommit=True)

        table = freeze(validate(load_data_to_internal_table()))
        remove_read_access_from_previous(refresh_view(table))

    return proxy_immutable_devices()


if __name__ == "__main__":
    _self_check()
else:
    proxy_immutable_devices_dag = _build_dag()
