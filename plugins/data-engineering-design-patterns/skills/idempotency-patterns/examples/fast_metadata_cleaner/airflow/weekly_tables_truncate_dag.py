r"""Fast Metadata Cleaner -- weekly idempotency tables behind a union view.

Problem: a daily job kept itself idempotent with DELETE (previous run's rows) +
INSERT on one ever-growing visits table. DELETE has to find rows and rewrite the
files holding them, so it gets slower every week.

Solution: split the dataset into weekly tables (the idempotency granularity) and
clean a week with a metadata operation (TRUNCATE TABLE or DROP TABLE), never a
row-level DELETE. A `visits` view unions all weekly tables so consumers keep a
single entry point.

Flow, chosen by CLEANING_MODE or the `cleaning_mode` DAG param:

  TRUNCATE:  route --(Monday / Jan 1)--> create_if_missing -> include_in_view -> truncate --+
                  \--(other days)--------------------------------------------------------- load_day
  DROP:      route --(Monday / Jan 1)--> exclude_from_view -> drop -> create -> include_in_view --+
                  \--(other days)--------------------------------------------------------------- load_day

Backfilling boundary == week: to fix Wednesday you replay from the Monday run
(the table-creation task), then only the load step for Tue..Sun, e.g.
  airflow tasks clear fast_metadata_cleaner_weekly -s 2024-01-08 -e 2024-01-14
Fine-grained backfills (one customer, one provider) are impossible: metadata
operations work on whole tables.

Metadata limits: one table per week per pipeline adds up (Redshift 200,000
tables, BigQuery 4,000 partitions per table). Add a freezing step that merges
closed weekly tables into a yearly immutable table and drops the weeklies once
no more corrections are expected.

Week naming: `visits_<year>_w<%W>` (Monday-based week of the calendar year, Jan 1
always opens a unit) so that "Monday or Jan 1" is exactly "a new unit starts".
ISO weeks would put Jan 1 into week 52/53 of the previous ISO year.

Run:   Airflow 2.x + apache-airflow-providers-postgres, connection `pg_dwh`.
       airflow dags test fast_metadata_cleaner_weekly 2024-01-08
Check: python weekly_tables_truncate_dag.py   (runs _self_check, no Airflow needed)
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta

log = logging.getLogger(__name__)

CONN_ID = os.environ.get("FMC_PG_CONN_ID", "pg_dwh")
SCHEMA = "dp"
VIEW = "visits"
TEMPLATE_TABLE = f"{SCHEMA}.visits_template"   # empty table holding the schema
SOURCE_TABLE = f"{SCHEMA}.visits_raw"          # daily input, partitioned by visit_date
CLEANING_MODE = os.environ.get("FMC_CLEANING_MODE", "truncate")  # truncate | drop
WEEKLY_TABLE_LIKE = "visits\\_____\\_w__"      # LIKE pattern: visits_YYYY_wNN


# --------------------------------------------------------------------------- pure helpers
def week_table_name(day: date) -> str:
    """Unqualified weekly table name for a logical date."""
    return f"visits_{day.year}_w{day.strftime('%W')}"


def should_start_new_unit(day: date) -> bool:
    """True when `day` opens a new idempotency unit (Monday or January 1)."""
    return week_table_name(day) != week_table_name(day - timedelta(days=1))


def build_view_sql(tables: list[str], schema: str = SCHEMA, view: str = VIEW,
                   template: str = TEMPLATE_TABLE) -> str:
    """CREATE OR REPLACE VIEW as a UNION ALL of the weekly tables.

    An empty list still yields a valid (empty) view from the template, so the
    DROP flow can detach the only existing table without breaking readers.
    """
    selects = [f"SELECT * FROM {schema}.{t}" for t in sorted(set(tables))]
    body = "\nUNION ALL\n".join(selects) if selects else f"SELECT * FROM {template} WHERE false"
    return f"CREATE OR REPLACE VIEW {schema}.{view} AS\n{body}"


def _self_check() -> None:
    assert week_table_name(date(2024, 1, 1)) == "visits_2024_w01"   # Monday Jan 1
    assert week_table_name(date(2025, 1, 1)) == "visits_2025_w00"   # Wednesday Jan 1
    assert week_table_name(date(2025, 1, 6)) == "visits_2025_w01"
    for d in (date(2024, 1, 1) + timedelta(days=i) for i in range(800)):
        assert should_start_new_unit(d) == (d.weekday() == 0 or (d.month, d.day) == (1, 1)), d
    sql = build_view_sql(["visits_2024_w02", "visits_2024_w01", "visits_2024_w01"])
    assert sql.count("UNION ALL") == 1 and sql.index("w01") < sql.index("w02")
    assert "WHERE false" in build_view_sql([])
    print("weekly_tables_truncate_dag self-check OK")


# --------------------------------------------------------------------------- DAG
def _build_dag():
    import pendulum
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.empty import EmptyOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    def hook() -> PostgresHook:
        return PostgresHook(postgres_conn_id=CONN_ID)

    def existing_weekly_tables() -> list[str]:
        rows = hook().get_records(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' AND table_name LIKE %s",
            parameters=(SCHEMA, WEEKLY_TABLE_LIKE),
        )
        return [r[0] for r in rows]

    def refresh_view(include: str | None = None, exclude: str | None = None) -> None:
        tables = set(existing_weekly_tables())
        if include:
            tables.add(include)
        if exclude:
            tables.discard(exclude)
        sql = build_view_sql(sorted(tables))
        log.info("Refreshing %s.%s over %d weekly tables", SCHEMA, VIEW, len(tables))
        hook().run(sql, autocommit=True)

    def table_for(ctx_date) -> str:
        return week_table_name(date(ctx_date.year, ctx_date.month, ctx_date.day))

    @dag(
        dag_id="fast_metadata_cleaner_weekly",
        schedule="@daily",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,  # 7 daily runs share one weekly table: serialise them
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        params={"cleaning_mode": Param(CLEANING_MODE, enum=["truncate", "drop"])},
        tags=["idempotency", "fast-metadata-cleaner"],
    )
    def fast_metadata_cleaner_weekly():
        @task.branch
        def route(logical_date=None, params=None) -> str:
            # Exclusive Choice on the execution date.
            mode = params["cleaning_mode"]
            if not should_start_new_unit(logical_date.date()):
                return "skip_table_management"
            return f"{mode}_branch_start"

        skip = EmptyOperator(task_id="skip_table_management")

        # ---- TRUNCATE-based branch: create if none exists -> include in view -> truncate
        truncate_start = EmptyOperator(task_id="truncate_branch_start")

        @task
        def create_if_missing(logical_date=None) -> None:
            t = table_for(logical_date)
            hook().run(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{t} "
                       f"(LIKE {TEMPLATE_TABLE} INCLUDING ALL)", autocommit=True)

        @task(task_id="include_in_view")
        def include_in_view_truncate(logical_date=None) -> None:
            refresh_view(include=table_for(logical_date))

        @task
        def truncate_weekly_table(logical_date=None) -> None:
            t = table_for(logical_date)
            # Metadata operation: no table scan, unlike DELETE FROM t.
            hook().run(f"TRUNCATE TABLE {SCHEMA}.{t}", autocommit=True)
            log.info("Truncated %s.%s", SCHEMA, t)

        # ---- DROP-based branch: remove from view -> drop -> create -> include in view
        drop_start = EmptyOperator(task_id="drop_branch_start")

        @task
        def exclude_from_view(logical_date=None) -> None:
            # Detach first: readers of the view never hit a dropped table.
            refresh_view(exclude=table_for(logical_date))

        @task
        def drop_weekly_table(logical_date=None) -> None:
            hook().run(f"DROP TABLE IF EXISTS {SCHEMA}.{table_for(logical_date)}", autocommit=True)

        @task
        def create_weekly_table(logical_date=None) -> None:
            # Recreated from the template: a new required column lands here on replay.
            hook().run(f"CREATE TABLE {SCHEMA}.{table_for(logical_date)} "
                       f"(LIKE {TEMPLATE_TABLE} INCLUDING ALL)", autocommit=True)

        @task(task_id="include_in_view_after_drop")
        def include_in_view_drop(logical_date=None) -> None:
            refresh_view(include=table_for(logical_date))

        # ---- common part: process and load the day into the weekly table
        @task(trigger_rule="none_failed_min_one_success")
        def load_day(logical_date=None, ds=None) -> int:
            t = f"{SCHEMA}.{table_for(logical_date)}"
            h = hook()
            # Day slice cleanup inside the (small) weekly table keeps a single-day
            # retry idempotent; the expensive cleanup of the unit stays metadata-only.
            with h.get_conn() as conn, conn.cursor() as cur:
                cur.execute(f"DELETE FROM {t} WHERE visit_date = %s", (ds,))
                cur.execute(f"INSERT INTO {t} SELECT * FROM {SOURCE_TABLE} WHERE visit_date = %s", (ds,))
                inserted = cur.rowcount
                conn.commit()
            if inserted == 0:
                raise ValueError(f"no input rows in {SOURCE_TABLE} for {ds}; refusing an empty load")
            log.info("Loaded %d rows for %s into %s", inserted, ds, t)
            return inserted

        branch = route()
        load = load_day()
        branch >> skip >> load
        branch >> truncate_start >> create_if_missing() >> include_in_view_truncate() \
            >> truncate_weekly_table() >> load
        branch >> drop_start >> exclude_from_view() >> drop_weekly_table() \
            >> create_weekly_table() >> include_in_view_drop() >> load

    return fast_metadata_cleaner_weekly()


if __name__ == "__main__":
    _self_check()
else:
    fast_metadata_cleaner_weekly_dag = _build_dag()
