"""Fast Metadata Cleaner on Spark + Delta catalog tables.

The visits dataset is stored as one Delta table per week (`dp.visits_2024_w01`)
and exposed through a persistent `dp.visits` view that unions them all. The
idempotency unit is the week, and cleaning it is a metadata operation:

  --cleaning-mode drop      exclude from view -> DROP TABLE -> CREATE TABLE -> include in view
  --cleaning-mode truncate  CREATE IF NOT EXISTS -> include in view -> remove all rows
                            (TRUNCATE TABLE where supported, else `DELETE FROM t` with no
                            predicate: Delta commits it as a metadata-only removal of every
                            file in the log, without scanning the data. OSS Delta rejects
                            TRUNCATE TABLE on some versions, hence the fallback.)

Only the first run of a unit (Monday or January 1) manages the table; every run
then writes its day with `replaceWhere visit_date = <day>`, so a single-day retry
inside the week replaces its own slice and nothing else.

--full-load: no weekly split. Every run re-creates (drop mode) or
empties (truncate mode) a single table and loads the full input into it.

Backfill boundary == week: replaying the Monday run wipes Tue..Sun, which then
have to be replayed too (load step only).

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    spark_weekly_tables_metadata_cleaner.py --execution-date 2024-01-08 \
    --input-path /data/raw/visits --cleaning-mode drop
  python spark_weekly_tables_metadata_cleaner.py --self-test
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fast_metadata_cleaner")

DATABASE = "dp"
VIEW = "visits"
WEEKLY_TABLE_RE = re.compile(r"^visits_\d{4}_w\d{2}$")
SCHEMA_DDL = "visit_id STRING, user_id BIGINT, page STRING, visit_time TIMESTAMP, visit_date DATE"


# --------------------------------------------------------------------------- pure helpers
def week_table_name(day: date) -> str:
    """Monday-based week of the calendar year; Jan 1 always opens week 00 or 01."""
    return f"visits_{day.year}_w{day.strftime('%W')}"


def should_start_new_unit(day: date) -> bool:
    return week_table_name(day) != week_table_name(day - timedelta(days=1))


def union_view_sql(tables: list[str], database: str = DATABASE, view: str = VIEW) -> str:
    weekly = sorted(t for t in set(tables) if WEEKLY_TABLE_RE.match(t))
    if not weekly:
        raise ValueError("no weekly tables to expose in the view")
    selects = [f"SELECT * FROM {database}.{t}" for t in weekly]
    return f"CREATE OR REPLACE VIEW {database}.{view} AS\n" + "\nUNION ALL\n".join(selects)


def _self_check() -> None:
    assert week_table_name(date(2024, 1, 1)) == "visits_2024_w01"
    assert week_table_name(date(2025, 1, 1)) == "visits_2025_w00"
    for i in range(800):
        d = date(2024, 1, 1) + timedelta(days=i)
        assert should_start_new_unit(d) == (d.weekday() == 0 or (d.month, d.day) == (1, 1)), d
    sql = union_view_sql(["visits_2024_w02", "visits_2024_w01", "visits_tmp", "devices"])
    assert sql.count("UNION ALL") == 1 and "visits_tmp" not in sql and "devices" not in sql
    try:
        union_view_sql(["devices"])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    print("spark_weekly_tables_metadata_cleaner self-check OK")


# --------------------------------------------------------------------------- Spark side
class WeeklyTablesCleaner:
    def __init__(self, spark, database: str = DATABASE):
        self.spark = spark
        self.db = database

    def weekly_tables(self) -> list[str]:
        return [t.name for t in self.spark.catalog.listTables(self.db)
                if t.tableType != "VIEW" and WEEKLY_TABLE_RE.match(t.name)]

    def refresh_view(self, include: str | None = None, exclude: str | None = None) -> None:
        tables = set(self.weekly_tables())
        if include:
            tables.add(include)
        if exclude:
            tables.discard(exclude)
        if not tables:
            # Nothing left to expose (first week, DROP flow): drop the view instead of
            # pointing it at a table that is about to disappear.
            self.spark.sql(f"DROP VIEW IF EXISTS {self.db}.{VIEW}")
            log.info("No weekly table left, view %s.%s dropped", self.db, VIEW)
            return
        self.spark.sql(union_view_sql(sorted(tables), self.db))
        log.info("View %s.%s now unions %d tables", self.db, VIEW, len(tables))

    def create_table(self, table: str, if_not_exists: bool) -> None:
        clause = "IF NOT EXISTS " if if_not_exists else ""
        self.spark.sql(f"CREATE TABLE {clause}{self.db}.{table} ({SCHEMA_DDL}) USING delta "
                       "PARTITIONED BY (visit_date)")

    def empty_table(self, table: str) -> None:
        fq = f"{self.db}.{table}"
        try:
            self.spark.sql(f"TRUNCATE TABLE {fq}")
        except Exception as exc:  # AnalysisException on Delta versions without TRUNCATE
            log.info("TRUNCATE unsupported (%s); using predicate-less DELETE (metadata-only)",
                     type(exc).__name__)
            self.spark.sql(f"DELETE FROM {fq}")

    def prepare_unit(self, table: str, mode: str, in_view: bool = True) -> None:
        if mode == "drop":
            if in_view:
                self.refresh_view(exclude=table)
            self.spark.sql(f"DROP TABLE IF EXISTS {self.db}.{table}")
            self.create_table(table, if_not_exists=False)
            if in_view:
                self.refresh_view(include=table)
        elif mode == "truncate":
            self.create_table(table, if_not_exists=True)
            if in_view:
                self.refresh_view(include=table)
            self.empty_table(table)
        else:
            raise ValueError(f"unknown cleaning mode {mode!r}")
        log.info("Unit %s.%s prepared with %s", self.db, table, mode)


def run(args: argparse.Namespace) -> None:
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.appName("fast-metadata-cleaner-weekly").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cleaner = WeeklyTablesCleaner(spark)
    day = date.fromisoformat(args.execution_date)

    raw = spark.read.json(args.input_path)
    if args.full_load:
        # Fully loaded dataset: one table re-created on every run.
        table = "visits_full"
        cleaner.prepare_unit(table, args.cleaning_mode, in_view=False)
        out = raw.withColumn("visit_date", F.to_date("visit_time"))
        out.selectExpr(*[c.split()[0] for c in SCHEMA_DDL.split(", ")]) \
            .write.format("delta").mode("append").saveAsTable(f"{DATABASE}.{table}")
        log.info("Full load into %s.%s done", DATABASE, table)
        return

    table = week_table_name(day)
    if should_start_new_unit(day) or table not in cleaner.weekly_tables():
        cleaner.prepare_unit(table, args.cleaning_mode)
    else:
        log.info("%s continues unit %s: skipping table management", day, table)

    daily = (raw.withColumn("visit_date", F.to_date("visit_time"))
             .filter(F.col("visit_date") == F.lit(day.isoformat()).cast("date"))
             .selectExpr(*[c.split()[0] for c in SCHEMA_DDL.split(", ")]))
    if daily.isEmpty():
        raise ValueError(f"no input rows for {day}; refusing to replace the day with nothing")
    (daily.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"visit_date = '{day.isoformat()}'")
     .saveAsTable(f"{DATABASE}.{table}"))
    log.info("Day %s written into %s.%s", day, DATABASE, table)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--execution-date", help="logical date, YYYY-MM-DD")
    p.add_argument("--input-path", help="raw visits (JSON)")
    p.add_argument("--cleaning-mode", choices=["truncate", "drop"], default="truncate")
    p.add_argument("--full-load", action="store_true", help="single-table variant (fully loaded dataset)")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if not args.self_test and not (args.execution_date and args.input_path):
        p.error("--execution-date and --input-path are required")
    return args


if __name__ == "__main__":
    a = parse_args(sys.argv[1:])
    _self_check() if a.self_test else run(a)
