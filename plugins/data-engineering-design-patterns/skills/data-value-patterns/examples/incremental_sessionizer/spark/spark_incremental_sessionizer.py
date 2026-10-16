"""Incremental Sessionizer -- PySpark batch over hourly partitions.

Three storage spaces:
  input              {input_path}/date=YYYY-MM-DD/hour=HH/   raw visits (JSON)
  pending sessions   Delta, PRIVATE, partitioned by execution_time_id; internal format
                     (pages with timestamps, expiration_batch_id)
  completed sessions Delta, PUBLIC, partitioned by execution_time_id; consumer format
                     (ordered page list, duration, is_completed flag)

One run = one execution_time_id (the orchestrator's immutable hourly id):
  1. clean-sessions / clean-pending: delete everything this AND later runs wrote
     (execution_time_id >= current). Sessions are forward dependent, so a rerun of
     hour H invalidates H+1, H+2, ....
  2. generate:
     a. load the hourly input once and persist it (the SQL version's temp table);
     b. shape new visits like pending sessions (GROUP BY visit_id, user_id);
     c. FULL OUTER JOIN with the pending sessions of exactly the previous run ->
        new session | restored session with new data | restored session without data;
     d. initialization (first visit), accumulation (ordered pages), finalization
        (closing page, or no data for INACTIVITY_PARTITIONS runs ->
        expiration_batch_id <= current);
     e. write still-open sessions to pending and finished ones to the public table
        (optionally also partial sessions flagged is_completed=false).
     Both writes use replaceWhere on the current execution_time_id, so a retry
     overwrites instead of appending.

Run (normally from airflow/incremental_sessionizer_dag.py):
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_incremental_sessionizer.py \
      --action generate --execution-time 2024-01-15T10:00
  python spark_incremental_sessionizer.py --self-test         # pure helpers, no Spark
Env: INPUT_PATH, PENDING_SESSIONS_PATH, SESSIONS_PATH, INACTIVITY_PARTITIONS,
     CLOSING_PAGES (comma list), EMIT_PARTIAL_SESSIONS.
"""
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("incremental_sessionizer")

EXECUTION_ID_FORMAT = "%Y-%m-%dT%H:00"    # fixed width -> string order == time order
INPUT_DDL = "visit_id STRING, user_id STRING, event_time TIMESTAMP, page STRING"


@dataclass(frozen=True)
class SessionizerConfig:
    input_path: str = os.environ.get("INPUT_PATH", "/data/bronze/visits")
    pending_path: str = os.environ.get("PENDING_SESSIONS_PATH", "/data/internal/pending_sessions")
    sessions_path: str = os.environ.get("SESSIONS_PATH", "/data/silver/sessions")
    partition_hours: int = 1
    # Problem statement: a session ends after 2 h without visits -> 2 hourly runs without data.
    inactivity_partitions: int = int(os.environ.get("INACTIVITY_PARTITIONS", "2"))
    closing_pages: tuple = field(default_factory=lambda: tuple(
        p for p in os.environ.get("CLOSING_PAGES", "logout").split(",") if p))
    emit_partial_sessions: bool = os.environ.get("EMIT_PARTIAL_SESSIONS", "false").lower() == "true"


# -- Pure helpers (self-tested) -----------------------------------------------------
def parse_execution_id(execution_id: str) -> datetime:
    return datetime.strptime(execution_id, EXECUTION_ID_FORMAT)


def shift_execution_id(execution_id: str, partitions: int, partition_hours: int = 1) -> str:
    """Airflow macros.ds_add(ds, N) equivalent for hourly runs."""
    shifted = parse_execution_id(execution_id) + timedelta(hours=partitions * partition_hours)
    return shifted.strftime(EXECUTION_ID_FORMAT)


def input_partition_path(base: str, execution_id: str) -> str:
    dt = parse_execution_id(execution_id)
    return f"{base}/date={dt:%Y-%m-%d}/hour={dt:%H}"


@dataclass(frozen=True)
class RunContext:
    current: str
    previous: str
    expiration_if_active: str

    @classmethod
    def of(cls, execution_id: str, cfg: SessionizerConfig) -> "RunContext":
        parse_execution_id(execution_id)     # validate early: a bad id would silently match nothing
        return cls(current=execution_id,
                   previous=shift_execution_id(execution_id, -1, cfg.partition_hours),
                   expiration_if_active=shift_execution_id(execution_id, cfg.inactivity_partitions,
                                                           cfg.partition_hours))


# -- Spark --------------------------------------------------------------------------
def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("incremental-sessionizer")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def clean_from(spark, path: str, run: RunContext) -> None:
    """Idempotency: drop this and every later run's output (forward dependency)."""
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, path):
        log.info("clean skipped, no table yet | path=%s", path)
        return
    DeltaTable.forPath(spark, path).delete(f"execution_time_id >= '{run.current}'")
    log.info("cleaned execution_time_id >= %s | path=%s", run.current, path)


def load_input(spark, cfg: SessionizerConfig, run: RunContext):
    """An empty or missing hour still has to run: pending sessions must expire."""
    from pyspark.errors import AnalysisException
    from pyspark import StorageLevel
    path = input_partition_path(cfg.input_path, run.current)
    try:
        visits = spark.read.schema(INPUT_DDL).json(path)
    except AnalysisException:
        log.warning("input partition missing, running with empty input | path=%s", path)
        visits = spark.createDataFrame([], INPUT_DDL)
    visits = visits.filter("visit_id IS NOT NULL AND event_time IS NOT NULL").persist(StorageLevel.MEMORY_AND_DISK)
    log.info("input loaded | path=%s rows=%d", path, visits.count())
    return visits


def load_pending(spark, cfg: SessionizerConfig, run: RunContext):
    """Only the previous run's pending sessions: reading 'all pending' would double count."""
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, cfg.pending_path):
        return None
    # ponytail: trusts the orchestrator (depends_on_past) that run `previous` happened;
    # add a run-marker table if runs can be skipped manually.
    return spark.read.format("delta").load(cfg.pending_path).filter(f"execution_time_id = '{run.previous}'")


def new_sessions_from_input(visits, cfg: SessionizerConfig):
    """New data shaped like a pending session (WINDOW/GROUP BY on visit_id, user_id)."""
    from pyspark.sql import functions as F
    closing = F.col("page").isin(list(cfg.closing_pages)) if cfg.closing_pages else F.lit(False)
    return (visits.groupBy("visit_id", "user_id")
            .agg(F.min("event_time").alias("start_time"),
                 F.max("event_time").alias("last_visit_time"),
                 F.collect_list(F.struct("event_time", "page")).alias("pages"),
                 F.max(closing.cast("int")).cast("boolean").alias("has_closing_page"))
            .withColumnRenamed("visit_id", "session_id"))


def classify_sessions(new_df, pending_df, run: RunContext):
    """FULL OUTER JOIN + initialization / accumulation / finalization."""
    from pyspark.sql import functions as F
    page_type = "ARRAY<STRUCT<event_time: TIMESTAMP, page: STRING>>"
    if pending_df is None:
        pending_df = new_df.sparkSession.createDataFrame(
            [], f"session_id STRING, user_id STRING, start_time TIMESTAMP, last_visit_time TIMESTAMP, "
                f"pages {page_type}, expiration_batch_id STRING, execution_time_id STRING")
    n, p = new_df.alias("n"), pending_df.alias("p")
    empty_pages = F.expr(f"CAST(array() AS {page_type})")
    joined = n.join(p, F.col("n.session_id") == F.col("p.session_id"), "full_outer")
    return joined.select(
        F.coalesce("p.session_id", "n.session_id").alias("session_id"),
        F.coalesce("p.user_id", "n.user_id").alias("user_id"),
        F.least("p.start_time", "n.start_time").alias("start_time"),          # least/greatest skip NULLs
        F.greatest("p.last_visit_time", "n.last_visit_time").alias("last_visit_time"),
        # array_distinct: duplicated input events must not inflate the session
        F.array_sort(F.array_distinct(F.concat(F.coalesce("p.pages", empty_pages),
                                               F.coalesce("n.pages", empty_pages)))).alias("pages"),
        F.when(F.col("n.session_id").isNull(), F.col("p.expiration_batch_id"))   # restored, no new data
         .when(F.col("n.has_closing_page"), F.lit(run.current))                  # finalization by event
         .otherwise(F.lit(run.expiration_if_active)).alias("expiration_batch_id"),  # new or extended
        F.when(F.col("p.session_id").isNull(), F.lit("new"))
         .when(F.col("n.session_id").isNull(), F.lit("restored_without_data"))
         .otherwise(F.lit("restored_with_data")).alias("_origin"),
    )


def to_public(df, is_completed: bool, run: RunContext):
    """Public contract differs from the private pending format."""
    from pyspark.sql import functions as F
    return df.select(
        "session_id", "user_id", "start_time", F.col("last_visit_time").alias("end_time"),
        F.transform("pages", lambda x: x["page"]).alias("pages"),
        (F.col("last_visit_time").cast("long") - F.col("start_time").cast("long")).alias("duration_seconds"),
        F.lit(is_completed).alias("is_completed"),
        F.lit(run.current).alias("execution_time_id"))


def write_partition(df, path: str, run: RunContext) -> None:
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"execution_time_id = '{run.current}'")
     .partitionBy("execution_time_id").save(path))


def generate(spark, cfg: SessionizerConfig, run: RunContext) -> dict:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    visits = load_input(spark, cfg, run)
    classified = classify_sessions(new_sessions_from_input(visits, cfg), load_pending(spark, cfg, run), run)
    classified = classified.persist(StorageLevel.MEMORY_AND_DISK)     # read twice by the two writes

    completed = classified.filter(F.col("expiration_batch_id") <= run.current)   # <= also catches overdue ones
    still_open = classified.filter(F.col("expiration_batch_id") > run.current)

    write_partition(still_open.drop("_origin").withColumn("execution_time_id", F.lit(run.current)),
                    cfg.pending_path, run)
    public = to_public(completed, True, run)
    if cfg.emit_partial_sessions:
        public = public.unionByName(to_public(still_open, False, run))
    write_partition(public, cfg.sessions_path, run)

    stats = {r["_origin"]: r["count"] for r in classified.groupBy("_origin").count().collect()}
    stats.update(completed=completed.count(), pending=still_open.count())
    log.info("run=%s previous=%s stats=%s", run.current, run.previous, stats)
    classified.unpersist()
    visits.unpersist()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["clean-sessions", "clean-pending", "generate", "all"])
    parser.add_argument("--execution-time", help=f"orchestrator run id, format {EXECUTION_ID_FORMAT}")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_check()
        return
    if not (args.action and args.execution_time):
        parser.error("--action and --execution-time are required")

    cfg = SessionizerConfig()
    run = RunContext.of(args.execution_time, cfg)
    spark = get_spark()
    try:
        if args.action in ("clean-sessions", "all"):
            clean_from(spark, cfg.sessions_path, run)
        if args.action in ("clean-pending", "all"):
            clean_from(spark, cfg.pending_path, run)
        if args.action in ("generate", "all"):
            generate(spark, cfg, run)
    finally:
        spark.stop()


def _self_check() -> None:
    cfg = SessionizerConfig(inactivity_partitions=2)
    run = RunContext.of("2024-01-15T23:00", cfg)
    assert run.previous == "2024-01-15T22:00"
    assert run.expiration_if_active == "2024-01-16T01:00"        # crosses midnight
    assert shift_execution_id("2024-03-01T00:00", -1) == "2024-02-29T23:00"
    assert input_partition_path("/in", "2024-01-05T07:00") == "/in/date=2024-01-05/hour=07"
    # String comparison used by DELETE/filters must follow time order.
    assert shift_execution_id("2024-01-15T09:00", 1) > "2024-01-15T09:00" > "2024-01-14T23:00"
    try:
        RunContext.of("2024-01-15", cfg)
        raise AssertionError("invalid id accepted")
    except ValueError:
        pass
    print("self-check OK")


if __name__ == "__main__":
    main()
