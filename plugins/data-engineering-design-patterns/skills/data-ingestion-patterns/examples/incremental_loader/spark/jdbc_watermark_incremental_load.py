# =============================================================================
# jdbc_watermark_incremental_load.py
#
# Incremental Loader, delta-column implementation with a remembered watermark
# ("last ingestion time = 10:00 ... set new last ingestion time to 10:02").
# Handles: watermark state table, look-back buffer, BOUNDED window per run (limits the
# compute needs of a backfill), dynamic JDBC partition bounds for parallel reads, dedup, Delta MERGE
# (rows may be updated), watermark persisted only after the MERGE.
#
# Hard deletes are invisible to this loader: ask for soft deletes or use CDC.
#
# Run: spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.3 #        jdbc_watermark_incremental_load.py      (DB_PASSWORD env var)
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional, Tuple

try:
    import psycopg2
    import psycopg2.extras
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType
except ImportError:          # lets `--self-test` run without Spark/psycopg2 installed
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# =============================================================================
# CONFIG — centralise all tunables in one place
# =============================================================================

@dataclass
class LoadConfig:
    # Source
    source_jdbc_url: str
    source_table: str
    source_user: str
    source_password: str

    # Target (Delta Lake)
    delta_table_path: str  # e.g. "/data/warehouse/orders"

    # State store (where watermarks are saved)
    state_jdbc_url: str
    state_table: str       # e.g. "incremental_state"
    state_user: str
    state_password: str

    # Partitioning
    partition_column: str  # must be numeric, e.g. "id"
    num_partitions: int = 16
    fetch_size: int = 10_000  # rows per JDBC fetch per partition

    # Watermark
    lookback_hours: int = 2              # late-data buffer
    bootstrap_days: int = 7             # how far back on first ever run
    # Upper bound of one run's window. Never use a row LIMIT instead: an unordered limit followed by
    # max(updated_at) as the new watermark silently skips the rows that were cut off.
    max_window_hours: int = 24

    # Delta MERGE key
    merge_key: str = "id"
    source_driver: str = "org.postgresql.Driver"


# =============================================================================
# WATERMARK STORE — reads and writes checkpoints to a Postgres state table
# =============================================================================
# DDL for the state table (run once):
#
#   CREATE TABLE incremental_state (
#       table_name      TEXT PRIMARY KEY,
#       last_watermark  TIMESTAMPTZ NOT NULL,
#       row_count       BIGINT,
#       updated_at      TIMESTAMPTZ DEFAULT NOW()
#   );

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def jdbc_to_libpq(url: str) -> str:
    """jdbc:postgresql://h:5432/db -> postgresql://h:5432/db (a URI psycopg2/libpq accepts)."""
    return url[len("jdbc:"):] if url.startswith("jdbc:") else url


def compute_window(watermark: datetime, lookback_hours: int, max_window_hours: int,
                   now: datetime) -> Tuple[datetime, datetime]:
    """(start, end] of this run: look back for late rows, never more than max_window_hours ahead."""
    if max_window_hours <= lookback_hours:
        raise ValueError("max_window_hours must exceed lookback_hours or the loader never advances")
    start = watermark - timedelta(hours=lookback_hours)
    return start, min(start + timedelta(hours=max_window_hours), now)


def _self_check() -> None:
    now = datetime(2024, 1, 10, tzinfo=timezone.utc)
    s, e = compute_window(datetime(2024, 1, 1, tzinfo=timezone.utc), 2, 24, now)
    assert (e - s) == timedelta(hours=24) and s.hour == 22
    s, e = compute_window(now - timedelta(hours=1), 2, 24, now)
    assert e == now
    assert jdbc_to_libpq("jdbc:postgresql://db:5432/x") == "postgresql://db:5432/x"
    try:
        compute_window(now, 2, 2, now)
        raise AssertionError("window <= lookback must fail")
    except ValueError:
        pass
    print("self-test OK")


class WatermarkStore:
    def __init__(self, cfg: LoadConfig):
        self._conn_kwargs = dict(
            dsn=jdbc_to_libpq(cfg.state_jdbc_url),
            user=cfg.state_user,
            password=cfg.state_password
        )
        self._table = cfg.state_table

    def _connect(self):
        return psycopg2.connect(**self._conn_kwargs)

    def get(self, table_name: str, bootstrap_days: int) -> datetime:
        """Return last saved watermark, or a bootstrap date if first run."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT last_watermark FROM {self._table} "
                    "WHERE table_name = %s",
                    (table_name,)
                )
                row = cur.fetchone()

        if row:
            wm = row[0]
            log.info("📍 Found watermark for '%s': %s", table_name, wm)
            return as_utc(wm)
        else:
            wm = utc_now() - timedelta(days=bootstrap_days)
            log.info(
                "📍 No watermark for '%s'. Bootstrapping to %s",
                table_name, wm
            )
            return wm

    def save(self, table_name: str, watermark: datetime, row_count: int):
        """Upsert the new watermark — only called after successful load."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._table}
                        (table_name, last_watermark, row_count, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (table_name) DO UPDATE
                        SET last_watermark = EXCLUDED.last_watermark,
                            row_count      = EXCLUDED.row_count,
                            updated_at     = NOW()
                    """,
                    (table_name, watermark, row_count)
                )
            conn.commit()
        log.info("✅ Watermark saved: %s (rows: %d)", watermark, row_count)


# =============================================================================
# PARTITION BOUNDS — query source to get the actual min/max id in the window
# =============================================================================

def get_partition_bounds(
    cfg: LoadConfig,
    effective_watermark: datetime,
    window_end: datetime
) -> Tuple[int, int]:
    """
    Query source for the actual min and max partition_column values
    within the incremental window. This avoids over-partitioning a
    sparse delta (e.g. only 10k new rows don't need bounds of 0–100M).
    """
    query = f"""
        SELECT
            MIN({cfg.partition_column}) AS lower_bound,
            MAX({cfg.partition_column}) AS upper_bound
        FROM {cfg.source_table}
        WHERE updated_at > %s AND updated_at <= %s
    """
    conn = psycopg2.connect(
        jdbc_to_libpq(cfg.source_jdbc_url),
        user=cfg.source_user,
        password=cfg.source_password
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, (effective_watermark, window_end))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or row[0] is None:
        return (0, 0)  # empty window

    lower, upper = int(row[0]), int(row[1])
    log.info(
        "📐 Partition bounds: lower=%d, upper=%d (range=%d)",
        lower, upper, upper - lower
    )
    return lower, upper


# =============================================================================
# EXTRACT — parallel partitioned JDBC read
# =============================================================================

def extract_incremental(
    spark: SparkSession,
    cfg: LoadConfig,
    effective_watermark: datetime,
    window_end: datetime,
    lower_bound: int,
    upper_bound: int
) -> DataFrame:
    """
    Read only changed rows using N parallel JDBC connections.
    Each Spark partition reads a separate id range — no single connection
    bottleneck, no full table scan.
    """
    # Subquery: only rows changed since watermark
    # The alias is required by Spark JDBC
    subquery = f"""
        (
            SELECT *
            FROM {cfg.source_table}
            WHERE updated_at > '{effective_watermark.isoformat()}'
              AND updated_at <= '{window_end.isoformat()}'
        ) AS incremental_data
    """

    log.info(
        "🔄 Starting partitioned extract | partitions=%d | fetchsize=%d",
        cfg.num_partitions, cfg.fetch_size
    )

    df = (
        spark.read
        .format("jdbc")
        .option("url",             cfg.source_jdbc_url)
        .option("dbtable",         subquery)
        .option("user",            cfg.source_user)
        .option("password",        cfg.source_password)
        .option("driver",          cfg.source_driver)
        # ── Partitioning options ──────────────────────────────────────
        .option("partitionColumn", cfg.partition_column)
        .option("lowerBound",      str(lower_bound))
        .option("upperBound",      str(upper_bound))
        .option("numPartitions",   str(cfg.num_partitions))
        # ── Performance tuning ────────────────────────────────────────
        .option("fetchsize",       str(cfg.fetch_size))
        # Push the WHERE clause into the DB — don't pull and filter in Spark
        .option("pushDownPredicate", "true")
        .load()
    )

    return df


# =============================================================================
# TRANSFORM — clean, deduplicate, add audit columns
# =============================================================================

def transform(df: DataFrame, merge_key: str) -> DataFrame:
    """
    1. Deduplicate: keep the latest version of each record (by updated_at).
       This handles the case where the same id appears in multiple partitions
       due to the look-back buffer overlap.
    2. Add audit columns.
    """
    from pyspark.sql.window import Window

    # Deduplication window: per merge_key, latest updated_at wins
    window = Window.partitionBy(merge_key).orderBy(F.desc("updated_at"))

    deduped_df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Add audit columns
    result_df = (
        deduped_df
        .withColumn("loaded_at",    F.current_timestamp())
        .withColumn("load_date",    F.current_date())
    )

    return result_df


# =============================================================================
# LOAD — Delta Lake MERGE (upsert)
# =============================================================================

def upsert_to_delta(
    spark: SparkSession,
    df: DataFrame,
    cfg: LoadConfig
) -> int:
    """
    Merge incoming records into the Delta table.
    - MATCHED:     update all columns
    - NOT MATCHED: insert new record
    Returns the number of rows written.
    """
    row_count = df.count()
    log.info("📦 Upserting %d rows into Delta table: %s",
             row_count, cfg.delta_table_path)

    df.createOrReplaceTempView("incoming_data")

    # Build the SET clause dynamically from the dataframe schema
    # (avoids hardcoding column names)
    set_clause = ", ".join([
        f"target.{col} = source.{col}"
        for col in df.columns
        if col != cfg.merge_key
    ])

    merge_sql = f"""
        MERGE INTO delta.`{cfg.delta_table_path}` AS target
        USING incoming_data AS source
          ON target.{cfg.merge_key} = source.{cfg.merge_key}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT *
    """

    spark.sql(merge_sql)
    log.info("✅ MERGE complete")
    return row_count


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_partitioned_incremental_load(
    spark: SparkSession,
    cfg: LoadConfig
):
    log.info("=" * 60)
    log.info("🚀 Starting Partitioned Incremental Load: %s", cfg.source_table)
    log.info("=" * 60)

    state_store = WatermarkStore(cfg)

    # ── Step 1: Get watermark ─────────────────────────────────────────
    raw_watermark = state_store.get(cfg.source_table, cfg.bootstrap_days)
    effective_watermark, window_end = compute_window(
        raw_watermark, cfg.lookback_hours, cfg.max_window_hours, utc_now())
    log.info("⏱  Window (%s, %s]", effective_watermark, window_end)

    # ── Step 2: Get partition bounds ──────────────────────────────────
    lower_bound, upper_bound = get_partition_bounds(cfg, effective_watermark, window_end)

    if lower_bound == upper_bound == 0:
        log.info("✅ No new data in window.")
        if window_end > raw_watermark:
            # an empty capped window (e.g. an outage gap) must still move the watermark forward
            state_store.save(cfg.source_table, window_end, 0)
        return

    # ── Step 3: Partitioned extract ───────────────────────────────────
    raw_df = extract_incremental(
        spark, cfg, effective_watermark, window_end, lower_bound, upper_bound
    )

    # ── Step 5: Transform + deduplicate ──────────────────────────────
    clean_df = transform(raw_df, cfg.merge_key)

    # Cache because we need count() AND the MERGE read the DF twice
    clean_df.cache()

    # ── Step 6: Upsert into Delta Lake ────────────────────────────────
    row_count = upsert_to_delta(spark, clean_df, cfg)

    # ── Step 7: Get new watermark from this batch ────────────────────
    new_watermark = (
        clean_df
        .agg(F.max("updated_at").cast(TimestampType()))
        .collect()[0][0]
    )

    clean_df.unpersist()

    # ── Step 8: Save watermark ONLY after successful load ─────────────
    state_store.save(cfg.source_table, new_watermark, row_count)

    log.info("🏁 Load complete | rows=%d | new_watermark=%s",
             row_count, new_watermark)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys
    if "--self-test" in sys.argv:
        _self_check()
        sys.exit(0)

    spark = (
        SparkSession.builder
        .appName("PartitionedIncrementalLoad")
        # Tune shuffle partitions to match your cluster
        .config("spark.sql.shuffle.partitions", "32")
        # Enable Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Avoid OOM on large JDBC partitions
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
        .getOrCreate()
    )

    cfg = LoadConfig(
        # Source
        source_jdbc_url  = "jdbc:postgresql://source-db:5432/mydb",
        source_table     = "orders",
        source_user      = "reader",
        source_password  = os.environ["DB_PASSWORD"],

        # Target
        delta_table_path = "/data/warehouse/orders",

        # State store
        state_jdbc_url   = "jdbc:postgresql://state-db:5432/pipeline_state",
        state_table      = "incremental_state",
        state_user       = "pipeline",
        state_password   = os.environ["DB_PASSWORD"],

        # Partition tuning
        partition_column = "id",
        num_partitions   = 16,      # tune to your cluster's executor count
        fetch_size       = 10_000,

        # Watermark
        lookback_hours   = 2,
        bootstrap_days   = 7,

        # Bounded window: a backfill after a long outage is processed in 24h slices
        max_window_hours = 24,

        # Delta merge key
        merge_key        = "id",
    )

    run_partitioned_incremental_load(spark, cfg)
    spark.stop()