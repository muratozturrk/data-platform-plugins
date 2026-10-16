# spark_keyed_idempotency_batch.py
#
# Keyed Idempotency -- Batch Mode with Delta MERGE
#
# Derives a deterministic idempotency key per row from:
#   source_name + partition_date + primary_key_value
# Uses Delta MERGE ON idempotency_key to guarantee exactly-one
# output row per logical event regardless of retry count.
#
# Idempotency key column: _idem_key (SHA-256, hex, 64 chars)
#
# Keys come from IMMUTABLE attributes only. Two modes:
#   --key-mode pk               natural PK (+ extra cols), one row per record.
#   --key-mode first-ingestion  group key (e.g. user_id) + the FIRST ingestion
#                               time of the group:
#       ... OVER (PARTITION BY user_id ORDER BY ingestion_time ASC, visit_time ASC)
#     Ascending order on an append-only ingestion time means late rows never
#     change the first row, so the key survives reruns with late data.
# MERGE (not INSERT) because Delta/RDBMS tables do not overwrite on key
# collision the way Cassandra/ScyllaDB do.
#
# Run:
#   spark-submit \
#     --packages io.delta:delta-spark_2.12:3.2.0 \
#     spark_keyed_idempotency_batch.py \
#     --source-path /data/silver/orders \
#     --source-name orders_silver \
#     --processing-date 2024-01-15
#
# Requirements: delta-spark, pyspark >= 3.3

import argparse
import hashlib
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class IdempotencyConfig:
    output_path:      str
    audit_path:       str
    source_name:      str
    # Column(s) that uniquely identify a logical record within the source
    primary_key_cols: list[str] = field(
        default_factory=lambda: ["order_id"]
    )
    # Additional columns to include in the key hash (optional -- add
    # event_type or version if the same PK can produce multiple logical events)
    hash_extra_cols:  list[str] = field(
        default_factory=lambda: []
    )
    # Column holding the record's processing date for partitioning
    partition_date_col: str = "_ingestion_date"
    # "pk" or "first-ingestion"
    key_mode:           str = "pk"
    group_col:          str = "user_id"
    ingestion_time_col: str = "ingestion_time"
    event_time_col:     str = "visit_time"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("KeyedIdempotencyBatch")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


# -- Step 1: Derive idempotency key ------------------------------------------
def add_idempotency_key(
    df:              DataFrame,
    source_name:     str,
    processing_date: str,
    cfg:             IdempotencyConfig,
) -> DataFrame:
    """
    Adds _idem_key column: SHA-256 hex of
      source_name || processing_date || pk_col_1 || ... || pk_col_N
      || extra_col_1 || ...
    All components cast to STRING and concatenated with '|' separator.
    SHA-256 chosen over MD5: negligible collision probability across
    billions of rows; no performance difference at Spark scale.
    """
    key_components = (
        [F.lit(source_name), F.lit(processing_date)]
        + [F.col(c).cast(StringType()) for c in cfg.primary_key_cols]
        + [F.col(c).cast(StringType()) for c in cfg.hash_extra_cols]
    )

    # Concatenate with separator to prevent component boundary collisions
    # e.g., pk="12" + pk="3" != pk="1" + pk="23"
    concat_expr = F.concat_ws("|", *key_components)

    return df.withColumn(
        "_idem_key",
        F.sha2(concat_expr, 256)
    )


def add_first_ingestion_key(df: DataFrame, cfg: IdempotencyConfig) -> DataFrame:
    """
    _idem_key = SHA-256(group || first ingestion time of the group).
    The first row is picked with an ascending window on the immutable
    ingestion time; event time is only a tie-breaker, never the anchor.
    """
    w = (Window.partitionBy(cfg.group_col)
         .orderBy(F.col(cfg.ingestion_time_col).asc(),
                  F.col(cfg.event_time_col).asc()))
    first_ingestion = F.first(F.col(cfg.ingestion_time_col)).over(
        w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    # Keep the group's first recorded activity: deterministic row per key,
    # so a rerun MERGEs identical content.
    return (
        df.withColumn(
            "_idem_key",
            F.sha2(F.concat_ws("|", F.col(cfg.group_col).cast(StringType()),
                               first_ingestion.cast(StringType())), 256))
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# -- Step 2: Ingest and key the arriving batch --------------------------------
def ingest_and_key(
    spark:           SparkSession,
    source_path:     str,
    processing_date: str,
    cfg:             IdempotencyConfig,
) -> DataFrame:
    """
    Reads the Silver partition, derives idempotency key per row,
    adds pipeline metadata columns.
    """
    df = (
        spark.read.format("delta").load(source_path)
        .filter(F.col(cfg.partition_date_col) == processing_date)
    )

    count = df.count()
    log.info(
        "Ingested %d rows | source=%s date=%s",
        count, cfg.source_name, processing_date,
    )

    # Validate: all primary key columns must be non-null
    key_cols = ([cfg.group_col, cfg.ingestion_time_col]
                if cfg.key_mode == "first-ingestion" else cfg.primary_key_cols)
    for pk_col in key_cols:
        null_count = df.filter(F.col(pk_col).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Primary key column '{pk_col}' has {null_count} null "
                f"rows in {processing_date}. Cannot derive idempotency key."
            )

    if cfg.key_mode == "first-ingestion":
        keyed = add_first_ingestion_key(df, cfg)
    else:
        keyed = add_idempotency_key(df, cfg.source_name, processing_date, cfg)
    keyed = keyed.withColumn("_processed_at", F.current_timestamp())

    # Dedup within the arriving batch itself (upstream may send duplicates)
    before = keyed.count()
    keyed  = keyed.dropDuplicates(["_idem_key"])
    after  = keyed.count()

    if before != after:
        log.warning(
            "Intra-batch duplicates removed: %d -> %d rows "
            "| source=%s date=%s",
            before, after, cfg.source_name, processing_date,
        )

    return keyed


# -- Step 3: Check existing keys in destination ------------------------------
def count_existing_keys(
    spark:    SparkSession,
    keyed_df: DataFrame,
    cfg:      IdempotencyConfig,
) -> int:
    """
    Counts how many idempotency keys in the arriving batch already exist
    in the output table. Used for audit and to detect unexpected re-runs.
    Returns 0 if the output table does not yet exist.
    """
    if not DeltaTable.isDeltaTable(spark, cfg.output_path):
        return 0

    incoming_keys = keyed_df.select("_idem_key")
    existing      = (
        spark.read.format("delta").load(cfg.output_path)
        .select("_idem_key")
        .join(incoming_keys, on="_idem_key", how="inner")
    )
    count = existing.count()
    if count > 0:
        log.info(
            "Pre-existing keys in destination: %d "
            "(legitimate retry or backfill).",
            count,
        )
    return count


# -- Step 4: Write via Delta MERGE -------------------------------------------
def write_idempotent(
    spark:    SparkSession,
    keyed_df: DataFrame,
    cfg:      IdempotencyConfig,
) -> dict:
    """
    Upserts keyed_df into the Delta output table using MERGE ON _idem_key.
    - whenMatched: update all columns (handles corrections / late updates)
    - whenNotMatched: insert
    Result: exactly one row per _idem_key in the output, regardless of
    how many times this function is called with the same input.
    """
    count = keyed_df.count()
    if count == 0:
        log.info("No rows to write.")
        return {"written": 0, "existing_keys": 0}

    existing_keys = count_existing_keys(spark, keyed_df, cfg)

    if DeltaTable.isDeltaTable(spark, cfg.output_path):
        (
            DeltaTable.forPath(spark, cfg.output_path).alias("target")
            .merge(
                keyed_df.alias("source"),
                "target._idem_key = source._idem_key"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        keyed_df.write.format("delta") \
                .mode("overwrite").save(cfg.output_path)

    history   = DeltaTable.forPath(spark, cfg.output_path).history(1).collect()[0]
    delta_ver = history["version"]

    log.info(
        "Write complete | rows=%d existing_keys=%d delta_v=%d",
        count, existing_keys, delta_ver,
    )
    return {
        "written":       count,
        "existing_keys": existing_keys,
        "delta_version": delta_ver,
    }


# -- Main --------------------------------------------------------------------
def run(
    source_path:     str,
    processing_date: str,
    cfg:             IdempotencyConfig,
):
    spark  = get_spark()
    log.info(
        "Keyed Idempotency Batch | source=%s date=%s",
        cfg.source_name, processing_date,
    )

    keyed_df = ingest_and_key(spark, source_path, processing_date, cfg)
    result   = write_idempotent(spark, keyed_df, cfg)

    spark.createDataFrame([{
        "source_name":     cfg.source_name,
        "processing_date": processing_date,
        **result,
        "run_at": datetime.now(tz=timezone.utc).isoformat(),
    }]).write.format("delta").mode("append") \
      .option("mergeSchema", "true").save(cfg.audit_path)

    log.info(
        "Run complete | source=%s written=%d existing=%d",
        cfg.source_name,
        result.get("written", 0),
        result.get("existing_keys", 0),
    )
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path",     required=True)
    parser.add_argument("--source-name",     required=True)
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--key-mode", choices=["pk", "first-ingestion"],
                        default="pk")
    args = parser.parse_args()

    cfg = IdempotencyConfig(
        output_path        = "/data/gold/orders",
        audit_path         = "/data/audit/idempotency",
        source_name        = args.source_name,
        primary_key_cols   = ["order_id"],
        hash_extra_cols    = ["event_type"],
        partition_date_col = "_ingestion_date",
        key_mode           = args.key_mode,
    )
    run(args.source_path, args.processing_date, cfg)