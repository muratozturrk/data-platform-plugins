# spark_streaming_static_joiner.py
#
# Static Joiner -- Spark Structured Streaming
# Reference loaded once at stream start as a static DataFrame.
# Applied to every micro-batch via foreachBatch broadcast join.
# Reference reloaded from Delta every REFERENCE_RELOAD_INTERVAL_BATCHES
# to absorb reference updates without restarting the stream.
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
#                io.delta:delta-core_2.12:2.4.0 \
#     spark_streaming_static_joiner.py
#
# Requirements: delta-spark, pyspark >= 3.4

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("StreamingStaticJoiner") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -- Config ------------------------------------------------------------------
KAFKA_BROKERS        = "localhost:9092"
SOURCE_TOPIC         = "silver.clickstream"
OUTPUT_PATH          = "/data/gold/clickstream_enriched"
CHECKPOINT_PATH      = "/data/checkpoints/streaming_static_joiner"
AUDIT_PATH           = "/data/audit/static_joiner_streaming"

REFERENCE_PATH       = "/data/dim/products"
REFERENCE_JOIN_KEY   = "product_id"
PRIMARY_JOIN_KEY     = "product_id"
REFERENCE_COLS       = ["product_name", "category", "brand", "price_tier"]

# Reload reference every N batches to absorb dimension updates
# 120 batches * 30 seconds/batch = ~1 hour between reference reloads
REFERENCE_RELOAD_INTERVAL_BATCHES = 120

EVENT_SCHEMA_STR = """
    product_id  STRING,
    user_id     STRING,
    event_type  STRING,
    event_time  TIMESTAMP,
    page        STRING
"""


# -- Reference state (module-level, shared across foreachBatch calls) --------
class ReferenceState:
    """
    Holds the current reference broadcast DataFrame.
    Reloaded every REFERENCE_RELOAD_INTERVAL_BATCHES batches.
    Thread-safe: foreachBatch is called sequentially by Spark;
    no concurrent access to _ref_df.
    """
    def __init__(self):
        self._ref_df:      DataFrame = None
        self._ref_version: int       = -1
        self._last_loaded_batch: int = -1

    def get_or_reload(
        self,
        batch_id: int,
        force:    bool = False,
    ) -> tuple[DataFrame, int]:
        """
        Returns current reference DataFrame.
        Reloads from Delta if:
          - Never loaded (first batch)
          - batch_id - last_loaded_batch >= REFERENCE_RELOAD_INTERVAL_BATCHES
          - force=True
        """
        should_reload = (
            self._ref_df is None
            or force
            or (batch_id - self._last_loaded_batch)
               >= REFERENCE_RELOAD_INTERVAL_BATCHES
        )

        if should_reload:
            keep_cols = list(
                set([REFERENCE_JOIN_KEY] + REFERENCE_COLS)
            )
            new_ref = (
                spark.read.format("delta").load(REFERENCE_PATH)
                .select(*keep_cols)
                .dropDuplicates([REFERENCE_JOIN_KEY])
            )
            new_version = (
                DeltaTable.forPath(spark, REFERENCE_PATH)
                .history(1).collect()[0]["version"]
            )

            if new_version != self._ref_version:
                # cache: the DataFrame is lazy, without it every micro-batch re-scans the table
                new_ref = new_ref.cache()
                if self._ref_df is not None:
                    self._ref_df.unpersist()
                self._ref_df      = new_ref
                self._ref_version = new_version
                self._last_loaded_batch = batch_id
                log.info(
                    "Reference reloaded | batch=%d "
                    "delta_v=%d rows=%d",
                    batch_id, new_version, new_ref.count(),
                )
            else:
                # Same version -- no need to replace broadcast
                self._last_loaded_batch = batch_id
                log.debug(
                    "Reference unchanged at delta_v=%d -- "
                    "reusing existing broadcast.",
                    new_version,
                )

        return self._ref_df, self._ref_version


# Module-level reference state -- persists across foreachBatch invocations
_ref_state = ReferenceState()


# -- foreachBatch: static join per micro-batch ------------------------------
def enrich_batch(batch_df, batch_id: int):
    """
    Applies broadcast static join to one micro-batch.
    Reference reloaded from Delta every
    REFERENCE_RELOAD_INTERVAL_BATCHES batches.
    """
    if batch_df.isEmpty():
        return

    ref_df, ref_version = _ref_state.get_or_reload(batch_id)

    enriched = batch_df.join(
        F.broadcast(ref_df),
        on  = batch_df[PRIMARY_JOIN_KEY] ==
              ref_df[REFERENCE_JOIN_KEY],
        how = "left",
    ).drop(ref_df[REFERENCE_JOIN_KEY])

    count = enriched.count()

    if DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
        (
            DeltaTable.forPath(spark, OUTPUT_PATH).alias("t")
            .merge(
                enriched.alias("s"),
                "t.product_id = s.product_id "
                "AND t.user_id = s.user_id "
                "AND t.event_time = s.event_time"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched.write.format("delta") \
                .mode("overwrite").save(OUTPUT_PATH)

    # Lightweight per-batch audit
    spark.createDataFrame([{
        "batch_id":        batch_id,
        "reference_version": ref_version,
        "output_rows":     count,
        "run_at":          datetime.now(tz=timezone.utc).isoformat(),
    }]).write.format("delta").mode("append") \
      .option("mergeSchema", "true").save(AUDIT_PATH)

    log.info(
        "Batch %d enriched | rows=%d ref_v=%d",
        batch_id, count, ref_version,
    )


# -- Read stream -------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast("string"),
            EVENT_SCHEMA_STR,
        ).alias("e")
    )
    .select("e.*")
    .filter(F.col(PRIMARY_JOIN_KEY).isNotNull())
)

# -- Write enriched stream ---------------------------------------------------
query = (
    parsed.writeStream
    .foreachBatch(enrich_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

log.info("Streaming static joiner started.")
query.awaitTermination()
