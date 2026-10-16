"""Dynamic Joiner -- stream-to-stream join with watermarks and a time-range condition.

Both datasets are in motion (visits and displayed ads, or visits and user CDC
events) and have different latencies. Spark buffers unmatched rows of each side
in the state store so the faster stream can wait for the slower one:

  * withWatermark on EACH side  -> GC watermark: buffered rows older than the
    watermark are evicted (space vs exactness trade-off)
  * time-range join condition   -> business rule ("an ad is displayed at most
    2 minutes after the visit") AND the bound that makes state eviction possible
  * left_outer                  -> visits without an ad are kept; their NULL row
    is emitted only AFTER the watermark passes event_time + range, so outer-join
    output latency is at least the watermark delay

Rows arriving later than the watermark lose their join. Track them
with the late-data patterns if they matter.
Alternative in Flink: temporal table join returns the ad version
valid at event_time (`update_time <= event_time`); Java/SQL only.

Sink: foreachBatch -> Delta append with txnAppId/txnVersion, so a micro-batch
replayed after a failure is skipped by Delta (idempotent write). Each batch
appends matched/unmatched counts to a JSONL stats file; the driver loop also
appends lastProgress (watermark, state rows) there. The Airflow health DAG
(airflow/dynamic_joiner_dag.py) reads that file.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
      spark_stream_stream_time_bounded_join.py --watermark "10 minutes" --max-display-delay "2 minutes"
Env: KAFKA_BOOTSTRAP, VISITS_TOPIC, ADS_TOPIC, OUTPUT_DIR, CHECKPOINT_DIR, STATS_FILE.
"""
import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("dynamic-joiner")

VISIT_SCHEMA = StructType([
    StructField("visit_id", StringType()), StructField("user_id", StringType()),
    StructField("page", StringType()), StructField("event_time", TimestampType())])
AD_SCHEMA = StructType([
    StructField("ad_id", StringType()), StructField("ad_page", StringType()),
    StructField("campaign", StringType()), StructField("display_time", TimestampType())])


@dataclass(frozen=True)
class JoinConfig:
    bootstrap: str
    visits_topic: str
    ads_topic: str
    output_dir: str
    checkpoint_dir: str
    stats_file: str
    app_id: str             # Delta txnAppId: must stay stable for the checkpoint's lifetime
    watermark: str          # GC watermark per side, sized from the measured latency gap
    max_display_delay: str  # business time boundary of the join
    trigger: str
    progress_interval_s: int

    @staticmethod
    def from_args() -> "JoinConfig":
        p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
        p.add_argument("--watermark", default=os.environ.get("JOIN_WATERMARK", "10 minutes"))
        p.add_argument("--max-display-delay", default=os.environ.get("MAX_DISPLAY_DELAY", "2 minutes"))
        p.add_argument("--trigger", default="30 seconds")
        p.add_argument("--progress-interval-s", type=int, default=60)
        a = p.parse_args()
        return JoinConfig(
            bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            visits_topic=os.environ.get("VISITS_TOPIC", "visits"),
            ads_topic=os.environ.get("ADS_TOPIC", "ads"),
            output_dir=os.environ.get("OUTPUT_DIR", "/data/silver/visits_with_ads"),
            checkpoint_dir=os.environ.get("CHECKPOINT_DIR", "/data/checkpoints/visits_with_ads"),
            stats_file=os.environ.get("STATS_FILE", "/data/stats/visits_with_ads.jsonl"),
            app_id=os.environ.get("DELTA_APP_ID", "dynamic-joiner-visits-ads"),
            watermark=a.watermark, max_display_delay=a.max_display_delay,
            trigger=a.trigger, progress_interval_s=a.progress_interval_s)


def append_stats(path: str, record: dict) -> None:
    """Driver-side append; foreachBatch and the poller both run on the driver."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    record = {"logged_at": datetime.now(timezone.utc).isoformat(), **record}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def read_topic(spark: SparkSession, cfg: JoinConfig, topic: str, schema: StructType) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg.bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())
    parsed = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
    # from_json yields NULL fields for malformed payloads; a NULL event time can never
    # be watermarked or matched, so drop it here (route to a dead-letter if needed).
    time_col = "event_time" if "event_time" in schema.fieldNames() else "display_time"
    return parsed.filter(F.col(time_col).isNotNull())


def build_join(visits: DataFrame, ads: DataFrame, cfg: JoinConfig) -> DataFrame:
    visits = visits.withWatermark("event_time", cfg.watermark)
    ads = ads.withWatermark("display_time", cfg.watermark)
    condition = F.expr(f"""
        page = ad_page AND
        display_time BETWEEN event_time AND event_time + INTERVAL {cfg.max_display_delay}
    """)
    return (visits.join(ads, condition, "left_outer")
            .withColumn("is_matched", F.col("ad_id").isNotNull())
            .withColumn("event_date", F.to_date("event_time")))


def make_batch_writer(cfg: JoinConfig):
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            counts = {r["is_matched"]: r["n"] for r in
                      batch_df.groupBy("is_matched").agg(F.count("*").alias("n")).collect()}
            (batch_df.write.format("delta").mode("append")
                .partitionBy("event_date")
                .option("txnAppId", cfg.app_id)       # replayed batch_id -> skipped by Delta
                .option("txnVersion", batch_id)
                .save(cfg.output_dir))
            matched, unmatched = counts.get(True, 0), counts.get(False, 0)
            append_stats(cfg.stats_file, {"kind": "batch", "batch_id": batch_id,
                                          "matched": matched, "unmatched": unmatched})
            log.info("batch %d written: matched=%d unmatched=%d", batch_id, matched, unmatched)
        finally:
            batch_df.unpersist()
    return write_batch


def progress_record(progress: dict) -> dict:
    """Extract what the health DAG checks from StreamingQuery.lastProgress."""
    state_ops = progress.get("stateOperators") or []
    return {
        "kind": "progress",
        "batch_id": progress.get("batchId"),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "max_event_time": (progress.get("eventTime") or {}).get("max"),
        "state_rows": sum(op.get("numRowsTotal", 0) for op in state_ops),
        "state_memory_bytes": sum(op.get("memoryUsedBytes", 0) for op in state_ops),
        "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in state_ops),
        "input_rows_per_second": progress.get("inputRowsPerSecond"),
    }


def main() -> None:
    cfg = JoinConfig.from_args()
    spark = (SparkSession.builder.appName("dynamic-joiner")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             # default "min": the slowest input drives the watermark -> no early eviction
             .config("spark.sql.streaming.multipleWatermarkPolicy", "min")
             .getOrCreate())
    log.info("starting join %s x %s watermark=%s delay=%s",
             cfg.visits_topic, cfg.ads_topic, cfg.watermark, cfg.max_display_delay)

    joined = build_join(read_topic(spark, cfg, cfg.visits_topic, VISIT_SCHEMA),
                        read_topic(spark, cfg, cfg.ads_topic, AD_SCHEMA), cfg)
    query = (joined.writeStream
             .queryName("visits_with_ads")
             .outputMode("append")              # the only mode for stream-stream joins
             .foreachBatch(make_batch_writer(cfg))
             .option("checkpointLocation", cfg.checkpoint_dir)
             .trigger(processingTime=cfg.trigger)
             .start())

    last_logged = None
    while query.isActive:
        query.awaitTermination(cfg.progress_interval_s)
        progress = query.lastProgress
        if progress and progress.get("batchId") != last_logged:
            last_logged = progress.get("batchId")
            append_stats(cfg.stats_file, progress_record(progress))
    if query.exception():
        raise RuntimeError(f"streaming join failed: {query.exception()}")


if __name__ == "__main__":
    main()
