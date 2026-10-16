"""Distributed Aggregator -- Spark Structured Streaming daily visit counts per axis.

Same reduce as the batch cube, on the Kafka visits stream: GROUP BY
(event_date, country, device_type) -> visit events. groupBy shuffles every row of
a key to one state-store partition (Exchange hashpartitioning), exactly as in batch.

Two modes:
  --mode native     groupBy(window(1 day), country, device_type).count() with a
                    watermark. Spark runs partial_count map-side BEFORE the exchange
                    (partial-aggregation optimization) -> default choice.
  --mode stateful   applyInPandasWithState: custom emission semantics. Each micro-batch
                    emits the running aggregate (is_final=false); when the watermark
                    passes the end of the day (EventTimeTimeout) the final row is
                    emitted once and the state removed. Price: raw rows are shuffled,
                    no map-side partial aggregation happens before a pandas state function.

flatMapGroupsWithState is Scala/Java only; PySpark uses applyInPandasWithState (3.4+).
Sink: Delta MERGE on (event_date, country, device_type); a final row is never
overwritten by a partial one, so replayed micro-batches are harmless.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
      spark_streaming_distributed_aggregator.py --mode stateful
  python spark_streaming_distributed_aggregator.py --self-test   # pure logic, no Spark
Env: KAFKA_BOOTSTRAP, VISITS_TOPIC, OUTPUT_PATH, CHECKPOINT_DIR, WATERMARK_DELAY.
"""
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("streaming_distributed_aggregator")

DAY_MS = 24 * 3600 * 1000
EVENT_DDL = ("visit_id STRING, event_time TIMESTAMP, page STRING, "
             "context STRUCT<user: STRUCT<country: STRING>, technical: STRUCT<dev_type: STRING>>")
OUTPUT_DDL = ("event_date DATE, country STRING, device_type STRING, events BIGINT, "
              "first_event_time TIMESTAMP, last_event_time TIMESTAMP, is_final BOOLEAN")
STATE_DDL = "events BIGINT, first_event_ms BIGINT, last_event_ms BIGINT"


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    visits_topic: str = os.environ.get("VISITS_TOPIC", "visits")
    output_path: str = os.environ.get("OUTPUT_PATH", "/data/gold/visits_daily_counts")
    checkpoint_dir: str = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints/streaming_distributed_aggregator")
    watermark_delay: str = os.environ.get("WATERMARK_DELAY", "15 minutes")
    trigger_interval: str = os.environ.get("TRIGGER_INTERVAL", "1 minute")


# -- Pure logic (self-tested) ---------------------------------------------------------
def to_epoch_ms(series: pd.Series) -> list[int]:
    return ((pd.to_datetime(series) - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)).tolist()


def merge_partial(state: tuple | None, times_ms: list[int]) -> tuple[int, int, int]:
    """(count, min, max) are decomposable: merging partials == aggregating raw rows."""
    if not times_ms:
        return state
    new = (len(times_ms), min(times_ms), max(times_ms))
    if state is None:
        return new
    return state[0] + new[0], min(state[1], new[1]), max(state[2], new[2])


def day_end_ms(event_date) -> int:
    return int((pd.Timestamp(event_date) - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)) + DAY_MS


def output_row(key: tuple, state: tuple, is_final: bool) -> dict:
    event_date, country, device_type = key
    return {"event_date": pd.Timestamp(event_date).date(), "country": country, "device_type": device_type,
            "events": state[0], "first_event_time": pd.Timestamp(state[1], unit="ms"),
            "last_event_time": pd.Timestamp(state[2], unit="ms"), "is_final": is_final}


def aggregate_day(key: Any, rows: Iterable[pd.DataFrame], state) -> Iterator[pd.DataFrame]:
    """applyInPandasWithState function: running partial per key, final at day end."""
    if state.hasTimedOut:
        final = output_row(key, state.get, is_final=True)
        state.remove()
        yield pd.DataFrame([final])
        return

    times = [t for df in rows for t in to_epoch_ms(df["event_time"])]
    current = merge_partial(state.get if state.exists else None, times)
    if current is None:
        return
    state.update(current)
    # Timeout must stay ahead of the watermark or Spark rejects it.
    state.setTimeoutTimestamp(max(day_end_ms(key[0]), state.getCurrentWatermarkMs() + 1))
    yield pd.DataFrame([output_row(key, current, is_final=False)])


# -- Spark wiring ------------------------------------------------------------------------
def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("streaming-distributed-aggregator")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "16")   # = state partitions; fixed by the checkpoint
            .getOrCreate())


def read_visits(spark, cfg: Config):
    from pyspark.sql import functions as F
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
            .option("subscribe", cfg.visits_topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), EVENT_DDL).alias("v"))
            .select(F.col("v.event_time").alias("event_time"),
                    F.to_date("v.event_time").alias("event_date"),
                    F.coalesce("v.context.user.country", F.lit("unknown")).alias("country"),
                    F.coalesce("v.context.technical.dev_type", F.lit("unknown")).alias("device_type"))
            .filter("event_time IS NOT NULL")
            .withWatermark("event_time", cfg.watermark_delay))


def aggregate_native(visits):
    from pyspark.sql import functions as F
    return (visits.groupBy(F.window("event_time", "1 day").alias("w"), "country", "device_type")
            .agg(F.count("*").alias("events"), F.min("event_time").alias("first_event_time"),
                 F.max("event_time").alias("last_event_time"))
            .select(F.to_date("w.start").alias("event_date"), "country", "device_type", "events",
                    "first_event_time", "last_event_time", F.lit(True).alias("is_final")))


def aggregate_stateful(visits):
    from pyspark.sql.streaming.state import GroupStateTimeout
    return (visits.groupBy("event_date", "country", "device_type")
            .applyInPandasWithState(aggregate_day, outputStructType=OUTPUT_DDL, stateStructType=STATE_DDL,
                                    outputMode="update", timeoutConf=GroupStateTimeout.EventTimeTimeout))


def make_writer(cfg: Config):
    keys = "t.event_date = s.event_date AND t.country = s.country AND t.device_type = s.device_type"

    def write_batch(batch_df, batch_id: int) -> None:
        from delta.tables import DeltaTable
        spark = batch_df.sparkSession
        if batch_df.isEmpty():
            return
        if not DeltaTable.isDeltaTable(spark, cfg.output_path):
            batch_df.write.format("delta").partitionBy("event_date").save(cfg.output_path)
        else:
            (DeltaTable.forPath(spark, cfg.output_path).alias("t")
             .merge(batch_df.alias("s"), keys)
             .whenMatchedUpdateAll(condition="t.is_final = false")
             .whenNotMatchedInsertAll()
             .execute())
        log.info("batch=%d rows=%d final=%d", batch_id, batch_df.count(), batch_df.filter("is_final").count())

    return write_batch


def run(mode: str, cfg: Config) -> None:
    spark = build_spark()
    visits = read_visits(spark, cfg)
    aggregates = aggregate_native(visits) if mode == "native" else aggregate_stateful(visits)
    query = (aggregates.writeStream
             .outputMode("append" if mode == "native" else "update")
             .foreachBatch(make_writer(cfg))
             .option("checkpointLocation", f"{cfg.checkpoint_dir}/{mode}")
             .trigger(processingTime=cfg.trigger_interval)
             .start())
    log.info("started | mode=%s watermark=%s", mode, cfg.watermark_delay)
    query.awaitTermination()


# -- Self-check ----------------------------------------------------------------------------
class _FakeState:
    def __init__(self, watermark_ms=0):
        self.value, self.hasTimedOut, self.timeout, self.wm = None, False, None, watermark_ms

    exists = property(lambda self: self.value is not None)
    get = property(lambda self: self.value)

    def update(self, v): self.value = v
    def remove(self): self.value = None
    def setTimeoutTimestamp(self, ts): self.timeout = ts
    def getCurrentWatermarkMs(self): return self.wm


def _self_check() -> None:
    key = (pd.Timestamp("2024-01-15").date(), "PL", "mobile")
    t = pd.Timestamp("2024-01-15 10:00:00")
    b1 = pd.DataFrame({"event_time": [t, t + pd.Timedelta(minutes=5)]})
    b2 = pd.DataFrame({"event_time": [t - pd.Timedelta(hours=1)]})
    st = _FakeState()
    out1 = list(aggregate_day(key, [b1], st))[0].iloc[0]
    assert out1["events"] == 2 and not out1["is_final"]
    assert st.timeout == day_end_ms("2024-01-15")
    out2 = list(aggregate_day(key, [b2], st))[0].iloc[0]
    assert out2["events"] == 3 and out2["first_event_time"] == t - pd.Timedelta(hours=1)
    st.hasTimedOut = True
    final = list(aggregate_day(key, [], st))[0].iloc[0]
    assert final["is_final"] and final["events"] == 3 and not st.exists
    # Partial merge == raw aggregation (decomposability).
    assert merge_partial(merge_partial(None, [5, 1]), [9]) == merge_partial(None, [5, 1, 9]) == (3, 1, 9)
    st2 = _FakeState(watermark_ms=day_end_ms("2024-01-15") + 10)
    list(aggregate_day(key, [b1], st2))
    assert st2.timeout == st2.wm + 1
    print("self-check OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["native", "stateful"], default="stateful")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_check()
    else:
        run(args.mode, Config())
