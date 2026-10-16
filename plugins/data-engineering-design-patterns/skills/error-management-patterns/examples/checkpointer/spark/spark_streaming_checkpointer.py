"""Checkpointer - Spark Structured Streaming.

Problem: a streaming job counts unique visits in 10-minute windows; a fatal
failure must not make it reprocess the topic from the beginning. Structured
Streaming is the "data processing framework based", "configuration driven"
flavour: set ``checkpointLocation`` and Spark persists, per micro-batch,
  offsets/<batch>   the source positions planned for the batch (write-ahead log),
  commits/<batch>   the marker that the batch's sink write finished,
  state/            the window aggregation state (here RocksDB, with changelog
                    checkpointing to shrink the per-batch state upload).
On restart Spark replays the last planned-but-uncommitted batch with exactly
the same offsets and state, so the sink sees the same batch id again.

The checkpoint frequency is the trigger interval (``--trigger``): shorter =
less to reprocess after a failure, but more checkpoint overhead per minute
(delivery guarantee vs latency trade-off).

Checkpointing alone gives at-least-once delivery: the foreachBatch sink
here is a MERGE on (window_start, window_end), so replaying a batch rewrites the
same rows (idempotency) and the output becomes effectively exactly-once.

``inspect`` reads the newest offsets/commits files - handy to
see where a restarted query will resume.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      spark_streaming_checkpointer.py run --trigger "1 minute"
  python spark_streaming_checkpointer.py inspect --checkpoint-dir /tmp/dp/checkpointer/checkpoint
  python spark_streaming_checkpointer.py --self-test
Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

log = logging.getLogger("streaming_checkpointer")


def parse_offsets_file(content: str) -> dict:
    """offsets/<n>: 'v1', batch metadata JSON, then one JSON line per source."""
    lines = [line for line in content.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("v"):
        raise ValueError("not a Structured Streaming offsets file")
    return {"version": lines[0], "metadata": json.loads(lines[1]),
            "sources": [json.loads(line) for line in lines[2:]]}


def latest_batch(directory: Path) -> Optional[int]:
    ids = [int(p.name) for p in directory.glob("*") if p.name.isdigit()]
    return max(ids) if ids else None


def inspect_checkpoint(checkpoint_dir: str) -> dict:
    root = Path(checkpoint_dir)
    planned, committed = latest_batch(root / "offsets"), latest_batch(root / "commits")
    result = {"planned_batch": planned, "committed_batch": committed,
              "resume": "replay batch" if planned is not None and planned != committed else "next batch"}
    if planned is not None:
        result["offsets"] = parse_offsets_file((root / "offsets" / str(planned)).read_text())["sources"]
    return result


def upsert_windows(output_path: str):
    def _upsert(batch_df, batch_id: int) -> None:
        from delta.tables import DeltaTable

        spark = batch_df.sparkSession
        rows = batch_df.select("window.start", "window.end", "unique_visits") \
            .withColumnRenamed("start", "window_start").withColumnRenamed("end", "window_end")
        if not DeltaTable.isDeltaTable(spark, output_path):
            rows.write.format("delta").save(output_path)
            return
        (DeltaTable.forPath(spark, output_path).alias("t")
         .merge(rows.alias("s"), "t.window_start = s.window_start AND t.window_end = s.window_end")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        log.info("batch %s merged", batch_id)
    return _upsert


def run(args: argparse.Namespace) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.appName("visits-checkpointer")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
             # how many batches of checkpoint metadata to keep for recovery
             .config("spark.sql.streaming.minBatchesToRetain", "100")
             .getOrCreate())

    visits = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                                                "localhost:9092"))
              .option("subscribe", args.input_topic)
              # Only used on the very first start; afterwards the checkpoint wins.
              .option("startingOffsets", "earliest")
              .load()
              .select(F.from_json(F.col("value").cast("string"),
                                  "visit_id STRING, event_time TIMESTAMP").alias("v"))
              .select("v.*"))
    unique_visits = (visits.withWatermark("event_time", "10 minutes")
                     .groupBy(F.window("event_time", "10 minutes"))
                     .agg(F.approx_count_distinct("visit_id").alias("unique_visits")))
    query = (unique_visits.writeStream.outputMode("update")
             .option("checkpointLocation", args.checkpoint_dir)
             .trigger(processingTime=args.trigger)
             .foreachBatch(upsert_windows(args.output_path))
             .start())
    log.info("query %s started; resume point: %s", query.id, inspect_checkpoint(args.checkpoint_dir))
    query.awaitTermination()


def _self_check() -> None:
    sample = 'v1\n{"batchWatermarkMs":0,"batchTimestampMs":1700000000000,"conf":{}}\n{"visits":{"1":1276,"0":1224}}\n'
    parsed = parse_offsets_file(sample)
    assert parsed["sources"] == [{"visits": {"1": 1276, "0": 1224}}]
    tmp = Path(os.environ.get("TMP", "/tmp")) / "checkpointer_selftest"
    (tmp / "offsets").mkdir(parents=True, exist_ok=True)
    (tmp / "commits").mkdir(parents=True, exist_ok=True)
    (tmp / "offsets" / "17").write_text(sample)
    (tmp / "offsets" / "18").write_text(sample)
    (tmp / "commits" / "17").write_text("v1\n{}\n")
    info = inspect_checkpoint(str(tmp))
    assert info["planned_batch"] == 18 and info["committed_batch"] == 17 and info["resume"] == "replay batch"
    for sub in ("offsets", "commits"):
        for f in (tmp / sub).iterdir():
            f.unlink()
        (tmp / sub).rmdir()
    tmp.rmdir()
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    base = "/tmp/dp/checkpointer"
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("command", choices=["run", "inspect"])
    p.add_argument("--checkpoint-dir", default=f"{base}/checkpoint")
    p.add_argument("--output-path", default=f"{base}/output/unique-visits")
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--trigger", default="1 minute")
    args = p.parse_args(argv)
    if args.command == "inspect":
        print(json.dumps(inspect_checkpoint(args.checkpoint_dir), indent=2))
    else:
        run(args)


if __name__ == "__main__":
    main()
