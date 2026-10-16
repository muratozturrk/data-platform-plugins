"""Local Aggregator -- aggregate inside each input partition, no shuffle.

Precondition: producers write every record of a visit to the same
partition (static key) and the partition count never changes. A Kafka source gives
one Spark partition per topic partition, so all rows of a visit_id are already in
one task. The job then:

  1. sortWithinPartitions(visit_id, event_time)  -- local sort, no Exchange
  2. foreachPartition: fold consecutive rows of one visit into one aggregate
     (pages, start, end, count) and send it to Kafka when visit_id changes
  3. flush + close the producer per partition; a delivery error fails the task

The physical plan is checked before running: an `Exchange` node means something
(repartition, groupBy, minPartitions, AQE coalescing...) re-introduced a shuffle.

Mode `bucketed` shows the table variant: a table bucketed by visit_id lets
groupBy(visit_id) run without Exchange as long as the bucket count is fixed
(see the Bucket pattern).

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark_local_aggregator_sorted_partitions.py [--mode kafka|bucketed]
  python spark_local_aggregator_sorted_partitions.py --self-test     # no Spark needed
Env: KAFKA_BOOTSTRAP, SOURCE_TOPIC, TARGET_TOPIC, BUCKETED_TABLE, BUCKETS.
Prereqs: pyspark>=3.3, confluent-kafka on the executors.
"""
import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("local-aggregator")


@dataclass(frozen=True)
class Config:
    bootstrap: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    source_topic: str = field(default_factory=lambda: os.environ.get("SOURCE_TOPIC", "visits"))
    target_topic: str = field(default_factory=lambda: os.environ.get("TARGET_TOPIC", "visits-aggregated"))
    bucketed_table: str = field(default_factory=lambda: os.environ.get("BUCKETED_TABLE", "dp.visits_bucketed"))
    buckets: int = field(default_factory=lambda: int(os.environ.get("BUCKETS", "12")))


def fold_sorted_visits(rows: Iterable[Dict]) -> Iterator[Dict]:
    """rows MUST be sorted by (visit_id, event_time). Yields one aggregate per visit.

    Pure logic of the KafkaWriter: keep one in-flight visit and
    emit it as soon as a different visit_id shows up.
    """
    in_flight: Optional[Dict] = None
    for row in rows:
        if in_flight is None or row["visit_id"] != in_flight["visit_id"]:
            if in_flight is not None:
                yield in_flight
            in_flight = {"visit_id": row["visit_id"], "start_time": row["event_time"],
                         "pages": [], "events": 0}
        elif row["event_time"] < in_flight["end_time"]:
            raise ValueError(f"input not sorted for visit {row['visit_id']}")
        in_flight["pages"].append(row["page"])
        in_flight["events"] += 1
        in_flight["end_time"] = row["event_time"]
    if in_flight is not None:
        yield in_flight


class KafkaWriter:
    """One producer per Spark partition; fails the task if any record was not delivered."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "linger.ms": 100, "compression.type": "lz4"})
        self.errors = 0
        self.sent = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1

    def send(self, aggregate: Dict) -> None:
        # visit_id as key: a retried task re-sends the same keys, so a compacted
        # target topic (or an upsert sink) keeps one aggregate per visit
        self.producer.produce(self.topic, key=aggregate["visit_id"],
                              value=json.dumps(aggregate, default=str), on_delivery=self._on_delivery)
        self.producer.poll(0)
        self.sent += 1

    def close(self) -> None:
        remaining = self.producer.flush(60)
        if remaining or self.errors:
            raise RuntimeError(f"{remaining} undelivered / {self.errors} failed aggregates")


def write_partition(cfg: Config):
    def _write(rows) -> None:
        writer = KafkaWriter(cfg.bootstrap, cfg.target_topic)
        for aggregate in fold_sorted_visits(r.asDict() for r in rows):
            writer.send(aggregate)
        writer.close()
    return _write


def assert_no_shuffle(df, label: str) -> None:
    plan = df._jdf.queryExecution().executedPlan().toString()
    if "Exchange" in plan:
        raise RuntimeError(f"{label}: physical plan contains a shuffle (Exchange); "
                           f"input partitioning is not key-aligned:\n{plan}")
    log.info("%s: no Exchange in the physical plan", label)


def run_kafka(spark, cfg: Config) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType([StructField("visit_id", StringType()), StructField("page", StringType()),
                         StructField("event_time", TimestampType())])
    # No minPartitions: it would split one topic partition into several Spark partitions
    visits = (spark.read.format("kafka")
              .option("kafka.bootstrap.servers", cfg.bootstrap)
              .option("subscribe", cfg.source_topic)
              .option("startingOffsets", "earliest").option("endingOffsets", "latest")
              .load()
              .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
              .filter(F.col("visit_id").isNotNull()))
    sorted_visits = visits.sortWithinPartitions("visit_id", "event_time")
    assert_no_shuffle(sorted_visits, "kafka source")
    log.info("aggregating %d input partitions locally", sorted_visits.rdd.getNumPartitions())
    sorted_visits.foreachPartition(write_partition(cfg))


def run_bucketed(spark, cfg: Config) -> None:
    """Bucketed-table variant: storage layout replaces the producer partitioning contract."""
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
    table = spark.table(cfg.bucketed_table)
    # the ingestion job writes the table with create_bucketed_table(): same key, same bucket count forever
    per_visit = (table.groupBy("visit_id")
                 .agg(F.min("event_time").alias("start_time"), F.max("event_time").alias("end_time"),
                      F.count("*").alias("events")))
    assert_no_shuffle(per_visit, "bucketed table")
    per_visit.write.mode("overwrite").saveAsTable(f"{cfg.bucketed_table}_per_visit")


def create_bucketed_table(df, cfg: Config) -> None:
    """Called by the ingestion job. Changing cfg.buckets later breaks colocation (full rewrite)."""
    df.write.bucketBy(cfg.buckets, "visit_id").sortBy("visit_id", "event_time") \
        .mode("overwrite").saveAsTable(cfg.bucketed_table)


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["kafka", "bucketed"], default="kafka")
    args = parser.parse_args()
    cfg = Config()
    spark = (SparkSession.builder.appName(f"local-aggregator-{args.mode}")
             .config("spark.sql.adaptive.enabled", "false")   # keep the plan check deterministic
             .getOrCreate())
    try:
        run_kafka(spark, cfg) if args.mode == "kafka" else run_bucketed(spark, cfg)
    finally:
        spark.stop()


def _self_test() -> None:
    rows = [{"visit_id": "a", "event_time": 1, "page": "home"},
            {"visit_id": "a", "event_time": 2, "page": "cart"},
            {"visit_id": "b", "event_time": 1, "page": "home"}]
    out = list(fold_sorted_visits(rows))
    assert [(o["visit_id"], o["pages"], o["start_time"], o["end_time"], o["events"]) for o in out] == [
        ("a", ["home", "cart"], 1, 2, 2), ("b", ["home"], 1, 1, 1)]
    assert list(fold_sorted_visits([])) == []
    try:
        list(fold_sorted_visits([{"visit_id": "a", "event_time": 2, "page": "x"},
                                 {"visit_id": "a", "event_time": 1, "page": "y"}]))
        raise AssertionError("unsorted input must fail")
    except ValueError:
        pass
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()
