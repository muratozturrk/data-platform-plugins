"""Online Observer - observation inside the streaming job with accumulators, PySpark.

The data generation job (Kafka `visits` -> Kafka `visits_enriched`) observes each
micro-batch in the same job. The reader keeps `partition` and `offset` columns, and a
single foreachPartition pass fills accumulators for:
  * invalid (missing) event_time / user_id / page and total rows,
  * max processed offset per partition (lag detection without a second query).
Observations are written as JSON lines; observation errors are caught so they never
fail data delivery. Accumulators are read on the driver via `.value` only.

Run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_streaming_online_observer.py
Env: KAFKA_BOOTSTRAP (localhost:9094), BASE_DIR (/tmp/dp/online_observer), SAMPLE_FRACTION (1.0).
"""
import dataclasses
import json
import os
from datetime import datetime, timezone
from typing import Iterator, List

from pyspark.accumulators import AccumulatorParam
from pyspark.sql import DataFrame, Row, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
BASE_DIR = os.environ.get("BASE_DIR", "/tmp/dp/online_observer")
SAMPLE_FRACTION = float(os.environ.get("SAMPLE_FRACTION", "1.0"))  # <1.0 trades insight for safety
visit_schema = StructType([StructField("visit_id", StringType()), StructField("event_time", TimestampType()),
                           StructField("user_id", StringType()), StructField("page", StringType())])


@dataclasses.dataclass
class PartitionWithOffset:
    partition: int
    offset: int


class PartitionToMaxOffsetAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return []

    def addInPlace(self, partitions_with_offsets: List, new_candidates: List):
        partitions_with_offsets.extend(new_candidates)
        return partitions_with_offsets


def write_to_kafka_with_observer(visits: DataFrame, batch_number: int) -> None:
    visits.persist()
    (visits.selectExpr("visit_id AS key", "to_json(struct(visit_id, event_time, user_id, page)) AS value")
     .write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", "visits_enriched").save())
    try:
        observe(visits, batch_number)
    except Exception as error:  # observation must never break data delivery
        print(f"Observation failed for batch {batch_number}: {error}")
    finally:
        visits.unpersist()


def observe(visits: DataFrame, batch_number: int) -> None:
    ctx = visits.sparkSession.sparkContext
    invalid = {f: ctx.accumulator(0) for f in ("event_time", "user_id", "page")}
    all_events = ctx.accumulator(0)
    max_offsets_tracker = ctx.accumulator([], PartitionToMaxOffsetAccumulatorParam())
    sampled = visits if SAMPLE_FRACTION >= 1.0 else visits.sample(fraction=SAMPLE_FRACTION)

    def analyze_generated_records(visits_iterator: Iterator[Row]):
        max_local = {}
        for visit_record in visits_iterator:
            all_events.add(1)
            for field, acc in invalid.items():
                if not visit_record[field]:
                    acc.add(1)
            if visit_record.offset > max_local.get(visit_record.partition, -1):
                max_local[visit_record.partition] = visit_record.offset
        max_offsets_tracker.add([PartitionWithOffset(p, o) for p, o in max_local.items()])

    sampled.foreachPartition(analyze_generated_records)  # action -> accumulators reliable

    max_offsets = {}
    for po in max_offsets_tracker.value:
        max_offsets[po.partition] = max(po.offset, max_offsets.get(po.partition, -1))
    observation_dump = {
        "@timestamp": datetime.now(timezone.utc).isoformat(), "batch": batch_number,
        **{f"invalid_{f}": acc.value for f, acc in invalid.items()},
        "all_events": all_events.value, "sample_fraction": SAMPLE_FRACTION,
        "max_offsets": {str(p): o for p, o in max_offsets.items()},
    }
    os.makedirs(BASE_DIR, exist_ok=True)
    with open(f"{BASE_DIR}/observations.jsonl", "a", encoding="utf-8") as out:
        out.write(json.dumps(observation_dump) + "\n")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("online-observer").getOrCreate()
    visits_to_analyze = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
                         .option("subscribe", "visits").option("startingOffsets", "EARLIEST").load()
                         .select(F.from_json(F.col("value").cast("string"), visit_schema).alias("visit"),
                                 "partition", "offset")
                         .selectExpr("visit.*", "partition", "offset"))
    (visits_to_analyze.writeStream.foreachBatch(write_to_kafka_with_observer)
     .option("checkpointLocation", f"{BASE_DIR}/checkpoint").start().awaitTermination())
