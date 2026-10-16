"""FIFO Orderer -- Spark Structured Streaming, Kafka -> Kinesis one record at a time.

Pattern: Kinesis PutRecords has partial commits, so a FIFO delivery uses the
single-record PutRecord API and chains SequenceNumberForOrdering per
partition key. Each write is acknowledged before the next.

Parallelism without losing order = "scopes of ordered records":
repartition by the entity (visit_id) so ONE task owns all records of an
entity, then each task delivers its records sequentially in event-time order.

Retries: a failed micro-batch is replayed from the checkpoint and records
already written are written again (FIFO != exactly-once). Consumers dedup
on event_id.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark_streaming_fifo_orderer_kinesis.py
Env: KAFKA_BOOTSTRAP, SOURCE_TOPIC, KINESIS_STREAM, CHECKPOINT_DIR.
Prereqs: pyspark>=3.4, boto3 on executors; AWS creds from the provider chain.
"""
import json
import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

STREAM = os.environ.get("KINESIS_STREAM", "visits-checkout")
SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("visit_id", StringType()),
    StructField("event_time", TimestampType()), StructField("page", StringType()),
])


def send_partition_fifo(rows) -> None:
    import boto3
    client = boto3.client("kinesis")
    last_seq = {}  # partition key -> sequence number of the previous acked write
    for row in rows:
        key = row.visit_id
        kwargs = {"StreamName": STREAM, "PartitionKey": key,
                  "Data": json.dumps(row.asDict(), default=str).encode("utf-8")}
        if key in last_seq:
            kwargs["SequenceNumberForOrdering"] = last_seq[key]
        # boto3 retries throttling internally; any raised error fails the task
        # and Spark replays the micro-batch (at-least-once).
        last_seq[key] = client.put_record(**kwargs)["SequenceNumber"]


def forward_batch(batch_df: DataFrame, batch_id: int) -> None:
    (batch_df
        .repartition("visit_id")
        .sortWithinPartitions("visit_id", "event_time")
        .foreachPartition(send_partition_fifo))


def main() -> None:
    spark = SparkSession.builder.appName("fifo-orderer").getOrCreate()
    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
              .option("subscribe", os.environ.get("SOURCE_TOPIC", "visits"))
              .option("startingOffsets", "earliest")
              .load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
              .select("e.*")
              .filter(F.col("page").startswith("/checkout")))

    (events.writeStream
        .foreachBatch(forward_batch)
        .option("checkpointLocation", os.environ.get("CHECKPOINT_DIR", "/tmp/chk/fifo-orderer"))
        .trigger(processingTime="5 seconds")
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
