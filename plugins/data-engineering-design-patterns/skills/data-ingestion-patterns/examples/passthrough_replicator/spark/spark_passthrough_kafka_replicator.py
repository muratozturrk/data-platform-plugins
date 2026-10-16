"""Passthrough Replicator: Kafka topic -> Kafka topic with ordering semantics.

A key/value copy is not enough for Kafka: the replica must also keep the headers and the order of records
inside each partition. The job therefore:
  * reads key, value, partition, headers and offset (``includeHeaders=true`` on the source);
  * in ``foreachBatch`` sorts every Spark partition by offset, drops the offset and writes with
    ``includeHeaders=true``; the ``partition`` column routes each record to the same partition number.

Why this keeps the order: the Kafka source maps one topic partition to one Spark partition (do not set
``minPartitions``, which would split a topic partition into several Spark tasks), and the sink writes a Spark
partition sequentially. ``sortWithinPartitions`` protects against any reordering inside the micro-batch.
The target topic must have the same partition count; the job checks it through the Kafka AdminClient
of the Spark Kafka package before starting.

Delivery is at-least-once (a failed micro-batch is replayed from the checkpoint), so the replica can contain
duplicates after a failure; MirrorMaker 2 (see ../kafka) is the infrastructure alternative.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_passthrough_kafka_replicator.py \
      --source-bootstrap prod-kafka:9092 --target-bootstrap staging-kafka:9092 \
      --source-topic visits --target-topic visits --checkpoint s3a://prod-checkpoints/visits-replicator
"""
import argparse
import logging
import sys

log = logging.getLogger("passthrough_kafka_replicator")


def partition_count(spark, bootstrap: str, topic: str) -> int:
    """Uses the Kafka client that ships with spark-sql-kafka (no extra Python dependency)."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    try:
        topics = jvm.java.util.Collections.singletonList(topic)
        description = admin.describeTopics(topics).allTopicNames().get().get(topic)
        return description.partitions().size()
    finally:
        admin.close()


def build_writer(target_bootstrap: str, target_topic: str):
    def write_sorted_events(events, batch_id: int) -> None:
        (events.sortWithinPartitions("offset", ascending=True).drop("offset")
         .write.format("kafka")
         .option("kafka.bootstrap.servers", target_bootstrap)
         .option("topic", target_topic)
         .option("includeHeaders", "true")
         .option("kafka.enable.idempotence", "true")
         .option("kafka.acks", "all")
         .save())
        log.info("micro-batch %d replicated", batch_id)
    return write_sorted_events


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source-bootstrap", required=True)
    p.add_argument("--target-bootstrap", required=True)
    p.add_argument("--source-topic", required=True)
    p.add_argument("--target-topic", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--max-offsets-per-trigger", default="100000")
    p.add_argument("--trigger", default="30 seconds")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"kafka-passthrough-{args.source_topic}").getOrCreate()

    src_parts = partition_count(spark, args.source_bootstrap, args.source_topic)
    dst_parts = partition_count(spark, args.target_bootstrap, args.target_topic)
    if src_parts != dst_parts:
        raise RuntimeError(f"Partition count differs ({src_parts} vs {dst_parts}); order per key would break")

    input_data_stream = (spark.readStream.format("kafka")
                         .option("kafka.bootstrap.servers", args.source_bootstrap)
                         .option("subscribe", args.source_topic)
                         .option("includeHeaders", "true")
                         .option("startingOffsets", "earliest")
                         .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                         # losing source data must stop the replica, not silently skip records
                         .option("failOnDataLoss", "true")
                         .load())

    events_to_replicate = input_data_stream.selectExpr("key", "value", "partition", "headers", "offset")

    query = (events_to_replicate.writeStream
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=args.trigger)
             .foreachBatch(build_writer(args.target_bootstrap, args.target_topic))
             .start())
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
