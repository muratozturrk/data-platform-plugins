"""Streaming Reprocessor - rewind a Kafka consumer group to a timestamp (confluent-kafka).

Same effect as
    kafka-consumer-groups.sh --group G --topic T --reset-offsets --to-datetime ... --execute
for jobs that keep their position in the consumer group (plain consumers, Kafka Streams,
Flink with committed offsets as start). Steps:
  1. STOP the consumer group (offsets of an active group cannot be safely rewritten);
  2. resolve the offsets of the first record >= timestamp per partition (offsets_for_times);
  3. print the plan (dry run, default) or commit it (--execute);
  4. restart the job -> it reprocesses everything after the timestamp.
Only works while the data is still retained (retention / tiered storage).

Run:  python reset_offsets_to_timestamp.py --group visits-sessionizer --topic visits --ts-ms 1717200000000 [--execute]
Env:  KAFKA_BOOTSTRAP
Deps: confluent-kafka
"""
import argparse
import os

from confluent_kafka import Consumer, TopicPartition


def plan(consumer: Consumer, topic: str, ts_ms: int) -> list[TopicPartition]:
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    resolved = consumer.offsets_for_times([TopicPartition(topic, p, ts_ms) for p in partitions], timeout=10)
    result = []
    for tp in resolved:
        if tp.offset < 0:  # no record after ts -> end of partition
            tp = TopicPartition(topic, tp.partition, consumer.get_watermark_offsets(tp, timeout=10)[1])
        result.append(tp)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--ts-ms", type=int, required=True)
    parser.add_argument("--execute", action="store_true", help="commit the new offsets (default: dry run)")
    args = parser.parse_args()

    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                         "group.id": args.group, "enable.auto.commit": False})
    try:
        current = {tp.partition: tp.offset for tp in consumer.committed(
            [TopicPartition(args.topic, p) for p in consumer.list_topics(args.topic).topics[args.topic].partitions],
            timeout=10)}
        target = plan(consumer, args.topic, args.ts_ms)
        for tp in target:
            print(f"partition {tp.partition}: committed {current.get(tp.partition)} -> {tp.offset}")
        if args.execute:
            # Fails with a rebalance/unknown member error if the group is still active: stop it first.
            consumer.commit(offsets=target, asynchronous=False)
            print("offsets committed - restart the job to reprocess")
        else:
            print("dry run - add --execute to apply (back up the printed committed offsets first)")
    finally:
        consumer.close()
