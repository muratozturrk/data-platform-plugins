"""Isolated Sequencer -- event-driven readiness contract over Kafka.

Instead of polling a marker file, the provider pipeline publishes a "dataset ready" event
to a shared, documented topic. Any number of consumer pipelines (other teams) subscribe
with their own consumer group and start their processing when an event arrives.
Coupling stays data-based: the provider doesn't know who consumes; consumers only depend
on the event schema {dataset, partition, location, rows}.

Run:
    export KAFKA_BOOTSTRAP=localhost:9092
    python readiness_events.py consume --group devices-aggregator   # consumer team
    python readiness_events.py publish --partition 2024-01-01        # provider team
Requires: confluent-kafka>=2.0.
"""
import argparse
import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "dataset-readiness"
DATASET = "devices"


def publish(partition: str) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    event = {
        "dataset": DATASET,
        "partition": partition,
        "location": f"s3://dp/devices/date={partition}",
        "rows": 100,
    }
    # Key = dataset/partition -> a compacted topic keeps the latest status per partition.
    producer.produce(TOPIC, key=f"{DATASET}/{partition}", value=json.dumps(event))
    producer.flush(10)
    print(f"published readiness for {DATASET}/{partition}")


def process_partition(event: dict) -> None:
    print(f"processing {event['location']} ({event['rows']} rows)")


def consume(group: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,  # each consuming team = own group = independent progress
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            event = json.loads(msg.value())
            if event.get("dataset") != DATASET:
                continue  # topic may carry readiness for many datasets
            process_partition(event)
            consumer.commit(msg, asynchronous=False)  # commit only after processing
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("publish")
    p.add_argument("--partition", required=True)
    c = sub.add_parser("consume")
    c.add_argument("--group", required=True)
    args = parser.parse_args()
    if args.cmd == "publish":
        publish(args.partition)
    else:
        consume(args.group)


if __name__ == "__main__":
    main()
