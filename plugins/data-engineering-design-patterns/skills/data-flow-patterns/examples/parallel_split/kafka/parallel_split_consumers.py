"""Parallel Split -- Kafka.

In Kafka the split is native: a topic written once can be read by any number of consumer
GROUPS, each with its own offsets, pace and failure domain. Here the same `devices` topic
feeds two isolated branches:
  * `legacy-csv-writer`   -> appends CSV lines for the legacy consumers
  * `delta-loader`        -> (stand-in) prints records that the new pipeline would load
A slow or crashing branch never blocks the other -- no "slowest branch gates all" issue,
only its own consumer lag grows.

Run:
    export KAFKA_BOOTSTRAP=localhost:9092
    python parallel_split_consumers.py produce
    python parallel_split_consumers.py branch --name legacy-csv-writer
    python parallel_split_consumers.py branch --name delta-loader
Requires: confluent-kafka>=2.0.
"""
import argparse
import csv
import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "devices"


def produce() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    for i in range(10):
        record = {"type": "android" if i % 2 else "ios", "full_name": f"device {i}", "version": "1.0"}
        producer.produce(TOPIC, key=str(i), value=json.dumps(record))
    producer.flush(10)


def write_csv(record: dict) -> None:
    with open("/tmp/dp_devices_legacy.csv", "a", newline="") as f:
        csv.writer(f).writerow([record["type"], record["full_name"], record["version"]])


def load_delta(record: dict) -> None:
    print(f"would load to Delta: {record}")


BRANCHES = {"legacy-csv-writer": write_csv, "delta-loader": load_delta}


def branch(name: str) -> None:
    handler = BRANCHES[name]
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": name,  # separate group == separate branch
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            handler(json.loads(msg.value()))
            # at-least-once: duplicates possible on retry -> make sinks idempotent
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("produce")
    b = sub.add_parser("branch")
    b.add_argument("--name", choices=sorted(BRANCHES), required=True)
    args = parser.parse_args()
    produce() if args.cmd == "produce" else branch(args.name)


if __name__ == "__main__":
    main()
