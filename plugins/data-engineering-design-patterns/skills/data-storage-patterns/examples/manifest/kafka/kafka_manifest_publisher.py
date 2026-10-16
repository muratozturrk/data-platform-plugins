"""Manifest - Kafka as the manifest channel between a file producer and its readers.

Many readers (Fan-Out) of the same object-store dataset would each list the
bucket. Instead the producer publishes ONE manifest message per completed run
to a compacted topic `dataset_manifests` (key = dataset/run id). Readers consume
manifests and load exactly the listed files; replays of a run reuse the same
manifest (idempotent loads). Keep the message small: list files, not rows; for
huge runs, put the file list in object storage and publish its URI.

Run:
    pip install confluent-kafka
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_manifest_publisher.py publish
    KAFKA_BOOTSTRAP=localhost:9092 python kafka_manifest_publisher.py read
"""
import json
import os
import sys

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "dataset_manifests"


def publish(dataset: str, run_id: str, files: list[str]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    manifest = {"dataset": dataset, "run_id": run_id,
                "entries": [{"url": f, "mandatory": True} for f in files]}
    producer.produce(TOPIC, key=f"{dataset}/{run_id}", value=json.dumps(manifest))
    producer.flush()


def read(group_id: str = "devices-loader") -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group_id,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([TOPIC])
    try:
        while (msg := consumer.poll(5.0)) is not None:
            if msg.error():
                continue
            manifest = json.loads(msg.value())
            files = [e["url"] for e in manifest["entries"]]
            print(f"load {manifest['dataset']} run {manifest['run_id']}: {files}")  # e.g. spark.read.parquet(*files)
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    if sys.argv[1:2] == ["read"]:
        read()
    else:
        publish("devices", "20250601_1031", ["s3://devices/dataset_1", "s3://devices/dataset_2"])
