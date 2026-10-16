"""Fine-Grained Tracker - row-level lineage propagation with plain Kafka clients.

The reducer consumes `visits-decorated`, and for every output record writes its own
lineage headers (job_name, job_version, batch_version) plus `parent_lineage`: the
upstream record's headers as JSON. Chained jobs produce a nested lineage trail,
queried with a separate tool (row-level lineage does not fit lineage UIs).

Run:  python row_lineage_propagation.py
Test: python row_lineage_propagation.py --self-test
Env:  KAFKA_BOOTSTRAP, JOB_VERSION
Deps: confluent-kafka
"""
import json
import os
import sys

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
JOB_NAME, JOB_VERSION = "visits_reducer_job", os.environ.get("JOB_VERSION", "v1.0.0")


def build_headers(upstream: list[tuple[str, bytes]] | None, batch_version: int) -> list[tuple[str, bytes]]:
    headers = [("job_name", JOB_NAME.encode()), ("job_version", JOB_VERSION.encode()),
               ("batch_version", str(batch_version).encode())]
    if upstream:
        parent = {k: v.decode("utf-8", errors="replace") for k, v in upstream}
        headers.append(("parent_lineage", json.dumps(parent).encode()))
    return headers


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": JOB_NAME,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe(["visits-decorated"])
    batch_version = 0
    try:
        while True:
            messages = consumer.consume(num_messages=500, timeout=1.0)
            if not messages:
                continue
            for msg in messages:
                if msg.error():
                    continue
                producer.produce("visits-reduced", key=msg.key(), value=msg.value(),
                                 headers=build_headers(msg.headers(), batch_version))
            producer.flush(30)
            consumer.commit(asynchronous=False)
            batch_version += 1
    finally:
        consumer.close()


def _self_test() -> None:
    root = build_headers(None, 3)
    assert [k for k, _ in root] == ["job_name", "job_version", "batch_version"]
    child = dict(build_headers(root, 7))
    assert json.loads(child["parent_lineage"])["batch_version"] == "3"
    print("self-test OK")


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()
