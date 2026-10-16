"""Anonymizer - Kafka consume -> anonymize -> produce relay (confluent-kafka).

Partners read `visits_shared`, never `visits_raw`. Each record is stripped of
PII (removal), its IP is generalized/perturbed and its email replaced with a
synthetic one. Offsets are committed only after the anonymized record is
delivered (at-least-once; dedupe downstream on visit_id if needed).

Never forward unknown fields: an ALLOWLIST of output fields means a new PII
column added upstream is dropped by default instead of leaking.

Run:
  pip install confluent-kafka faker
  export KAFKA_BOOTSTRAP=localhost:9092
  python anonymizing_relay.py           # python anonymizing_relay.py test  -> offline self-check
"""
import json
import os
import sys

from faker import Faker

SOURCE, TARGET = "visits_raw", "visits_shared"
ALLOWED_FIELDS = {"visit_id", "event_time", "page", "browser", "ip", "email"}
faker = Faker()


def anonymize(visit: dict) -> dict:
    out = {k: v for k, v in visit.items() if k in ALLOWED_FIELDS}  # removal by allowlist
    if out.get("ip"):
        out["ip"] = ".".join(out["ip"].split(".")[:2] + ["0", "0"])  # perturb: drop host part
    if "email" in out:
        out["email"] = faker.email()  # synthetic replacement
    return out


def run() -> None:
    from confluent_kafka import Consumer, Producer
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "visits-anonymizer",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    consumer.subscribe([SOURCE])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            anonymized = anonymize(json.loads(msg.value()))
            producer.produce(TARGET, key=msg.key(), value=json.dumps(anonymized).encode())
            if producer.flush(10) > 0:
                raise RuntimeError("delivery failed, not committing")
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


def self_check() -> None:
    visit = {"visit_id": "v1", "page": "/", "ip": "123.45.67.89", "email": "jane@example.com",
             "user_id": "u1", "birthday": "1990-01-01"}
    out = anonymize(visit)
    assert "user_id" not in out and "birthday" not in out
    assert out["ip"] == "123.45.0.0" and out["email"] != visit["email"]
    print("ok", out)


if __name__ == "__main__":
    self_check() if sys.argv[1:] == ["test"] else run()
