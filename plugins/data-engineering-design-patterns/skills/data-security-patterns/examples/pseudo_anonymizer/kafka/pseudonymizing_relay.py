"""Pseudo-Anonymizer - Kafka per-record keyed hashing and masking (confluent-kafka).

* user_id / email -> HMAC-SHA256 with a secret key: the same input always gives
  the same pseudonym, so consumers can still count distinct users and join
  topics - but can't reverse it without the key. Rotating the key breaks
  joinability with older data (plan rotations as re-pseudonymization).
* phone  -> masked, keeps the last 2 digits
* The message KEY is pseudonymized too, keeping partitioning per user.

Run:
  pip install confluent-kafka
  export KAFKA_BOOTSTRAP=localhost:9092 PSEUDO_HMAC_KEY=<secret>
  python pseudonymizing_relay.py        |  python pseudonymizing_relay.py test
"""
import hashlib
import hmac
import json
import os
import sys

SOURCE, TARGET = "users_raw", "users_pseudonymized"
HASHED_FIELDS = ("user_id", "email")


def pseudonym(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def mask_phone(phone: str) -> str:
    digits = [c for c in phone if c.isdigit()]
    return "*" * (len(digits) - 2) + "".join(digits[-2:])


def pseudonymize(record: dict, key: bytes) -> dict:
    out = dict(record)
    for field in HASHED_FIELDS:
        if out.get(field) is not None:
            out[field] = pseudonym(str(out[field]), key)
    if out.get("phone"):
        out["phone"] = mask_phone(out["phone"])
    return out


def run() -> None:
    from confluent_kafka import Consumer, Producer
    key = os.environ["PSEUDO_HMAC_KEY"].encode()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "users-pseudonymizer",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    consumer.subscribe([SOURCE])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            record = pseudonymize(json.loads(msg.value()), key)
            out_key = pseudonym(msg.key().decode(), key).encode() if msg.key() else None
            producer.produce(TARGET, key=out_key, value=json.dumps(record).encode())
            if producer.flush(10) > 0:
                raise RuntimeError("delivery failed, not committing")
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()


def self_check() -> None:
    key = b"test-key"
    a = pseudonymize({"user_id": "u1", "email": "x@y.z", "phone": "+48 600 100 200"}, key)
    b = pseudonymize({"user_id": "u1", "email": "x@y.z"}, key)
    assert a["user_id"] == b["user_id"] != "u1", "pseudonym must be stable and differ from input"
    assert a["phone"].endswith("00") and a["phone"].count("*") == 9
    print("ok", a)


if __name__ == "__main__":
    self_check() if sys.argv[1:] == ["test"] else run()
