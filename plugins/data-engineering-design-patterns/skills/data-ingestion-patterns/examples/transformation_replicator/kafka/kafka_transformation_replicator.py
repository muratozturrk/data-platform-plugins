"""Transformation Replicator for Kafka: prod topic -> staging topic with PII removed.

A consumer on the production cluster reads every record, applies the PII contract (``../pii_rules.json``)
to the JSON value as a mapping function, and produces to the staging cluster. Everything that is not PII is
preserved like in the Passthrough Replicator: key, headers, timestamp and the partition number, so the
per-key order of the source survives. Offsets are committed only after the producer has flushed
(at-least-once; staging consumers should tolerate duplicates).

Records whose value is not valid JSON, or that contain fields missing from the contract while
``default_action`` is ``fail``, are NOT forwarded: they go to a quarantine topic that stays in production.

Run:
  pip install confluent-kafka
  export PII_HASH_SALT=...  PROD_BOOTSTRAP=prod-kafka:9092  STAGING_BOOTSTRAP=staging-kafka:9092
  python kafka_transformation_replicator.py --source-topic devices --target-topic devices \
      --rules ../pii_rules.json
  python kafka_transformation_replicator.py --self-test
"""
import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sys
from typing import Optional, Tuple

log = logging.getLogger("kafka_transformation_replicator")
EMAIL_MASK = re.compile(r"^(.).*(@.*)$")


# ── Mapping function (pure, unit-tested) ───────────────────────────────────────────────────────────

def transform_record(value: dict, rules: dict, salt: str) -> Tuple[Optional[dict], Optional[str]]:
    """Return (transformed_value, None) or (None, reason) when the record must be quarantined."""
    specs, default = rules["columns"], rules.get("default_action", "fail")
    unknown = [k for k in value if k not in specs]
    if unknown and default == "fail":
        return None, f"unclassified fields {sorted(unknown)}"
    out = {}
    for field, raw in value.items():
        spec = specs.get(field)
        if spec is None or spec["action"] == "drop":
            continue
        action = spec["action"]
        if action == "keep" or raw is None:
            out[field] = raw
        elif action == "hash":
            out[field] = hashlib.sha256((salt + str(raw)).encode()).hexdigest()
        elif action == "mask":
            out[field] = EMAIL_MASK.sub(r"\1***\2", str(raw))
        elif action == "nullify":
            out[field] = None
        elif action == "expr":
            # SQL expressions only run in Spark; in a plain consumer use an explicit python rule
            out[field] = PY_RULES[field](raw) if field in PY_RULES else None
    return out, None


PY_RULES = {"full_name": lambda v: str(v)[1:]}   # mirror of SUBSTRING(full_name, 2, LENGTH(full_name))


def _self_check() -> None:
    rules = {"default_action": "fail", "columns": {
        "id": {"action": "keep"}, "ip": {"action": "drop"}, "email": {"action": "mask"},
        "user_id": {"action": "hash"}, "full_name": {"action": "expr", "expr": "..."}}}
    out, err = transform_record({"id": 1, "ip": "1.2.3.4", "email": "bob@x.io", "user_id": "u1",
                                 "full_name": "Bob"}, rules, "salt")
    assert err is None and "ip" not in out and out["email"] == "b***@x.io" and out["full_name"] == "ob"
    assert out["user_id"] == hashlib.sha256(b"saltu1").hexdigest()
    out, err = transform_record({"id": 1, "phone": "123"}, rules, "s")
    assert out is None and "phone" in err
    out, err = transform_record({"id": 1, "phone": "123"}, {**rules, "default_action": "drop"}, "s")
    assert out == {"id": 1}
    print("self-test OK")


# ── Replication loop ───────────────────────────────────────────────────────────────────────────────

def run(args, rules) -> None:
    from confluent_kafka import Consumer, KafkaException, Producer

    salt = os.environ["PII_HASH_SALT"]
    consumer = Consumer({
        "bootstrap.servers": os.environ["PROD_BOOTSTRAP"],
        "group.id": args.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    staging = Producer({"bootstrap.servers": os.environ["STAGING_BOOTSTRAP"],
                        "enable.idempotence": True, "acks": "all", "linger.ms": 50})
    quarantine = Producer({"bootstrap.servers": os.environ["PROD_BOOTSTRAP"], "acks": "all"})

    src_parts = len(consumer.list_topics(args.source_topic, timeout=10).topics[args.source_topic].partitions)
    tgt_meta = staging.list_topics(args.target_topic, timeout=10).topics[args.target_topic]
    if tgt_meta.error or len(tgt_meta.partitions) != src_parts:
        raise RuntimeError("Staging topic must exist with the same partition count to keep partition order")

    delivery_errors = []

    def on_delivery(err, _msg):
        if err:
            delivery_errors.append(err)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe([args.source_topic])
    stats = {"replicated": 0, "quarantined": 0}
    try:
        while running:
            batch = consumer.consume(num_messages=args.batch_size, timeout=1.0)
            if not batch:
                continue
            for msg in batch:
                if msg.error():
                    raise KafkaException(msg.error())
                if msg.value() is None:            # tombstone: no payload, forward the delete as is
                    staging.produce(args.target_topic, key=msg.key(), value=None,
                                    partition=msg.partition(), on_delivery=on_delivery)
                    continue
                try:
                    value, reason = transform_record(json.loads(msg.value()), rules, salt)
                except (ValueError, TypeError) as exc:
                    value, reason = None, f"invalid JSON: {exc}"
                if value is None:
                    headers = (msg.headers() or []) + [("quarantine_reason", reason.encode())]
                    quarantine.produce(args.quarantine_topic, key=msg.key(), value=msg.value(),
                                       headers=headers, on_delivery=on_delivery)
                    stats["quarantined"] += 1
                    continue
                staging.produce(args.target_topic, key=msg.key(), value=json.dumps(value).encode(),
                                partition=msg.partition(), headers=msg.headers(),
                                timestamp=msg.timestamp()[1], on_delivery=on_delivery)
                stats["replicated"] += 1
                staging.poll(0)
            # flush BEFORE commit: a crash in between replays the batch instead of losing it
            if staging.flush(30) or quarantine.flush(30) or delivery_errors:
                raise RuntimeError(f"Undelivered records, not committing: {delivery_errors[:3]}")
            consumer.commit(asynchronous=False)
            log.info("Committed batch of %d (%s)", len(batch), stats)
    finally:
        consumer.close()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source-topic")
    p.add_argument("--target-topic")
    p.add_argument("--quarantine-topic", default="devices.pii-quarantine")
    p.add_argument("--rules")
    p.add_argument("--group-id", default="transformation-replicator-staging")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if args.self_test:
        _self_check()
        return 0
    if not (args.source_topic and args.target_topic and args.rules):
        p.error("--source-topic, --target-topic and --rules are required")
    with open(args.rules, encoding="utf-8") as f:
        rules = json.load(f)
    run(args, rules)
    return 0


if __name__ == "__main__":
    sys.exit(main())
