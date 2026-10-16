"""Merger -- Debezium-style CDC topic MERGEd into PostgreSQL 15+ with soft deletes.

Flow per poll batch:
  1. consume up to BATCH_SIZE change events (manual offset commits);
  2. keep the latest change per key (type, version) ordered by (ts_ms, offset);
  3. in ONE database transaction: CREATE TEMP TABLE changed_devices (LIKE dp.devices)
     + is_deleted, bulk insert the batch, MERGE (soft-delete semantics), COMMIT;
  4. only then commit the Kafka offsets.
A crash between 3 and 4 replays the batch: at-least-once delivery + MERGE on an
immutable key = idempotent result. Committing offsets first would lose changes.

Debezium envelope handled: {"op": "c|u|d|r", "before": {...}, "after": {...},
"ts_ms": ...}; tombstones (value=None) are skipped, op="d" becomes is_deleted=true.

Env: KAFKA_BOOTSTRAP, CDC_TOPIC, KAFKA_GROUP, PG_HOST, PG_DB, PG_USER, PG_PASSWORD.
Run:  python kafka_cdc_merge_postgres.py            (needs confluent-kafka, psycopg2)
      python kafka_cdc_merge_postgres.py --self-test
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys

log = logging.getLogger("cdc-merger")

KEY = ("type", "version")
VALUE_COLUMNS = ("full_name",)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
POLL_TIMEOUT_S = float(os.environ.get("POLL_TIMEOUT_S", "5"))
TARGET_TABLE = os.environ.get("TARGET_TABLE", "dp.devices")

CREATE_TEMP_SQL = f"""
CREATE TEMPORARY TABLE IF NOT EXISTS changed_devices (LIKE {TARGET_TABLE}) ON COMMIT DELETE ROWS;
ALTER TABLE changed_devices ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT false;
"""
# LIKE keeps a single schema definition; ON COMMIT DELETE ROWS empties it per batch.

MERGE_SQL = f"""
MERGE INTO {TARGET_TABLE} AS d
USING changed_devices AS c_d
   ON c_d.type = d.type AND c_d.version = d.version
 WHEN MATCHED AND c_d.is_deleted THEN DELETE
 WHEN MATCHED AND NOT c_d.is_deleted THEN UPDATE SET full_name = c_d.full_name
 WHEN NOT MATCHED AND NOT c_d.is_deleted THEN
      INSERT (type, full_name, version) VALUES (c_d.type, c_d.full_name, c_d.version)
"""


# --------------------------------------------------------------------------- pure logic
def parse_change(raw: bytes | None, offset: int) -> dict | None:
    """Debezium envelope -> flat change row, or None for tombstones / irrelevant ops."""
    if raw is None:
        return None
    event = json.loads(raw)
    payload = event.get("payload", event)  # with or without the schema wrapper
    op = payload.get("op")
    if op not in ("c", "u", "d", "r"):
        return None
    image = payload["before"] if op == "d" else payload["after"]
    if not image or any(image.get(k) is None for k in KEY):
        raise ValueError(f"change at offset {offset} has no usable key: {payload}")
    row = {k: image.get(k) for k in (*KEY, *VALUE_COLUMNS)}
    row["is_deleted"] = op == "d"
    row["_order"] = (int(payload.get("ts_ms") or 0), offset)
    return row


def latest_per_key(rows: list[dict]) -> list[dict]:
    """MERGE rejects two source rows for one target row: keep the last change per key."""
    best: dict[tuple, dict] = {}
    for r in rows:
        k = tuple(r[c] for c in KEY)
        if k not in best or r["_order"] > best[k]["_order"]:
            best[k] = r
    return list(best.values())


def offsets_to_commit(messages) -> dict[tuple[str, int], int]:
    """Next offset to read per (topic, partition) = last consumed + 1."""
    out: dict[tuple[str, int], int] = {}
    for m in messages:
        tp = (m.topic(), m.partition())
        out[tp] = max(out.get(tp, -1), m.offset() + 1)
    return out


def _self_check() -> None:
    upd = json.dumps({"op": "u", "after": {"type": "tv", "version": "1", "full_name": "B"}, "ts_ms": 2})
    ins = json.dumps({"payload": {"op": "c", "after": {"type": "tv", "version": "1", "full_name": "A"},
                                  "ts_ms": 1}})
    dele = json.dumps({"op": "d", "before": {"type": "pc", "version": "2", "full_name": "P"}, "ts_ms": 3})
    rows = [parse_change(ins.encode(), 10), parse_change(upd.encode(), 11), parse_change(dele.encode(), 12)]
    assert parse_change(None, 13) is None
    latest = {(r["type"], r["version"]): r for r in latest_per_key(rows)}
    assert latest[("tv", "1")]["full_name"] == "B" and not latest[("tv", "1")]["is_deleted"]
    assert latest[("pc", "2")]["is_deleted"]

    class M:  # minimal stand-in for confluent_kafka.Message
        def __init__(self, p, o): self.p, self.o = p, o
        def topic(self): return "cdc"
        def partition(self): return self.p
        def offset(self): return self.o
    assert offsets_to_commit([M(0, 5), M(0, 7), M(1, 2)]) == {("cdc", 0): 8, ("cdc", 1): 3}
    print("self-check OK")


# --------------------------------------------------------------------------- I/O side
def pg_connect():
    import psycopg2
    return psycopg2.connect(host=os.environ.get("PG_HOST", "localhost"),
                            dbname=os.environ.get("PG_DB", "dp"),
                            user=os.environ.get("PG_USER", "dp"),
                            password=os.environ["PG_PASSWORD"])


def merge_batch(conn, rows: list[dict]) -> int:
    from psycopg2.extras import execute_values
    cols = (*KEY, *VALUE_COLUMNS, "is_deleted")
    with conn:  # one transaction: temp load + MERGE commit together or not at all
        with conn.cursor() as cur:
            cur.execute(CREATE_TEMP_SQL)
            execute_values(cur, f"INSERT INTO changed_devices ({', '.join(cols)}) VALUES %s",
                           [tuple(r[c] for c in cols) for r in rows])
            cur.execute(MERGE_SQL)
            return cur.rowcount


def run() -> None:
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": os.environ.get("KAFKA_GROUP", "devices-merger"),
        "enable.auto.commit": False,          # offsets follow the DB commit, never precede it
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    consumer.subscribe([os.environ.get("CDC_TOPIC", "dbserver.dp.devices")])
    conn = pg_connect()
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    try:
        while not stop["flag"]:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_S)
            if not messages:
                continue
            errors = [m for m in messages if m.error()]
            if errors:
                raise RuntimeError(f"Kafka error: {errors[0].error()}")
            rows = [r for m in messages if (r := parse_change(m.value(), m.offset())) is not None]
            if rows:
                latest = latest_per_key(rows)
                merged = merge_batch(conn, latest)
                log.info("batch: %d events, %d keys, %d rows merged", len(messages), len(latest), merged)
            consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in
                                     offsets_to_commit(messages).items()], asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _self_check()
        sys.exit(0)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run()
