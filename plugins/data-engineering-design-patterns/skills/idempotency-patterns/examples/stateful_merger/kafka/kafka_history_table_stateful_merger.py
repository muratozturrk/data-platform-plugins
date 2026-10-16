"""Stateful Merger without a versioned store -- Kafka slice -> PostgreSQL history table.

Stateful Merger adaptation for data stores without versioning.

PostgreSQL has no time travel, so the raw increments are kept in dp.devices_history
with the execution_time that loaded them. For one execution window [start, end):

  1. read the change topic slice whose broker timestamps fall in [start, end)
     (offsets_for_times -> deterministic and replayable as long as retention keeps it);
  2. BEGIN
     backfill? = SELECT COUNT(*) > 0 FROM devices_history WHERE execution_time > start
       yes -> DELETE history rows with execution_time >= start
              rebuild dp.devices from the remaining history (latest change per key,
              deleted keys dropped) = the table as the previous execution left it
       no  -> DELETE history rows of this execution only (plain retry)
     INSERT the slice into devices_history
     MERGE the slice (latest per key) into dp.devices
     COMMIT    -- consumers never see the truncated/rebuilt intermediate state
Re-running a window gives the same table; replaying an old window first rewinds the table.
Use a topic with message.timestamp.type=LogAppendTime: with CreateTime, producer clocks
decide the slice and late producers make it non-deterministic.

Env: KAFKA_BOOTSTRAP, CHANGES_TOPIC, PG_HOST, PG_DB, PG_USER, PG_PASSWORD.
Run:  python kafka_history_table_stateful_merger.py --execution-time 2024-10-07T00:00:00+00:00 \
          --window-hours 24
      python kafka_history_table_stateful_merger.py --self-test
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta

log = logging.getLogger("history-stateful-merger")

HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS dp.devices_history (
  type TEXT NOT NULL, version TEXT NOT NULL, full_name TEXT,
  is_deleted BOOLEAN NOT NULL, change_ts BIGINT NOT NULL,
  kafka_partition INT NOT NULL, kafka_offset BIGINT NOT NULL,
  execution_time TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (kafka_partition, kafka_offset));
CREATE INDEX IF NOT EXISTS devices_history_exec_idx ON dp.devices_history (execution_time);
"""
IS_BACKFILL_SQL = "SELECT COUNT(*) > 0 FROM dp.devices_history WHERE execution_time > %s"  # backfill detection
# Windowed Deduplicator over the history: last change per key, in load then log order.
LATEST_FROM_HISTORY = """
SELECT DISTINCT ON (type, version) type, version, full_name, is_deleted
  FROM dp.devices_history {where}
 ORDER BY type, version, execution_time DESC, change_ts DESC, kafka_partition DESC, kafka_offset DESC
"""
REBUILD_SQL = ("TRUNCATE dp.devices; INSERT INTO dp.devices (type, version, full_name) "
               "SELECT type, version, full_name FROM (" + LATEST_FROM_HISTORY.format(where="")
               + ") h WHERE NOT h.is_deleted")
MERGE_SQL = ("MERGE INTO dp.devices AS d USING (" + LATEST_FROM_HISTORY.format(where="WHERE execution_time = %s")
             + """) AS c ON c.type = d.type AND c.version = d.version
 WHEN MATCHED AND c.is_deleted THEN DELETE
 WHEN MATCHED AND NOT c.is_deleted THEN UPDATE SET full_name = c.full_name
 WHEN NOT MATCHED AND NOT c.is_deleted THEN INSERT (type, version, full_name)
      VALUES (c.type, c.version, c.full_name)""")


# --------------------------------------------------------------------------- pure logic
def slice_bounds(start_offsets: dict[int, int], end_offsets: dict[int, int],
                 high_watermarks: dict[int, int]) -> dict[int, tuple[int, int]]:
    """Per partition [first, last) offsets. offsets_for_times returns -1 when no message is
    at/after the timestamp: for the start that means 'empty slice', for the end 'up to HWM'."""
    out = {}
    for p, hwm in high_watermarks.items():
        first = start_offsets.get(p, -1)
        last = end_offsets.get(p, -1)
        first = hwm if first < 0 else first
        last = hwm if last < 0 else last
        if first < last:
            out[p] = (first, last)
    return out


def latest_per_key(rows: list[dict]) -> dict[tuple, dict]:
    best: dict[tuple, dict] = {}
    for r in rows:
        k = (r["type"], r["version"])
        order = (r["change_ts"], r["kafka_partition"], r["kafka_offset"])
        if k not in best or order > (best[k]["change_ts"], best[k]["kafka_partition"], best[k]["kafka_offset"]):
            best[k] = r
    return best


def _self_check() -> None:
    assert slice_bounds({0: 10, 1: -1, 2: 5}, {0: 20, 1: -1, 2: -1}, {0: 30, 1: 7, 2: 9}) == {0: (10, 20), 2: (5, 9)}
    rows = [
        {"type": "tv", "version": "1", "change_ts": 1, "kafka_partition": 0, "kafka_offset": 1, "is_deleted": False},
        {"type": "tv", "version": "1", "change_ts": 1, "kafka_partition": 0, "kafka_offset": 4, "is_deleted": True},
        {"type": "pc", "version": "1", "change_ts": 2, "kafka_partition": 1, "kafka_offset": 0, "is_deleted": False},
    ]
    latest = latest_per_key(rows)
    assert latest[("tv", "1")]["is_deleted"] and not latest[("pc", "1")]["is_deleted"]
    print("self-check OK")


# --------------------------------------------------------------------------- I/O side
def read_slice(topic: str, start: datetime, end: datetime) -> list[dict]:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                  "group.id": "stateful-merger-slice-reader",   # offsets never committed
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    try:
        partitions = list(c.list_topics(topic, timeout=10).topics[topic].partitions)
        ts = lambda d: int(d.timestamp() * 1000)
        starts = {tp.partition: tp.offset for tp in
                  c.offsets_for_times([TopicPartition(topic, p, ts(start)) for p in partitions], timeout=10)}
        ends = {tp.partition: tp.offset for tp in
                c.offsets_for_times([TopicPartition(topic, p, ts(end)) for p in partitions], timeout=10)}
        hwms = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
        bounds = slice_bounds(starts, ends, hwms)
        if not bounds:
            return []
        c.assign([TopicPartition(topic, p, first) for p, (first, _) in bounds.items()])
        rows, done = [], set()
        while len(done) < len(bounds):
            msg = c.poll(5.0)
            # ponytail: a slice ending on transaction markers never yields its last offset and
            # times out here; compare c.position() with the end offset if that topic is transactional.
            if msg is None:
                raise TimeoutError(f"slice not fully read, missing partitions {set(bounds) - done}")
            if msg.error():
                raise RuntimeError(msg.error())
            p, off = msg.partition(), msg.offset()
            if p in done or off >= bounds[p][1]:
                done.add(p)
                continue
            v = json.loads(msg.value())
            rows.append({"type": v["type"], "version": v["version"], "full_name": v.get("full_name"),
                         "is_deleted": bool(v.get("is_deleted", False)),
                         "change_ts": int(v.get("change_ts") or msg.timestamp()[1]),
                         "kafka_partition": p, "kafka_offset": off})
            if off + 1 >= bounds[p][1]:
                done.add(p)
        return rows
    finally:
        c.close()


def apply_window(conn, rows: list[dict], execution_time: datetime) -> str:
    from psycopg2.extras import execute_values
    with conn:                                   # single transaction for the whole window
        with conn.cursor() as cur:
            cur.execute(HISTORY_DDL)
            cur.execute(IS_BACKFILL_SQL, (execution_time,))
            backfill = cur.fetchone()[0]
            if backfill:
                cur.execute("DELETE FROM dp.devices_history WHERE execution_time >= %s", (execution_time,))
                cur.execute(REBUILD_SQL)         # restore = state left by the previous execution
            else:
                cur.execute("DELETE FROM dp.devices_history WHERE execution_time = %s", (execution_time,))
            if rows:
                execute_values(cur, "INSERT INTO dp.devices_history (type, version, full_name, is_deleted, "
                                    "change_ts, kafka_partition, kafka_offset, execution_time) VALUES %s",
                               [(r["type"], r["version"], r["full_name"], r["is_deleted"], r["change_ts"],
                                 r["kafka_partition"], r["kafka_offset"], execution_time) for r in rows])
                cur.execute(MERGE_SQL, (execution_time,))
    return "backfill" if backfill else "normal"


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_check()
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--execution-time", required=True, help="window start, ISO-8601 with timezone")
    p.add_argument("--window-hours", type=int, default=24)
    a = p.parse_args(argv)
    start = datetime.fromisoformat(a.execution_time)
    if start.tzinfo is None:
        p.error("--execution-time needs a timezone offset, e.g. +00:00")
    end = start + timedelta(hours=a.window_hours)

    import psycopg2
    rows = read_slice(os.environ.get("CHANGES_TOPIC", "devices_changes"), start, end)
    conn = psycopg2.connect(host=os.environ.get("PG_HOST", "localhost"), dbname=os.environ.get("PG_DB", "dp"),
                            user=os.environ.get("PG_USER", "dp"), password=os.environ["PG_PASSWORD"])
    try:
        mode = apply_window(conn, rows, start)
        log.info("window %s..%s applied in %s mode, %d changes (%d keys)",
                 start, end, mode, len(rows), len(latest_per_key(rows)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
