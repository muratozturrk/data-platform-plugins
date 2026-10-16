"""Dynamic Late Data Integrator - partition update tracker fed by Kafka.

For stores without a native "last update time" per partition (BigQuery's
INFORMATION_SCHEMA.PARTITIONS or Iceberg's partitions metadata table have one),
you must generate it yourself. This consumer does it from the
ingestion stream (e.g. the late-data topic produced by the Late Data Detector,
or the raw topic feeding the partitioned table):

  * per poll batch, it computes MAX(ingestion time) per event-date partition
    and upserts ``last_update_time`` in the PostgreSQL state table,
    with GREATEST so redelivered batches never move it back;
  * offsets are committed after the DB transaction (at-least-once, harmless
    because the upsert is idempotent).

``plan`` mode is the backfill planner on the same table. Instead
of Airflow's depends_on_past, it serializes concurrent planners with row locks:
``FOR UPDATE SKIP LOCKED`` + setting is_processed in one statement, so two
planners can never return the same partition.

State table:
  CREATE TABLE partition_state (
    partition DATE PRIMARY KEY, last_processed_time TIMESTAMPTZ,
    last_update_time TIMESTAMPTZ, is_processed BOOLEAN NOT NULL DEFAULT false);

Run:  python kafka_partition_update_tracker.py track
      python kafka_partition_update_tracker.py plan --current-partition 2024-12-20
      python kafka_partition_update_tracker.py --self-test
Requires: pip install kafka-python psycopg2-binary. Env: KAFKA_BOOTSTRAP_SERVERS, PG_DSN.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple

log = logging.getLogger("partition_update_tracker")

UPSERT_UPDATE_TIME = """
INSERT INTO partition_state (partition, last_update_time) VALUES (%s, %s)
ON CONFLICT (partition) DO UPDATE
  SET last_update_time = GREATEST(partition_state.last_update_time, EXCLUDED.last_update_time)
"""

# Planner query + atomic claim: no other planner can select the same rows.
PLAN_BACKFILL = """
UPDATE partition_state SET is_processed = true
WHERE partition IN (
  SELECT partition FROM partition_state
  WHERE last_update_time > last_processed_time
    AND partition < %(current)s
    AND partition >= %(oldest)s
    AND is_processed = false
  ORDER BY partition
  FOR UPDATE SKIP LOCKED)
RETURNING partition
"""

MARK_PROCESSED = """
UPDATE partition_state SET last_processed_time = %s, is_processed = false WHERE partition = %s
"""


def latest_update_per_partition(events: Iterable[Tuple[str, dt.datetime]]) -> Dict[str, dt.datetime]:
    """(event_time ISO string, ingestion time) -> {event date: MAX(ingestion time)}."""
    latest: Dict[str, dt.datetime] = {}
    for event_time, ingested_at in events:
        partition = dt.datetime.fromisoformat(event_time).date().isoformat()
        if partition not in latest or ingested_at > latest[partition]:
            latest[partition] = ingested_at
    return latest


def track(args: argparse.Namespace) -> None:
    import psycopg2
    from kafka import KafkaConsumer

    conn = psycopg2.connect(os.environ["PG_DSN"])
    consumer = KafkaConsumer(args.topic, group_id=args.group, enable_auto_commit=False,
                             auto_offset_reset="earliest",
                             bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            if not polled:
                continue
            events = []
            for records in polled.values():
                for record in records:
                    visit = json.loads(record.value)
                    visit = visit.get("visit", visit)  # accepts Late Data Detector envelopes
                    # Broker timestamp = when the row became available to the table loader.
                    ingested = dt.datetime.fromtimestamp(record.timestamp / 1000, dt.timezone.utc)
                    events.append((visit["event_time"], ingested))
            updates = latest_update_per_partition(events)
            with conn, conn.cursor() as cur:   # one transaction per poll
                cur.executemany(UPSERT_UPDATE_TIME, list(updates.items()))
            consumer.commit()
            log.info("updated last_update_time for %s partition(s)", len(updates))
    finally:
        consumer.close()
        conn.close()


def plan(args: argparse.Namespace) -> List[str]:
    import psycopg2

    oldest = (dt.date.fromisoformat(args.current_partition)
              - dt.timedelta(days=args.max_lookback_days)).isoformat()
    with psycopg2.connect(os.environ["PG_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(PLAN_BACKFILL, {"current": args.current_partition, "oldest": oldest})
        partitions = sorted(row[0].isoformat() for row in cur.fetchall())
    print(json.dumps({"partitions": [{"event_time": p} for p in partitions]}))
    return partitions


def mark_processed(args: argparse.Namespace) -> None:
    import psycopg2

    with psycopg2.connect(os.environ["PG_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(MARK_PROCESSED, (args.processed_at or dt.datetime.now(dt.timezone.utc),
                                     args.current_partition))


def _self_check() -> None:
    u = dt.timezone.utc
    latest = latest_update_per_partition([
        ("2024-12-18T10:00:00", dt.datetime(2024, 12, 20, 10, 12, tzinfo=u)),
        ("2024-12-18T23:59:00", dt.datetime(2024, 12, 20, 9, 0, tzinfo=u)),
        ("2024-12-17T01:00:00", dt.datetime(2024, 12, 17, 3, 0, tzinfo=u)),
    ])
    assert latest == {"2024-12-18": dt.datetime(2024, 12, 20, 10, 12, tzinfo=u),
                      "2024-12-17": dt.datetime(2024, 12, 17, 3, 0, tzinfo=u)}
    assert "FOR UPDATE SKIP LOCKED" in PLAN_BACKFILL and "is_processed = false" in PLAN_BACKFILL
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("mode", choices=["track", "plan", "mark-processed"])
    p.add_argument("--topic", default="visits")
    p.add_argument("--group", default="partition-update-tracker")
    p.add_argument("--current-partition")
    p.add_argument("--max-lookback-days", type=int, default=60)
    p.add_argument("--processed-at", type=dt.datetime.fromisoformat, default=None)
    args = p.parse_args(argv)
    if args.mode != "track" and not args.current_partition:
        p.error(f"{args.mode} requires --current-partition")
    {"track": track, "plan": plan, "mark-processed": mark_processed}[args.mode](args)


if __name__ == "__main__":
    main()
