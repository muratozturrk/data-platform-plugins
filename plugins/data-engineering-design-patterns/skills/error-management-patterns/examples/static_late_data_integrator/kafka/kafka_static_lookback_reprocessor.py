"""Static Late Data Integrator - batch job over a Kafka topic.

Same static lookback window, applied to a log instead of partitioned files. A
daily run for execution date E rebuilds the event-time partitions
[E - N, E] from the topic:
  1. seek every partition to the first offset whose broker timestamp is
     >= (E - N) 00:00 UTC (``offsets_for_times``) - a record for day D cannot
     be appended before D, so nothing in the window is skipped;
  2. read up to the end offsets captured at start (bounded batch, the run does
     not chase new data);
  3. aggregate per event date; records older than E - N are beyond the static
     window and ignored, records newer than E belong to the next run;
  4. overwrite one output file per date atomically (tmp + rename), so reruns
     and overlapping runs are idempotent.

No consumer group commits: the window, not the offsets, defines what to read,
which is what makes the job rerunnable for any date.

Run:  python kafka_static_lookback_reprocessor.py --execution-date 2024-12-31 --lookback-days 14
      python kafka_static_lookback_reprocessor.py --self-test
Requires: pip install kafka-python. Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

log = logging.getLogger("kafka_static_lookback")


def window_bounds(execution_date: str, lookback_days: int) -> Tuple[dt.date, dt.date]:
    end = dt.date.fromisoformat(execution_date)
    return end - dt.timedelta(days=lookback_days), end


def aggregate(visits: Iterable[dict], start: dt.date, end: dt.date) -> Dict[str, Dict[str, int]]:
    """visits per (event date, referral) inside [start, end]; the rest is ignored."""
    stats: Dict[str, Dict[str, int]] = collections.defaultdict(lambda: collections.defaultdict(int))
    for visit in visits:
        event_date = dt.datetime.fromisoformat(visit["event_time"]).date()
        if start <= event_date <= end:
            stats[event_date.isoformat()][visit.get("referral") or "direct"] += 1
    return stats


def write_partition(output_dir: Path, date: str, counts: Dict[str, int]) -> None:
    target = output_dir / f"event_date={date}" / "stats.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps({"event_date": date, "visits_per_referral": counts,
                               "computed_at": dt.datetime.now(dt.timezone.utc).isoformat()}))
    os.replace(tmp, target)


def read_window(topic: str, start: dt.date) -> Iterable[dict]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                             enable_auto_commit=False, group_id=None)
    try:
        partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or []]
        consumer.assign(partitions)
        start_ms = int(dt.datetime.combine(start, dt.time(), dt.timezone.utc).timestamp() * 1000)
        starts = consumer.offsets_for_times({tp: start_ms for tp in partitions})
        ends = consumer.end_offsets(partitions)
        remaining = {}
        for tp in partitions:
            if starts.get(tp) is None or starts[tp].offset >= ends[tp]:
                continue  # no record in the window for this partition
            consumer.seek(tp, starts[tp].offset)
            remaining[tp] = ends[tp]
        while remaining:
            for tp, records in consumer.poll(timeout_ms=1000).items():
                for record in records:
                    if record.offset < remaining.get(tp, -1):
                        yield json.loads(record.value)
                if tp in remaining and consumer.position(tp) >= remaining[tp]:
                    del remaining[tp]
                    consumer.pause(tp)
    finally:
        consumer.close()


def run(args: argparse.Namespace) -> None:
    start, end = window_bounds(args.execution_date, args.lookback_days)
    stats = aggregate(read_window(args.topic, start), start, end)
    output_dir = Path(args.output_dir)
    day = start
    while day <= end:  # also rewrite dates without data (they may have lost rows)
        write_partition(output_dir, day.isoformat(), dict(stats.get(day.isoformat(), {})))
        day += dt.timedelta(days=1)
    log.info("rebuilt %s partitions %s..%s", (end - start).days + 1, start, end)


def _self_check() -> None:
    start, end = window_bounds("2024-12-31", 14)
    assert start == dt.date(2024, 12, 17) and end == dt.date(2024, 12, 31)
    visits = [{"event_time": "2024-12-31T10:00:00", "referral": "google"},
              {"event_time": "2024-12-20T08:00:00", "referral": "google"},   # late, in window
              {"event_time": "2024-12-15T08:00:00", "referral": "bing"},     # beyond window
              {"event_time": "2024-12-20T09:00:00"}]
    stats = aggregate(visits, start, end)
    assert stats["2024-12-20"] == {"google": 1, "direct": 1}
    assert "2024-12-15" not in stats and stats["2024-12-31"]["google"] == 1
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--execution-date", required=True)
    p.add_argument("--lookback-days", type=int, default=14)
    p.add_argument("--topic", default="visits")
    p.add_argument("--output-dir", default="/tmp/dp/static-late-data-integrator/kafka-stats")
    run(p.parse_args(argv))


if __name__ == "__main__":
    main()
