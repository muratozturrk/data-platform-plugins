"""Dynamic Joiner -- hourly health checks for the streaming visits x ads join.

Airflow does not run the stream-to-stream join; the Spark job
(spark/spark_stream_stream_time_bounded_join.py) does. This DAG watches the
two trade-offs of the pattern and fails (-> alert) when they drift:

  * space vs exactness  -> match rate too low (buffer/watermark too small for
                           the real latency gap) or state rows growing without
                           bound (one side silent, watermark not advancing)
  * late data           -> rows dropped by the watermark; watermark lag vs
                           wall clock (job stuck or a source stopped)

Input: the JSONL stats file the Spark job appends to ("batch" and "progress"
records). Only records logged inside the run's data interval are evaluated.

Run: drop into dags/. Variables (all optional):
  dynamic_joiner_stats_file   (default /data/stats/visits_with_ads.jsonl)
  dynamic_joiner_thresholds   JSON, keys as in DEFAULT_THRESHOLDS
Prereqs: apache-airflow>=2.4.
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

log = logging.getLogger(__name__)

DEFAULT_THRESHOLDS = {
    "min_match_rate": 0.60,          # matched / (matched + unmatched)
    "min_rows_for_rate": 1000,       # don't judge the rate on tiny samples
    "max_watermark_lag_min": 30,     # data_interval_end - latest watermark
    "max_state_growth_factor": 2.0,  # last / first state_rows inside the hour
    "max_state_rows": 50_000_000,
    "max_dropped_ratio": 0.01,       # rows dropped by watermark / joined rows
}


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def evaluate(batches: List[Dict], progress: List[Dict], interval_end: datetime, t: Dict) -> List[str]:
    """Return the list of violated checks (empty list = healthy). Pure logic."""
    problems = []
    matched = sum(b["matched"] for b in batches)
    unmatched = sum(b["unmatched"] for b in batches)
    total = matched + unmatched
    if total >= t["min_rows_for_rate"] and matched / total < t["min_match_rate"]:
        problems.append(f"match rate {matched / total:.2%} < {t['min_match_rate']:.0%}: "
                        "widen the watermark/time boundary or check the ads source lag")
    if progress:
        wm = [p for p in progress if p.get("watermark")]
        if wm:
            lag_min = (interval_end - _parse_ts(wm[-1]["watermark"])).total_seconds() / 60
            if lag_min > t["max_watermark_lag_min"]:
                problems.append(f"watermark lag {lag_min:.0f} min > {t['max_watermark_lag_min']} min: "
                                "a source is silent or the query is stuck")
        first, last = progress[0]["state_rows"], progress[-1]["state_rows"]
        if last > t["max_state_rows"]:
            problems.append(f"state rows {last} > {t['max_state_rows']}")
        if first > 0 and last / first > t["max_state_growth_factor"]:
            problems.append(f"state grew x{last / first:.1f} in one hour: GC watermark not evicting")
        dropped = sum(p.get("rows_dropped_by_watermark", 0) for p in progress)
        if total and dropped / total > t["max_dropped_ratio"]:
            problems.append(f"{dropped} rows dropped as late ({dropped / total:.2%} of output): "
                            "consider a late-data integrator")
    return problems


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["data-value", "enrichment", "streaming-health"],
)
def dynamic_joiner_health():

    @task
    def load_stats(data_interval_start=None, data_interval_end=None) -> Dict:
        path = Variable.get("dynamic_joiner_stats_file", default_var="/data/stats/visits_with_ads.jsonl")
        batches, progress = [], []
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    rec = json.loads(line)
                    if not data_interval_start <= _parse_ts(rec["logged_at"]) < data_interval_end:
                        continue
                    (batches if rec["kind"] == "batch" else progress).append(rec)
        except FileNotFoundError:
            raise AirflowException(f"stats file {path} missing: is the streaming join running?")
        if not batches and not progress:
            raise AirflowException("no stats in this interval: the streaming query is not making progress")
        log.info("loaded %d batch and %d progress records", len(batches), len(progress))
        return {"batches": batches, "progress": progress}

    @task
    def check_health(stats: Dict, data_interval_end=None) -> Dict:
        thresholds = {**DEFAULT_THRESHOLDS,
                      **Variable.get("dynamic_joiner_thresholds", default_var={}, deserialize_json=True)}
        problems = evaluate(stats["batches"], stats["progress"], data_interval_end, thresholds)
        for p in problems:
            log.error(p)
        if problems:
            raise AirflowException(f"{len(problems)} dynamic joiner health check(s) failed")
        return {"batches": len(stats["batches"]), "healthy": True}

    check_health(load_stats())


dynamic_joiner_health()


if __name__ == "__main__":
    # self-check of the pure evaluation logic
    from datetime import timezone
    end = datetime(2024, 6, 1, 11, tzinfo=timezone.utc)
    ok = evaluate([{"matched": 900, "unmatched": 100}], [
        {"watermark": "2024-06-01T10:55:00Z", "state_rows": 100, "rows_dropped_by_watermark": 0},
        {"watermark": "2024-06-01T10:58:00Z", "state_rows": 120, "rows_dropped_by_watermark": 1}],
        end, {**DEFAULT_THRESHOLDS, "min_rows_for_rate": 10})
    assert ok == [], ok
    bad = evaluate([{"matched": 100, "unmatched": 900}], [
        {"watermark": "2024-06-01T09:00:00Z", "state_rows": 100, "rows_dropped_by_watermark": 0},
        {"watermark": "2024-06-01T09:00:00Z", "state_rows": 500, "rows_dropped_by_watermark": 50}],
        end, {**DEFAULT_THRESHOLDS, "min_rows_for_rate": 10})
    assert len(bad) == 4, bad
    print("self-test OK")
