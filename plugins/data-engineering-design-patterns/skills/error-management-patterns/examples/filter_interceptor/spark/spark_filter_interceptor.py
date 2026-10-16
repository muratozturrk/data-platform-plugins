"""Filter Interceptor - PySpark programmatic API and SQL.

Problem: after a release, the filtered volume jumps from 15% to 90%, and the
execution plan cannot tell which condition is responsible because the optimizer
collapses all filters into one step. The pattern wraps every filter condition
with a counter so the job reports how many rows each condition removed.

``--mode programmatic``
    ``FilterWithAccumulator`` = name + predicate + Spark accumulator. A
    ``mapInPandas`` function evaluates the filters of each attribute in order;
    the first failing filter increments its accumulator and drops the row.
    Accumulators are read on the driver after the write action.

``--mode sql``
    A subquery exposes the filter outcome as a ``status_flag`` column
    (CASE ... WHEN cond IS FALSE THEN 'name'); one query counts flags, another
    writes the rows whose flag is NULL. The flagged view is cached so the input
    is evaluated once for both.

Both modes write ``_filter_stats/<partition>.json`` (input rows, kept rows,
per-filter counts) for the Airflow regression check.

Accumulator caveat: updates made inside transformations are not exactly-once;
a retried or speculative task counts again. Keep speculation off, or use the
SQL mode, whose counts come from a query result, when exact numbers matter.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_filter_interceptor.py \
      --mode programmatic --partition-date 2024-12-31
  python spark_filter_interceptor.py --self-test
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

log = logging.getLogger("filter_interceptor")

# name, attribute, predicate (True = keep). Order matters: first failure wins.
FILTER_DEFINITIONS: List[tuple] = [
    ("null_type", "type", lambda d: d["type"] is not None),
    ("short_type", "type", lambda d: len(d["type"]) > 2),
    ("null_full_name", "full_name", lambda d: d["full_name"] is not None),
    ("null_version", "version", lambda d: d["version"] is not None),
]

SQL_FLAGS = """
SELECT
  CASE
    WHEN (type IS NOT NULL) IS FALSE THEN 'null_type'
    WHEN (LENGTH(type) > 2) IS FALSE THEN 'short_type'
    WHEN (full_name IS NOT NULL) IS FALSE THEN 'null_full_name'
    WHEN (version IS NOT NULL) IS FALSE THEN 'null_version'
    ELSE NULL
  END AS status_flag,
  type, full_name, version
FROM input
"""


@dataclasses.dataclass
class FilterWithAccumulator:
    name: str
    filter: Callable[[Any], bool]
    accumulator: Any   # pyspark Accumulator[int]; any object with .add() in tests


def build_filters(make_accumulator: Callable[[], Any]) -> Dict[str, List[FilterWithAccumulator]]:
    filters: Dict[str, List[FilterWithAccumulator]] = {}
    for name, attribute, predicate in FILTER_DEFINITIONS:
        filters.setdefault(attribute, []).append(
            FilterWithAccumulator(name, predicate, make_accumulator()))
    return filters


def keep_row(row: Dict[str, Any], filters: Dict[str, List[FilterWithAccumulator]]) -> bool:
    """Evaluate the wrapped filters; count the first one that rejects the row."""
    for attribute, attribute_filters in filters.items():
        for wrapped in attribute_filters:
            if not wrapped.filter(row):
                wrapped.accumulator.add(1)
                return False
    return True


def stats_document(partition_date: str, mode: str, input_rows: int, kept_rows: int,
                   per_filter: Dict[str, int]) -> dict:
    return {"partition_date": partition_date, "mode": mode, "input_rows": input_rows,
            "kept_rows": kept_rows, "filtered_rows": input_rows - kept_rows,
            "filtered_ratio": round((input_rows - kept_rows) / input_rows, 6) if input_rows else 0.0,
            "per_filter": per_filter, "written_at": dt.datetime.now(dt.timezone.utc).isoformat()}


def run_programmatic(spark, input_df, output_dir: str) -> Dict[str, int]:
    filters = build_filters(lambda: spark.sparkContext.accumulator(0))

    def filter_devices(devices: Iterator["pd.DataFrame"]) -> Iterator["pd.DataFrame"]:
        for devices_df in devices:
            mask = devices_df.apply(lambda device: keep_row(device.to_dict(), filters), axis=1)
            yield devices_df[mask.astype(bool)] if len(devices_df) else devices_df

    valid = input_df.mapInPandas(filter_devices, input_df.schema)
    valid.write.mode("overwrite").format("delta").save(output_dir)   # the action
    return {f.name: f.accumulator.value for fs in filters.values() for f in fs}


def run_sql(spark, input_df, output_dir: str) -> Dict[str, int]:
    input_df.createOrReplaceTempView("input")
    flagged = spark.sql(SQL_FLAGS)
    flagged.cache()
    try:
        flagged.createOrReplaceTempView("input_with_flags")
        counts = {r["status_flag"]: r["filtered"] for r in spark.sql(
            "SELECT status_flag, COUNT(*) AS filtered FROM input_with_flags "
            "WHERE status_flag IS NOT NULL GROUP BY status_flag").collect()}
        (spark.sql("SELECT type, full_name, version FROM input_with_flags WHERE status_flag IS NULL")
         .write.mode("overwrite").format("delta").save(output_dir))
    finally:
        flagged.unpersist()
    return {name: counts.get(name, 0) for name, _, _ in FILTER_DEFINITIONS}


def _self_check() -> None:
    class Counter:
        def __init__(self):
            self.value = 0

        def add(self, n):
            self.value += n

    filters = build_filters(Counter)
    rows = [{"type": None, "full_name": "x", "version": "1"},
            {"type": "ab", "full_name": "x", "version": "1"},
            {"type": "abc", "full_name": None, "version": None},
            {"type": "abc", "full_name": "x", "version": "1"}]
    kept = [r for r in rows if keep_row(r, filters)]
    counts = {f.name: f.accumulator.value for fs in filters.values() for f in fs}
    assert kept == [rows[3]]
    # one count per rejected row, attributed to the first failing filter
    assert counts == {"null_type": 1, "short_type": 1, "null_full_name": 1, "null_version": 0}
    doc = stats_document("2024-12-31", "programmatic", 4, 1, counts)
    assert doc["filtered_ratio"] == 0.75 and doc["filtered_rows"] == 3
    assert "'short_type'" in SQL_FLAGS
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=["programmatic", "sql"], default="programmatic")
    p.add_argument("--partition-date", required=True)
    p.add_argument("--base-dir", default="/tmp/dp/filter-interceptor")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(f"filter-interceptor-{args.mode}")
             .config("spark.speculation", "false")   # accumulators must not double count
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    input_df = (spark.read.schema("type STRING, full_name STRING, version STRING")
                .json(f"{args.base_dir}/input/{args.partition_date}")).cache()
    input_rows = input_df.count()
    output_dir = f"{args.base_dir}/output/devices-valid-{args.mode}/{args.partition_date}"
    runner = run_programmatic if args.mode == "programmatic" else run_sql
    per_filter = runner(spark, input_df, output_dir)
    kept = spark.read.format("delta").load(output_dir).count()

    doc = stats_document(args.partition_date, args.mode, input_rows, kept, per_filter)
    target = Path(args.base_dir) / "_filter_stats" / f"{args.partition_date}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    os.replace(tmp, target)
    for name, count in per_filter.items():
        log.info("filter %-15s removed %s rows", name, count)
    log.info("kept %s of %s rows (%.1f%% filtered)", kept, input_rows, doc["filtered_ratio"] * 100)


if __name__ == "__main__":
    main()
