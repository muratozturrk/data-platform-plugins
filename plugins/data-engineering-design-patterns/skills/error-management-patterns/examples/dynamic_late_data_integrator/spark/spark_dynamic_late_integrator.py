"""Dynamic Late Data Integrator - Delta Lake state table + partition tracker.

Instead of a fixed lookback, only partitions that really received late data are
backfilled. The state table has one row per partition:

  partition | last_processed_version | last_update_version | is_processed

Delta reasons in table versions rather than update times. The
last update version per partition is read from the transaction log: every
``_delta_log/<version>.json`` commit lists add/remove actions with their
``partitionValues`` - the PySpark counterpart of Scala
``DeltaLog.getChanges``.

Subcommands, called by the Airflow DAG in this order for partition D:
  mark-processing  is_processed = true for D, so a concurrent run does not plan D
                   as a backfill.
  process          recompute D from the raw table, overwrite output partition D.
  mark-processed   last_processed_version = version read by ``process``,
                   is_processed = false (D can be replayed if late data comes).
  plan             partitions where last_update_version > last_processed_version
                   AND NOT is_processed AND partition < D,
                   optionally capped by --max-lookback-days (very late data on
                   stateful pipelines). Marks them is_processed = true and writes
                   them to a per-run JSON file read by Dynamic Task Mapping.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_dynamic_late_integrator.py \
      plan --partition 2024-12-20 --output-file /tmp/dp/dynamic/plans/2024-12-20/run1.json
  python spark_dynamic_late_integrator.py --self-test
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger("dynamic_late_integrator")

PARTITION_COLUMN = "event_date"


@dataclass
class PartitionState:
    partition: str
    last_processed_version: Optional[int]
    is_processed: bool


def select_partitions_to_backfill(states: List[PartitionState], last_update_versions: Dict[str, int],
                                  current_partition: str,
                                  max_lookback_days: Optional[int] = None) -> List[str]:
    """Pure version of the planner query on Delta versions."""
    oldest_allowed = None
    if max_lookback_days is not None:
        oldest_allowed = (dt.date.fromisoformat(current_partition)
                          - dt.timedelta(days=max_lookback_days)).isoformat()
    selected = []
    for state in states:
        if state.last_processed_version is None or state.is_processed:
            continue  # never processed by the main run, or already planned/running
        if state.partition >= current_partition:
            continue
        if oldest_allowed and state.partition < oldest_allowed:
            log.warning("partition %s has late data beyond the accepted lookback", state.partition)
            continue
        if last_update_versions.get(state.partition, -1) > state.last_processed_version:
            selected.append(state.partition)
    return sorted(selected)


def get_spark():
    from pyspark.sql import SparkSession

    return (SparkSession.builder.appName("dynamic-late-data-integrator")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def last_version_per_partition(spark, table_path: str) -> Dict[str, int]:
    """Latest data-changing commit per partition, from the JSON commit files.

    Commit files older than delta.logRetentionDuration (30 days by default) are
    cleaned up; the state table keeps last_update_version as a running MAX so
    information already captured is not lost.
    """
    from pyspark.sql import functions as F

    log_df = spark.read.json(f"{table_path}/_delta_log/*.json")
    version = F.regexp_extract(F.input_file_name(), r"(\d{20})\.json$", 1).cast("long")
    frames = []
    for action in ("add", "remove"):
        if action in log_df.columns:
            frames.append(log_df.filter(F.col(f"{action}.dataChange"))
                          .select(F.col(f"{action}.partitionValues.{PARTITION_COLUMN}").alias("partition"),
                                  version.alias("version")))
    if not frames:
        return {}
    changes = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])
    rows = changes.filter("partition IS NOT NULL").groupBy("partition").agg(F.max("version").alias("v"))
    return {r["partition"]: r["v"] for r in rows.collect()}


def current_version(spark, table_path: str) -> int:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, table_path).history(1).collect()[0]["version"]


def upsert_state(spark, state_path: str, rows: List[dict]) -> None:
    """MERGE rows (partition + any subset of state columns) into the state table."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    schema = ("partition STRING, last_processed_version LONG, last_update_version LONG, "
              "is_processed BOOLEAN")
    full_rows = [{"partition": r["partition"],
                  "last_processed_version": r.get("last_processed_version"),
                  "last_update_version": r.get("last_update_version"),
                  "is_processed": r.get("is_processed")} for r in rows]
    updates = spark.createDataFrame(full_rows, schema).withColumn("updated_at", F.current_timestamp())
    if not DeltaTable.isDeltaTable(spark, state_path):
        updates.write.format("delta").save(state_path)
        return
    set_expr = {c: f"coalesce(s.{c}, t.{c})" for c in ("last_processed_version", "is_processed")}
    set_expr["last_update_version"] = "greatest(s.last_update_version, t.last_update_version)"
    set_expr["updated_at"] = "s.updated_at"
    (DeltaTable.forPath(spark, state_path).alias("t")
     .merge(updates.alias("s"), "t.partition = s.partition")
     .whenMatchedUpdate(set=set_expr)
     .whenNotMatchedInsertAll()
     .execute())


def cmd_process(spark, args) -> None:
    from pyspark.sql import functions as F

    version = current_version(spark, args.raw_table)
    raw = (spark.read.format("delta").option("versionAsOf", version).load(args.raw_table)
           .filter(F.col(PARTITION_COLUMN) == args.partition))
    stats = (raw.groupBy(PARTITION_COLUMN, "referral").agg(F.count("*").alias("visits"))
             .withColumn("source_version", F.lit(version)))
    (stats.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"{PARTITION_COLUMN} = '{args.partition}'")
     .partitionBy(PARTITION_COLUMN).save(args.output_table))
    # Hand the version actually read over to mark-processed (no race with late writes).
    marker = Path(args.work_dir) / "processed-versions" / f"{args.partition}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"partition": args.partition, "version": version}))
    log.info("processed %s at raw version %s", args.partition, version)


def cmd_mark_processed(spark, args) -> None:
    marker = Path(args.work_dir) / "processed-versions" / f"{args.partition}.json"
    version = json.loads(marker.read_text())["version"]
    upsert_state(spark, args.state_table, [{"partition": args.partition,
                                            "last_processed_version": version,
                                            "is_processed": False}])


def cmd_plan(spark, args) -> List[str]:
    from pyspark.sql import functions as F

    updates = last_version_per_partition(spark, args.raw_table)
    upsert_state(spark, args.state_table, [{"partition": p, "last_update_version": v}
                                           for p, v in updates.items()])
    state_rows = (spark.read.format("delta").load(args.state_table)
                  .select("partition", "last_processed_version", "last_update_version",
                          "is_processed")
                  .filter(F.col("partition") < args.partition).collect())
    states = [PartitionState(r["partition"], r["last_processed_version"], bool(r["is_processed"]))
              for r in state_rows]
    versions = {r["partition"]: r["last_update_version"] for r in state_rows
                if r["last_update_version"] is not None}
    to_backfill = select_partitions_to_backfill(states, versions, args.partition,
                                                args.max_lookback_days)
    if to_backfill:
        upsert_state(spark, args.state_table, [{"partition": p, "is_processed": True}
                                               for p in to_backfill])
    output = Path(args.output_file)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(".tmp")
    tmp.write_text(json.dumps({"current_partition": args.partition,
                               "partitions": [{"event_time": p} for p in to_backfill]}))
    os.replace(tmp, output)
    log.info("partitions to backfill from %s: %s", args.partition, to_backfill)
    return to_backfill


def _self_check() -> None:
    states = [PartitionState("2024-12-17", 5, False),   # state table: no late data
              PartitionState("2024-12-18", 6, False),   # updated after processing -> late data
              PartitionState("2024-12-19", 7, True),    # already planned by another run
              PartitionState("2024-11-01", 1, False),   # very late, beyond lookback cap
              PartitionState("2024-12-21", 8, False)]   # future partition
    versions = {"2024-12-17": 5, "2024-12-18": 9, "2024-12-19": 10, "2024-11-01": 12, "2024-12-21": 11}
    assert select_partitions_to_backfill(states, versions, "2024-12-20") == ["2024-11-01", "2024-12-18"]
    assert select_partitions_to_backfill(states, versions, "2024-12-20", 30) == ["2024-12-18"]
    assert select_partitions_to_backfill([PartitionState("2024-12-01", None, False)],
                                         {"2024-12-01": 3}, "2024-12-20") == []
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("command", choices=["mark-processing", "process", "mark-processed", "plan"])
    p.add_argument("--partition", required=True, type=lambda s: dt.date.fromisoformat(s).isoformat())
    base = os.environ.get("DP_DYNAMIC_LATE_BASE_DIR", "/tmp/dp/dynamic-late-data-integrator")
    p.add_argument("--raw-table", default=f"{base}/raw/visits")
    p.add_argument("--output-table", default=f"{base}/output/referral-stats")
    p.add_argument("--state-table", default=f"{base}/state/partitions")
    p.add_argument("--work-dir", default=f"{base}/work")
    p.add_argument("--output-file", help="plan: per-run JSON file with partitions to backfill")
    p.add_argument("--max-lookback-days", type=int, default=None)
    args = p.parse_args(argv)
    if args.command == "plan" and not args.output_file:
        p.error("plan requires --output-file")

    spark = get_spark()
    if args.command == "mark-processing":
        upsert_state(spark, args.state_table, [{"partition": args.partition, "is_processed": True}])
    elif args.command == "process":
        cmd_process(spark, args)
    elif args.command == "mark-processed":
        cmd_mark_processed(spark, args)
    else:
        cmd_plan(spark, args)


if __name__ == "__main__":
    main()
