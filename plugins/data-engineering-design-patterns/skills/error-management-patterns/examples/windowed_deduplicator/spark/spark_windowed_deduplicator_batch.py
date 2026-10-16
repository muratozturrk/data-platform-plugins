"""Windowed Deduplicator - Spark batch.

In batch the "window" is implicit: the deduplication scope is the dataset being
processed (one partition). The job:
  1. reads the partition given by ``--partition-date``;
  2. removes duplicates on the deduplication attributes with one of the
     methods: ``drop_duplicates``, ``row_number`` window
     (with an ORDER BY so the kept row is deterministic) or ``sql``;
  3. optionally widens the scope with ``--lookback-days N``: keys already
     written in the N previous output partitions are anti-joined away. This is
     the "include datasets processed in the past" option - more correct, but
     more compute;
  4. overwrites the output partition with ``replaceWhere`` (idempotent rerun)
     and writes a metrics JSON (duplicate ratio) for the orchestrator.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_windowed_deduplicator_batch.py \
      --base-dir /tmp/dp/dedup --partition-date 2024-12-31 --keys type,full_name,version
  python spark_windowed_deduplicator_batch.py --self-test
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

log = logging.getLogger("windowed_deduplicator_batch")

METHODS = ("drop_duplicates", "row_number", "sql")


def lookback_dates(partition_date: str, days: int) -> List[str]:
    """Previous partitions included in an extended deduplication scope."""
    current = dt.date.fromisoformat(partition_date)
    return [(current - dt.timedelta(days=d)).isoformat() for d in range(days, 0, -1)]


def row_number_sql(table: str, columns: List[str], keys: List[str], order_by: Optional[str]) -> str:
    """ROW_NUMBER() window, plus a real ORDER BY so reruns keep the same row."""
    key_list = ", ".join(keys)
    order = order_by or key_list
    return (f"SELECT {', '.join(columns)} FROM ("
            f" SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_list} ORDER BY {order}) AS position"
            f" FROM {table}) WHERE position = 1")


def duplicate_ratio(before: int, after: int) -> float:
    return (before - after) / before if before else 0.0


def deduplicate(spark, df, keys: List[str], method: str, order_by: Optional[str]):
    if method == "drop_duplicates":
        # Native: keeps an arbitrary row per key - fine when duplicates are identical.
        return df.dropDuplicates(keys)
    if method == "row_number":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        order_cols = [F.expr(c) for c in (order_by.split(",") if order_by else keys)]
        window = Window.partitionBy(*keys).orderBy(*order_cols)
        return (df.withColumn("position", F.row_number().over(window))
                .filter("position = 1").drop("position"))
    df.createOrReplaceTempView("duplicated_devices")
    return spark.sql(row_number_sql("duplicated_devices", df.columns, keys, order_by))


def run(spark, args: argparse.Namespace) -> dict:
    from pyspark.sql import functions as F

    keys = args.keys.split(",")
    output_path = f"{args.base_dir}/output/devices"
    dataset = (spark.read.schema(args.schema).format("json")
               .load(f"{args.base_dir}/input/{args.partition_date}"))
    dataset.persist()
    try:
        before = dataset.count()
        deduplicated = deduplicate(spark, dataset, keys, args.method, args.order_by)

        if args.lookback_days:
            past = lookback_dates(args.partition_date, args.lookback_days)
            try:
                seen_keys = (spark.read.format("delta").load(output_path)
                             .filter(F.col("partition_date").isin(past))
                             .select(*keys).distinct())
                # null-safe anti-join so NULL key parts also match
                condition = [deduplicated[k].eqNullSafe(seen_keys[k]) for k in keys]
                deduplicated = deduplicated.join(seen_keys, condition, "left_anti")
            except Exception as error:  # first run: no output table yet
                if "is not a Delta table" not in str(error) and "Path does not exist" not in str(error):
                    raise
                log.info("no previous output, lookback scope skipped")

        result = deduplicated.withColumn("partition_date", F.lit(args.partition_date).cast("date"))
        (result.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"partition_date = '{args.partition_date}'")
         .partitionBy("partition_date").save(output_path))
        after = spark.read.format("delta").load(output_path) \
            .filter(F.col("partition_date") == args.partition_date).count()
    finally:
        dataset.unpersist()

    metrics = {"partition_date": args.partition_date, "method": args.method, "keys": keys,
               "lookback_days": args.lookback_days, "input_rows": before, "output_rows": after,
               "duplicate_ratio": round(duplicate_ratio(before, after), 6)}
    metrics_file = Path(args.base_dir) / "_metrics" / f"{args.partition_date}.json"
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = metrics_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(metrics))
    os.replace(tmp, metrics_file)
    log.info("deduplication done: %s", metrics)
    return metrics


def _self_check() -> None:
    assert lookback_dates("2024-12-31", 2) == ["2024-12-29", "2024-12-30"]
    assert lookback_dates("2024-03-01", 1) == ["2024-02-29"]
    sql = row_number_sql("t", ["type", "version", "v"], ["type", "version"], None)
    assert sql.startswith("SELECT type, version, v FROM")
    assert "PARTITION BY type, version ORDER BY type, version" in sql and "position = 1" in sql
    assert duplicate_ratio(100, 90) == 0.1 and duplicate_ratio(0, 0) == 0.0
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--base-dir", default="/tmp/dp/dedup")
    p.add_argument("--partition-date", required=True)
    p.add_argument("--keys", default="type,full_name,version", help="deduplication attributes")
    p.add_argument("--schema", default="type STRING, full_name STRING, version STRING, ingested_at TIMESTAMP")
    p.add_argument("--method", choices=METHODS, default="drop_duplicates")
    p.add_argument("--order-by", default=None,
                   help="row_number/sql only, e.g. 'ingested_at DESC' to keep the latest copy")
    p.add_argument("--lookback-days", type=int, default=0)
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("windowed-deduplicator-batch")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    run(spark, args)


if __name__ == "__main__":
    main()
