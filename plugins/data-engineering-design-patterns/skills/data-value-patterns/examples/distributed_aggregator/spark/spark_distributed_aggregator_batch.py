"""Distributed Aggregator -- PySpark OLAP cube over visits.

Problem: reduce Silver-layer visits (daily event-time partitions) into an OLAP cube
with count and average duration across several axes (geography, device), in daily
and weekly views. Related rows live on different nodes AND in different stores:

  visits   JSON files  {visits_path}/date=YYYY-MM-DD/      (local FS / object store)
  devices  PostgreSQL  dp.devices via JDBC

The code looks like a local group-by + reduce; Spark turns every groupBy/join into
a shuffle (Exchange hashpartitioning) so related rows meet on one reducer:

  1. visit-level reduce: GROUP BY visit_id -> duration, page count       (shuffle 1)
  2. join with devices on (type, version)                                 (shuffle 2/3,
     both sides; a broadcast join would avoid it for a small table)
  3. cube(country, device) with decomposable measures (visits, sum duration);
     partial_count/partial_sum run map-side before the exchange          (shuffle 4)
     --salt-buckets N: two-stage salted aggregation for a skewed axis
  4. --view weekly rolls the 7 daily cubes up (sum of sums / sum of counts) instead of
     rescanning 7 days of raw visits.

Writes are idempotent: Delta replaceWhere on the view partition.
--explain logs whether the plan contains `Exchange hashpartitioning`.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.3 \
      spark_distributed_aggregator_batch.py --view daily --date 2024-01-15 --explain
  spark-submit ... --view weekly --date 2024-01-21          # date = last day of the week
  python spark_distributed_aggregator_batch.py --self-test  # pure helpers, no Spark
Env: VISITS_PATH, DAILY_CUBE_PATH, WEEKLY_CUBE_PATH, DEVICES_JDBC_URL, DEVICES_TABLE,
     DEVICES_DB_USER, DEVICES_DB_PASSWORD (never hardcode), SALT_BUCKETS.
"""
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("distributed_aggregator")

DIMENSIONS = ("country", "device_type")
UNKNOWN = "unknown"     # cube() uses NULL for "all values"; real NULLs must not collide with it
VISITS_DDL = ("visit_id STRING, user_id STRING, event_time TIMESTAMP, page STRING, "
              "context STRUCT<user: STRUCT<country: STRING>, "
              "technical: STRUCT<dev_type: STRING, dev_version: STRING>>")


@dataclass(frozen=True)
class AggregatorConfig:
    visits_path: str = os.environ.get("VISITS_PATH", "/data/silver/visits")
    daily_cube_path: str = os.environ.get("DAILY_CUBE_PATH", "/data/gold/visits_cube_daily")
    weekly_cube_path: str = os.environ.get("WEEKLY_CUBE_PATH", "/data/gold/visits_cube_weekly")
    jdbc_url: str = os.environ.get("DEVICES_JDBC_URL", "jdbc:postgresql://localhost:5432/dp")
    devices_table: str = os.environ.get("DEVICES_TABLE", "dp.devices")
    salt_buckets: int = int(os.environ.get("SALT_BUCKETS", "0"))     # 0 = no salting

    def jdbc_properties(self) -> dict:
        try:
            user, password = os.environ["DEVICES_DB_USER"], os.environ["DEVICES_DB_PASSWORD"]
        except KeyError as missing:
            raise RuntimeError(f"missing credential env var {missing}") from None
        return {"user": user, "password": password, "driver": "org.postgresql.Driver"}


# -- Pure helpers (self-tested) -----------------------------------------------------
def week_days(week_end: str) -> list[str]:
    end = date.fromisoformat(week_end)
    return [(end - timedelta(days=d)).isoformat() for d in range(6, -1, -1)]


def has_shuffle(plan_text: str) -> bool:
    return "Exchange hashpartitioning" in plan_text or "Exchange SinglePartition" in plan_text


# -- Spark ----------------------------------------------------------------------------
def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("distributed-aggregator")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # AQE: coalesces small shuffle partitions and splits skewed join partitions.
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Shuffle files outlive executors -> idle executors can be released mid-job.
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.dynamicAllocation.enabled", "true")
            .getOrCreate())


def load_visits(spark, cfg: AggregatorConfig, day: str):
    from pyspark.sql import functions as F
    path = f"{cfg.visits_path}/date={day}"
    visits = (spark.read.schema(VISITS_DDL).json(path)
              .filter(F.col("visit_id").isNotNull() & F.col("event_time").isNotNull())
              .select("visit_id", "user_id", "event_time", "page",
                      F.col("context.user.country").alias("country"),
                      F.col("context.technical.dev_type").alias("dev_type"),
                      F.col("context.technical.dev_version").alias("dev_version")))
    return visits


def load_devices(spark, cfg: AggregatorConfig):
    """Second, physically isolated store. Spark reads both and exchanges rows itself."""
    return (spark.read.jdbc(url=cfg.jdbc_url, table=cfg.devices_table, properties=cfg.jdbc_properties())
            .select("type", "version", "full_name"))


def visits_per_session(visits):
    """Reduce 1: events -> one row per visit (a visit's events can sit on any node)."""
    from pyspark.sql import functions as F
    return (visits.groupBy("visit_id")
            .agg(F.first("country", ignorenulls=True).alias("country"),
                 F.first("dev_type", ignorenulls=True).alias("dev_type"),
                 F.first("dev_version", ignorenulls=True).alias("dev_version"),
                 (F.max("event_time").cast("long") - F.min("event_time").cast("long")).alias("duration_s"),
                 F.count("*").alias("pages")))


def with_devices(per_visit, devices):
    from pyspark.sql import functions as F
    joined = per_visit.join(devices, (devices.type == per_visit.dev_type) &
                            (devices.version == per_visit.dev_version), "left")
    return joined.select("visit_id", "duration_s", "pages",
                         F.coalesce("country", F.lit(UNKNOWN)).alias("country"),
                         F.coalesce("full_name", "dev_type", F.lit(UNKNOWN)).alias("device_type"))


def build_cube(per_visit, salt_buckets: int):
    """Decomposable measures only (counts, sums): they survive salting and the weekly rollup.
    avg is derived at the end; exact distinct counts/medians would need sketches."""
    from pyspark.sql import functions as F
    if salt_buckets > 0:
        # Stage 1 spreads a hot key over N reducers; stage 2 cubes the much smaller partials.
        partial = (per_visit.withColumn("salt", (F.rand(seed=42) * salt_buckets).cast("int"))
                   .groupBy(*DIMENSIONS, "salt")
                   .agg(F.count("*").alias("visits"), F.sum("duration_s").alias("duration_s"),
                        F.sum("pages").alias("pages")))
        source, visits_expr = partial, F.sum("visits")
    else:
        source, visits_expr = per_visit, F.count("*")
    return (source.cube(*DIMENSIONS)
            .agg(visits_expr.alias("visits"),
                 F.sum("duration_s").alias("total_duration_s"),
                 F.sum("pages").alias("total_pages"),
                 F.grouping_id().alias("grouping_id"))       # tells "all countries" apart from a value
            .withColumn("avg_duration_s", F.col("total_duration_s") / F.col("visits")))


def rollup_weekly(spark, cfg: AggregatorConfig, week_end: str):
    """Weekly view from the 7 daily cubes: sums of sums, never averages of averages."""
    from pyspark.sql import functions as F
    days = week_days(week_end)
    daily = spark.read.format("delta").load(cfg.daily_cube_path).filter(F.col("view_date").isin(days))
    found = {r["view_date"] for r in daily.select("view_date").distinct().collect()}
    missing = sorted(set(days) - found)
    if missing:
        raise RuntimeError(f"weekly view {week_end}: daily cubes missing for {missing}")
    return (daily.groupBy("grouping_id", *DIMENSIONS)
            .agg(F.sum("visits").alias("visits"), F.sum("total_duration_s").alias("total_duration_s"),
                 F.sum("total_pages").alias("total_pages"))
            .withColumn("avg_duration_s", F.col("total_duration_s") / F.col("visits")))


def log_plan(df, label: str) -> None:
    plan = df._jdf.queryExecution().executedPlan().toString()   # same text explain() prints
    log.info("%s | shuffle=%s exchanges=%d", label, has_shuffle(plan), plan.count("Exchange"))


def write_view(df, path: str, view_date: str) -> int:
    from pyspark.sql import functions as F
    out = df.withColumn("view_date", F.lit(view_date))
    rows = out.count()
    if rows == 0:
        raise RuntimeError(f"empty cube for {view_date}; refusing to overwrite {path}")
    (out.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"view_date = '{view_date}'")
     .partitionBy("view_date").save(path))
    log.info("written | path=%s view_date=%s rows=%d", path, view_date, rows)
    return rows


def run(view: str, day: str, explain: bool, cfg: AggregatorConfig) -> None:
    date.fromisoformat(day)
    spark = get_spark()
    try:
        if view == "daily":
            per_visit = with_devices(visits_per_session(load_visits(spark, cfg, day)), load_devices(spark, cfg))
            cube = build_cube(per_visit, cfg.salt_buckets)
            if explain:
                log_plan(cube, f"daily cube {day}")
            write_view(cube, cfg.daily_cube_path, day)
        else:
            weekly = rollup_weekly(spark, cfg, day)
            if explain:
                log_plan(weekly, f"weekly cube {day}")
            write_view(weekly, cfg.weekly_cube_path, day)
    finally:
        spark.stop()


def _self_check() -> None:
    days = week_days("2024-01-21")
    assert days[0] == "2024-01-15" and days[-1] == "2024-01-21" and len(days) == 7
    assert week_days("2024-03-03")[0] == "2024-02-26"          # leap-year February
    assert has_shuffle("+- Exchange hashpartitioning(type#20, version#22, 200)")
    assert not has_shuffle("+- BroadcastHashJoin ... +- FileScan json")
    print("self-check OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--view", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--date", help="YYYY-MM-DD; for weekly, the last day of the week")
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_check()
    elif not args.date:
        parser.error("--date is required")
    else:
        run(args.view, args.date, args.explain, AggregatorConfig())
