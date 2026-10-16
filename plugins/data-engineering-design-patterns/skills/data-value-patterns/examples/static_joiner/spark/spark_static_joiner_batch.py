"""Static Joiner -- batch enrichment against an at-rest reference.

Two join modes:
  key   : plain keyed join, reference holds one row per key
          (visits.user_id = users.id). Reference is broadcast.
  scd2  : time-sensitive join against an SCD type 2 reference:
            ON u.id = v.user_id AND <as_of> BETWEEN u.start_date AND u.end_date
          <as_of> is either the event time (point-in-time value) or the
          orchestrator's execution date. Never NOW(): a backfill must see the
          same reference rows as the original run (idempotency).

Writes are idempotent: the output partition is replaced (replaceWhere) and the
audit record for the processing date is overwritten, so reruns converge.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_static_joiner_batch.py \
      --processing-date 2024-01-15 --mode scd2 --as-of execution_date
  python spark_static_joiner_batch.py --self-test     # pure logic, no Spark
Env: PRIMARY_PATH, REFERENCE_PATH, OUTPUT_PATH, AUDIT_PATH.
"""
import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("static-joiner-batch")


@dataclass
class StaticJoinerConfig:
    primary_path: str = os.environ.get("PRIMARY_PATH", "/data/silver/visits")
    primary_join_key: str = "user_id"
    primary_partition_col: str = "event_date"
    primary_time_col: str = "event_time"

    reference_path: str = os.environ.get("REFERENCE_PATH", "/data/dim/users")
    reference_join_key: str = "id"
    reference_cols: List[str] = field(default_factory=lambda: ["login", "email", "registered_at"])
    scd_start_col: str = "start_date"
    scd_end_col: str = "end_date"           # current row: '9999-12-31'

    output_path: str = os.environ.get("OUTPUT_PATH", "/data/gold/visits_enriched")
    audit_path: str = os.environ.get("AUDIT_PATH", "/data/audit/static_joiner")

    mode: str = "key"                       # key | scd2
    as_of: str = "event_time"               # event_time | execution_date (scd2 only)
    join_type: str = "left"                 # primary dataset is the source of truth
    max_broadcast_rows: int = 2_000_000     # above this, let Spark shuffle-join instead
    max_miss_rate: float = 0.05


def scd2_condition(cfg: StaticJoinerConfig, processing_date: str) -> str:
    """SQL condition for the SCD2 join. Pure function -> unit-testable."""
    if cfg.as_of == "event_time":
        as_of = f"p.{cfg.primary_time_col}"
    elif cfg.as_of == "execution_date":
        as_of = f"CAST('{processing_date}' AS TIMESTAMP)"
    else:
        raise ValueError(f"unknown as_of={cfg.as_of!r}")
    # half-open interval: a row ending at 10:00 and the next starting at 10:00 never both match
    return (f"p.{cfg.primary_join_key} = r.{cfg.reference_join_key} "
            f"AND {as_of} >= r.{cfg.scd_start_col} AND {as_of} < r.{cfg.scd_end_col}")


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("StaticJoinerBatch")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def load_reference(spark, cfg: StaticJoinerConfig):
    """Projects only the needed columns and fails on key duplicates (row explosion)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    cols = [cfg.reference_join_key, *cfg.reference_cols]
    unique_key = [cfg.reference_join_key]
    if cfg.mode == "scd2":
        cols += [cfg.scd_start_col, cfg.scd_end_col]
        unique_key.append(cfg.scd_start_col)
    ref = spark.read.format("delta").load(cfg.reference_path).select(*cols)

    dupes = ref.groupBy(*unique_key).count().filter(F.col("count") > 1).limit(5).collect()
    if dupes:
        raise ValueError(f"reference not unique on {unique_key}, e.g. {dupes}: the join would multiply rows")

    version = DeltaTable.forPath(spark, cfg.reference_path).history(1).collect()[0]["version"]
    rows = ref.count()
    log.info("reference loaded path=%s delta_v=%s rows=%d", cfg.reference_path, version, rows)
    return ref, version, rows


def enrich(primary, ref, ref_rows: int, cfg: StaticJoinerConfig, processing_date: str):
    from pyspark.sql import functions as F

    ref_side = F.broadcast(ref) if ref_rows <= cfg.max_broadcast_rows else ref
    p, r = primary.alias("p"), ref_side.alias("r")
    if cfg.mode == "scd2":
        cond = F.expr(scd2_condition(cfg, processing_date))
    else:
        cond = F.col(f"p.{cfg.primary_join_key}") == F.col(f"r.{cfg.reference_join_key}")
    joined = p.join(r, cond, cfg.join_type)
    return joined.select("p.*", *[F.col(f"r.{c}") for c in cfg.reference_cols],
                         F.col(f"r.{cfg.reference_join_key}").isNull().alias("_ref_miss"))


def write_audit(spark, cfg: StaticJoinerConfig, record: dict) -> None:
    """One JSON file per processing date, overwritten on rerun (readable by the DAG without Spark)."""
    path = f"{cfg.audit_path}/processing_date={record['processing_date']}"
    spark.createDataFrame([record]).coalesce(1).write.mode("overwrite").json(path)


def run(processing_date: str, cfg: StaticJoinerConfig) -> dict:
    from pyspark.sql import functions as F

    spark = get_spark()
    try:
        ref, ref_version, ref_rows = load_reference(spark, cfg)
        primary = (spark.read.format("delta").load(cfg.primary_path)
                   .filter(F.col(cfg.primary_partition_col) == processing_date))
        enriched = enrich(primary, ref, ref_rows, cfg, processing_date).cache()

        stats = enriched.agg(F.count("*").alias("rows"),
                             F.sum(F.col("_ref_miss").cast("int")).alias("misses")).first()
        primary_rows = primary.count()
        output_rows, misses = stats["rows"], stats["misses"] or 0
        if output_rows != primary_rows:
            raise ValueError(f"left join changed row count {primary_rows} -> {output_rows}: "
                             "overlapping SCD intervals or duplicate keys")

        (enriched.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"{cfg.primary_partition_col} = '{processing_date}'")
            .partitionBy(cfg.primary_partition_col)
            .save(cfg.output_path))

        record = {
            "processing_date": processing_date, "mode": cfg.mode, "as_of": cfg.as_of,
            "primary_rows": primary_rows, "output_rows": output_rows, "miss_count": int(misses),
            "miss_rate": (misses / output_rows) if output_rows else 0.0,
            "reference_rows": ref_rows, "reference_version": ref_version,
            "run_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        write_audit(spark, cfg, record)
        log.info("run complete %s", json.dumps(record))
        if record["miss_rate"] > cfg.max_miss_rate:
            log.warning("miss rate %.2f%% above %.2f%%: reference late or keys misaligned",
                        record["miss_rate"] * 100, cfg.max_miss_rate * 100)
        return record
    finally:
        spark.stop()


def _self_test() -> None:
    cfg = StaticJoinerConfig(mode="scd2", as_of="execution_date")
    cond = scd2_condition(cfg, "2024-06-21")
    assert "CAST('2024-06-21' AS TIMESTAMP) >= r.start_date" in cond
    assert "< r.end_date" in cond and cond.startswith("p.user_id = r.id")
    cfg.as_of = "event_time"
    assert "p.event_time >= r.start_date" in scd2_condition(cfg, "2024-06-21")
    cfg.as_of = "now"
    try:
        scd2_condition(cfg, "2024-06-21")
        raise AssertionError("NOW()-style as_of must be rejected")
    except ValueError:
        pass
    print("self-test OK")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--processing-date", required=True)
    p.add_argument("--mode", choices=["key", "scd2"], default="key")
    p.add_argument("--as-of", choices=["event_time", "execution_date"], default="event_time")
    args = p.parse_args()
    run(args.processing_date, StaticJoinerConfig(mode=args.mode, as_of=args.as_of))


if __name__ == "__main__":
    _self_test() if "--self-test" in sys.argv else main()
