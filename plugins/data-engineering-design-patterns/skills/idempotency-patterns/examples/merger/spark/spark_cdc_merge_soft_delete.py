"""Merger -- idempotent MERGE of a CDC increment into a Delta table, with soft deletes.

Problem: a batch job replicates changes captured by Change Data Capture (Kafka ->
landing zone) into a Delta table that must mirror the source without duplicates.
Each run only brings a subset of rows, so the dataset cannot be overwritten.

Mechanics:
  * identity = immutable, unique key; here the composite (type, version);
  * WHEN MATCHED AND is_deleted       -> DELETE (or soft-delete UPDATE, --soft-delete-target)
  * WHEN MATCHED AND NOT is_deleted   -> UPDATE
  * WHEN NOT MATCHED AND NOT is_deleted -> INSERT  (the guard stops the first run from
    inserting rows that are already deleted)
Re-running the same increment produces the same table, so retries are safe.
Known limit: a backfill starts from the *latest* table state; use the
Stateful Merger if consumers need point-in-time consistency during replays.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_cdc_merge_soft_delete.py \
      --changes /landing/cdc/devices/2024-10-07 --changes-format json \
      --target /lake/silver/devices
  python spark_cdc_merge_soft_delete.py --self-test      # pure-logic checks, no Spark needed
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

log = logging.getLogger("merger")

DEFAULT_KEY = ("type", "version")
DEFAULT_VALUE_COLUMNS = ("full_name",)
# Ordering of changes for one key: change timestamp first, log position as tie-breaker
# (Debezium source.lsn / Kafka offset). Without a tie-breaker two changes in the same
# millisecond would make the "latest" row non-deterministic -> non-idempotent replays.
ORDER_COLUMNS = ("change_ts", "change_lsn")


@dataclass(frozen=True)
class MergeConfig:
    changes_path: str
    changes_format: str
    target_path: str
    key: tuple[str, ...] = DEFAULT_KEY
    value_columns: tuple[str, ...] = DEFAULT_VALUE_COLUMNS
    soft_delete_target: bool = False  # keep the row with is_deleted=true instead of DELETE
    dry_run: bool = False


# --------------------------------------------------------------------------- pure logic
def merge_condition(key: tuple[str, ...], target: str = "t", source: str = "c") -> str:
    if not key:
        raise ValueError("Merger needs at least one key column")
    return " AND ".join(f"{target}.{k} = {source}.{k}" for k in key)


def insert_values(key: tuple[str, ...], values: tuple[str, ...], soft_delete_target: bool,
                  source: str = "c") -> dict[str, str]:
    cols = list(dict.fromkeys([*key, *values]))
    out = {c: f"{source}.{c}" for c in cols}
    if soft_delete_target:
        out["is_deleted"] = "false"
    return out


def latest_per_key(rows: list[dict], key: tuple[str, ...]) -> list[dict]:
    """Plain-python twin of the Spark window: last change per key wins."""
    best: dict[tuple, dict] = {}
    for r in rows:
        k = tuple(r[c] for c in key)
        order = tuple(r.get(c) or 0 for c in ORDER_COLUMNS)
        if k not in best or order > tuple(best[k].get(c) or 0 for c in ORDER_COLUMNS):
            best[k] = r
    return list(best.values())


def apply_merge(target: dict[tuple, dict], changes: list[dict], key: tuple[str, ...]) -> dict:
    """Reference semantics of the MERGE (hard-delete mode) used by the self-check."""
    out = dict(target)
    for c in latest_per_key(changes, key):
        k = tuple(c[x] for x in key)
        if c["is_deleted"]:
            out.pop(k, None)
        else:
            out[k] = {x: v for x, v in c.items() if x not in ("is_deleted", *ORDER_COLUMNS)}
    return out


def _self_check() -> None:
    key = DEFAULT_KEY
    assert merge_condition(key) == "t.type = c.type AND t.version = c.version"
    assert insert_values(key, ("full_name",), True)["is_deleted"] == "false"
    changes = [
        {"type": "phone", "version": "1", "full_name": "A", "is_deleted": False, "change_ts": 1, "change_lsn": 1},
        {"type": "phone", "version": "1", "full_name": "A2", "is_deleted": False, "change_ts": 2, "change_lsn": 5},
        {"type": "tv", "version": "1", "full_name": "T", "is_deleted": True, "change_ts": 1, "change_lsn": 2},
        {"type": "pc", "version": "2", "full_name": "P", "is_deleted": False, "change_ts": 3, "change_lsn": 3},
    ]
    # first run on an empty table: the deleted "tv" row must NOT be inserted
    first = apply_merge({}, changes, key)
    assert set(first) == {("phone", "1"), ("pc", "2")}
    assert first[("phone", "1")]["full_name"] == "A2"
    # idempotency: replaying the same increment changes nothing
    assert apply_merge(first, changes, key) == first
    # later delete of an existing row
    second = apply_merge(first, [{"type": "pc", "version": "2", "full_name": "P", "is_deleted": True,
                                  "change_ts": 9, "change_lsn": 9}], key)
    assert set(second) == {("phone", "1")}
    # same timestamp: log position decides
    tie = latest_per_key([
        {"type": "x", "version": "1", "change_ts": 5, "change_lsn": 2, "full_name": "old"},
        {"type": "x", "version": "1", "change_ts": 5, "change_lsn": 7, "full_name": "new"},
    ], key)
    assert tie[0]["full_name"] == "new"
    print("self-check OK")


# --------------------------------------------------------------------------- spark side
def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("merger-cdc-soft-delete")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def read_changes(spark, cfg: MergeConfig):
    from pyspark.sql import functions as F
    df = spark.read.format(cfg.changes_format).load(cfg.changes_path)
    missing = {*cfg.key, *cfg.value_columns, "is_deleted", "change_ts"} - set(df.columns)
    if missing:
        raise ValueError(f"change set {cfg.changes_path} misses columns {sorted(missing)}")
    if "change_lsn" not in df.columns:
        df = df.withColumn("change_lsn", F.lit(0).cast("long"))
    null_keys = df.filter(" OR ".join(f"{k} IS NULL" for k in cfg.key)).limit(1).count()
    if null_keys:
        raise ValueError("null key in change set: rows cannot be identified, MERGE would duplicate them")
    return df.withColumn("is_deleted", F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)))


def dedup_latest(df, key: tuple[str, ...]):
    """MERGE fails ('multiple source rows matched') when one target row matches twice."""
    from pyspark.sql import Window, functions as F
    w = Window.partitionBy(*key).orderBy(*(F.col(c).desc() for c in ORDER_COLUMNS))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def ensure_target(spark, cfg: MergeConfig, changes) -> None:
    """First run: create an empty target with the change-set schema (no data, no deleted rows)."""
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, cfg.target_path):
        return
    cols = list(dict.fromkeys([*cfg.key, *cfg.value_columns]))
    schema_df = changes.select(*cols)
    if cfg.soft_delete_target:
        schema_df = schema_df.withColumn("is_deleted", changes["is_deleted"])
    log.info("target %s does not exist, creating it empty", cfg.target_path)
    schema_df.limit(0).write.format("delta").save(cfg.target_path)


def run_merge(spark, cfg: MergeConfig) -> dict:
    from delta.tables import DeltaTable
    changes = dedup_latest(read_changes(spark, cfg), cfg.key).cache()
    stats = changes.groupBy("is_deleted").count().collect()
    log.info("increment after per-key dedup: %s", {r.is_deleted: r["count"] for r in stats})
    if cfg.dry_run:
        return {"dry_run": True}
    ensure_target(spark, cfg, changes)

    updates = {c: f"c.{c}" for c in cfg.value_columns}
    builder = (DeltaTable.forPath(spark, cfg.target_path).alias("t")
               .merge(changes.alias("c"), merge_condition(cfg.key)))
    if cfg.soft_delete_target:
        builder = builder.whenMatchedUpdate(condition="c.is_deleted = true", set={"is_deleted": "true"})
        updates["is_deleted"] = "false"
    else:
        builder = builder.whenMatchedDelete(condition="c.is_deleted = true")
    (builder
     .whenMatchedUpdate(condition="c.is_deleted = false", set=updates)
     .whenNotMatchedInsert(condition="c.is_deleted = false",
                           values=insert_values(cfg.key, cfg.value_columns, cfg.soft_delete_target))
     .execute())

    last = DeltaTable.forPath(spark, cfg.target_path).history(1).collect()[0]
    metrics = dict(last["operationMetrics"] or {})
    log.info("MERGE committed as version %s: %s", last["version"], metrics)
    changes.unpersist()
    return {"version": last["version"], **metrics}


def parse_args(argv: list[str]) -> MergeConfig:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--changes", required=True, help="path of the CDC increment")
    p.add_argument("--changes-format", default="delta", choices=["delta", "json", "parquet"])
    p.add_argument("--target", required=True, help="Delta path of the merged table")
    p.add_argument("--key", default=",".join(DEFAULT_KEY))
    p.add_argument("--value-columns", default=",".join(DEFAULT_VALUE_COLUMNS))
    p.add_argument("--soft-delete-target", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    return MergeConfig(a.changes, a.changes_format, a.target,
                       tuple(a.key.split(",")), tuple(a.value_columns.split(",")),
                       a.soft_delete_target, a.dry_run)


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_check()
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = parse_args(argv)
    spark = build_spark()
    try:
        log.info("result: %s", run_merge(spark, cfg))
    except Exception:
        log.exception("merge failed; the Delta commit is atomic, the target is unchanged")
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
