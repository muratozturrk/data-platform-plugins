"""Stateful Merger -- Delta MERGE + `versions` state table + restore before backfill.

A plain Merger replays a backfill on top of the *latest* table state. The Stateful
Merger stores, for every execution time, the Delta version its MERGE produced
 and runs three steps:

  restore  : previous execution has no version  -> empty the table (first run / its backfill)
             previous version == last MERGE ver -> normal run, nothing to do
             previous version <  last MERGE ver -> backfill: restore to
                 version(current execution) - 1 if this execution ran before (this
                 keeps compaction/OPTIMIZE commits made in between),
                 else the previous execution's version
  merge    : MERGE the increment (soft deletes, guarded INSERT)
  update-state : upsert (execution_time -> MAX(version)) into the state table

Run (Delta Lake 3.x):
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 delta_version_restore_merger.py \
      --execution-time 2024-10-07 --previous-execution-time 2024-10-06 \
      --input /landing/devices/2024-10-07 --step all
  python delta_version_restore_merger.py --self-test       # pure decision logic, no Spark
Steps can run as separate tasks (see ../airflow/stateful_merger_dag.py); runs must be
serialised: restore -> merge -> update-state of one execution must not interleave with another.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

log = logging.getLogger("stateful-merger")

KEY = ("type", "version")
VALUE_COLUMNS = ("full_name",)
STATE_DDL = ("CREATE TABLE IF NOT EXISTS {state} (execution_time STRING NOT NULL, "
             "delta_table_version INT NOT NULL) USING delta")


# --------------------------------------------------------------------------- pure logic
@dataclass(frozen=True)
class RestoreDecision:
    action: str                  # "truncate" | "none" | "restore"
    version: int | None = None
    reason: str = ""


def decide_restore(previous_job_version: int | None, last_merge_version: int | None,
                   current_execution_version: int | None,
                   earliest_available_version: int | None = None) -> RestoreDecision:
    if previous_job_version is None:
        return RestoreDecision("truncate", reason="no version for the previous execution: "
                                                  "first run or backfill of the first run")
    if last_merge_version is None or previous_job_version >= last_merge_version:
        return RestoreDecision("none", reason="previous execution produced the last MERGE: normal run")
    if current_execution_version is not None:
        target, why = current_execution_version - 1, "version(current execution) - 1"
    else:
        target, why = previous_job_version, "previous execution's version (current never ran)"
    if earliest_available_version is not None and target < earliest_available_version:
        raise RuntimeError(
            f"cannot restore v{target}: history starts at v{earliest_available_version} "
            "(VACUUM / log retention). Backfilling beyond retention is not possible.")
    return RestoreDecision("restore", target, f"backfill detected, restore to {why}")


def _self_check() -> None:
    # Normal runs: 10-05->1, 10-06->2, 10-07->3, 10-08->4 ; last MERGE = 4
    assert decide_restore(4, 4, None).action == "none"                      # normal run 10-09
    assert decide_restore(None, 4, 1).action == "truncate"                  # rerun 10-05
    d = decide_restore(2, 4, 3)                                             # rerun 10-07
    assert (d.action, d.version) == ("restore", 2)
    # Backfill: after backfilling 10-07 -> 5, rerun 10-08 sees previous=5 == last MERGE=5
    assert decide_restore(5, 5, 4).action == "none"
    # With compactions: state 05->5, 06->7, 07->9, 08->10 ; compactions at 6 and 8 ; last MERGE=10
    d = decide_restore(7, 10, 9)                                            # backfill 10-07
    assert (d.action, d.version) == ("restore", 8)                          # keeps compaction v8
    d = decide_restore(9, 10, 10)                                           # backfill 10-08
    assert (d.action, d.version) == ("restore", 9)
    # never-run execution in the middle of history falls back to previous version
    assert decide_restore(7, 10, None).version == 7
    try:
        decide_restore(2, 10, 3, earliest_available_version=5)
        raise AssertionError("expected vacuum guard")
    except RuntimeError:
        pass
    print("self-check OK")


# --------------------------------------------------------------------------- spark side
def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("stateful-merger")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def history_versions(spark, table: str) -> tuple[int | None, int | None, int | None]:
    """(earliest available, last any-operation, last MERGE) from DESCRIBE HISTORY."""
    from pyspark.sql import functions as F
    h = spark.sql(f"DESCRIBE HISTORY {table}")
    row = h.agg(F.min("version").alias("first"), F.max("version").alias("last"),
                F.max(F.when(F.col("operation") == "MERGE", F.col("version"))).alias("merge")).first()
    return row["first"], row["last"], row["merge"]


def version_for(spark, state_table: str, execution_time: str) -> int | None:
    from pyspark.sql import functions as F
    rows = (spark.table(state_table).filter(F.col("execution_time") == F.lit(execution_time))
            .select("delta_table_version").collect())
    return rows[0].delta_table_version if rows else None


def step_restore(spark, a) -> RestoreDecision:
    from delta.tables import DeltaTable
    earliest, _, last_merge = history_versions(spark, a.target_table)
    decision = decide_restore(version_for(spark, a.state_table, a.previous_execution_time), last_merge,
                              version_for(spark, a.state_table, a.execution_time), earliest)
    log.info("restore decision for %s: %s", a.execution_time, decision)
    if a.dry_run or decision.action == "none":
        return decision
    if decision.action == "truncate":
        # The generic step is TRUNCATE TABLE. OSS Delta rejects TRUNCATE on Delta tables, but a
        # predicate-less DELETE is a metadata-only commit there (files are just unlinked
        # from the log, no data scan), so it is the Delta equivalent.
        spark.sql(f"DELETE FROM {a.target_table}")
    else:
        # RESTORE itself fails if the data files were vacuumed even though the log entry
        # survived; that error is the second line of defence after the history check.
        DeltaTable.forName(spark, a.target_table).restoreToVersion(decision.version)
    return decision


def step_merge(spark, a) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F
    changes = spark.read.format(a.input_format).load(a.input)
    if "change_ts" in changes.columns:  # one source row per key, or MERGE fails
        w = Window.partitionBy(*KEY).orderBy(F.col("change_ts").desc())
        changes = changes.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    changes = changes.withColumn("is_deleted", F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)))
    cols = {c: f"c.{c}" for c in (*KEY, *VALUE_COLUMNS)}
    (DeltaTable.forName(spark, a.target_table).alias("d")
     .merge(changes.alias("c"), " AND ".join(f"d.{k} = c.{k}" for k in KEY))
     .whenMatchedDelete(condition="c.is_deleted = true")
     .whenMatchedUpdate(condition="c.is_deleted = false", set={c: f"c.{c}" for c in VALUE_COLUMNS})
     .whenNotMatchedInsert(condition="c.is_deleted = false", values=cols)
     .execute())
    log.info("MERGE done for %s", a.execution_time)


def step_update_state(spark, a) -> int:
    from delta.tables import DeltaTable
    from pyspark.sql import Row
    _, last, last_merge = history_versions(spark, a.target_table)
    if last != last_merge:
        # Another commit landed after our MERGE: recording it would point backfills at the
        # wrong version. Fail loudly; the target must have a single writer between steps.
        raise RuntimeError(f"last commit v{last} is not a MERGE (last MERGE v{last_merge})")
    new = spark.createDataFrame([Row(execution_time=a.execution_time, delta_table_version=int(last))])
    # MERGE, not INSERT: a backfill overwrites the version of an existing execution_time.
    (DeltaTable.forName(spark, a.state_table).alias("old_versions")
     .merge(new.alias("new_version"), "old_versions.execution_time = new_version.execution_time")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    log.info("state: %s -> v%s", a.execution_time, last)
    return int(last)


def parse_args(argv: list[str]):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--execution-time", required=True)
    p.add_argument("--previous-execution-time", required=True)
    p.add_argument("--input", help="increment for this execution (required for merge/all)")
    p.add_argument("--input-format", default="delta", choices=["delta", "json", "parquet"])
    p.add_argument("--step", default="all", choices=["restore", "merge", "update-state", "all"])
    p.add_argument("--target-table", default="default.devices")
    p.add_argument("--state-table", default="default.versions")
    p.add_argument("--dry-run", action="store_true", help="only log the restore decision")
    a = p.parse_args(argv)
    if a.step in ("merge", "all") and not a.input:
        p.error("--input is required for the merge step")
    return a


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        _self_check()
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    a = parse_args(argv)
    spark = build_spark()
    try:
        spark.sql(STATE_DDL.format(state=a.state_table))
        if a.step in ("restore", "all"):
            step_restore(spark, a)
        if a.dry_run:
            return 0
        if a.step in ("merge", "all"):
            step_merge(spark, a)
        if a.step in ("update-state", "all"):
            step_update_state(spark, a)
    except Exception:
        log.exception("step %s failed for %s", a.step, a.execution_time)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
