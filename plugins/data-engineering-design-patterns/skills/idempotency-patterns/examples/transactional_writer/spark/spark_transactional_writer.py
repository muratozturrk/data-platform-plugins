"""
Transactional Writer -- one Spark job = one Delta Lake commit.

Job-level transaction: every Spark task writes its Parquet data
files, but readers see NONE of them until the driver writes the next
`_delta_log/000...N.json` commit after all tasks succeed. A preempted spot node
only makes its tasks retry; consumers never see half a partition. Compare with
`df.write.json(path)`: each task's file is readable as soon as it lands, so a
failed or retried job exposes partial data and duplicates.

What the commit does NOT give you: a rerun of
the job opens a NEW transaction. This job therefore also:
  * scopes the write with `replaceWhere` on the run's partition, so a rerun
    replaces instead of appending (Data Overwrite);
  * sets Delta's idempotent-writer options `txnAppId` / `txnVersion`
    (Delta 2.0+). Delta records (appId, version) in the commit; a later write
    with the same appId and a version <= the recorded one is skipped. A retried
    job that already committed becomes a no-op.

Trap: txnVersion must grow per appId. With one appId for the whole job and
version = date, a backfill of an OLDER date would be silently skipped. The
appId is therefore per partition and the version is a run version you bump
when you deliberately reprocess (`--run-version`).

Delta has no multi-table transactions: two tables = two commits. When several
tables must change atomically, run the writes inside a SQL warehouse / RDBMS
transaction (see ../airflow/transactional_writer_dag.py) or expose them through
a single switch (Proxy pattern).

Failed commits leave orphan data files that no commit references; they are
invisible and removed by VACUUM after the retention period.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
    spark_transactional_writer.py --input /data/raw/visits \
    --output /data/silver/visits --date 2024-11-20 --run-version 1
Self-test (no Spark needed):  python spark_transactional_writer.py --self-test
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("transactional_writer")

JOB_NAME = "visits_silver"
PARTITION_COL = "visit_date"


@dataclass(frozen=True)
class TxnIds:
    app_id: str
    version: int
    user_metadata: str


def txn_ids(job: str, partition_date: str, run_version: int) -> TxnIds:
    """Delta idempotent-writer identifiers for one partition write.

    app_id per (job, partition) so backfills of older dates are never skipped;
    version = run version, bumped on deliberate reprocessing.
    """
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", partition_date):
        raise ValueError(f"bad partition date {partition_date!r}")
    if run_version < 0:
        raise ValueError("run_version must be >= 0")
    app_id = f"{job}:{PARTITION_COL}={partition_date}"
    return TxnIds(app_id, run_version, f"{app_id}:v{run_version}")


def get_spark():
    from pyspark.sql import SparkSession

    return (SparkSession.builder.appName("transactional-writer")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def transform(spark, input_path: str, partition_date: str):
    from pyspark.sql import functions as F

    raw = (spark.read.schema("visit_id STRING, user_id LONG, page STRING, "
                             "event_time TIMESTAMP")
           .json(f"{input_path}/{PARTITION_COL}={partition_date}"))
    return (raw.filter(F.col("visit_id").isNotNull())
            .dropDuplicates(["visit_id"])
            .withColumn(PARTITION_COL, F.lit(partition_date).cast("date")))


def write_in_one_commit(df, output_path: str, partition_date: str, ids: TxnIds) -> None:
    """Single DataFrameWriter call -> single Delta commit (all or nothing)."""
    (df.write.format("delta")
     .mode("overwrite")
     .option("replaceWhere", f"{PARTITION_COL} = '{partition_date}'")
     .option("txnAppId", ids.app_id)
     .option("txnVersion", ids.version)
     .option("userMetadata", ids.user_metadata)   # visible in DESCRIBE HISTORY
     .partitionBy(PARTITION_COL)
     .save(output_path))


def committed(spark, output_path: str, ids: TxnIds) -> bool:
    """True when a commit with this run's userMetadata exists in the log."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, output_path):
        return False
    hist = DeltaTable.forPath(spark, output_path).history(50)
    return hist.filter(hist.userMetadata == ids.user_metadata).limit(1).count() == 1


def run(input_path: str, output_path: str, partition_date: str, run_version: int) -> None:
    spark = get_spark()
    ids = txn_ids(JOB_NAME, partition_date, run_version)
    if committed(spark, output_path, ids):
        # Delta would skip the write anyway; skipping here also saves the compute.
        log.info("%s already committed, nothing to do", ids.user_metadata)
        return
    df = transform(spark, input_path, partition_date)
    if df.limit(1).count() == 0:
        # An empty overwrite would commit an empty partition.
        raise ValueError(f"no input rows for {partition_date}; refusing to commit")
    write_in_one_commit(df, output_path, partition_date, ids)
    if not committed(spark, output_path, ids):
        # Delta skipped it: same appId with a higher version is already there.
        raise RuntimeError(f"write for {ids.user_metadata} was skipped; "
                           "bump --run-version above the recorded one")
    log.info("committed %s", ids.user_metadata)


def _self_check() -> None:
    a = txn_ids("visits_silver", "2024-11-20", 1)
    assert a == txn_ids("visits_silver", "2024-11-20", 1)   # retry -> same txn
    older = txn_ids("visits_silver", "2024-11-01", 1)
    assert older.app_id != a.app_id                          # backfill not skipped
    assert txn_ids("visits_silver", "2024-11-20", 2).version > a.version
    for bad in [("2024/11/20", 1), ("2024-11-20", -1)]:
        try:
            txn_ids("visits_silver", *bad)
        except ValueError:
            continue
        raise AssertionError(bad)
    print("spark_transactional_writer self-check OK")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _self_check()
        sys.exit(0)
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--run-version", type=int, default=1)
    args = p.parse_args()
    run(args.input, args.output, args.date, args.run_version)
