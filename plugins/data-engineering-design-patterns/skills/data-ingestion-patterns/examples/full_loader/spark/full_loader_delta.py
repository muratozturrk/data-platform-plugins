"""Full Loader: reload a whole reference dataset into a Delta Lake table.

Problem: device reference data from an external provider, < 1M rows, changes a few times a
week and has no attribute telling which rows changed. Every run therefore takes the complete dataset.

Mechanics:
  1. extract  - read the provider export with an explicit schema (no inference drift between runs);
  2. guard    - refuse to publish an empty or suspiciously shrunken dataset (compared with the current
                table version), because a full overwrite would replace good data with a broken export;
  3. load     - ``mode("overwrite")`` into Delta. The overwrite is one transaction: readers keep seeing the
                previous version until the commit, never an empty table;
  4. rollback - Delta time travel keeps previous versions: ``--rollback-to N`` runs RESTORE.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.1.0 full_loader_delta.py \
      --input s3a://provider/devices/list --table-path s3a://master/devices
  spark-submit ... full_loader_delta.py --table-path s3a://master/devices --rollback-to 41
  python full_loader_delta.py --self-test
"""
import argparse
import logging
import sys
from typing import Optional

log = logging.getLogger("full_loader_delta")

DEVICES_SCHEMA_DDL = "type STRING, full_name STRING, version STRING"


def check_publishable(new_count: int, previous_count: Optional[int], min_rows: int, max_shrink: float) -> None:
    """Raise if the new snapshot must not replace the current one."""
    if new_count < min_rows:
        raise ValueError(f"Extract has {new_count} rows, below the minimum of {min_rows}")
    if previous_count and new_count < previous_count * (1 - max_shrink):
        raise ValueError(f"Extract shrank from {previous_count} to {new_count} rows "
                         f"(more than {max_shrink:.0%}); refusing to overwrite")


def _self_check() -> None:
    check_publishable(100, None, 1, 0.5)
    check_publishable(60, 100, 1, 0.5)
    for args in [(0, None, 1, 0.5), (40, 100, 1, 0.5)]:
        try:
            check_publishable(*args)
            raise AssertionError(f"{args} must fail")
        except ValueError:
            pass
    print("self-test OK")


def build_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("full-loader-devices")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def current_count(spark, table_path: str) -> Optional[int]:
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, table_path):
        return None                                   # first load: nothing to compare with
    return spark.read.format("delta").load(table_path).count()


def full_load(spark, args) -> None:
    input_data = spark.read.schema(args.schema).json(args.input)
    if args.dedupe_key:
        input_data = input_data.dropDuplicates([args.dedupe_key])
    input_data = input_data.cache()                   # counted, then written: read the provider once
    new_count = input_data.count()
    check_publishable(new_count, current_count(spark, args.table_path), args.min_rows, args.max_shrink)

    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", f"full-load from {args.input}")
    (input_data.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "false")              # a schema change must be a deliberate migration
     .save(args.table_path))

    from delta.tables import DeltaTable
    version = DeltaTable.forPath(spark, args.table_path).history(1).collect()[0]["version"]
    log.info("Full load committed: %d rows, table version %s", new_count, version)


def rollback(spark, table_path: str, version: int) -> None:
    spark.sql(f"RESTORE TABLE delta.`{table_path}` TO VERSION AS OF {int(version)}")
    log.info("Restored %s to version %d", table_path, version)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--input")
    p.add_argument("--table-path")
    p.add_argument("--schema", default=DEVICES_SCHEMA_DDL)
    p.add_argument("--dedupe-key", default=None)
    p.add_argument("--min-rows", type=int, default=1)
    p.add_argument("--max-shrink", type=float, default=0.5, help="max allowed row-count drop vs current")
    p.add_argument("--rollback-to", type=int, default=None)
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if args.self_test:
        _self_check()
        return 0
    if not args.table_path or (args.rollback_to is None and not args.input):
        p.error("--table-path and --input (or --rollback-to) are required")

    spark = build_spark()
    try:
        if args.rollback_to is not None:
            rollback(spark, args.table_path, args.rollback_to)
        else:
            full_load(spark, args)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
