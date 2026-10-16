"""Schema Migrator - rename `from_page` -> `referral` on a Delta table, PySpark.

Phases (pass one as argv[1]):
  add      : add `referral`, backfill it from `from_page` (old column untouched)
  write    : grace-period producer -> dual-writes both columns from one source value
  finalize : after the agreed deadline, drop `from_page` (needs column mapping)

Never do `ALTER TABLE ... RENAME COLUMN from_page TO referral` in one step: every
consumer query referencing `from_page` breaks immediately.

Run: spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_delta_schema_migrator.py add|write|finalize
Env: WAREHOUSE_DIR (/tmp/dp/warehouse), MIGRATION_DEADLINE (ISO date, default 2025-01-31).
"""
import datetime as dt
import os
import sys

from pyspark.sql import SparkSession, functions as F

DEADLINE = dt.date.fromisoformat(os.environ.get("MIGRATION_DEADLINE", "2025-01-31"))
spark = (SparkSession.builder.appName("schema-migrator")
         .config("spark.sql.warehouse.dir", os.environ.get("WAREHOUSE_DIR", "/tmp/dp/warehouse"))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

spark.sql("""CREATE TABLE IF NOT EXISTS default.visits (visit_id STRING, page STRING, from_page STRING)
             USING delta""")


def add_new_column():
    if "referral" not in spark.table("default.visits").columns:
        spark.sql("ALTER TABLE default.visits ADD COLUMNS (referral STRING "
                  "COMMENT 'Replaces from_page; from_page removed after deadline')")
    spark.sql("ALTER TABLE default.visits ALTER COLUMN from_page COMMENT "
              f"'DEPRECATED: use referral, removed after {DEADLINE}'")
    spark.sql("UPDATE default.visits SET referral = from_page WHERE referral IS NULL")


def dual_write(new_visits):
    """Grace period: one source value, both columns populated -> they can't diverge."""
    (new_visits.withColumn("referral", F.col("source_referral"))
     .withColumn("from_page", F.col("source_referral")).drop("source_referral")
     .writeTo("default.visits").append())


def finalize():
    if dt.date.today() <= DEADLINE:
        raise SystemExit(f"Grace period not over (deadline {DEADLINE}); consumers may still read from_page")
    # DROP COLUMN is metadata-only in Delta but requires column mapping by name
    spark.sql("""ALTER TABLE default.visits SET TBLPROPERTIES (
                 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5',
                 'delta.columnMapping.mode' = 'name')""")
    spark.sql("ALTER TABLE default.visits DROP COLUMN from_page")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "add"
    if phase == "add":
        add_new_column()
    elif phase == "write":
        dual_write(spark.createDataFrame([("v10", "home", "google.com")],
                                         "visit_id STRING, page STRING, source_referral STRING"))
    elif phase == "finalize":
        finalize()
    spark.table("default.visits").show(truncate=False)
