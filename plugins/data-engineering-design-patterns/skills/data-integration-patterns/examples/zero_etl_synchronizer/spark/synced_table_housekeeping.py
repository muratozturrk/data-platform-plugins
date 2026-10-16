"""Zero-ETL Synchronizer - the "not zero effort" part, on the synced Iceberg table (PySpark).

The broker writes `dp.visits` for you, but you still own:
  1. freshness monitoring: age of the latest snapshot (sync stalled?);
  2. housekeeping: compaction of small files, snapshot expiration, manifest rewrite;
  3. transformations: zero transform at sync time, so derived data goes to a NEW table
     (Transformation Replicator); the raw synced table is never rewritten in place.

Run:  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1 synced_table_housekeeping.py
Env:  ICEBERG_REST_URI (default http://localhost:8181), WAREHOUSE (default s3://dp/iceberg)
"""
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

TABLE = "lakehouse.dp.visits"
DERIVED_TABLE = "lakehouse.dp.visits_cleaned"
MAX_SNAPSHOT_AGE = timedelta(hours=1)


def build_session() -> SparkSession:
    return (SparkSession.builder.appName("zero_etl_housekeeping")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.session.timeZone", "UTC")  # committed_at is compared with UTC now
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"))
            .config("spark.sql.catalog.lakehouse.warehouse", os.environ.get("WAREHOUSE", "s3://dp/iceberg"))
            .getOrCreate())


def check_freshness(spark: SparkSession) -> None:
    last = spark.sql(f"SELECT MAX(committed_at) AS last FROM {TABLE}.snapshots").collect()[0].last
    age = datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)
    if age > MAX_SNAPSHOT_AGE:
        raise RuntimeError(f"{TABLE} not synced for {age} - check the broker sync / permissions")


def housekeeping(spark: SparkSession) -> None:
    spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{TABLE.split('.', 1)[1]}', "
              "options => map('target-file-size-bytes', '134217728', 'min-input-files', '5'))")
    # Time travel is limited by this retention: do not treat it as your immutability guarantee.
    older_than = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{TABLE.split('.', 1)[1]}', "
              f"older_than => TIMESTAMP '{older_than}', retain_last => 50)")
    spark.sql(f"CALL lakehouse.system.rewrite_manifests('{TABLE.split('.', 1)[1]}')")


def transform_to_new_table(spark: SparkSession) -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DERIVED_TABLE} USING iceberg AS
                  SELECT * FROM {TABLE} WHERE 1 = 0""")
    # ponytail: full overwrite; switch to incremental MERGE on snapshot ranges when the table grows.
    spark.sql(f"""INSERT OVERWRITE {DERIVED_TABLE}
                  SELECT * FROM {TABLE} WHERE page IS NOT NULL""")


if __name__ == "__main__":
    session = build_session()
    check_freshness(session)
    housekeeping(session)
    transform_to_new_table(session)
