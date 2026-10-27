"""Iceberg features as a pipeline uses them: evolution, time travel/rollback, write-audit-publish.

Procedures and DDL follow the Iceberg Spark docs:
https://iceberg.apache.org/docs/latest/spark-ddl/ , .../spark-procedures/ , .../branching/
"""
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

from common.lakehouse import CATALOG


def data_file_paths(spark: SparkSession, table: str) -> set[str]:
    return {r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}


def current_snapshot_id(spark: SparkSession, table: str) -> int:
    """Head of main. Not the newest snapshot: rollbacks and branch commits make those differ."""
    return spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first().snapshot_id


def rollback_to(spark: SparkSession, table: str, snapshot_id: int) -> None:
    """Undo bad loads: make an older snapshot current again (metadata-only, no data copied)."""
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{table}', {snapshot_id})")


@contextmanager
def wap_branch(spark: SparkSession, table: str, branch: str):
    """Write-audit-publish: writes inside the block go to `branch`, readers of main see nothing."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS {branch}")
    spark.conf.set("spark.wap.branch", branch)
    try:
        yield
    finally:
        spark.conf.unset("spark.wap.branch")


def audit(spark: SparkSession, table: str, branch: str) -> list[str]:
    """Data quality checks run against the branch before publishing. Returns failures."""
    df: DataFrame = spark.read.option("branch", branch).table(table)
    failures = []
    if df.where("amount < 0").limit(1).count():
        failures.append("negative amount")
    if df.groupBy("order_id").count().where("count > 1").limit(1).count():
        failures.append("duplicate order_id")
    return failures


def publish(spark: SparkSession, table: str, branch: str) -> None:
    spark.sql(f"CALL {CATALOG}.system.fast_forward('{table}', 'main', '{branch}')")
