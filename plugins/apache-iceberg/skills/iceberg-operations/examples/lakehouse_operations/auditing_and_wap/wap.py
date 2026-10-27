"""Write-Audit-Publish with Iceberg branches (+ tagged restore points).

Pattern from the Iceberg docs (Branching and Tagging, "Audit branch") and Spark `spark.wap.branch`:
  1. write: the batch is committed to an audit branch; main does not move
  2. audit: data quality checks run against the branch only
  3. publish: fast-forward main to the branch (or cherry-pick if main moved), tag the audited snapshot
     reject:  drop the branch; the batch was never visible on main
"""
from contextlib import contextmanager
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from common.lakehouse import CATALOG
from auditing_and_wap.audit import write_with_audit


@contextmanager
def wap_branch(spark: SparkSession, table: str, branch: str):
    """Route every write of this session to `branch` (requires write.wap.enabled=true on the table)."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS `{branch}` RETAIN 7 DAYS")
    spark.conf.set("spark.wap.branch", branch)
    try:
        yield f"{table}.branch_{branch}"
    finally:
        spark.conf.unset("spark.wap.branch")


def quality_checks(spark: SparkSession, ref: str) -> list[str]:
    """Checks on the audit branch. Each returns a failure message or nothing."""
    r = spark.sql(f"""
        SELECT count(*) AS rows,
               count_if(customer_id IS NULL)          AS null_customers,
               count_if(amount IS NULL OR amount <= 0) AS bad_amounts,
               count(*) - count(DISTINCT order_id)     AS duplicate_ids
        FROM {ref}""").first()
    failures = []
    if r.rows == 0:
        failures.append("empty table")
    if r.null_customers:
        failures.append(f"{r.null_customers} orders without customer_id")
    if r.bad_amounts:
        failures.append(f"{r.bad_amounts} orders with non-positive or missing amount")
    if r.duplicate_ids:
        failures.append(f"{r.duplicate_ids} duplicate order_id values")
    return failures


@dataclass
class WapResult:
    published: bool
    failures: list[str] = field(default_factory=list)
    snapshot_id: int | None = None
    method: str | None = None            # "fast_forward" | "cherrypick"
    tag: str | None = None


def branch_head(spark: SparkSession, table: str, branch: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{branch}'").first()
    return row.snapshot_id if row else None


def write_audit_publish(spark: SparkSession, table: str, batch: DataFrame, run_id: str,
                        job_id: str = "orders_ingest", on_main_moved=None) -> WapResult:
    branch = f"audit_{run_id}"
    with wap_branch(spark, table, branch) as ref:
        write_with_audit(batch, table, job_id, run_id)           # lands on the branch, not on main
    if on_main_moved:                                            # test hook: a concurrent writer on main
        on_main_moved()

    failures = quality_checks(spark, ref)
    staged = branch_head(spark, table, branch)
    if failures:
        spark.sql(f"ALTER TABLE {table} DROP BRANCH `{branch}`")
        return WapResult(False, failures, staged)

    try:
        spark.sql(f"CALL {CATALOG}.system.fast_forward('{table}', 'main', '{branch}')")
        method = "fast_forward"
    except Exception as e:                                       # main is no longer an ancestor of the branch
        if "ancestor" not in str(e):
            raise
        spark.sql(f"CALL {CATALOG}.system.cherrypick_snapshot('{table}', {staged})")
        method = "cherrypick"
    published = branch_head(spark, table, "main")
    tag = f"audited_{run_id}"
    spark.sql(f"ALTER TABLE {table} CREATE TAG `{tag}` AS OF VERSION {published} RETAIN 30 DAYS")
    spark.sql(f"ALTER TABLE {table} DROP BRANCH `{branch}`")
    return WapResult(True, [], published, method, tag)
