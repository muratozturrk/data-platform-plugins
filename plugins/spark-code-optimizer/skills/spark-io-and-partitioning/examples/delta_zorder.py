"""delta_zorder: OPTIMIZE ... ZORDER BY on a Delta table, with partition-column guard.

sparkopt fires when estimated_bytes >= 10 GiB (heuristic) and defaults to
["event_date", "user_id"], which are placeholders. Delta rejects Z-ordering by a partition
column, so this helper drops partition columns first.

Requires delta-spark matching your Spark version (pip install delta-spark).
Run: python delta_zorder.py /path/to/delta_table user_id [more_cols...]
"""
import sys

from pyspark.sql import SparkSession


def zorder_columns(requested, partition_cols, max_cols=4):
    """Drop partition columns (Delta raises on them) and cap at max_cols."""
    cols = [c for c in requested if c not in set(partition_cols)]
    return cols[:max_cols]


def optimize_zorder(spark, path, columns, where=None):
    from delta.tables import DeltaTable

    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    cols = zorder_columns(columns, detail["partitionColumns"])
    if not cols:
        raise ValueError("no Z-order column left after removing partition columns")
    builder = DeltaTable.forPath(spark, path).optimize()
    if where:  # must reference partition columns only
        builder = builder.where(where)
    return builder.executeZOrderBy(*cols)


if __name__ == "__main__":
    assert zorder_columns(["event_date", "user_id"], ["event_date"]) == ["user_id"]
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(0)
    spark = (
        SparkSession.builder.appName("delta_zorder")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    metrics = optimize_zorder(spark, sys.argv[1], sys.argv[2:])
    metrics.select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
    spark.stop()
