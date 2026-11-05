"""Unit tests for a PySpark transform step (T-spark-001). Local SparkSession, chispa equality, no I/O."""
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession, functions as F, types as T

@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("unit")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()

def dedupe_orders(df):
    """Keep the latest row per order_id by _updated_at (the unit under test)."""
    w = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("order_id").orderBy(F.col("_updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

SCHEMA = T.StructType([T.StructField("order_id", T.StringType(), False),
                       T.StructField("amount", T.DecimalType(10, 2), True),
                       T.StructField("_updated_at", T.TimestampType(), False)])

@pytest.mark.tid("T-spark-001")
def test_dedupe_keeps_latest_per_key(spark):
    from datetime import datetime
    from decimal import Decimal
    rows = [("o1", Decimal("10.00"), datetime(2026, 11, 1, 10)), ("o1", Decimal("12.00"), datetime(2026, 11, 1, 11)),
            ("o2", None, datetime(2026, 11, 1, 9))]
    expected = [("o1", Decimal("12.00"), datetime(2026, 11, 1, 11)), ("o2", None, datetime(2026, 11, 1, 9))]
    actual = dedupe_orders(spark.createDataFrame(rows, SCHEMA))
    assert_df_equality(actual, spark.createDataFrame(expected, SCHEMA), ignore_row_order=True, ignore_nullable=True)

@pytest.mark.tid("T-spark-003")
def test_dedupe_is_idempotent(spark):
    from datetime import datetime
    df = spark.createDataFrame([("o1", None, datetime(2026, 11, 1))], SCHEMA)
    once, twice = dedupe_orders(df), dedupe_orders(dedupe_orders(df))
    assert_df_equality(once, twice, ignore_row_order=True)
