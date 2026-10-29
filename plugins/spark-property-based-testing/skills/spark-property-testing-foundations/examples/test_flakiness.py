"""
The usual sources of flaky Spark assertions and the stable way to write each:
row order, floating point, time zones, partition-dependent ids.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_flakiness.py
"""

import math
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

from spark_prop_testing import ColumnConfig, DataFrameConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles

register_spark_profiles()
FAST = settings(settings.get_profile("spark"), max_examples=3)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-flakiness")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


SCHEMA = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("x",  DoubleType(),  nullable=False),
])
CFG = DataFrameConfig(min_rows=1, max_rows=12, column_configs={
    "id": ColumnConfig(custom_strategy=st.integers(0, 1_000), unique=True),
    # bounded floats: unbounded doubles overflow to inf when summed
    "x":  ColumnConfig(custom_strategy=st.floats(-1e6, 1e6, allow_nan=False)),
})


def test_row_order_compare_as_multisets(spark):
    """Row order: a repartitioned DataFrame has the same rows, compared order-free."""
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, CFG))
    def check(df):
        shuffled = df.repartition(3)
        assert shuffled.collect() != [] and sorted(shuffled.collect()) == sorted(df.collect())
        # or entirely in Spark (multiset difference, duplicates count):
        assert shuffled.exceptAll(df).count() == 0 and df.exceptAll(shuffled).count() == 0
    check()


def test_float_addition_is_not_associative():
    """Floats: the same numbers summed in another order give a different double."""
    assert (0.1 + 0.2) + 0.3 != 0.1 + (0.2 + 0.3)


def test_float_sum_compared_with_tolerance(spark):
    """Floats: Spark's sum equals Python's fsum within a relative/absolute tolerance."""
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, CFG))
    def check(df):
        spark_sum = df.repartition(2).agg(F.sum("x")).first()[0]
        exact = math.fsum(r["x"] for r in df.collect())
        assert math.isclose(spark_sum, exact, rel_tol=1e-9, abs_tol=1e-6)
    check()


def test_timestamps_compared_as_epoch_seconds(spark):
    """Time zones: the instant (epoch seconds) is zone-independent, the wall-clock text is not."""
    ts_schema = StructType([StructField("ts", TimestampType(), nullable=False)])
    cfg = DataFrameConfig(min_rows=1, max_rows=5, column_configs={"ts": ColumnConfig(
        custom_strategy=st.datetimes(datetime(1900, 1, 1), datetime(2100, 1, 1),
                                     timezones=st.just(timezone.utc)))})

    def epoch_and_text(df):
        # cast in Spark: no Python datetime conversion (collecting a pre-1970
        # TimestampType value fails on Windows, and collected values are naive
        # datetimes in the *Python process'* local zone)
        rows = df.select(F.col("ts").cast("long"), F.date_format("ts", "HH")).collect()
        return [r[0] for r in rows], [r[1] for r in rows]

    @FAST
    @given(df=spark_dataframes(spark, ts_schema, cfg))
    def check(df):
        utc_secs, utc_hours = epoch_and_text(df)
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        try:
            ny_secs, ny_hours = epoch_and_text(df)
        finally:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        assert ny_secs == utc_secs              # same instants
        assert ny_hours != utc_hours            # different wall clock (NY is never UTC+0)
    check()


def test_monotonically_increasing_id_depends_on_partitioning(spark):
    """Partition-dependent ids: monotonically_increasing_id jumps between partitions."""
    df = spark.range(4).repartition(2)
    ids = sorted(r[0] for r in df.select(F.monotonically_increasing_id()).collect())
    assert ids != [0, 1, 2, 3]               # second partition starts at 1 << 33
    assert max(ids) >= 1 << 33

    # stable alternative: row_number over an explicit, total ordering
    stable = df.select(F.row_number().over(Window.orderBy("id")).alias("rn"))
    assert sorted(r[0] for r in stable.collect()) == [1, 2, 3, 4]
