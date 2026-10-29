"""
A SparkSession configured for property tests, and checks that the settings
that make results deterministic are really in effect.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_session_setup.py
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles

register_spark_profiles()
FAST = settings(settings.get_profile("spark"), max_examples=3)


@pytest.fixture(scope="session")
def spark():
    """One local session for the whole test run (starting Spark is the slow part)."""
    session = (
        SparkSession.builder
        .master("local[1]")                                  # 1 core: no worker races, low memory
        .appName("property-tests")
        .config("spark.sql.shuffle.partitions", "1")         # default 200 = 200 tiny tasks per shuffle
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")                 # no port binding, faster start
        .config("spark.sql.adaptive.enabled", "false")       # AQE re-plans at runtime: keep plans fixed
        .config("spark.sql.session.timeZone", "UTC")         # timestamp functions independent of the host
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_session_is_configured_for_tests(spark):
    """The session runs locally, on one core, with AQE off and UTC time."""
    conf = spark.conf
    assert spark.sparkContext.master == "local[1]"
    assert conf.get("spark.sql.shuffle.partitions") == "1"
    assert conf.get("spark.sql.adaptive.enabled") == "false"
    assert conf.get("spark.sql.session.timeZone") == "UTC"


def test_aqe_off_gives_a_static_plan(spark):
    """With AQE off the physical plan has no AdaptiveSparkPlan node."""
    df = spark.range(10).groupBy((F.col("id") % 3).alias("k")).count()
    plan = df._jdf.queryExecution().executedPlan().toString()
    assert "AdaptiveSparkPlan" not in plan


SCHEMA = StructType([
    StructField("k", IntegerType(), nullable=False),
    StructField("v", IntegerType(), nullable=False),
])


def test_aggregation_output_has_configured_partitions(spark):
    """After a shuffle the result has exactly spark.sql.shuffle.partitions partitions."""
    cfg = DataFrameConfig(min_rows=1, max_rows=10, column_configs={
        "k": ColumnConfig(custom_strategy=st.integers(0, 3)),
        "v": ColumnConfig(custom_strategy=st.integers(0, 100)),
    })

    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, cfg))
    def check(df):
        out = df.groupBy("k").agg(F.sum("v").alias("s"))
        assert out.rdd.getNumPartitions() == 1
    check()
