"""
GX expectations as Hypothesis properties: GEPropertyValidator inside an @given loop,
the strategy constrained to match the suite (unique keys, bounded values), the
unconstrained anti-pattern, and a broken transformation caught by the suite.

Every example runs a full GX validation (several Spark jobs): keep max_examples at 2-3.

Run:
    GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib python -m pytest -q test_ge_property_bridge.py
"""

import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import pytest
from hypothesis import Phase, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, ColumnExpectationSpec, DataFrameConfig, ExpectationSuiteBuilder,
    GEPropertyValidator, auto_generate_suite, spark_dataframes,
)
from spark_prop_testing._hypothesis import register_spark_profiles

register_spark_profiles()
GX_FAST = settings(settings.get_profile("spark"), max_examples=2)
NO_SHRINK = settings(GX_FAST, phases=[Phase.generate])   # failing demos: shrinking = many GX runs

STATUSES = ["new", "paid", "shipped"]
ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])

SUITE = (
    ExpectationSuiteBuilder("orders_contract")
    .add_row_count(min_rows=1, max_rows=20)
    .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
    .add_column(ColumnExpectationSpec("status", not_null=True, value_set=STATUSES))
    .add_column(ColumnExpectationSpec("amount", min_value=0, max_value=10_000))
    .build()
)

# The strategy states the same contract as the suite: unique, bounded keys; the status set; amount range.
MATCHING = DataFrameConfig(min_rows=1, max_rows=20, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(STATUSES)),
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 5_000)),
})
# Anti-pattern: key without unique=True. 5+ rows over 3 key values guarantee a duplicate.
UNCONSTRAINED_KEYS = DataFrameConfig(min_rows=5, max_rows=10, column_configs={
    **MATCHING.column_configs,
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 3)),
})


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-gx-bridge")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def validator():
    return GEPropertyValidator()          # build the ephemeral context once, not per example


def apply_discount(df):
    return df.withColumn("amount", (F.col("amount") * 0.9).cast("int"))


def apply_discount_buggy(df):
    return df.withColumn("amount", F.col("amount") - 10_000)      # absolute instead of relative


def test_generated_data_satisfies_suite(spark, validator):
    """Any DataFrame from the matching strategy passes the orders contract suite."""
    @GX_FAST
    @given(df=spark_dataframes(spark, ORDERS, MATCHING))
    def check(df):
        validator.validate(df, SUITE).assert_success()
    check()


def test_transformation_preserves_contract(spark, validator):
    """For any valid input, apply_discount() output still passes the contract suite."""
    @GX_FAST
    @given(df=spark_dataframes(spark, ORDERS, MATCHING))
    def check(df):
        result = validator.validate(apply_discount(df), SUITE)
        assert result.success, result.summary()
    check()


def test_auto_suite_holds_for_generated_data(spark, validator):
    """The schema-derived baseline suite (NOT NULL + integer type bounds) holds for type-correct data."""
    auto = auto_generate_suite(ORDERS, suite_name="orders_auto")
    @GX_FAST
    @given(df=spark_dataframes(spark, ORDERS, DataFrameConfig(min_rows=1, max_rows=10)))
    def check(df):
        validator.validate(df, auto).assert_success()
    check()


@pytest.mark.xfail(strict=True, reason="demonstrates GX catching a transformation that breaks the amount range")
def test_buggy_transformation_is_caught(spark, validator):
    """For any valid input, apply_discount_buggy() output passes the suite (it does not: negative amounts)."""
    @NO_SHRINK
    @given(df=spark_dataframes(spark, ORDERS, MATCHING))
    def check(df):
        result = validator.validate(apply_discount_buggy(df), SUITE)
        assert result.success, result.summary()
    check()


@pytest.mark.xfail(strict=True, reason="anti-pattern: unique expectation without unique=True in the strategy")
def test_anti_pattern_unconstrained_keys(spark, validator):
    """The suite holds for generated data (it does not: the key strategy produces duplicates)."""
    @NO_SHRINK
    @given(df=spark_dataframes(spark, ORDERS, UNCONSTRAINED_KEYS))
    def check(df):
        validator.validate(df, SUITE).assert_success()
    check()
