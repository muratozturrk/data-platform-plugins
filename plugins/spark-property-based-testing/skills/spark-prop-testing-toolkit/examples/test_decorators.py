"""
@spark_property and @prop: generated `df` hidden from pytest, `spark` and other fixtures still
injected, function and method style, SPARK_PROP_MAX_EXAMPLES override; @prop metadata.

Uses the plugin's `spark` fixture. Installed with pip the plugin loads itself (pytest11 entry
point); from a PYTHONPATH checkout load it explicitly:
    SPARK_PROP_MASTER=local[1] PYTHONPATH=<plugin>/lib \
        python -m pytest -q -p spark_prop_testing.plugin test_decorators.py
"""

import inspect

import pytest
from hypothesis import strategies as st
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, DataFrameConfig, input_columns_preserved, prop, row_count_non_increasing,
    spark_property,
)

ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
SMALL = DataFrameConfig(min_rows=1, max_rows=5, column_configs={
    "status": ColumnConfig(custom_strategy=st.sampled_from(["paid", "cancelled"])),
    "amount": ColumnConfig(custom_strategy=st.integers(0, 1_000)),
})


@pytest.fixture
def threshold():
    return 100


@spark_property(schema=ORDERS, config=SMALL, max_examples=3)
def test_filter_never_adds_rows(spark, df):
    """For any orders, filtering on status never adds rows (spark = the plugin's fixture)."""
    assert df.filter(F.col("status") == "paid").count() <= df.count()


@spark_property(schema=ORDERS, config=SMALL, max_examples=2)
def test_other_fixtures_stay_visible(df, threshold):
    """Fixtures other than df are resolved by pytest; spark comes from the active session."""
    big = df.filter(F.col("amount") >= threshold)
    assert big.filter(F.col("amount") < threshold).count() == 0


class TestOrderInvariants:
    @spark_property(schema=ORDERS, config=SMALL, max_examples=2)
    def test_method_style(self, spark, df):
        """Method style works: self, then fixtures, then the generated df."""
        assert set(df.columns) == {"order_id", "status", "amount"}
        assert df.sparkSession is spark


def test_df_is_hidden_from_pytest():
    """The wrapper's signature drops df, so pytest never looks for a 'df' fixture."""
    assert list(inspect.signature(test_filter_never_adds_rows).parameters) == ["spark"]
    assert list(inspect.signature(test_other_fixtures_stay_visible).parameters) == ["threshold"]
    with pytest.raises(TypeError, match="must take a 'df' parameter"):
        spark_property(schema=ORDERS)(lambda spark: None)


def test_env_override_beats_the_decorator(spark, monkeypatch):
    """SPARK_PROP_MAX_EXAMPLES (= --spark-prop-max-examples) overrides max_examples=50."""
    seen = []

    @spark_property(schema=ORDERS, config=SMALL, max_examples=50)
    def check(spark, df):
        seen.append(df.count())

    monkeypatch.setenv("SPARK_PROP_MAX_EXAMPLES", "2")
    check(spark=spark)                     # read at call time, not at decoration time
    assert 1 <= len(seen) <= 2


@prop(row_count_non_increasing, input_columns_preserved, max_examples=5)
def paid_orders(df):
    return df.filter(F.col("status") == "paid")


def test_prop_only_annotates(spark):
    """@prop stores properties and max_examples on the function; calling it is unchanged."""
    assert paid_orders._is_prop_transform
    assert paid_orders._prop_properties == [row_count_non_increasing, input_columns_preserved]
    assert paid_orders._prop_max_examples == 5
    df = spark.createDataFrame([(1, "paid", 5), (2, "cancelled", 7)], ORDERS)
    assert paid_orders(df).count() == 1
