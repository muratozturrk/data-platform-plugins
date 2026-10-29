"""
spark_dataframes + DataFrameConfig + ColumnConfig: row bounds, custom strategies,
unique keys, nullability, default_null_config and the bundled profiles.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_dataframe_config.py
"""

import pytest
from hypothesis import find, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, NullConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles
from spark_prop_testing.strategies.profiles import (
    edge_case_profile, high_null_profile, production_like_profile, stress_test_profile,
)

register_spark_profiles()
FAST = settings(settings.get_profile("spark"), max_examples=3)

STATUSES = ["new", "paid", "shipped"]
SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("qty",      IntegerType(), nullable=False),
])
CONFIG = DataFrameConfig(
    min_rows=2,
    max_rows=15,
    column_configs={
        "order_id": ColumnConfig(custom_strategy=st.integers(1, 20), unique=True),
        "status":   ColumnConfig(custom_strategy=st.sampled_from(STATUSES)),
        "qty":      ColumnConfig(custom_strategy=st.integers(1, 99),
                                 null_config=NullConfig(null_rate=1.0)),   # no effect: NOT NULL column
    },
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-dataframe-config")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_config_is_honoured(spark):
    """Rows within [min_rows, max_rows], custom strategies respected, unique keys distinct."""
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, CONFIG))
    def check(df):
        rows = df.collect()
        assert 2 <= len(rows) <= 15
        ids = [r.order_id for r in rows]
        assert len(ids) == len(set(ids)) and all(1 <= i <= 20 for i in ids)
        assert {r.status for r in rows} <= set(STATUSES)
    check()


def test_non_nullable_columns_ignore_null_config(spark):
    """NullConfig only applies to nullable fields: a NOT NULL column never gets nulls."""
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, CONFIG))
    def check(df):
        assert df.filter(F.col("qty").isNull()).count() == 0
    check()


def test_default_config_and_empty_frames(spark):
    """min_rows=0 can produce an empty DataFrame that still has the schema."""
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, DataFrameConfig(min_rows=0, max_rows=0)))
    def check(df):
        assert df.count() == 0 and df.schema == SCHEMA
    check()


def test_default_null_config_applies_to_unconfigured_nullable_columns(spark):
    """default_null_config: nullable columns without a ColumnConfig get its nulls; others don't."""
    schema = StructType([
        StructField("id",   IntegerType(), nullable=False),
        StructField("note", StringType(),  nullable=True),    # no ColumnConfig → default
        StructField("qty",  IntegerType(), nullable=True),    # explicit ColumnConfig wins
    ])
    cfg = DataFrameConfig(min_rows=1, max_rows=10,
                          default_null_config=NullConfig(null_rate=1.0),
                          column_configs={"qty": ColumnConfig(custom_strategy=st.integers(0, 9))})
    assert cfg.column("note").null_config.null_rate == 1.0
    assert cfg.column("qty").null_config.null_rate == 0.0

    @FAST
    @given(df=spark_dataframes(spark, schema, cfg))
    def check(df):
        n = df.count()
        assert df.filter(F.col("note").isNull()).count() == n
        assert df.filter(F.col("qty").isNull() | F.col("id").isNull()).count() == 0
    check()


def test_profiles():
    """Profiles: row bounds plus default nulls (high_null, production_like) and id columns."""
    assert (edge_case_profile().min_rows, edge_case_profile().max_rows) == (1, 10)
    assert (stress_test_profile().min_rows, stress_test_profile().max_rows) == (1_000, 10_000)

    hn = high_null_profile(null_rate=0.4)
    assert (hn.min_rows, hn.max_rows, hn.column_configs) == (50, 500, {})
    assert hn.column("anything").null_config.null_rate == 0.4

    pl = production_like_profile(id_cardinality=5, id_columns=["user_id"])
    assert (pl.min_rows, pl.max_rows) == (100, 1_000)
    assert pl.default_null_config.null_rate == 0.02
    assert set(pl.column_configs) == {"user_id"}
    # the id column draws from 1..id_cardinality (its own ColumnConfig: no background nulls)
    assert pl.column("user_id").null_config.null_rate == 0.0
    ids = pl.column("user_id").custom_strategy
    quick = settings(database=None, max_examples=100)
    assert find(ids, lambda v: True, settings=quick) == 1
    assert find(ids, lambda v: v >= 5, settings=quick) == 5
