"""
NullConfig semantics (rate 1.0, fractional per-DataFrame rate, always_include_null,
unique + nulls) and the distribution helpers.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_nulls_and_edge_cases.py
"""

from collections import Counter

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, DataFrameConfig, NullConfig, edge_case_integers, edge_case_strings,
    realistic_id_strategy, skewed_integers, spark_dataframes,
)
from spark_prop_testing._hypothesis import register_spark_profiles
from spark_prop_testing.strategies.null_strategy import with_nulls

register_spark_profiles()
# too_slow: the first st.text draw builds Hypothesis' unicode table (seconds, cached in .hypothesis/)
PURE = settings(database=None, max_examples=200, suppress_health_check=[HealthCheck.too_slow])
FAST = settings(settings.get_profile("spark"), max_examples=3)


# ── pure Hypothesis ──────────────────────────────────────────────────────────
def test_null_rate_is_validated():
    """null_rate outside [0, 1] is rejected."""
    with pytest.raises(ValueError):
        NullConfig(null_rate=1.5)


@PURE
@given(with_nulls(st.integers(), NullConfig(null_rate=1.0)))
def test_with_nulls_rate_one_is_always_none(v):
    """with_nulls at rate 1.0 is st.none()."""
    assert v is None


def test_with_nulls_shrinks_towards_none():
    """with_nulls at a fractional rate shrinks to None, the usual interesting case."""
    strat = with_nulls(st.integers(), NullConfig(null_rate=0.001))
    assert find(strat, lambda v: True, settings=PURE) is None


@PURE
@given(skewed_integers(0, 100, skew_value=42, skew_probability=1.0))
def test_skew_probability_one_always_hot_key(v):
    """skew_probability=1.0 always yields the skew value."""
    assert v == 42


def test_skewed_integers_validates_probability():
    """skew_probability outside [0, 1] raises ValueError."""
    with pytest.raises(ValueError):
        skewed_integers(skew_probability=1.2)


def test_skewed_integers_shrink_to_hot_key():
    """Shrinking moves to the skew value (defaults to min_value)."""
    assert find(skewed_integers(10, 1_000), lambda v: True, settings=PURE) == 10


@PURE
@given(edge_case_integers(-5, 5), realistic_id_strategy(max_id=7))
def test_bounded_helpers_respect_bounds(x, fk):
    """edge_case_integers and realistic_id_strategy stay in their bounds."""
    assert -5 <= x <= 5 and 1 <= fk <= 7


def test_edge_case_strings_cover_pipeline_breakers():
    """edge_case_strings can produce '', 'NULL', SQL injection text and 1000-char strings."""
    for target in ["", "NULL", "'DROP TABLE users;--", "a" * 1000]:
        assert find(edge_case_strings(), lambda s, t=target: s == t, settings=PURE) == target


# ── Spark: nulls in generated DataFrames ─────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-nulls")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


SCHEMA = StructType([
    StructField("id",     IntegerType(), nullable=True),
    StructField("always", StringType(),  nullable=True),
    StructField("some",   IntegerType(), nullable=True),
    StructField("half",   IntegerType(), nullable=True),
])


def test_null_config_semantics(spark):
    """rate 1.0 → all null; always_include_null → ≥1 null; unique column → ≤1 null."""
    cfg = DataFrameConfig(min_rows=1, max_rows=20, column_configs={
        "id":     ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True,
                               null_config=NullConfig(null_rate=0.5)),
        "always": ColumnConfig(null_config=NullConfig(null_rate=1.0)),
        "some":   ColumnConfig(custom_strategy=st.integers(0, 9),
                               null_config=NullConfig(always_include_null=True)),
    })

    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, cfg))
    def check(df):
        n = df.count()
        nulls = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
        assert nulls["always"] == n
        assert nulls["some"] >= 1
        assert nulls["id"] <= 1
        assert nulls["half"] == 0                       # default NullConfig: no nulls
    check()


def test_fractional_rate_applies_per_dataframe(spark):
    """null_rate=0.5 on 100-150 rows gives roughly half nulls (not 0% or 100%)."""
    schema = StructType([StructField("half", IntegerType(), nullable=True)])
    cfg = DataFrameConfig(min_rows=100, max_rows=150, column_configs={
        "half": ColumnConfig(custom_strategy=st.integers(0, 9), null_config=NullConfig(0.5)),
    })

    @FAST
    @given(df=spark_dataframes(spark, schema, cfg))
    def check(df):
        counts = Counter(r.half is None for r in df.collect())
        frac = counts[True] / sum(counts.values())
        assert 0.25 < frac < 0.75, frac
    check()
