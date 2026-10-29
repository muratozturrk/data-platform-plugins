"""
One transformation, five kinds of property: invariant, idempotency,
metamorphic relation, oracle/differential and determinism.

Run (one file at a time, foreground):
    PYTHONPATH=<plugin>/lib python -m pytest -q test_property_taxonomy.py
"""

from collections import defaultdict

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles

register_spark_profiles()
# Inherit deadline=None and the Spark health checks, only lower the example count.
FAST = settings(settings.get_profile("spark"), max_examples=3)

ORDERS = StructType([
    StructField("order_id",    IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("amount",      IntegerType(), nullable=False),
    StructField("status",      StringType(),  nullable=False),
])

CONFIG = DataFrameConfig(
    min_rows=0,
    max_rows=15,
    column_configs={
        "order_id":    ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
        "customer_id": ColumnConfig(custom_strategy=st.integers(1, 4)),
        # bounded: sums of IntegerType must not overflow
        "amount":      ColumnConfig(custom_strategy=st.integers(-50, 1_000)),
        "status":      ColumnConfig(custom_strategy=st.sampled_from(["paid", "cancelled", "refunded"])),
    },
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-taxonomy")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── transformation under test ────────────────────────────────────────────────
def clean(df):
    return df.filter((F.col("status") != "cancelled") & (F.col("amount") >= 0))


def totals(df):
    return clean(df).groupBy("customer_id").agg(F.sum("amount").alias("total"))


def as_dict(df):
    return {r["customer_id"]: r["total"] for r in df.collect()}


# ── 1. invariant ─────────────────────────────────────────────────────────────
def test_invariant_clean_never_adds_rows_or_negatives(spark):
    """Invariant: clean() keeps at most the input rows and no negative amounts."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG))
    def check(df):
        out = clean(df)
        assert out.count() <= df.count()
        assert out.filter(F.col("amount") < 0).count() == 0
    check()


# ── 2. idempotency ───────────────────────────────────────────────────────────
def test_idempotency_clean_twice_equals_once(spark):
    """Idempotency: clean(clean(df)) has exactly the rows of clean(df)."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG))
    def check(df):
        once, twice = clean(df), clean(clean(df))
        assert once.exceptAll(twice).count() == 0
        assert twice.exceptAll(once).count() == 0
    check()


# ── 3. metamorphic relation ──────────────────────────────────────────────────
def test_metamorphic_duplicating_input_doubles_totals(spark):
    """Metamorphic: totals(df ∪ df) == 2 × totals(df) per customer."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG))
    def check(df):
        single = as_dict(totals(df))
        doubled = as_dict(totals(df.unionAll(df)))
        assert doubled == {k: 2 * v for k, v in single.items()}
    check()


# ── 4. oracle / differential ─────────────────────────────────────────────────
def python_totals(rows):
    out = defaultdict(int)
    for r in rows:
        if r["status"] != "cancelled" and r["amount"] >= 0:
            out[r["customer_id"]] += r["amount"]
    return dict(out)


def test_oracle_spark_matches_plain_python(spark):
    """Oracle: Spark totals equal an obviously-correct pure-Python model."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG))
    def check(df):
        assert as_dict(totals(df)) == python_totals(df.collect())
    check()


# ── 5. determinism ───────────────────────────────────────────────────────────
def test_determinism_partitioning_does_not_change_result(spark):
    """Determinism: totals are independent of how the input is partitioned."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG), parts=st.integers(1, 3))
    def check(df, parts):
        assert as_dict(totals(df.repartition(parts))) == as_dict(totals(df))
    check()
