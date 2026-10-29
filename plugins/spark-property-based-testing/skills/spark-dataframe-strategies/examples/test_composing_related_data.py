"""
Related columns and related DataFrames: date ranges with end >= start,
foreign keys between customers and orders, bounded money amounts.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_composing_related_data.py
"""

from datetime import date, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles

register_spark_profiles()
PURE = settings(database=None, max_examples=200)
FAST = settings(settings.get_profile("spark"), max_examples=3)

# ── bounded money: DecimalType(12, 2), 0.00 .. 100 000.00 ────────────────────
MONEY = DecimalType(12, 2)
money = st.decimals(min_value=Decimal("0.00"), max_value=Decimal("100000.00"),
                    places=2, allow_nan=False, allow_infinity=False)


# ── related columns in one row: st.composite ─────────────────────────────────
@st.composite
def subscription_rows(draw):
    start = draw(st.dates(date(2000, 1, 1), date(2030, 12, 31)))
    length = draw(st.integers(0, 365))
    return {
        "sub_id":     draw(st.integers(1, 10_000)),
        "start_date": start,
        "end_date":   start + timedelta(days=length),
        "fee":        draw(money),
    }


SUBS = StructType([
    StructField("sub_id",     IntegerType(), False),
    StructField("start_date", DateType(),    False),
    StructField("end_date",   DateType(),    False),
    StructField("fee",        MONEY,         False),
])

# same relation with flatmap: the second draw depends on the first
date_ranges = st.dates(date(2000, 1, 1), date(2030, 12, 31)).flatmap(
    lambda s: st.tuples(st.just(s), st.dates(min_value=s, max_value=s + timedelta(days=365)))
)


@PURE
@given(date_ranges)
def test_flatmap_date_range_is_ordered(rng):
    """flatmap: the end date is drawn from dates >= the start date."""
    start, end = rng
    assert start <= end <= start + timedelta(days=365)


@PURE
@given(money)
def test_money_strategy_fits_decimal_12_2(m):
    """Money values fit DecimalType(12,2): two places, no more than 10 integer digits."""
    assert m.as_tuple().exponent >= -2 and Decimal("0") <= m <= Decimal("100000.00")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-composing")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_composite_rows_keep_end_after_start(spark):
    """Composite rows: every generated subscription has end_date >= start_date; fee sums exactly."""
    rows_strategy = st.lists(subscription_rows(), min_size=1, max_size=15,
                             unique_by=lambda r: r["sub_id"])

    @FAST
    @given(rows=rows_strategy)
    def check(rows):
        df = spark.createDataFrame(rows, SUBS)
        assert df.filter(F.col("end_date") < F.col("start_date")).count() == 0
        # decimals sum exactly (no float tolerance needed)
        assert df.agg(F.sum("fee")).first()[0] == sum(r["fee"] for r in rows)
    check()


# ── related DataFrames: foreign keys ─────────────────────────────────────────
CUSTOMERS = StructType([StructField("customer_id", IntegerType(), False)])
ORDERS = StructType([
    StructField("order_id",    IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount",      MONEY,         False),
])


def customers_and_orders(spark):
    """Draw the parent keys first, then build the child DataFrame from them."""
    customer_ids = st.lists(st.integers(1, 500), min_size=1, max_size=8, unique=True)

    def build(ids):
        customers = spark.createDataFrame([(i,) for i in ids], CUSTOMERS)
        orders = spark_dataframes(spark, ORDERS, DataFrameConfig(
            min_rows=0, max_rows=15, column_configs={
                "order_id":    ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
                "customer_id": ColumnConfig(custom_strategy=st.sampled_from(ids)),
                "amount":      ColumnConfig(custom_strategy=money),
            }))
        return st.tuples(st.just(customers), orders)

    return customer_ids.flatmap(build)


def test_every_order_references_a_customer(spark):
    """FK integrity: no order without a customer, and the inner join keeps every order."""
    @FAST
    @given(pair=customers_and_orders(spark))
    def check(pair):
        customers, orders = pair
        assert orders.join(customers, "customer_id", "left_anti").count() == 0
        assert orders.join(customers, "customer_id").count() == orders.count()
    check()
