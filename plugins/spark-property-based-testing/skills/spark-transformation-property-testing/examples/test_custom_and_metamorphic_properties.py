"""
Custom transformation properties ((input_df, output_df) -> None, raise AssertionError with a
useful message) and metamorphic properties: change the input in a known way and check how the
output must change, without knowing the expected output.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_custom_and_metamorphic_properties.py
"""

import math

import pytest
from hypothesis import Phase
from hypothesis import strategies as st
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, TransformationPropertyTester

ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer", StringType(),  nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
GEN = DataFrameConfig(min_rows=1, max_rows=6, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True),
    "customer": ColumnConfig(custom_strategy=st.sampled_from(["ann", "bob", "cy"])),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["paid", "cancelled"])),
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 1_000)),
})


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("custom-metamorphic")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Known-failing demos skip shrinking (each shrink step is another Spark run).
NO_SHRINK = (Phase.explicit, Phase.generate)


# ── helpers every custom property needs ─────────────────────────────────────

def assert_same_rows(expected: DataFrame, actual: DataFrame, label: str) -> None:
    """Multiset equality, independent of row order and partitioning."""
    cols = expected.columns
    extra = actual.select(*cols).exceptAll(expected.select(*cols))
    missing = expected.select(*cols).exceptAll(actual.select(*cols))
    n_extra, n_missing = extra.count(), missing.count()
    if n_extra or n_missing:
        raise AssertionError(
            f"{label}: {n_extra} unexpected rows (e.g. {extra.limit(3).collect()}), "
            f"{n_missing} missing rows (e.g. {missing.limit(3).collect()})")


# ── the transformations under test ──────────────────────────────────────────

def revenue_by_customer(df):
    return (df.filter(F.col("status") != "cancelled")
              .groupBy("customer").agg(F.sum("amount").alias("revenue")))


def revenue_including_cancelled(df):          # bug: forgets the status filter
    return df.groupBy("customer").agg(F.sum("amount").alias("revenue"))


def dedupe_orders(df):                        # keep one row per order_id (highest amount)
    w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ── custom properties ───────────────────────────────────────────────────────

def revenue_only_from_paid_orders(input_df, output_df):
    """Plain invariant: total revenue == sum of paid amounts (exact: integer sums)."""
    paid = input_df.filter("status = 'paid'").agg(F.sum("amount")).first()[0] or 0
    total = output_df.agg(F.sum("revenue")).first()[0] or 0
    assert total == paid, f"revenue {total} != sum of paid amounts {paid}"


def one_row_per_customer(input_df, output_df):
    dupes = output_df.groupBy("customer").count().filter("count > 1")
    assert dupes.count() == 0, f"customers with several rows: {dupes.limit(5).collect()}"


def make_scaling_property(transform, factor=3):
    """Metamorphic: amount × k  ⇒  revenue × k for every customer."""
    def revenue_scales_with_amount(input_df, output_df):
        scaled = transform(input_df.withColumn("amount", F.col("amount") * factor))
        expected = output_df.withColumn("revenue", F.col("revenue") * factor)
        assert_same_rows(expected, scaled, f"amount x{factor}")
    return revenue_scales_with_amount


def make_cancelled_order_property(transform):
    """Metamorphic: adding a cancelled order leaves every customer's revenue unchanged."""
    def cancelled_order_changes_nothing(input_df, output_df):
        new_id = input_df.agg(F.max("order_id")).first()[0] + 1
        customer = input_df.first()["customer"]
        extra = input_df.sparkSession.createDataFrame(
            [(new_id, customer, "cancelled", 999)], input_df.schema)
        assert_same_rows(output_df, transform(input_df.unionByName(extra)), "+1 cancelled order")
    return cancelled_order_changes_nothing


def make_duplicate_row_property(transform):
    """Metamorphic: a re-delivered (duplicate) row does not change the deduplicated output."""
    def duplicate_row_changes_nothing(input_df, output_df):
        again = transform(input_df.unionByName(input_df.limit(1)))
        assert_same_rows(output_df, again, "+1 duplicate row")
    return duplicate_row_changes_nothing


def test_custom_properties_run_on_the_tester(spark):
    """Custom invariants and the scaling relation hold for revenue_by_customer on generated orders."""
    report = (
        TransformationPropertyTester(spark, revenue_by_customer, ORDERS, df_config=GEN)
        .add_property(revenue_only_from_paid_orders)
        .add_property(one_row_per_customer)
        .add_property(make_scaling_property(revenue_by_customer))
        .run(max_examples=2)
    )
    report.assert_passed()


def test_metamorphic_cancelled_order_and_duplicate(spark):
    """+1 cancelled order keeps revenue; +1 duplicate row keeps the deduplicated output."""
    revenue = (TransformationPropertyTester(spark, revenue_by_customer, ORDERS, df_config=GEN)
               .add_property(make_cancelled_order_property(revenue_by_customer)).run(max_examples=2))
    dedupe = (TransformationPropertyTester(spark, dedupe_orders, ORDERS, df_config=GEN)
              .add_property(make_duplicate_row_property(dedupe_orders)).run(max_examples=2))
    revenue.assert_passed()
    dedupe.assert_passed()


@pytest.mark.xfail(strict=True, reason="demonstrates the cancelled-order relation catching a missing status filter")
def test_metamorphic_relation_finds_the_bug(spark):
    """revenue_including_cancelled() keeps revenue unchanged when a cancelled order is added (it does not)."""
    (TransformationPropertyTester(spark, revenue_including_cancelled, ORDERS, df_config=GEN)
     .add_property(make_cancelled_order_property(revenue_including_cancelled))
     .run(max_examples=2, phases=NO_SHRINK)
     .assert_passed())


def test_messages_say_what_broke(spark):
    """A good custom property names the rule, the numbers and sample rows."""
    df = spark.createDataFrame([(1, "ann", "paid", 10), (2, "ann", "cancelled", 5)], ORDERS)
    with pytest.raises(AssertionError, match="revenue 15 != sum of paid amounts 10"):
        revenue_only_from_paid_orders(df, revenue_including_cancelled(df))
    with pytest.raises(AssertionError, match=r"\+1 cancelled order: 1 unexpected rows .*revenue=1014"):
        make_cancelled_order_property(revenue_including_cancelled)(df, revenue_including_cancelled(df))


def test_no_row_order_assumptions(spark):
    """Compare as multisets (exceptAll) or sorted collects, never collect() == collect()."""
    df = spark.createDataFrame([(i, "ann" if i % 2 else "bob", "paid", i) for i in range(1, 9)], ORDERS)
    shuffled = df.repartition(3, "amount")
    assert_same_rows(revenue_by_customer(df), revenue_by_customer(shuffled), "repartitioned")
    assert sorted(map(tuple, df.collect())) == sorted(map(tuple, shuffled.collect()))
    # exceptAll keeps duplicates: {a, a} vs {a} differ (subtract() would say they are equal)
    two = df.limit(1).unionByName(df.limit(1))
    assert two.subtract(df.limit(1)).count() == 0
    with pytest.raises(AssertionError, match="1 unexpected rows"):
        assert_same_rows(df.limit(1), two, "duplicates")


AMOUNTS = StructType([StructField("customer", StringType(), False),
                      StructField("amount", DoubleType(), False)])


def average_within_min_max(input_df, output_df):
    """avg(amount) per customer lies in [min, max]: compared with a tolerance, not exactly."""
    stats = input_df.groupBy("customer").agg(F.min("amount").alias("lo"), F.max("amount").alias("hi"))
    for row in output_df.join(stats, "customer").collect():
        tol = 1e-9 * max(1.0, abs(row.lo), abs(row.hi))
        assert row.lo - tol <= row.avg_amount <= row.hi + tol, f"avg out of range: {row.asDict()}"


def test_float_tolerance(spark):
    """Floating-point sums depend on order: use math.isclose / a tolerance."""
    df = spark.createDataFrame([("ann", 0.1), ("ann", 0.2), ("bob", 1e16), ("bob", 1.0)], AMOUNTS)
    ann = df.filter("customer = 'ann'").agg(F.sum("amount")).first()[0]
    assert ann != 0.3 and math.isclose(ann, 0.3, rel_tol=1e-9)
    report = (
        TransformationPropertyTester(
            spark, lambda d: d.groupBy("customer").agg(F.avg("amount").alias("avg_amount")), AMOUNTS,
            df_config=DataFrameConfig(min_rows=1, max_rows=6, column_configs={
                "customer": ColumnConfig(custom_strategy=st.sampled_from(["ann", "bob"])),
                "amount":   ColumnConfig(custom_strategy=st.floats(-1e12, 1e12, allow_nan=False)),
            }))
        .add_property(average_within_min_max)
        .run(max_examples=3)
    )
    report.assert_passed()
