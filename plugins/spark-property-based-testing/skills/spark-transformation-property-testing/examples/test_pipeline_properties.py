"""
Pipeline-level properties: join fan-out detection, composing stage properties into an
end-to-end check, determinism under repartitioning, and exact money conservation with decimals.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_pipeline_properties.py
"""

from decimal import Decimal

import pytest
from hypothesis import Phase
from hypothesis import strategies as st
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, DataFrameConfig, TransformationPropertyTester, make_idempotency_property,
    make_sum_linearity_property, row_count_exactly_preserved, row_count_non_increasing,
)

MONEY = DecimalType(12, 2)
ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer", StringType(),  nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   MONEY,         nullable=False),
])
CUSTOMERS = ["ann", "bob", "cy"]
GEN = DataFrameConfig(min_rows=1, max_rows=6, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 50)),          # duplicates allowed: re-deliveries
    "customer": ColumnConfig(custom_strategy=st.sampled_from(CUSTOMERS)),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["paid", "cancelled"])),
    "amount":   ColumnConfig(custom_strategy=st.decimals(0, 10_000, places=2)),
})


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-properties")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Known-failing demos skip shrinking (each shrink step is another Spark run).
NO_SHRINK = (Phase.explicit, Phase.generate)


def assert_same_rows(expected: DataFrame, actual: DataFrame, label: str) -> None:
    cols = expected.columns
    extra = actual.select(*cols).exceptAll(expected.select(*cols)).count()
    missing = expected.select(*cols).exceptAll(actual.select(*cols)).count()
    assert extra == 0 and missing == 0, f"{label}: {extra} unexpected, {missing} missing rows"


# ── join fan-out ────────────────────────────────────────────────────────────

def test_join_fan_out_is_detected(spark):
    """A dimension with a duplicate key multiplies rows and money; count + sum properties catch it."""
    dim = spark.createDataFrame([("ann", "EU"), ("ann", "US"), ("bob", "EU")], "customer string, region string")

    def enrich(df):
        return df.join(dim, "customer", "left")

    nonzero = DataFrameConfig(min_rows=1, max_rows=6, column_configs={
        **GEN.column_configs, "amount": ColumnConfig(custom_strategy=st.decimals(1, 10_000, places=2))})
    report = (
        TransformationPropertyTester(spark, enrich, ORDERS, df_config=nonzero)
        .add_property(row_count_exactly_preserved)
        .add_property(make_sum_linearity_property("amount"))           # conservation of money
        .run(max_examples=2, phases=NO_SHRINK)
    )
    assert not report.passed
    count, money = report.results
    assert count.error_message.startswith("row_count_exactly_preserved violated: input=1, output=2")
    assert money.property_name == "sum_conservation[amount]" and not money.passed
    # the cheap guard: the join key must be unique on the dimension side
    with pytest.raises(AssertionError, match=r"duplicate keys in \['customer'\]"):
        make_idempotency_property(["customer"])(dim, dim)


# ── a three-stage pipeline ──────────────────────────────────────────────────

def dedupe(df):
    """One delivery per order_id; a total order (amount desc, status, customer) → deterministic."""
    w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "status", "customer")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def paid_only(df):
    return df.filter(F.col("status") == "paid")


def revenue(df):
    return df.groupBy("customer").agg(F.sum("amount").alias("amount"))


def pipeline(df):
    return revenue(paid_only(dedupe(df)))


def model(rows):
    """Plain-Python oracle of the pipeline over collected rows."""
    best = {}
    for r in sorted(rows, key=lambda r: (-r.amount, r.status, r.customer)):
        best.setdefault(r.order_id, r)
    out = {}
    for r in best.values():
        if r.status == "paid":
            out[r.customer] = out.get(r.customer, Decimal("0")) + r.amount
    return out


def test_stage_properties(spark):
    """Each stage keeps its own contract: dedupe → unique keys, filter → no new rows."""
    dedupe_report = (TransformationPropertyTester(spark, dedupe, ORDERS, df_config=GEN)
                     .add_property(make_idempotency_property(["order_id"], transform=dedupe))
                     .run(max_examples=2))
    filter_report = (TransformationPropertyTester(spark, paid_only, ORDERS, df_config=GEN)
                     .add_property(row_count_non_increasing).run(max_examples=2))
    dedupe_report.assert_passed()
    filter_report.assert_passed()


def test_end_to_end_against_a_model(spark):
    """pipeline(df) equals the plain-Python model on the same rows (differential property)."""
    def matches_model(input_df, output_df):
        got = {r.customer: r.amount for r in output_df.collect()}
        want = model(input_df.collect())
        assert got == want, f"pipeline {got} != model {want}"

    (TransformationPropertyTester(spark, pipeline, ORDERS, df_config=GEN)
     .add_property(matches_model).run(max_examples=3).assert_passed())


def test_pipeline_is_deterministic(spark):
    """pipeline(df) == pipeline(df.repartition(n)) == a second run: no partition-dependent results."""
    def deterministic(input_df, output_df):
        assert_same_rows(output_df, pipeline(input_df.repartition(3)), "repartition(3)")
        assert_same_rows(output_df, pipeline(input_df.orderBy(F.rand(7))), "reordered")

    (TransformationPropertyTester(spark, pipeline, ORDERS, df_config=GEN)
     .add_property(deterministic).run(max_examples=2).assert_passed())


# ── money conservation ──────────────────────────────────────────────────────

def split_naive(df, n=3):
    """Bug: every installment is round(amount / n, 2), cents get lost or created."""
    return (df.withColumn("i", F.explode(F.sequence(F.lit(1), F.lit(n))))
              .withColumn("installment", F.round(F.col("amount") / n, 2).cast(MONEY)))


def split_exact(df, n=3):
    """Floor to the cent; the last installment takes the remainder."""
    base = F.floor(F.col("amount") * 100 / n) / 100
    return (df.withColumn("i", F.explode(F.sequence(F.lit(1), F.lit(n))))
              .withColumn("installment", F.when(F.col("i") < n, base)
                          .otherwise(F.col("amount") - base * (n - 1)).cast(MONEY)))


def installments_sum_to_amount(input_df, output_df):
    """Exact (Decimal) money conservation per order: no tolerance for cents."""
    per_order = output_df.groupBy("order_id", "amount").agg(F.sum("installment").alias("paid"))
    bad = per_order.filter(F.col("paid") != F.col("amount"))
    assert bad.count() == 0, f"installments do not add up: {bad.limit(3).collect()}"


def test_money_is_conserved_exactly(spark):
    """For any order amount, the installments of split_exact add up to the cent."""
    unique_orders = DataFrameConfig(min_rows=1, max_rows=6, column_configs={
        **GEN.column_configs,
        "order_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True)})
    (TransformationPropertyTester(spark, split_exact, ORDERS, df_config=unique_orders)
     .add_property(installments_sum_to_amount).run(max_examples=3).assert_passed())
    assert split_exact(spark.createDataFrame([(1, "ann", "paid", Decimal("100.00"))], ORDERS)) \
        .agg(F.sum("installment")).first()[0] == Decimal("100.00")


def test_naive_rounding_loses_a_cent(spark):
    """split_naive(100.00) pays 3 × 33.33 = 99.99: the property names the order."""
    df = spark.createDataFrame([(1, "ann", "paid", Decimal("100.00"))], ORDERS)
    with pytest.raises(AssertionError, match=r"paid=Decimal\('99.99'\)"):
        installments_sum_to_amount(df, split_naive(df))
