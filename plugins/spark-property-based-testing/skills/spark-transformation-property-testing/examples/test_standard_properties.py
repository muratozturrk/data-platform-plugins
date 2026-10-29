"""
The standard property library (spark_prop_testing.transformations.standard_properties):
each property is a plain (input_df, output_df) -> None function, so it can be called directly on
fixed DataFrames (fast, exact) or registered on a TransformationPropertyTester (generated inputs).

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_standard_properties.py
"""

from decimal import Decimal

import pytest
from hypothesis import strategies as st
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from spark_prop_testing import (
    ColumnConfig, DataFrameConfig, TransformationPropertyTester, column_types_preserved,
    input_columns_preserved, make_idempotency_property, make_left_join_preserves_left_keys,
    make_sum_linearity_property, make_window_rank_property, no_new_nulls_introduced,
    row_count_exactly_preserved, row_count_non_decreasing, row_count_non_increasing,
)
from spark_prop_testing.transformations.standard_properties import (
    make_aggregation_monotonicity_property,
)

ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer", StringType(),  nullable=True),
    StructField("amount",   DoubleType(),  nullable=False),
])
ROWS = [(1, "ann", 10.0), (2, "bob", 0.5), (3, "ann", 7.25), (4, None, 3.0), (5, "cy", 1.0)]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("standard-properties")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def orders(spark):
    return spark.createDataFrame(ROWS, ORDERS)


def test_row_count_properties(orders):
    """row_count_*: filters never add rows, enrichments never lose rows, column maps keep the count."""
    filtered = orders.filter("amount > 1")
    row_count_non_increasing(orders, filtered)
    row_count_non_decreasing(orders, orders.unionByName(orders))
    row_count_exactly_preserved(orders, orders.withColumn("x", F.lit(1)))
    with pytest.raises(AssertionError, match=r"input=5, output=3"):
        row_count_exactly_preserved(orders, filtered)
    with pytest.raises(AssertionError, match="output lost 2 rows"):
        row_count_non_decreasing(orders, filtered)


def test_columns_nulls_and_types(orders):
    """Dropped columns, new nulls in a null-free column and full-type drift are all violations."""
    with pytest.raises(AssertionError, match=r"columns dropped by transformation: \['customer'\]"):
        input_columns_preserved(orders, orders.drop("customer"))

    # customer already had a null in the input, so more nulls there are allowed ...
    no_new_nulls_introduced(orders, orders.withColumn("customer", F.lit(None).cast("string")))
    # ... but amount had none
    with pytest.raises(AssertionError, match="column 'amount': input had 0 nulls, output has 1"):
        no_new_nulls_introduced(orders, orders.withColumn(
            "amount", F.when(F.col("order_id") == 1, None).otherwise(F.col("amount"))))

    with pytest.raises(AssertionError, match="type changed double → float"):
        column_types_preserved(orders, orders.withColumn("amount", F.col("amount").cast("float")))


def test_column_types_compare_the_full_type(spark):
    """decimal(10,2) → decimal(38,2) and array<int> → array<bigint> are type drift too."""
    schema = StructType([StructField("price", DecimalType(10, 2)),
                         StructField("tags", ArrayType(IntegerType()))])
    df = spark.createDataFrame([(Decimal("1.50"), [1, 2])], schema)
    with pytest.raises(AssertionError, match=r"decimal\(10,2\) → decimal\(38,2\)"):
        column_types_preserved(df, df.withColumn("price", F.col("price").cast("decimal(38,2)")))
    with pytest.raises(AssertionError, match="array<int> → array<bigint>"):
        column_types_preserved(df, df.withColumn("tags", F.col("tags").cast("array<bigint>")))


def test_sum_conservation_without_transform(orders):
    """Without transform: sum(output[col]) == sum(input[col]) within a float tolerance."""
    conservation = make_sum_linearity_property("amount")
    assert conservation.__name__ == "sum_conservation[amount]"
    per_customer = orders.groupBy("customer").agg(F.sum("amount").alias("amount"))
    conservation(orders, per_customer)                       # re-grouping keeps the total
    # 0.1 + 0.2 != 0.3 exactly; math.isclose with rel_tol=1e-9 accepts it
    make_sum_linearity_property("amount")(
        orders.limit(1).withColumn("amount", F.lit(0.3)),
        orders.limit(2).withColumn("amount", F.when(F.col("order_id") == 1, 0.1).otherwise(0.2)))
    with pytest.raises(AssertionError, match="sum conservation violated on 'amount'"):
        conservation(orders, orders.unionByName(orders.limit(1)))   # a duplicated row creates money


def test_sum_linearity_with_transform(orders):
    """With transform: sum(f(A ∪ B)) == sum(f(A)) + sum(f(B)) for a data-based split of the input."""
    def to_cents(df):                                        # row-wise: linear
        return df.withColumn("amount", F.col("amount") * 100)

    def share_of_total(df):                                  # depends on all rows: not linear
        total = df.agg(F.sum("amount")).first()[0]
        return df.withColumn("amount", F.col("amount") / F.lit(total))

    linear = make_sum_linearity_property("amount", transform=to_cents)
    assert linear.__name__ == "sum_linearity[amount]"
    linear(orders, to_cents(orders))
    with pytest.raises(AssertionError, match="sum_linearity violated on 'amount'"):
        make_sum_linearity_property("amount", transform=share_of_total)(orders, share_of_total(orders))


def test_aggregation_monotonicity(orders):
    """Adding a copy of an input row must not decrease sum/count/max of f's output (nor increase min)."""
    def total(df):
        return df.groupBy().agg(F.sum("amount").alias("amount"))

    def average(df):
        return df.groupBy().agg(F.avg("amount").alias("amount"))

    make_aggregation_monotonicity_property("amount", total, "sum")(orders, total(orders))
    make_aggregation_monotonicity_property("amount", lambda df: df, "min")(orders, orders)
    # an average is not monotone: duplicating the smallest row lowers it
    low_first = orders.orderBy("amount")                     # limit(1) picks the 0.5 row
    with pytest.raises(AssertionError, match=r"\[max\] violated"):
        make_aggregation_monotonicity_property("amount", average, "max")(low_first, average(low_first))
    with pytest.raises(ValueError, match="agg_fn must be"):
        make_aggregation_monotonicity_property("amount", total, "avg")


def test_idempotency(orders):
    """Unique keys in the output; with transform also f(f(x)) == f(x) as a multiset of rows."""
    dup = orders.unionByName(orders)

    def dedupe(df):                                          # highest amount per key, via a window
        w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    def dedupe_and_fee(df):                                  # adds a fee on every call
        return dedupe(df).withColumn("amount", F.col("amount") + 1)

    make_idempotency_property(["order_id"])(dup, dedupe(dup))
    make_idempotency_property(["order_id"], transform=dedupe)(dup, dedupe(dup))
    with pytest.raises(AssertionError, match="duplicate keys in"):
        make_idempotency_property(["order_id"])(dup, dup)
    with pytest.raises(AssertionError, match=r"f\(f\(x\)\) differs from f\(x\)"):
        make_idempotency_property(["order_id"], transform=dedupe_and_fee)(dup, dedupe_and_fee(dup))


def test_idempotency_with_subset_drop_duplicates(orders):
    """dropDuplicates(subset) is idempotent; the property compares row multisets on the driver."""
    def dedupe(df):
        return df.dropDuplicates(["order_id"])

    dup = orders.unionByName(orders)
    make_idempotency_property(["order_id"], transform=dedupe)(dup, dedupe(dup))
    with pytest.raises(AssertionError, match="duplicate keys in"):
        make_idempotency_property(["customer"], transform=dedupe)(dup, dedupe(dup))


def test_left_join_key_preservation(spark, orders):
    """Every non-null left key appears in the output; an inner join that drops a key is caught."""
    names = spark.createDataFrame([("ann", "Ann"), ("bob", "Bob")], "customer string, name string")
    keep = make_left_join_preserves_left_keys("customer")
    keep(orders, orders.join(names, "customer", "left"))
    # the inner join loses the null key AND 'cy'; only 'cy' counts (null keys never match)
    with pytest.raises(AssertionError, match=r"1 keys from left table missing in output. Sample missing: \['cy'\]"):
        keep(orders, orders.join(names, "customer", "inner"))
    names_cy = names.unionByName(spark.createDataFrame([("cy", "Cy")], names.schema))
    keep(orders, orders.join(names_cy, "customer", "inner"))  # only the null-key row is lost: passes


def test_window_rank(orders):
    """Ranks start at 1 per partition; dense=True also requires contiguous ranks (no gaps)."""
    tied = orders.withColumn("amount", F.floor("amount"))   # 0.5 and 1.0 → 0 and 1; ann: 10, 7
    w = Window.partitionBy("customer").orderBy(F.col("amount").desc())
    ranked = tied.unionByName(tied.limit(1)).withColumn("rnk", F.rank().over(w))   # duplicate → tie
    make_window_rank_property("customer", "rnk")(tied, ranked)      # rank(): 1, 1, 3 is valid
    with pytest.raises(AssertionError, match="partitions have gaps in 'rnk'"):
        make_window_rank_property("customer", "rnk", dense=True)(tied, ranked)
    numbered = ranked.withColumn("rnk", F.row_number().over(w))
    make_window_rank_property("customer", "rnk", dense=True)(tied, numbered)
    make_window_rank_property("customer", "missing_col")(tied, ranked)  # column absent → skipped


CUSTOMERS = ["ann", "bob", "cy"]
GEN = DataFrameConfig(min_rows=1, max_rows=5, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True),
    "customer": ColumnConfig(custom_strategy=st.sampled_from(CUSTOMERS)),
    "amount":   ColumnConfig(custom_strategy=st.floats(0, 1_000, allow_nan=False)),
})


def test_battery_on_generated_inputs(spark):
    """A left join to a unique dimension keeps rows, keys, columns, types and the amount total."""
    dim = spark.createDataFrame([(c, c.upper()) for c in CUSTOMERS[:2]], "customer string, name string")

    def enrich(df):
        return df.join(F.broadcast(dim), "customer", "left")

    report = (
        TransformationPropertyTester(spark, enrich, ORDERS, name="enrich", df_config=GEN)
        .add_property(row_count_exactly_preserved)
        .add_property(make_left_join_preserves_left_keys("customer"))
        .add_property(make_sum_linearity_property("amount"))
        .run(max_examples=2)
    )
    report.assert_passed()
    # cheap structural checks need no generated data
    sample = spark.createDataFrame(ROWS, ORDERS)
    input_columns_preserved(sample, enrich(sample))
    column_types_preserved(sample, enrich(sample))
