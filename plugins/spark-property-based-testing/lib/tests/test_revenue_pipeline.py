
"""
Tests for a realistic revenue calculation pipeline.
The pipeline: raw orders → cleaned → aggregated → ranked

We test each stage with appropriate property invariants.
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, TimestampType,
)
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from spark_prop_testing.transformations.base import TransformationPropertyTester
from spark_prop_testing.transformations.standard_properties import (
    row_count_non_increasing,
    row_count_exactly_preserved,
    row_count_non_decreasing,
    input_columns_preserved,
    no_new_nulls_introduced,
    column_types_preserved,
    output_is_non_empty_when_input_is_non_empty,
    make_sum_linearity_property,
    make_idempotency_property,
    make_window_rank_property,
    make_left_join_preserves_left_keys,
)
from spark_prop_testing.strategies.dataframe_strategy import (
    spark_dataframes, DataFrameConfig, ColumnConfig,
)
from spark_prop_testing.strategies.distribution_strategy import (
    skewed_integers,
)
from spark_prop_testing.strategies.null_strategy import NullConfig


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Schemas ───────────────────────────────────────────────────────────────────
RAW_ORDER_SCHEMA = StructType([
    StructField("order_id",   IntegerType(),  nullable=False),
    StructField("user_id",    IntegerType(),  nullable=False),
    StructField("amount",     DoubleType(),   nullable=True),
    StructField("status",     StringType(),   nullable=True),
    StructField("created_at", TimestampType(),nullable=True),
])


# ══════════════════════════════════════════════════════════════
# STAGE 1: Data Cleaning Transformation
# ══════════════════════════════════════════════════════════════

def clean_orders(df: DataFrame) -> DataFrame:
    """
    Stage 1: Clean raw orders.
    - Remove cancelled orders
    - Fill null amounts with 0.0
    - Normalize status to lowercase
    """
    return (
        df
        .filter(F.col("status") != "cancelled")
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("status", F.lower(F.col("status")))
    )


class TestCleanOrdersProperties:

    def test_cleaning_never_adds_rows(self, spark):
        """Filtering cannot increase row count."""
        tester = (
            TransformationPropertyTester(
                spark=spark,
                transform=clean_orders,
                input_schema=RAW_ORDER_SCHEMA,
                name="clean_orders",
                df_config=DataFrameConfig(
                    min_rows=1, max_rows=100,
                    column_configs={
                        "status": ColumnConfig(
                            custom_strategy=st.sampled_from([
                                "pending", "shipped",
                                "delivered", "cancelled", None,
                            ])
                        ),
                        "amount": ColumnConfig(
                            null_config=NullConfig(null_rate=0.2)
                        ),
                    }
                ),
            )
            .add_property(row_count_non_increasing)
            .add_property(input_columns_preserved)
            .add_property(column_types_preserved)
        )

        report = tester.run(max_examples=20)
        report.assert_passed()
        print(report.summary())


    def test_cleaning_eliminates_nulls_in_amount(self, spark):
        """After cleaning, 'amount' must have zero nulls."""

        def amount_has_no_nulls(input_df: DataFrame, output_df: DataFrame):
            null_amounts = output_df.filter(
                F.col("amount").isNull()
            ).count()
            assert null_amounts == 0, (
                f"clean_orders left {null_amounts} null amounts"
            )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=clean_orders,
            input_schema=RAW_ORDER_SCHEMA,
            name="clean_orders_null_check",
            df_config=DataFrameConfig(
                min_rows=5, max_rows=50,
                column_configs={
                    "amount": ColumnConfig(
                        null_config=NullConfig(null_rate=0.5)
                    ),
                    "status": ColumnConfig(
                        custom_strategy=st.sampled_from([
                            "pending", "shipped", "delivered"
                        ])
                    ),
                }
            ),
        ).add_property(amount_has_no_nulls)

        report = tester.run(max_examples=20)
        report.assert_passed()


    def test_cleaning_only_removes_cancelled(self, spark):
        """
        METAMORPHIC: An input with ZERO cancelled orders
        must produce output with SAME row count.
        """

        def non_cancelled_input_unchanged(
            input_df: DataFrame,
            output_df: DataFrame,
        ):
            cancelled_count = input_df.filter(
                F.col("status") == "cancelled"
            ).count()

            if cancelled_count == 0:
                assert output_df.count() == input_df.count(), (
                    f"clean_orders removed rows despite no cancelled orders. "
                    f"Input: {input_df.count()}, Output: {output_df.count()}"
                )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=clean_orders,
            input_schema=RAW_ORDER_SCHEMA,
            name="clean_orders_metamorphic",
            df_config=DataFrameConfig(
                min_rows=1, max_rows=50,
                column_configs={
                    "status": ColumnConfig(
                        # Intentionally NO "cancelled" here
                        custom_strategy=st.sampled_from([
                            "pending", "shipped", "delivered"
                        ])
                    ),
                }
            ),
        ).add_property(non_cancelled_input_unchanged)

        report = tester.run(max_examples=20)
        report.assert_passed()


# ══════════════════════════════════════════════════════════════
# STAGE 2: Revenue Aggregation Transformation
# ══════════════════════════════════════════════════════════════

def aggregate_revenue_by_user(df: DataFrame) -> DataFrame:
    """
    Stage 2: Aggregate cleaned orders to user-level revenue.
    Output schema: user_id, total_revenue, order_count, avg_order_value
    """
    return (
        df
        .groupBy("user_id")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.avg("amount").alias("avg_order_value"),
        )
    )


class TestRevenueAggregationProperties:

    def test_aggregation_reduces_rows(self, spark):
        """Aggregation by user_id: output rows ≤ input rows."""
        tester = TransformationPropertyTester(
            spark=spark,
            transform=aggregate_revenue_by_user,
            input_schema=RAW_ORDER_SCHEMA,
            name="aggregate_revenue",
            df_config=DataFrameConfig(
                min_rows=5, max_rows=100,
                column_configs={
                    "user_id": ColumnConfig(
                        # Few unique users → real aggregation happens
                        custom_strategy=skewed_integers(
                            min_value=1, max_value=10
                        )
                    ),
                    "amount": ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=0.01, max_value=1000.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    ),
                }
            ),
        ).add_property(row_count_non_increasing)

        report = tester.run(max_examples=20)
        report.assert_passed()


    def test_total_revenue_equals_sum_of_inputs(self, spark):
        """
        INVARIANT: Sum of total_revenue in output == sum of amount in input.
        This is a critical financial correctness property.
        """

        def revenue_conservation(input_df: DataFrame, output_df: DataFrame):
            input_sum  = (
                input_df.agg(F.sum("amount")).collect()[0][0] or 0.0
            )
            output_sum = (
                output_df.agg(F.sum("total_revenue")).collect()[0][0] or 0.0
            )
            assert abs(input_sum - output_sum) < 1e-6, (
                f"Revenue not conserved: "
                f"input_sum={input_sum:.4f}, "
                f"output_sum={output_sum:.4f}, "
                f"delta={abs(input_sum-output_sum):.8f}"
            )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=aggregate_revenue_by_user,
            input_schema=RAW_ORDER_SCHEMA,
            name="revenue_conservation",
            df_config=DataFrameConfig(
                min_rows=1, max_rows=100,
                column_configs={
                    "user_id": ColumnConfig(
                        custom_strategy=st.integers(min_value=1, max_value=20)
                    ),
                    "amount": ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=0.0, max_value=100.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    ),
                }
            ),
        ).add_property(revenue_conservation)

        report = tester.run(max_examples=25)
        report.assert_passed()


    def test_order_count_equals_input_rows(self, spark):
        """
        INVARIANT: Sum of order_count in output == total input rows.
        Catches bugs where COUNT(*) is accidentally replaced with COUNT(col).
        """

        def order_count_conservation(input_df: DataFrame, output_df: DataFrame):
            input_rows   = input_df.count()
            summed_count = (
                output_df.agg(F.sum("order_count")).collect()[0][0] or 0
            )
            assert summed_count == input_rows, (
                f"order_count not conserved: "
                f"input rows={input_rows}, "
                f"sum(order_count)={summed_count}"
            )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=aggregate_revenue_by_user,
            input_schema=RAW_ORDER_SCHEMA,
            name="order_count_conservation",
            df_config=DataFrameConfig(
                min_rows=5, max_rows=80,
                column_configs={
                    "user_id": ColumnConfig(
                        custom_strategy=st.integers(min_value=1, max_value=10)
                    ),
                }
            ),
        ).add_property(order_count_conservation)

        report = tester.run(max_examples=20)
        report.assert_passed()


    def test_avg_order_value_bounded_by_min_max(self, spark):
        """
        INVARIANT: avg_order_value must be between
        the global min and max of amount.
        """

        def avg_within_bounds(input_df: DataFrame, output_df: DataFrame):
            stats      = input_df.agg(
                F.min("amount").alias("min_amt"),
                F.max("amount").alias("max_amt"),
            ).collect()[0]

            global_min = stats["min_amt"]
            global_max = stats["max_amt"]

            if global_min is None or global_max is None:
                return

            violations = output_df.filter(
                (F.col("avg_order_value") < global_min - 1e-9) |
                (F.col("avg_order_value") > global_max + 1e-9)
            ).count()

            assert violations == 0, (
                f"avg_order_value out of bounds [{global_min}, {global_max}]: "
                f"{violations} violations"
            )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=aggregate_revenue_by_user,
            input_schema=RAW_ORDER_SCHEMA,
            name="avg_bounded",
            df_config=DataFrameConfig(
                min_rows=5, max_rows=80,
                column_configs={
                    "amount": ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=1.0, max_value=500.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    ),
                    "user_id": ColumnConfig(
                        custom_strategy=st.integers(min_value=1, max_value=15)
                    ),
                }
            ),
        ).add_property(avg_within_bounds)

        report = tester.run(max_examples=20)
        report.assert_passed()


# ══════════════════════════════════════════════════════════════
# STAGE 3: Window Ranking Transformation
# ══════════════════════════════════════════════════════════════

def rank_users_by_revenue(df: DataFrame) -> DataFrame:
    """
    Stage 3: Rank users by total_revenue within each region.
    Adds: revenue_rank (dense_rank descending within region).
    """
    AGGREGATED_SCHEMA = StructType([
        StructField("user_id",        IntegerType(), nullable=False),
        StructField("region",         StringType(),  nullable=False),
        StructField("total_revenue",  DoubleType(),  nullable=True),
    ])

    window = Window.partitionBy("region").orderBy(
        F.col("total_revenue").desc()
    )
    return df.withColumn("revenue_rank", F.dense_rank().over(window))


AGGREGATED_SCHEMA = StructType([
    StructField("user_id",       IntegerType(), nullable=False),
    StructField("region",        StringType(),  nullable=False),
    StructField("total_revenue", DoubleType(),  nullable=True),
])


class TestWindowRankingProperties:

    def test_window_rank_properties(self, spark):
        """
        Tests all window rank invariants:
        - Exact row count preserved (withColumn)
        - All input columns still present
        - Ranks are valid (≥1, start from 1 per partition)
        """
        tester = (
            TransformationPropertyTester(
                spark=spark,
                transform=rank_users_by_revenue,
                input_schema=AGGREGATED_SCHEMA,
                name="rank_users_by_revenue",
                df_config=DataFrameConfig(
                    min_rows=5, max_rows=80,
                    column_configs={
                        "region": ColumnConfig(
                            # Few regions → multiple users per region
                            custom_strategy=st.sampled_from(
                                ["EMEA", "APAC", "AMER", "LATAM"]
                            )
                        ),
                        "total_revenue": ColumnConfig(
                            custom_strategy=st.floats(
                                min_value=0.0, max_value=100_000.0,
                                allow_nan=False, allow_infinity=False,
                            )
                        ),
                    }
                ),
            )
            .add_property(row_count_exactly_preserved)
            .add_property(input_columns_preserved)
            .add_property(
                make_window_rank_property(
                    partition_col="region",
                    rank_col="revenue_rank",
                )
            )
        )

        report = tester.run(max_examples=20)
        report.assert_passed()
        print(report.summary())


    def test_top_ranked_user_has_highest_revenue(self, spark):
        """
        INVARIANT: Within each region, rank=1 user must have
        the highest (or tied-highest) total_revenue.
        """

        def top_rank_is_highest_revenue(
            input_df: DataFrame,
            output_df: DataFrame,
        ):
            # For each region, compare rank=1 revenue vs max revenue
            rank1 = (
                output_df
                .filter(F.col("revenue_rank") == 1)
                .groupBy("region")
                .agg(F.min("total_revenue").alias("rank1_min_revenue"))
            )
            max_rev = (
                output_df
                .groupBy("region")
                .agg(F.max("total_revenue").alias("max_revenue"))
            )

            violations = (
                rank1.join(max_rev, on="region")
                .filter(
                    F.col("rank1_min_revenue") < F.col("max_revenue") - 1e-9
                )
                .count()
            )
            assert violations == 0, (
                f"top_rank_is_highest_revenue: {violations} regions where "
                f"rank=1 user doesn't have the highest revenue"
            )

        tester = TransformationPropertyTester(
            spark=spark,
            transform=rank_users_by_revenue,
            input_schema=AGGREGATED_SCHEMA,
            name="top_rank_revenue_check",
            df_config=DataFrameConfig(
                min_rows=4, max_rows=60,
                column_configs={
                    "region": ColumnConfig(
                        custom_strategy=st.sampled_from(["EMEA", "APAC", "AMER"])
                    ),
                    "total_revenue": ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=0.0, max_value=10_000.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    ),
                }
            ),
        ).add_property(top_rank_is_highest_revenue)

        report = tester.run(max_examples=20)
        report.assert_passed()