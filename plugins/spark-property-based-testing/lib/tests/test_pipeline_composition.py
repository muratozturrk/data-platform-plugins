
"""
Tests for end-to-end pipeline composition properties.
These test the PIPELINE as a whole, not individual stages.
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from spark_prop_testing.strategies.dataframe_strategy import spark_dataframes, DataFrameConfig, ColumnConfig
from spark_prop_testing.strategies.null_strategy import NullConfig


RAW_ORDER_SCHEMA = StructType([
    StructField("order_id",   IntegerType(),  nullable=False),
    StructField("user_id",    IntegerType(),  nullable=False),
    StructField("amount",     DoubleType(),   nullable=True),
    StructField("status",     StringType(),   nullable=True),
    StructField("created_at", TimestampType(),nullable=True),
])


def full_pipeline(df: DataFrame) -> DataFrame:
    """The complete 3-stage pipeline."""
    from test_revenue_pipeline import (
        clean_orders, aggregate_revenue_by_user
    )
    return aggregate_revenue_by_user(clean_orders(df))


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


class TestPipelineCompositionProperties:

    def test_pipeline_is_deterministic(self, spark):
        """
        INVARIANT: Running the pipeline twice on the same input
        must produce identical results.
        Catches non-determinism from random sampling or unstable sorts.
        """
        config = DataFrameConfig(
            min_rows=10, max_rows=80,
            column_configs={
                "user_id": ColumnConfig(
                    custom_strategy=st.integers(min_value=1, max_value=20)
                ),
                "status": ColumnConfig(
                    custom_strategy=st.sampled_from(
                        ["pending", "shipped", "delivered", "cancelled"]
                    )
                ),
                "amount": ColumnConfig(
                    custom_strategy=st.floats(
                        min_value=0.01, max_value=500.0,
                        allow_nan=False, allow_infinity=False,
                    )
                ),
            }
        )
        strategy = spark_dataframes(spark, RAW_ORDER_SCHEMA, config)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
        @given(df=strategy)
        def inner(df):
            result_1 = full_pipeline(df)
            result_2 = full_pipeline(df)

            # Compare sorted results by user_id
            rows_1 = sorted(
                result_1.collect(), key=lambda r: r["user_id"]
            )
            rows_2 = sorted(
                result_2.collect(), key=lambda r: r["user_id"]
            )

            assert rows_1 == rows_2, (
                "Pipeline is non-deterministic: "
                "two runs on same input produced different results"
            )

        inner()


    def test_pipeline_revenue_never_exceeds_input(self, spark):
        """
        INVARIANT: Total output revenue ≤ total input amount.
        (Cancellations can only remove revenue, never add it.)
        """
        config = DataFrameConfig(
            min_rows=5, max_rows=100,
            column_configs={
                "user_id": ColumnConfig(
                    custom_strategy=st.integers(min_value=1, max_value=30)
                ),
                "status": ColumnConfig(
                    custom_strategy=st.sampled_from(
                        ["pending", "shipped", "delivered", "cancelled"]
                    )
                ),
                "amount": ColumnConfig(
                    custom_strategy=st.floats(
                        min_value=0.0, max_value=1000.0,
                        allow_nan=False, allow_infinity=False,
                    ),
                    null_config=NullConfig(null_rate=0.1),
                ),
            }
        )

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(df=spark_dataframes(spark, RAW_ORDER_SCHEMA, config))
        def inner(df):
            input_total  = df.agg(
                F.sum("amount")
            ).collect()[0][0] or 0.0

            output_total = full_pipeline(df).agg(
                F.sum("total_revenue")
            ).collect()[0][0] or 0.0

            assert output_total <= input_total + 1e-6, (
                f"Pipeline revenue exceeds input: "
                f"input={input_total:.2f}, output={output_total:.2f}"
            )

        inner()