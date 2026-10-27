
"""
Full end-to-end test using all four chapters together.
Tests an e-commerce orders pipeline with:
  - Schema contracts
  - Content contracts (GE)
  - Custom business-rule properties
  - Property-based generation over many inputs
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, TimestampType,
)
from hypothesis import given, settings, HealthCheck

from spark_prop_testing.data_quality_suite          import (
    DataQualityPropertySuite, QualitySuiteConfig, UnifiedQualityReport,
)
from spark_prop_testing.ge_integration.expectation_builder import (
    ExpectationSuiteBuilder, ColumnExpectationSpec,
)
from spark_prop_testing.strategies.dataframe_strategy import (
    DataFrameConfig, ColumnConfig,
)
from spark_prop_testing.strategies.null_strategy import NullConfig
from spark_prop_testing.strategies.distribution_strategy import (
    skewed_integers,
)
from hypothesis import strategies as st


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Schema ────────────────────────────────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",  IntegerType(),   nullable=False),
    StructField("user_id",   IntegerType(),   nullable=False),
    StructField("amount",    DoubleType(),    nullable=True),
    StructField("status",    StringType(),    nullable=False),
    StructField("created_at",TimestampType(), nullable=True),
])

# ── GE Suite ──────────────────────────────────────────────────────────────────
def build_orders_ge_suite():
    return (
        ExpectationSuiteBuilder("orders_quality_suite")
        .add_row_count(min_rows=1)
        .add_column(ColumnExpectationSpec(
            column="order_id",
            not_null=True,
            unique=True,
            min_value=1,
        ))
        .add_column(ColumnExpectationSpec(
            column="user_id",
            not_null=True,
            min_value=1,
        ))
        .add_column(ColumnExpectationSpec(
            column="amount",
            min_value=0.0,
            max_value=100_000.0,
        ))
        .add_column(ColumnExpectationSpec(
            column="status",
            not_null=True,
            value_set=["pending", "shipped", "delivered", "cancelled"],
        ))
        .build()
    )


# ── Custom business-rule properties ───────────────────────────────────────────
def orders_total_is_non_negative(df):
    """Business rule: total revenue of any batch must be >= 0."""
    total = df.agg(F.sum("amount")).collect()[0][0] or 0.0
    assert total >= 0.0, f"Total order amount is negative: {total}"


def cancelled_orders_have_no_revenue(df):
    """Business rule: cancelled orders should not contribute to revenue."""
    cancelled_revenue = (
        df.filter(F.col("status") == "cancelled")
          .agg(F.sum("amount"))
          .collect()[0][0] or 0.0
    )
    # In this system, cancelled orders must have amount=0 or NULL
    assert cancelled_revenue == 0.0, (
        f"Cancelled orders show revenue: {cancelled_revenue}"
    )


# ── DataFrame config with realistic distributions ────────────────────────────
def build_orders_df_config():
    return DataFrameConfig(
        min_rows=10,
        max_rows=200,
        column_configs={
            "order_id": ColumnConfig(
                custom_strategy=st.integers(min_value=1, max_value=999_999)
            ),
            "user_id": ColumnConfig(
                custom_strategy=skewed_integers(
                    min_value=1, max_value=1000,
                    skew_value=42, skew_probability=0.3,
                )
            ),
            "amount": ColumnConfig(
                custom_strategy=st.floats(
                    min_value=0.01, max_value=5000.0,
                    allow_nan=False, allow_infinity=False,
                ),
                null_config=NullConfig(null_rate=0.05),
            ),
            "status": ColumnConfig(
                custom_strategy=st.sampled_from(
                    ["pending", "shipped", "delivered", "cancelled"]
                )
            ),
        }
    )


class TestOrdersPipelineQuality:

    @pytest.fixture
    def quality_suite(self, spark):
        config = QualitySuiteConfig(
            suite_name="orders_pipeline",
            expected_schema=ORDER_SCHEMA,
            ge_suite=build_orders_ge_suite(),
            df_config=build_orders_df_config(),
            max_examples=15,
            custom_properties=[
                orders_total_is_non_negative,
            ],
        )
        return DataQualityPropertySuite(spark, config)


    def test_valid_orders_pass_all_checks(self, spark, quality_suite):
        """
        PROPERTY: DataFrames generated from the correct schema + distribution
        should always pass the full quality suite.
        """
        report = quality_suite.run_property_loop()

        print(report.summary())  # visible with pytest -s
        assert report.is_valid, (
            f"Quality suite failed:\n{report.summary()}"
        )


    def test_broken_pipeline_caught_by_ge(self, spark, quality_suite):
        """
        PROPERTY: A pipeline bug that introduces invalid status values
        should always be caught by the GE suite.
        """
        from spark_prop_testing.strategies.dataframe_strategy import spark_dataframes

        strategy = spark_dataframes(spark, ORDER_SCHEMA, build_orders_df_config())

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=10)
        @given(df=strategy)
        def inner(df):
            # Simulate pipeline bug: introduces an unknown status value
            broken_df = df.withColumn(
                "status",
                F.when(F.col("order_id") % 5 == 0, F.lit("unknown_status"))
                 .otherwise(F.col("status"))
            )

            ge_validator = quality_suite._ge_validator
            ge_result    = ge_validator.validate(broken_df, build_orders_ge_suite())

            # The GE suite MUST catch the invalid status
            assert not ge_result.success, (
                "GE suite should catch invalid 'unknown_status' value "
                "but it passed!"
            )

        inner()


    def test_amount_range_violation_caught(self, spark, quality_suite):
        """
        PROPERTY: Negative amounts (pipeline transformation bug)
        should be caught by GE's value range expectation.
        """
        from spark_prop_testing.strategies.dataframe_strategy import spark_dataframes

        strategy = spark_dataframes(spark, ORDER_SCHEMA, build_orders_df_config())

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=10)
        @given(df=strategy)
        def inner(df):
            # Introduce negative amounts (e.g. buggy refund logic)
            broken_df = df.withColumn(
                "amount",
                F.when(F.col("order_id") % 3 == 0, F.lit(-99.99))
                 .otherwise(F.col("amount"))
            )

            ge_validator = quality_suite._ge_validator
            ge_result    = ge_validator.validate(broken_df, build_orders_ge_suite())

            assert not ge_result.success, (
                "GE suite should catch negative amounts but passed!"
            )

        inner()


    def test_unified_report_structure(self, spark, quality_suite):
        """
        PROPERTY: UnifiedQualityReport always has all three layers populated
        when schema, GE suite, and custom properties are all configured.
        """
        from spark_prop_testing.strategies.dataframe_strategy import spark_dataframes

        strategy = spark_dataframes(spark, ORDER_SCHEMA, build_orders_df_config())

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=10)
        @given(df=strategy)
        def inner(df):
            report = quality_suite.run(df)

            # All three layers must produce output
            assert len(report.schema_results) > 0, \
                "Schema validation layer produced no output"
            assert len(report.ge_results) > 0, \
                "GE validation layer produced no output"
            # (custom_properties are only in failures if they fail)

        inner()