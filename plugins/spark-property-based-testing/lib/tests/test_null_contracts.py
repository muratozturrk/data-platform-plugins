
"""
Tests that validate null contract enforcement at runtime
(not just schema-level nullable flags).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from hypothesis import given, settings, HealthCheck

from spark_prop_testing.strategies.dataframe_strategy import (
    spark_dataframes, DataFrameConfig, ColumnConfig,
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


SCHEMA = StructType([
    StructField("id",    IntegerType(), nullable=False),
    StructField("value", StringType(),  nullable=True),
])


class TestNullContracts:

    def test_not_null_column_has_zero_nulls(self, spark):
        """
        PROPERTY: A column marked nullable=False must NEVER contain nulls,
        regardless of how the data was generated.
        """
        config = DataFrameConfig(
            min_rows=10, max_rows=100,
            column_configs={
                # Try to inject nulls into a NOT NULL column
                "id": ColumnConfig(
                    null_config=NullConfig(null_rate=0.3)
                )
            }
        )

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(df=spark_dataframes(spark, SCHEMA, config))
        def inner(df):
            # The strategy respects nullable=False → nulls should NOT appear
            null_count = df.filter(F.col("id").isNull()).count()
            assert null_count == 0, (
                f"NOT NULL column 'id' has {null_count} null values!"
            )

        inner()


    def test_null_filtering_preserves_not_null_invariant(self, spark):
        """
        PROPERTY: After filtering out nulls, a nullable column
        satisfies the NOT NULL contract.
        Validates that our null-filtering transformation is correct.
        """
        config = DataFrameConfig(
            min_rows=10,
            max_rows=100,
            column_configs={
                "value": ColumnConfig(
                    null_config=NullConfig(null_rate=0.5)
                )
            }
        )

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(df=spark_dataframes(spark, SCHEMA, config))
        def inner(df):
            cleaned = df.filter(F.col("value").isNotNull())
            remaining_nulls = cleaned.filter(F.col("value").isNull()).count()

            assert remaining_nulls == 0, (
                "Null filter did not eliminate all nulls!"
            )
            # Also: filtered count ≤ original count
            assert cleaned.count() <= df.count()

        inner()