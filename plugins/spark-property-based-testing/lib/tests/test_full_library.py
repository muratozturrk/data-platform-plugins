# tests/test_full_library.py

"""
Full integration test using the complete public API.
Demonstrates how a new user would use the library from scratch.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType,
)
from hypothesis import strategies as st

import spark_prop_testing as spt


TRANSACTION_SCHEMA = StructType([
    StructField("txn_id",   IntegerType(), nullable=False),
    StructField("account",  StringType(),  nullable=False),
    StructField("amount",   DoubleType(),  nullable=True),
    StructField("category", StringType(),  nullable=True),
])


def enrich_transactions(df):
    """Adds a 'is_large' flag for amounts > 1000."""
    return df.withColumn(
        "is_large",
        F.when(F.col("amount") > 1000, True).otherwise(False),
    )


class TestCompleteLibraryAPI:

    def test_using_decorator_api(self, spark):
        """Demonstrates the @spark_property decorator."""

        @spt.spark_property(
            schema=TRANSACTION_SCHEMA,
            config=spt.DataFrameConfig(
                min_rows=5, max_rows=50,
                column_configs={
                    "amount": spt.ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=0.0, max_value=5000.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    )
                }
            ),
            max_examples=15,
            spark=spark,
        )
        def check_enrichment_preserves_rows(df):
            result = enrich_transactions(df)
            assert result.count() == df.count(), (
                "Enrichment changed row count"
            )
            assert "is_large" in result.columns, (
                "is_large column missing from enriched output"
            )
            # is_large should only be True when amount > 1000
            violations = result.filter(
                (F.col("is_large") == True) &
                (F.col("amount") <= 1000)
            ).count()
            assert violations == 0

        check_enrichment_preserves_rows()


    def test_using_tester_api(self, spark):
        """Demonstrates the TransformationPropertyTester API."""
        report = (
            spt.TransformationPropertyTester(
                spark=spark,
                transform=enrich_transactions,
                input_schema=TRANSACTION_SCHEMA,
                name="enrich_transactions",
                df_config=spt.DataFrameConfig(
                    min_rows=5, max_rows=60,
                    column_configs={
                        "amount": spt.ColumnConfig(
                            custom_strategy=st.floats(
                                min_value=0.0, max_value=5000.0,
                                allow_nan=False, allow_infinity=False,
                            ),
                            null_config=spt.NullConfig(null_rate=0.1),
                        ),
                        "category": spt.ColumnConfig(
                            custom_strategy=st.sampled_from([
                                "food", "travel", "tech", "health"
                            ])
                        ),
                    }
                ),
            )
            .add_property(spt.row_count_exactly_preserved)
            .add_property(spt.input_columns_preserved)
            .add_property(spt.column_types_preserved)
            .run(max_examples=20)
        )

        print(report.summary())
        report.assert_passed()


    def test_using_quality_suite_api(self, spark):
        """Demonstrates the DataQualityPropertySuite API."""

        ge_suite = (
            spt.ExpectationSuiteBuilder("txn_suite")
            .add_row_count(min_rows=1)
            .add_column(spt.ColumnExpectationSpec(
                column="txn_id",
                not_null=True,
                unique=True,
                min_value=1,
            ))
            .add_column(spt.ColumnExpectationSpec(
                column="amount",
                min_value=0.0,
                max_value=5000.0,
            ))
            .build()
        )

        config = spt.QualitySuiteConfig(
            suite_name="transactions",
            expected_schema=TRANSACTION_SCHEMA,
            ge_suite=ge_suite,
            df_config=spt.DataFrameConfig(
                min_rows=10, max_rows=100,
                column_configs={
                    # the GE suite requires txn_id >= 1 and unique
                    "txn_id": spt.ColumnConfig(
                        custom_strategy=st.integers(min_value=1, max_value=2**31 - 1),
                        unique=True,
                    ),
                    "amount": spt.ColumnConfig(
                        custom_strategy=st.floats(
                            min_value=0.01, max_value=4999.0,
                            allow_nan=False, allow_infinity=False,
                        )
                    ),
                    "category": spt.ColumnConfig(
                        custom_strategy=st.sampled_from(
                            ["food", "travel", "tech", "health"]
                        )
                    ),
                }
            ),
            max_examples=15,
        )

        suite  = spt.DataQualityPropertySuite(spark, config)
        report = suite.run_property_loop()

        print(report.summary())
        assert report.is_valid, report.summary()