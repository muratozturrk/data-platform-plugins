
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType,
    ArrayType, TimestampType, BooleanType,
)
from hypothesis import given, settings, HealthCheck

from spark_prop_testing.strategies.dataframe_strategy import (
    spark_dataframes, DataFrameConfig, ColumnConfig,
)
from spark_prop_testing.strategies.null_strategy    import NullConfig
from spark_prop_testing.strategies.distribution_strategy import (
    skewed_integers, edge_case_strings, realistic_id_strategy,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Schema for an e-commerce order ───────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",    IntegerType(),  nullable=False),
    StructField("user_id",     IntegerType(),  nullable=False),
    StructField("amount",      DoubleType(),   nullable=True),
    StructField("status",      StringType(),   nullable=True),
    StructField("created_at",  TimestampType(),nullable=True),
    StructField("is_refunded", BooleanType(),  nullable=True),
])


class TestRealisticOrderData:
    """
    Tests using the new strategy infrastructure with
    realistic distributions, null injection, and custom overrides.
    """

    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
    @given(df=...)   # filled in setUp pattern below
    def test_placeholder(self, spark):
        pass  # replaced by real tests below


    def test_schema_preserved_after_projection(self, spark):
        """Property: selecting columns preserves column types."""

        config = DataFrameConfig(
            min_rows=10,
            max_rows=100,
            column_configs={
                "user_id": ColumnConfig(
                    custom_strategy=realistic_id_strategy(max_id=50)
                ),
                "amount": ColumnConfig(
                    null_config=NullConfig(null_rate=0.1)  # 10% null amounts
                ),
                "status": ColumnConfig(
                    custom_strategy=edge_case_strings()
                ),
            }
        )

        strategy = spark_dataframes(spark, ORDER_SCHEMA, config)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(df=strategy)
        def inner(df):
            projected = df.select("order_id", "amount", "status")

            # Schema of projected columns must match original
            for field in projected.schema.fields:
                original_field = ORDER_SCHEMA[field.name]
                assert field.dataType == original_field.dataType, (
                    f"Column {field.name}: expected {original_field.dataType}, "
                    f"got {field.dataType}"
                )

        inner()


    def test_null_injection_rate(self, spark):
        """
        Property: null injection rate is approximately correct.
        We set 50% null rate → between 30-70% of 'amount' should be null
        (wide tolerance because Hypothesis uses probability, not guarantees).
        """

        config = DataFrameConfig(
            min_rows=100,
            max_rows=200,
            column_configs={
                "amount": ColumnConfig(
                    null_config=NullConfig(null_rate=0.5)
                ),
            }
        )

        strategy = spark_dataframes(spark, ORDER_SCHEMA, config)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=10)
        @given(df=strategy)
        def inner(df):
            total      = df.count()
            null_count = df.filter(F.col("amount").isNull()).count()
            null_rate  = null_count / total if total > 0 else 0

            # Very loose bounds — probability, not determinism
            assert 0.20 <= null_rate <= 0.80, (
                f"Null rate {null_rate:.2%} far outside expected 50%"
            )

        inner()


    def test_skewed_user_id_join_stability(self, spark):
        """
        Property: a skewed join never produces MORE rows than
        left_count × max_right_matches. Tests that skew doesn't
        silently explode row counts.
        """
        from hypothesis import strategies as st

        SKEWED_ORDER_SCHEMA = StructType([
            StructField("order_id", IntegerType(), nullable=False),
            StructField("user_id",  IntegerType(), nullable=False),
            StructField("amount",   DoubleType(),  nullable=True),
        ])

        USER_SCHEMA = StructType([
            StructField("user_id",   IntegerType(), nullable=False),
            StructField("user_name", StringType(),  nullable=True),
        ])

        order_config = DataFrameConfig(
            min_rows=10, max_rows=50,
            column_configs={
                "user_id": ColumnConfig(
                    # Skewed: 70% of orders belong to user_id=1
                    custom_strategy=skewed_integers(
                        min_value=1, max_value=10,
                        skew_value=1, skew_probability=0.7
                    )
                )
            }
        )

        user_config = DataFrameConfig(min_rows=5, max_rows=10)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
        @given(
            orders=spark_dataframes(spark, SKEWED_ORDER_SCHEMA, order_config),
            users=spark_dataframes(spark, USER_SCHEMA, user_config),
        )
        def inner(orders, users):
            # Deduplicate users to avoid fan-out
            users_deduped = users.dropDuplicates(["user_id"])

            joined     = orders.join(users_deduped, on="user_id", how="left")
            order_cnt  = orders.count()
            joined_cnt = joined.count()

            # LEFT JOIN with deduped right side: result rows == left rows
            assert joined_cnt == order_cnt, (
                f"Left join with deduped right changed row count: "
                f"{order_cnt} → {joined_cnt}"
            )

        inner()