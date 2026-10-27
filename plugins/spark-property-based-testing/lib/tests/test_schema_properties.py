
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType,
    DoubleType, BooleanType, TimestampType,
)
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from spark_prop_testing.schema.schema_validator    import SchemaValidator
from spark_prop_testing.schema.evolution_checker   import SchemaEvolutionChecker
from spark_prop_testing.strategies.dataframe_strategy import (
    spark_dataframes, DataFrameConfig,
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


EXPECTED_SCHEMA = StructType([
    StructField("user_id",    IntegerType(),  nullable=False),
    StructField("email",      StringType(),   nullable=True),
    StructField("age",        IntegerType(),  nullable=True),
    StructField("balance",    DoubleType(),   nullable=True),
    StructField("created_at", TimestampType(),nullable=True),
])


class TestSchemaValidatorProperties:

    # ── Property: a conforming DataFrame always passes validation ─────────────
    def test_conforming_df_always_passes(self, spark):
        validator = SchemaValidator(EXPECTED_SCHEMA, schema_name="users")
        config    = DataFrameConfig(min_rows=1, max_rows=50)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(df=spark_dataframes(spark, EXPECTED_SCHEMA, config))
        def inner(df):
            result = validator.validate(df)
            assert result.is_valid, (
                f"Conforming DataFrame failed validation:\n{result.summary()}"
            )

        inner()


    # ── Property: dropping required column always causes ERROR ─────────────────
    def test_missing_column_always_errors(self, spark):
        validator = SchemaValidator(EXPECTED_SCHEMA, schema_name="users")
        config    = DataFrameConfig(min_rows=1, max_rows=20)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=20)
        @given(
            df=spark_dataframes(spark, EXPECTED_SCHEMA, config),
            # Pick any column name from the schema to drop
            col_to_drop=st.sampled_from(
                [f.name for f in EXPECTED_SCHEMA.fields]
            ),
        )
        def inner(df, col_to_drop):
            broken_df = df.drop(col_to_drop)
            result    = validator.validate(broken_df)

            assert not result.is_valid, (
                f"Dropping column '{col_to_drop}' should cause ERROR "
                f"but validation passed!"
            )
            error_cols = [e.column for e in result.errors]
            assert col_to_drop in error_cols, (
                f"Expected error on '{col_to_drop}', got errors on: {error_cols}"
            )

        inner()


    # ── Property: incompatible type cast always causes ERROR ───────────────────
    def test_incompatible_type_always_errors(self, spark):
        validator = SchemaValidator(EXPECTED_SCHEMA, schema_name="users")
        config    = DataFrameConfig(min_rows=1, max_rows=20)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
        @given(df=spark_dataframes(spark, EXPECTED_SCHEMA, config))
        def inner(df):
            # Cast 'age' (IntegerType) to BooleanType → INCOMPATIBLE
            broken_df = df.withColumn(
                "age", F.col("age").cast(BooleanType())
            )
            result = validator.validate(broken_df)

            assert not result.is_valid
            error_msgs = [e.message for e in result.errors]
            assert any("age" in m for m in error_msgs), (
                f"Expected error mentioning 'age', got: {error_msgs}"
            )

        inner()


    # ── Property: extra columns don't break validation by default ─────────────
    def test_extra_columns_dont_break_validation(self, spark):
        validator = SchemaValidator(
            EXPECTED_SCHEMA,
            allow_extra_cols=True,
            schema_name="users",
        )
        config = DataFrameConfig(min_rows=1, max_rows=20)

        @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
        @given(df=spark_dataframes(spark, EXPECTED_SCHEMA, config))
        def inner(df):
            # Add an unexpected column
            augmented = df.withColumn("extra_col", F.lit("unexpected"))
            result    = validator.validate(augmented)

            # Should still be valid (extra_col → INFO, not ERROR)
            assert result.is_valid, (
                f"Extra column caused failure:\n{result.summary()}"
            )

        inner()


class TestSchemaEvolutionProperties:

    # ── Property: adding nullable column is always safe ───────────────────────
    def test_adding_nullable_column_is_safe(self):
        checker = SchemaEvolutionChecker()

        old_schema = StructType([
            StructField("id",   IntegerType(), nullable=False),
            StructField("name", StringType(),  nullable=True),
        ])
        new_schema = StructType([
            StructField("id",         IntegerType(), nullable=False),
            StructField("name",       StringType(),  nullable=True),
            StructField("new_column", StringType(),  nullable=True),  # added
        ])

        report = checker.check(old_schema, new_schema)
        assert not report.has_breaking_changes, (
            f"Adding nullable column should be safe:\n{report.summary()}"
        )


    # ── Property: removing ANY column is always breaking ─────────────────────
    @given(col_index=st.integers(min_value=0, max_value=2))
    def test_removing_column_is_always_breaking(self, col_index):
        checker = SchemaEvolutionChecker()

        fields = [
            StructField("id",      IntegerType(), nullable=False),
            StructField("name",    StringType(),  nullable=True),
            StructField("balance", DoubleType(),  nullable=True),
        ]

        old_schema = StructType(fields)
        new_fields = [f for i, f in enumerate(fields) if i != col_index]
        new_schema = StructType(new_fields)

        report = checker.check(old_schema, new_schema)
        assert report.has_breaking_changes, (
            f"Removing column index {col_index} should be breaking"
        )


    # ── Property: numeric widening is safe, narrowing is not ─────────────────
    def test_type_widening_is_safe_narrowing_is_not(self):
        checker = SchemaEvolutionChecker()

        # INT → LONG (widening) → safe
        old = StructType([StructField("val", IntegerType(), nullable=True)])
        new = StructType([StructField("val", LongType(),    nullable=True)])

        report = checker.check(old, new)
        assert not report.has_breaking_changes, "INT→LONG should be safe"

        # LONG → INT (narrowing) → breaking
        report2 = checker.check(new, old)
        assert report2.has_breaking_changes, "LONG→INT should be breaking"


    # ── Property: evolution check is NOT symmetric ────────────────────────────
    @given(col_index=st.integers(min_value=0, max_value=1))
    def test_evolution_is_asymmetric(self, col_index):
        """
        Forward evolution (old→new) ≠ reverse (new→old).
        This is a metamorphic property: swapping direction changes the result.
        """
        checker = SchemaEvolutionChecker()

        schema_v1 = StructType([
            StructField("id",   IntegerType(), nullable=False),
            StructField("name", StringType(),  nullable=True),
        ])
        schema_v2 = StructType([
            StructField("id",        IntegerType(), nullable=False),
            StructField("name",      StringType(),  nullable=True),
            StructField("new_field", LongType(),    nullable=True),
        ])

        forward  = checker.check(schema_v1, schema_v2)
        backward = checker.check(schema_v2, schema_v1)

        # Forward (add nullable): safe
        assert not forward.has_breaking_changes

        # Backward (remove column): breaking
        assert backward.has_breaking_changes