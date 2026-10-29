"""
Runtime null contracts: for any generated input (with nulls), the output of a transformation
has no nulls in the columns its contract declares NOT NULL, and its schema keeps the contract.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_null_contracts.py
"""

import pytest
from hypothesis import Phase, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import ColumnConfig, DataFrameConfig, NullConfig, SchemaValidator, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles
from spark_prop_testing.schema.validation_result import Severity

register_spark_profiles()
FAST = settings(settings.get_profile("spark"), max_examples=3)

RAW = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("country",     StringType(),  nullable=True),
    StructField("orders",      IntegerType(), nullable=True),
])
OUTPUT_CONTRACT = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("country",     StringType(),  nullable=False),
    StructField("orders",      IntegerType(), nullable=False),
])
CONFIG = DataFrameConfig(
    min_rows=1,
    max_rows=15,
    default_null_config=NullConfig(null_rate=0.5, always_include_null=True),   # >= 1 null per nullable column
    column_configs={"customer_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True)},
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-null-contracts")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── transformation under test ────────────────────────────────────────────────

def standardise(df):
    return df.select(
        "customer_id",
        F.coalesce(F.upper(F.trim("country")), F.lit("UNKNOWN")).alias("country"),
        F.coalesce("orders", F.lit(0)).alias("orders"),
    )


def standardise_buggy(df):
    # `when` without `otherwise` yields NULL for the unmatched rows
    return df.select(
        "customer_id",
        F.coalesce("country", F.lit("UNKNOWN")).alias("country"),
        F.when(F.col("orders").isNotNull(), F.col("orders")).alias("orders"),
    )


def null_counts(df, contract):
    """Nulls per NOT NULL contract column, in one aggregation."""
    cols = [f.name for f in contract.fields if not f.nullable]
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    return {c: (row[c] or 0) for c in cols}


def assert_null_contract(df, contract):
    counts = null_counts(df, contract)
    assert all(n == 0 for n in counts.values()), f"NOT NULL columns with nulls: {counts}"


# ── properties ───────────────────────────────────────────────────────────────

def test_output_honours_null_contract(spark):
    """For any input with nulls, standardise() output has no nulls in its NOT NULL columns."""
    @FAST
    @given(df=spark_dataframes(spark, RAW, CONFIG))
    def check(df):
        assert_null_contract(standardise(df), OUTPUT_CONTRACT)
    check()


def test_output_schema_keeps_not_null_flags(spark):
    """coalesce with a literal makes Spark infer NOT NULL, so the validator reports no nullable weakening."""
    df = spark.createDataFrame([(1, None, None)], RAW)
    result = SchemaValidator(OUTPUT_CONTRACT).validate(standardise(df))
    assert result.issues == [], result.summary()

    weakened = SchemaValidator(OUTPUT_CONTRACT).validate(standardise_buggy(df))
    assert weakened.is_valid                          # schema-level: only a WARNING ...
    assert [i.column for i in weakened.warnings] == ["orders"]
    assert weakened.warnings[0].severity is Severity.WARNING


def test_schema_flags_are_not_enforced_on_derived_frames(spark):
    """createDataFrame rejects None in a NOT NULL field, but derived frames are only checked at runtime."""
    with pytest.raises(Exception, match="CANNOT_BE_NONE|nullable"):   # PySpark 3.5: [CANNOT_BE_NONE]
        spark.createDataFrame([(1, None, 3)], OUTPUT_CONTRACT)
    # a nullable source read back with the contract's names: nulls pass through silently
    loose = spark.createDataFrame([(1, None, None)], RAW)
    assert null_counts(loose, OUTPUT_CONTRACT) == {"customer_id": 0, "country": 1, "orders": 1}


@pytest.mark.xfail(strict=True, reason="demonstrates the null contract catching `when` without `otherwise`")
def test_buggy_transformation_breaks_contract(spark):
    """For any input with nulls, standardise_buggy() output has no nulls in NOT NULL columns (it does)."""
    @settings(FAST, phases=[Phase.generate])     # skip shrinking: every input fails, each step is a Spark job
    @given(df=spark_dataframes(spark, RAW, CONFIG))
    def check(df):
        assert_null_contract(standardise_buggy(df), OUTPUT_CONTRACT)
    check()
