"""
GX 1.x suites with the bundled builder: ExpectationSuiteBuilder + ColumnExpectationSpec,
add_custom, auto_generate_suite, and the GX pitfalls (one expectation per type+column,
`mostly`, nulls and unexpected_percent).

Needs the `ge` extra (great-expectations>=1.3). Set GX_ANALYTICS_ENABLED=false.

Run:
    GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib python -m pytest -q test_gx_suite_builder.py
"""

import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnExpectationSpec, ExpectationSuiteBuilder, GEPropertyValidator, auto_generate_suite,
)

SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=True),
    StructField("qty",      IntegerType(), nullable=True),
])


def kinds(suite):
    """{(expectation type, column): kwargs} of a built suite."""
    out = {}
    for e in suite.expectations:
        kw = dict(e.configuration.kwargs)
        out[(e.expectation_type, kw.get("column"))] = kw
    return out


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-gx-builder")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def validator():
    return GEPropertyValidator()          # one ephemeral context for the whole session


# ── suite structure (no Spark) ───────────────────────────────────────────────

def test_spec_fields_map_to_expectations():
    """Each ColumnExpectationSpec field becomes one GX expectation with the documented kwargs."""
    suite = (
        ExpectationSuiteBuilder("orders")
        .add_row_count(min_rows=1, max_rows=1_000)
        .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
        .add_column(ColumnExpectationSpec("status", value_set=["new", "paid"], max_null_fraction=0.1))
        .add_column(ColumnExpectationSpec("sku", regex_pattern=r"^[A-Z]{3}-\d+$", min_length=5, max_length=12))
        .add_custom("expect_column_values_to_be_of_type", {"column": "qty", "type_": "IntegerType"})
        .build()
    )
    k = kinds(suite)
    assert k[("expect_table_row_count_to_be_between", None)]["max_value"] == 1_000
    assert ("expect_column_values_to_be_unique", "order_id") in k
    assert k[("expect_column_values_to_not_be_null", "status")]["mostly"] == pytest.approx(0.9)
    assert ("expect_column_values_to_match_regex", "sku") in k
    assert k[("expect_column_value_lengths_to_be_between", "sku")]["max_value"] == 12
    assert ("expect_column_values_to_be_of_type", "qty") in k
    assert len(suite.expectations) == 9


def test_one_expectation_per_type_and_column():
    """GX keeps one expectation per type+column: a second range on the same column replaces the first."""
    suite = (
        ExpectationSuiteBuilder("dup")
        .add_column(ColumnExpectationSpec("qty", min_value=0, max_value=100))
        .add_custom("expect_column_values_to_be_between", {"column": "qty", "min_value": 1, "max_value": 5})
        .build()
    )
    (only,) = suite.expectations
    assert (only.configuration.kwargs["min_value"], only.configuration.kwargs["max_value"]) == (1, 5)

    # not_null wins over max_null_fraction: both would be expect_column_values_to_not_be_null
    both = ExpectationSuiteBuilder("nn").add_column(
        ColumnExpectationSpec("status", not_null=True, max_null_fraction=0.5)).build()
    assert "mostly" not in kinds(both)[("expect_column_values_to_not_be_null", "status")]


def test_auto_generate_suite_from_schema():
    """auto_generate_suite: row count >= min_rows, NOT NULL for non-nullable, type bounds for integers/floats."""
    schema = StructType(SCHEMA.fields + [StructField("ratio", FloatType(), True)])
    k = kinds(auto_generate_suite(schema, suite_name="auto", min_rows=1))
    assert k[("expect_table_row_count_to_be_between", None)]["min_value"] == 1
    assert ("expect_column_values_to_not_be_null", "order_id") in k
    assert ("expect_column_values_to_not_be_null", "status") not in k          # nullable column
    assert k[("expect_column_values_to_be_between", "qty")]["max_value"] == 2_147_483_647
    assert k[("expect_column_values_to_be_between", "ratio")]["max_value"] == pytest.approx(3.4028235e38)  # float32 max
    assert ("expect_column_values_to_be_between", "status") not in k           # strings: nothing


# ── validation semantics (Spark) ─────────────────────────────────────────────

def test_mostly_and_unexpected_percent(spark, validator):
    """`mostly` is the fraction that must pass; unexpected_pct is a 0-1 fraction of the NON-NULL values."""
    df = spark.createDataFrame(
        [(1, "new", 3), (2, None, None), (3, "paid", 50), (4, None, 2)], SCHEMA)
    suite = (
        ExpectationSuiteBuilder("semantics")
        .add_column(ColumnExpectationSpec("status", max_null_fraction=0.5))     # 2/4 null: passes
        .add_column(ColumnExpectationSpec("qty", max_value=10))                 # 50 violates
        .build()
    )
    result = validator.validate(df, suite)
    by_type = {r.expectation_type: r for r in result.results}
    assert by_type["expect_column_values_to_not_be_null"].success
    between = by_type["expect_column_values_to_be_between"]
    assert not between.success and between.unexpected_count == 1
    assert between.unexpected_pct == pytest.approx(1 / 3)        # 1 of 3 non-null values, not 1/4
    assert not result.success and [f.column for f in result.failures] == ["qty"]
    with pytest.raises(AssertionError, match="GE suite validation failed"):
        result.assert_success()
