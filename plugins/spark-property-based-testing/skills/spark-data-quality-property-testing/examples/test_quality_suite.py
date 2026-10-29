"""
DataQualityPropertySuite: schema + GX + custom properties in one UnifiedQualityReport,
run() on one (real) DataFrame vs run_property_loop() over generated ones, and suites
declared in YAML (`ge_expectations`) loaded with ConfigLoader, as the CLI does.

Run:
    GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib python -m pytest -q test_quality_suite.py
"""

import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import sys
import textwrap

import pytest
from hypothesis import Phase
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, ColumnExpectationSpec, DataFrameConfig, DataQualityPropertySuite,
    ExpectationSuiteBuilder, QualitySuiteConfig,
)
from spark_prop_testing.config import ConfigLoader

STATUSES = ["new", "paid", "shipped"]
ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
GE_SUITE = (
    ExpectationSuiteBuilder("orders_quality")
    .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
    .add_column(ColumnExpectationSpec("status", value_set=STATUSES))
    .add_column(ColumnExpectationSpec("amount", min_value=0, max_value=10_000))
    .build()
)
DF_CONFIG = DataFrameConfig(min_rows=1, max_rows=10, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(STATUSES)),
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 10_000)),
})


def no_duplicate_rows(df):
    # explicit raise: pytest rewrites `assert` in test modules and appends its own diff to the message
    if df.count() != df.distinct().count():
        raise AssertionError("duplicate rows")


CONFIG = QualitySuiteConfig(
    suite_name="orders",
    expected_schema=ORDERS,
    ge_suite=GE_SUITE,
    df_config=DF_CONFIG,
    max_examples=2,
    custom_properties=[no_duplicate_rows],
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-quality-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def quality(spark):
    return DataQualityPropertySuite(spark, CONFIG)


def test_run_on_a_real_batch(spark, quality):
    """run(df): a clean batch passes every layer; the report keeps one entry per layer."""
    batch = spark.createDataFrame([(1, "new", 10), (2, "paid", 250), (3, "shipped", 0)], ORDERS)
    report = quality.run(batch)
    report.assert_valid()
    assert report.schema_valid == [True] and len(report.ge_results) == 1
    assert report.property_failures == []


def test_run_reports_each_failing_layer(spark, quality):
    """A batch with an extra column, a duplicate key and a bad status fails GX and the custom property only."""
    rows = [(1, "new", 10, "x"), (1, "new", 10, "x"), (2, "lost", 5, "y")]
    batch = spark.createDataFrame(rows, StructType(ORDERS.fields + [StructField("note", StringType(), True)]))
    report = quality.run(batch)
    assert not report.is_valid
    assert report.schema_valid == [True]                       # extra column = INFO only
    assert {f.expectation_type for f in report.ge_results[0].failures} == {
        "expect_column_values_to_be_unique", "expect_column_values_to_be_in_set"}
    assert report.property_failures == ["duplicate rows"]
    with pytest.raises(AssertionError, match="Quality Suite: 'orders'"):
        report.assert_valid()


def test_run_property_loop(quality):
    """run_property_loop(): the contracts hold for every generated DataFrame (strategy matches the suite)."""
    report = quality.run_property_loop()
    assert report.is_valid, report.summary()
    # per-layer results of the last generated example are kept in the report
    assert report.schema_valid == [True] and len(report.ge_results) == 1
    assert report.ge_results[0].success and report.property_failures == []


# ── transform under test: generate inputs, validate the OUTPUT ───────────────

RAW_ORDERS = StructType([
    StructField("order_id",     IntegerType(), nullable=False),
    StructField("status",       StringType(),  nullable=False),
    StructField("amount_cents", IntegerType(), nullable=False),
])
RAW_CONFIG = DataFrameConfig(min_rows=1, max_rows=10, column_configs={
    "order_id":     ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
    "status":       ColumnConfig(custom_strategy=st.sampled_from([" NEW", "Paid ", "SHIPPED"])),  # raw spelling
    "amount_cents": ColumnConfig(custom_strategy=st.integers(0, 1_000_000)),
})


def clean_orders(df):
    return df.select(
        "order_id",
        F.lower(F.trim("status")).alias("status"),
        (F.col("amount_cents") / 100).cast("int").alias("amount"),
    )


def clean_orders_buggy(df):
    # forgets to normalise status: " NEW" is not in the value set
    return df.select("order_id", "status", (F.col("amount_cents") / 100).cast("int").alias("amount"))


def transform_suite(spark, transform, phases=None):
    return DataQualityPropertySuite(spark, QualitySuiteConfig(
        suite_name="orders_clean", expected_schema=ORDERS, input_schema=RAW_ORDERS,
        transform=transform, ge_suite=GE_SUITE, df_config=RAW_CONFIG, max_examples=2,
        phases=phases,
    ))


def test_transform_output_meets_contract(spark):
    """For any raw input, clean_orders() output passes the schema and the GX suite."""
    report = transform_suite(spark, clean_orders).run_property_loop()
    report.assert_valid()
    assert report.schema_valid == [True] and report.ge_results[0].success


@pytest.mark.xfail(strict=True, reason="demonstrates run_property_loop catching a transform that skips status normalisation")
def test_buggy_transform_is_caught(spark):
    """For any raw input, clean_orders_buggy() output passes the GX suite (it does not)."""
    # skip shrinking: every shrink step would be another full GX validation
    report = transform_suite(
        spark, clean_orders_buggy, phases=(Phase.explicit, Phase.generate)).run_property_loop()
    report.assert_valid()


def test_schema_drift_in_a_pipeline_batch(spark, quality):
    """run(df) after a transformation: a retyped column is a schema ERROR."""
    batch = spark.createDataFrame([(1, "new", 10)], ORDERS)
    drifted = batch.withColumn("amount", F.col("amount").cast("string"))
    report = quality.run(drifted)
    assert report.schema_valid == [False] and not report.is_valid


# ── YAML suites (the CLI's `spark-prop-test run`) ───────────────────────────

YAML = """\
suites:
  orders_generated:                 # no transform: the generator is aligned with the columns block
    schema_module: pbt_example_module
    schema_name: ORDERS
    max_examples: 2
    min_rows: 1
    max_rows: 5
    ge_expectations:
      row_count_min: 1
      columns:
        order_id: {not_null: true, unique: true, min_value: 1}
        status:   {value_set: [new, paid, shipped]}
        amount:   {min_value: 0, max_value: 10000}
  orders_clean:                     # transform under test: inputs from RAW_ORDERS, output validated
    schema_module: pbt_example_module
    schema_name: ORDERS
    input_schema_name: RAW_ORDERS
    transform: pbt_example_module:clean
    max_examples: 2
    min_rows: 1
    max_rows: 5
    ge_expectations:
      columns:
        order_id: {not_null: true, unique: true}
        amount:   {min_value: 0}
"""

MODULE = """
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
RAW_ORDERS = StructType([
    StructField("order_id",     IntegerType(), nullable=False),
    StructField("status",       StringType(),  nullable=False),
    StructField("amount_cents", IntegerType(), nullable=False),
])

def clean(df):          # must hold for ANY raw input: no aligned generator here
    return (df.dropDuplicates(["order_id"])
              .select("order_id", "status",
                      (F.greatest("amount_cents", F.lit(0)) / 100).cast("int").alias("amount")))
"""


def run_yaml_suite(spark, cfg):
    return DataQualityPropertySuite(spark, QualitySuiteConfig(
        suite_name=cfg.name, expected_schema=cfg.schema, ge_suite=cfg.ge_suite,
        df_config=cfg.df_config, max_examples=cfg.max_examples,
        transform=cfg.transform, input_schema=cfg.input_schema,
    )).run_property_loop()                 # what `spark-prop-test run` does per suite


@pytest.fixture
def yaml_suites(tmp_path, monkeypatch):
    (tmp_path / "pbt_example_module.py").write_text(textwrap.dedent(MODULE), encoding="utf-8")
    (tmp_path / "spark_prop_test.yaml").write_text(YAML, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))          # the CLI adds the working directory itself
    yield ConfigLoader(str(tmp_path / "spark_prop_test.yaml")).load()
    sys.modules.pop("pbt_example_module", None)


def test_yaml_generator_is_aligned_with_expectations(spark, yaml_suites):
    """Without a transform, unique/min/max/value_set in YAML shape the generator, so the suite holds."""
    cfg = yaml_suites["orders_generated"]
    assert cfg.transform is None
    assert cfg.df_config.column("order_id").unique
    assert cfg.df_config.column("status").custom_strategy is not None
    report = run_yaml_suite(spark, cfg)
    assert report.is_valid, report.summary()


def test_yaml_transform_suite(spark, yaml_suites):
    """With `transform`, inputs come from input_schema_name and the transform's output is validated."""
    cfg = yaml_suites["orders_clean"]
    assert cfg.input_schema.fieldNames() == ["order_id", "status", "amount_cents"]
    assert cfg.df_config.column_configs == {}            # no alignment: the transform must cope with any input
    report = run_yaml_suite(spark, cfg)
    assert report.is_valid, report.summary()
    assert report.schema_valid == [True]
