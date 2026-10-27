# spark_prop_testing/data_quality_suite.py

"""
The unified API that combines:
  1. Schema validation (SchemaValidator)
  2. GX content validation (Great Expectations 1.x)
  3. Property-based generation (Hypothesis DataFrame strategies)

This is the primary interface users of the library will interact with.
"""

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from hypothesis import given
from hypothesis import strategies as st

from ._hypothesis                       import spark_settings
from .schema.schema_validator          import SchemaValidator
from .ge_integration.property_bridge   import GEPropertyValidator, SuiteValidationResult
from .ge_integration.expectation_builder import ExpectationSuiteBuilder, ColumnExpectationSpec
from .strategies.dataframe_strategy   import spark_dataframes, DataFrameConfig


# ── Custom property type: a function that takes a DataFrame and asserts ───────
DataFrameProperty = Callable[[DataFrame], None]


@dataclass
class QualitySuiteConfig:
    """Full configuration for a DataQualityPropertySuite run."""
    suite_name:      str
    expected_schema: Optional[StructType]          = None
    ge_suite:        Optional[Any]                  = None   # gx.ExpectationSuite
    df_config:       DataFrameConfig               = field(
                         default_factory=DataFrameConfig
                     )
    max_examples:    int                           = 20
    custom_properties: List[DataFrameProperty]     = field(default_factory=list)


@dataclass
class UnifiedQualityReport:
    """Combined result from all three validation layers."""
    suite_name:        str
    schema_results:    List[str]                   = field(default_factory=list)
    schema_valid:      List[bool]                  = field(default_factory=list)
    ge_results:        List[SuiteValidationResult] = field(default_factory=list)
    property_failures: List[str]                   = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        # Structured flags, not string matching: a column named "ERROR_CODE"
        # must not fail the report.
        schema_ok   = all(self.schema_valid)
        ge_ok       = all(r.success for r in self.ge_results)
        props_ok    = len(self.property_failures) == 0
        return schema_ok and ge_ok and props_ok

    def summary(self) -> str:
        lines = [
            f"{'='*60}",
            f"Quality Suite: '{self.suite_name}'",
            f"Overall: {'PASSED ✓' if self.is_valid else 'FAILED ✗'}",
            f"{'='*60}",
        ]

        if self.schema_results:
            lines.append("\n── Schema Validation ──")
            lines.extend(f"  {r}" for r in self.schema_results)

        if self.ge_results:
            lines.append("\n── Content Validation (GE) ──")
            for ge_r in self.ge_results:
                lines.append(ge_r.summary())

        if self.property_failures:
            lines.append("\n── Custom Property Failures ──")
            lines.extend(f"  ✗ {f}" for f in self.property_failures)

        return "\n".join(lines)

    def assert_valid(self):
        """Raises AssertionError with the full summary if any layer failed."""
        if not self.is_valid:
            raise AssertionError(self.summary())


class DataQualityPropertySuite:
    """
    The top-level library class.

    Runs schema validation, GE content validation, and custom
    property-based tests against generated DataFrames in one pass.

    Usage:
        suite = DataQualityPropertySuite(spark, config)
        report = suite.run(my_dataframe)
        report.assert_valid()  # raises if anything failed
    """

    def __init__(self, spark: SparkSession, config: QualitySuiteConfig):
        self._spark      = spark
        self._config     = config
        self._ge_validator = GEPropertyValidator()
        self._schema_validator = (
            SchemaValidator(config.expected_schema, schema_name=config.suite_name)
            if config.expected_schema
            else None
        )

    def run(self, df: DataFrame) -> UnifiedQualityReport:
        """
        Run all configured validations against df.
        Returns a UnifiedQualityReport — never raises directly.
        """
        report = UnifiedQualityReport(suite_name=self._config.suite_name)

        # ── Layer 1: Schema validation ────────────────────────────────────────
        if self._schema_validator:
            schema_result = self._schema_validator.validate(df)
            report.schema_results.append(schema_result.summary())
            report.schema_valid.append(schema_result.is_valid)

        # ── Layer 2: GE content validation ────────────────────────────────────
        if self._config.ge_suite:
            ge_result = self._ge_validator.validate(df, self._config.ge_suite)
            report.ge_results.append(ge_result)

        # ── Layer 3: Custom property assertions ───────────────────────────────
        for prop_fn in self._config.custom_properties:
            try:
                prop_fn(df)
            except AssertionError as e:
                report.property_failures.append(str(e))

        return report

    def run_property_loop(self) -> UnifiedQualityReport:
        """
        Generates DataFrames via Hypothesis and runs all validations
        across many generated examples.

        Use this instead of run() when you want to test that your
        quality contracts hold FOR ALL POSSIBLE inputs, not just one.
        """
        report       = UnifiedQualityReport(suite_name=self._config.suite_name)
        schema       = self._config.expected_schema
        df_config    = self._config.df_config
        max_examples = self._config.max_examples

        if schema is None:
            raise ValueError(
                "run_property_loop() requires expected_schema in config"
            )

        @spark_settings(max_examples)
        @given(df=spark_dataframes(self._spark, schema, df_config))
        def _inner(df):
            single_report = self.run(df)
            if not single_report.is_valid:
                raise AssertionError(
                    f"Quality suite failed on generated DataFrame:\n"
                    f"{single_report.summary()}"
                )

        try:
            _inner()
        except Exception as e:
            # Hypothesis re-raises the shrunk (minimal) failing example.
            report.property_failures.append(str(e))

        return report