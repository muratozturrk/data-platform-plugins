# spark_prop_testing/transformations/base.py

"""
Base class and decorator for property-testing PySpark transformations.

Every transformation under test should be wrapped in a
TransformationUnderTest descriptor that declares:
  - The transformation function
  - Input schema
  - Expected property categories
  - Optional pre/post conditions
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from hypothesis import given

from .._hypothesis import spark_settings
from ..strategies.dataframe_strategy import (
    spark_dataframes, DataFrameConfig,
)


# A transformation is just a function DataFrame → DataFrame
Transformation = Callable[[DataFrame], DataFrame]
# A property is a function (input_df, output_df) → None (raises on failure)
TransformationProperty = Callable[[DataFrame, DataFrame], None]


@dataclass
class PropertyTestResult:
    """Result of running one property against one transformation."""
    property_name:  str
    passed:         bool
    failure_input:  Optional[str] = None   # str repr of failing case
    error_message:  Optional[str] = None

    def __str__(self):
        status = "✓" if self.passed else "✗"
        msg    = f" — {self.error_message}" if not self.passed else ""
        rows   = (f"\n      minimal failing input:\n      "
                  + self.failure_input.replace("\n", "\n      ")) if self.failure_input else ""
        return f"{status} {self.property_name}{msg}{rows}"


@dataclass
class TransformationTestReport:
    """All property results for one transformation."""
    transformation_name: str
    results: List[PropertyTestResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        lines = [
            f"Transformation: '{self.transformation_name}' "
            f"— {'PASSED ✓' if self.passed else 'FAILED ✗'}",
        ]
        for r in self.results:
            lines.append(f"  {r}")
        return "\n".join(lines)

    def assert_passed(self):
        if not self.passed:
            raise AssertionError(
                f"Transformation properties failed:\n{self.summary()}"
            )


class TransformationPropertyTester:
    """
    Runs a battery of property tests against a transformation function.

    Usage:
        tester = TransformationPropertyTester(
            spark=spark,
            transform=my_transform_fn,
            input_schema=MY_SCHEMA,
            name="my_transform",
        )
        tester.add_property(row_count_non_increasing)
        tester.add_property(output_columns_preserved)
        report = tester.run(max_examples=20)
        report.assert_passed()
    """

    def __init__(
        self,
        spark:        SparkSession,
        transform:    Transformation,
        input_schema: StructType,
        name:         str              = "unnamed_transformation",
        df_config:    DataFrameConfig  = None,
    ):
        self._spark        = spark
        self._transform    = transform
        self._schema       = input_schema
        self._name         = name
        self._df_config    = df_config or DataFrameConfig(
            min_rows=1, max_rows=100
        )
        self._properties:  List[TransformationProperty] = []
        self._default_max_examples: int = 20

    def add_property(
        self,
        prop: TransformationProperty,
    ) -> "TransformationPropertyTester":
        """Fluent method to add a property check."""
        self._properties.append(prop)
        return self

    def run(self, max_examples: Optional[int] = None) -> TransformationTestReport:
        """
        Runs all registered properties across Hypothesis-generated inputs.
        Each property is tested independently. Never raises for property
        failures or transformation crashes; call ``report.assert_passed()``.
        """
        report = TransformationTestReport(
            transformation_name=self._name
        )
        if max_examples is None:
            max_examples = self._default_max_examples

        for prop in self._properties:
            result = self._test_one_property(prop, max_examples)
            report.results.append(result)

        return report

    def _test_one_property(
        self,
        prop:         TransformationProperty,
        max_examples: int,
    ) -> PropertyTestResult:

        failures = []   # (message, rows) per failing example, in run order
        name = getattr(prop, "__name__", repr(prop))

        @spark_settings(max_examples)
        @given(
            input_df=spark_dataframes(
                self._spark, self._schema, self._df_config
            )
        )
        def _inner(input_df):
            try:
                output_df = self._transform(input_df)
                prop(input_df, output_df)
            except Exception as e:
                # A crash in the transformation is a finding too.
                kind = "" if isinstance(e, AssertionError) else f"{type(e).__name__}: "
                failures.append((f"{kind}{e}", _rows_preview(input_df)))
                raise  # Let Hypothesis shrink the failure

        try:
            _inner()
            return PropertyTestResult(property_name=name, passed=True)
        except Exception as e:
            # Hypothesis replays the minimal (shrunk) example last.
            message, rows = failures[-1] if failures else (f"{type(e).__name__}: {e}", None)
            return PropertyTestResult(
                property_name=name,
                passed=False,
                failure_input=rows,
                error_message=message,
            )

    @classmethod
    def from_prop(
        cls,
        spark:        SparkSession,
        transform:    Transformation,
        input_schema: StructType,
        df_config:    DataFrameConfig = None,
    ) -> "TransformationPropertyTester":
        """Builds a tester from a transformation decorated with ``@prop(...)``."""
        if not getattr(transform, "_is_prop_transform", False):
            raise TypeError(f"{transform!r} is not decorated with @prop")
        tester = cls(spark, transform, input_schema,
                     name=getattr(transform, "__name__", "transformation"), df_config=df_config)
        for p in transform._prop_properties:
            tester.add_property(p)
        tester._default_max_examples = transform._prop_max_examples
        return tester


def _rows_preview(df: DataFrame, limit: int = 10) -> Optional[str]:
    try:
        rows = df.limit(limit).collect()
        return "\n".join(str(r.asDict()) for r in rows)
    except Exception:
        return None