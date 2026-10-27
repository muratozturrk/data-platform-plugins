# spark_prop_testing/ge_integration/property_bridge.py

"""
The core integration layer.

Converts GX validation results into property-test assertions,
and wraps GX suite validation in Hypothesis @given loops
so the same suite is tested against many generated DataFrames.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame

from .context_factory import build_ge_context, df_to_ge_batch


@dataclass
class ExpectationResult:
    """Result for a single GX expectation."""
    expectation_type: str
    column:           Optional[str]
    success:          bool
    unexpected_count: int   = 0
    unexpected_pct:   float = 0.0
    details:          str   = ""

    def __str__(self):
        status = "✓" if self.success else "✗"
        col    = f"[{self.column}]" if self.column else "[table]"
        extra = ""
        if not self.success:
            extra = (f" — {self.details}" if self.details else
                     f" — {self.unexpected_count} violations ({self.unexpected_pct:.1%})")
        return f"{status} {col} {self.expectation_type}{extra}"


@dataclass
class SuiteValidationResult:
    """Aggregated result of running a full GX suite against one DataFrame."""
    suite_name:  str
    success:     bool
    results:     List[ExpectationResult] = field(default_factory=list)

    @property
    def failures(self) -> List[ExpectationResult]:
        return [r for r in self.results if not r.success]

    def summary(self) -> str:
        lines = [
            f"GE Suite: '{self.suite_name}' — "
            f"{'PASSED' if self.success else 'FAILED'}",
            f"  Total expectations : {len(self.results)}",
            f"  Passed             : {len(self.results) - len(self.failures)}",
            f"  Failed             : {len(self.failures)}",
            "─" * 60,
        ]
        for r in self.results:
            lines.append(f"  {r}")
        return "\n".join(lines)

    def assert_success(self):
        if not self.success:
            raise AssertionError(
                f"GE suite validation failed:\n{self.summary()}"
            )


class GEPropertyValidator:
    """
    Runs a GX ExpectationSuite against a PySpark DataFrame
    and returns a structured result compatible with property testing.

    This is the bridge between GX's validator and Hypothesis's test runner.
    One validator (and its ephemeral context) can validate many DataFrames.
    """

    def __init__(self, context=None):
        self._context = context or build_ge_context()

    def validate(self, df: DataFrame, suite) -> SuiteValidationResult:
        """
        Validates df against suite.
        Returns SuiteValidationResult — never raises for data problems.
        """
        batch = df_to_ge_batch(self._context, df, asset_name=suite.name)
        ge_result = batch.validate(suite, result_format="COMPLETE")
        return self._parse_ge_result(ge_result, suite.name)

    def _parse_ge_result(self, ge_result, suite_name: str) -> SuiteValidationResult:
        """Converts a raw GX validation result into our structured type."""
        results = []

        for exp_result in ge_result.results:
            config = exp_result.expectation_config
            result_dict = exp_result.result or {}
            details = ""
            exc = getattr(exp_result, "exception_info", None) or {}
            if isinstance(exc, dict) and exc.get("raised_exception"):
                # An expectation that errored is a failure, not a pass.
                details = f"error: {exc.get('exception_message', '')}".strip()

            results.append(ExpectationResult(
                expectation_type=config.type,
                column=config.kwargs.get("column"),
                success=bool(exp_result.success),
                unexpected_count=result_dict.get("unexpected_count", 0) or 0,
                # GX reports percent (0–100)
                unexpected_pct=(result_dict.get("unexpected_percent", 0.0) or 0.0) / 100.0,
                details=details,
            ))

        return SuiteValidationResult(
            suite_name=suite_name,
            success=bool(ge_result.success),
            results=results,
        )
