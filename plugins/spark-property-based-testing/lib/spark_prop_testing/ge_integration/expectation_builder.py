# spark_prop_testing/ge_integration/expectation_builder.py

"""
Fluent builder for constructing GX (Great Expectations 1.x) expectation suites
programmatically. Designed to work with property-based testing — expectations
are generated FROM schema and data profiles, not hand-coded.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import great_expectations as gx
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration


@dataclass
class ColumnExpectationSpec:
    """
    All expected properties for a single column.
    Only set the fields you want to validate — unset = not checked.
    """
    column:             str
    not_null:           bool               = False
    unique:             bool               = False
    min_value:          Optional[Any]      = None
    max_value:          Optional[Any]      = None
    value_set:          Optional[List]     = None   # allowed discrete values
    regex_pattern:      Optional[str]      = None
    min_length:         Optional[int]      = None
    max_length:         Optional[int]      = None
    max_null_fraction:  Optional[float]    = None   # e.g. 0.05 → max 5% null


class ExpectationSuiteBuilder:
    """
    Builds a GX ExpectationSuite from a list of ColumnExpectationSpecs.

    Usage:
        suite = (
            ExpectationSuiteBuilder("orders_suite")
            .add_column(ColumnExpectationSpec(
                column="order_id",
                not_null=True,
                unique=True,
                min_value=1,
            ))
            .add_column(ColumnExpectationSpec(
                column="status",
                value_set=["pending", "shipped", "delivered", "cancelled"],
            ))
            .add_row_count(min_rows=1, max_rows=1_000_000)
            .build()
        )
    """

    def __init__(self, suite_name: str):
        self.suite_name = suite_name
        self._column_specs: List[ColumnExpectationSpec] = []
        self._row_count_min: Optional[int] = None
        self._row_count_max: Optional[int] = None
        self._custom_expectations: List[ExpectationConfiguration] = []

    def add_column(self, spec: ColumnExpectationSpec) -> "ExpectationSuiteBuilder":
        self._column_specs.append(spec)
        return self

    def add_row_count(
        self,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> "ExpectationSuiteBuilder":
        self._row_count_min = min_rows
        self._row_count_max = max_rows
        return self

    def add_custom(
        self,
        expectation_type: str,
        kwargs: Dict[str, Any],
    ) -> "ExpectationSuiteBuilder":
        """Escape hatch for any GX expectation, by its snake_case type name."""
        self._custom_expectations.append(
            ExpectationConfiguration(type=expectation_type, kwargs=kwargs)
        )
        return self

    def build(self):
        """Compile all specs into a GX ExpectationSuite."""
        suite = gx.ExpectationSuite(name=self.suite_name)

        if self._row_count_min is not None or self._row_count_max is not None:
            _add(suite, "expect_table_row_count_to_be_between", {
                "min_value": self._row_count_min,
                "max_value": self._row_count_max,
            })

        for spec in self._column_specs:
            self._build_column_expectations(suite, spec)

        for exp in self._custom_expectations:
            suite.add_expectation_configuration(exp)

        return suite

    def _build_column_expectations(self, suite, spec: ColumnExpectationSpec):
        col = spec.column

        if spec.not_null:
            _add(suite, "expect_column_values_to_not_be_null", {"column": col})
        elif spec.max_null_fraction is not None:
            # GX's `mostly` = fraction that MUST pass (inverse of null fraction).
            # Only when not_null is off: GX keeps one expectation per type+column,
            # and not_null is strictly stronger anyway.
            _add(suite, "expect_column_values_to_not_be_null", {
                "column": col,
                "mostly": 1.0 - spec.max_null_fraction,
            })

        if spec.unique:
            _add(suite, "expect_column_values_to_be_unique", {"column": col})

        if spec.min_value is not None or spec.max_value is not None:
            _add(suite, "expect_column_values_to_be_between", {
                "column": col,
                "min_value": spec.min_value,
                "max_value": spec.max_value,
            })

        if spec.value_set is not None:
            _add(suite, "expect_column_values_to_be_in_set", {
                "column": col,
                "value_set": spec.value_set,
            })

        if spec.regex_pattern is not None:
            _add(suite, "expect_column_values_to_match_regex", {
                "column": col,
                "regex": spec.regex_pattern,
            })

        if spec.min_length is not None or spec.max_length is not None:
            _add(suite, "expect_column_value_lengths_to_be_between", {
                "column": col,
                "min_value": spec.min_length,
                "max_value": spec.max_length,
            })


def _add(suite, expectation_type: str, kwargs: Dict[str, Any]) -> None:
    suite.add_expectation_configuration(
        ExpectationConfiguration(type=expectation_type, kwargs=kwargs)
    )
