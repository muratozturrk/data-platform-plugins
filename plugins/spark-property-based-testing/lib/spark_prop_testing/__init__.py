# spark_prop_testing/__init__.py

"""
spark-prop-testing
==================
Property-based testing library for PySpark pipelines.

Quick start:
    from spark_prop_testing import (
        spark_dataframes,
        SchemaValidator,
        DataQualityPropertySuite,
        TransformationPropertyTester,
        prop,   # property decorator
    )

Great Expectations is optional: the strategy, schema, transformation and
decorator layers import without it. The GX symbols (ExpectationSuiteBuilder,
GEPropertyValidator, auto_generate_suite, DataQualityPropertySuite, ...) load
on first access and need ``great-expectations>=1.3``.
"""

from importlib import import_module

# ── Strategy layer ─────────────────────────────────────────────────────────────
from .strategies.dataframe_strategy import (
    spark_dataframes,
    DataFrameConfig,
    ColumnConfig,
)
from .strategies.null_strategy import NullConfig
from .strategies.distribution_strategy import (
    skewed_integers,
    edge_case_strings,
    edge_case_integers,
    realistic_id_strategy,
)

# ── Schema layer ───────────────────────────────────────────────────────────────
from .schema.schema_validator   import SchemaValidator
from .schema.evolution_checker  import SchemaEvolutionChecker
from .schema.validation_result  import SchemaValidationResult

# ── Transformation layer ───────────────────────────────────────────────────────
from .transformations.base import TransformationPropertyTester
from .transformations.standard_properties import (
    row_count_non_increasing,
    row_count_non_decreasing,
    row_count_exactly_preserved,
    input_columns_preserved,
    no_new_nulls_introduced,
    column_types_preserved,
    make_sum_linearity_property,
    make_idempotency_property,
    make_window_rank_property,
    make_left_join_preserves_left_keys,
)

# ── Decorators ─────────────────────────────────────────────────────────────────
from .decorators import prop, spark_property

# ── Great Expectations layer (lazy) ────────────────────────────────────────────
_LAZY = {
    "ExpectationSuiteBuilder":  ".ge_integration.expectation_builder",
    "ColumnExpectationSpec":    ".ge_integration.expectation_builder",
    "GEPropertyValidator":      ".ge_integration.property_bridge",
    "auto_generate_suite":      ".ge_integration.auto_expectations",
    "DataQualityPropertySuite": ".data_quality_suite",
    "QualitySuiteConfig":       ".data_quality_suite",
    "UnifiedQualityReport":     ".data_quality_suite",
}


def __getattr__(name):
    if name in _LAZY:
        try:
            module = import_module(_LAZY[name], __name__)
        except ImportError as e:
            raise ImportError(
                f"spark_prop_testing.{name} needs Great Expectations: "
                f"pip install 'great-expectations>=1.3' ({e})"
            ) from e
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__version__ = "1.1.0"
__all__ = [
    # Strategies
    "spark_dataframes", "DataFrameConfig", "ColumnConfig",
    "NullConfig", "skewed_integers", "edge_case_strings",
    "edge_case_integers", "realistic_id_strategy",
    # Schema
    "SchemaValidator", "SchemaEvolutionChecker", "SchemaValidationResult",
    # GE
    "ExpectationSuiteBuilder", "ColumnExpectationSpec",
    "GEPropertyValidator", "auto_generate_suite",
    "DataQualityPropertySuite", "QualitySuiteConfig", "UnifiedQualityReport",
    # Transformations
    "TransformationPropertyTester",
    "row_count_non_increasing", "row_count_non_decreasing",
    "row_count_exactly_preserved", "input_columns_preserved",
    "no_new_nulls_introduced", "column_types_preserved",
    "make_sum_linearity_property", "make_idempotency_property",
    "make_window_rank_property", "make_left_join_preserves_left_keys",
    # Decorators
    "prop", "spark_property",
]
