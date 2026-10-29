# spark_prop_testing/ge_integration/auto_expectations.py

"""
Automatically generates a baseline GX ExpectationSuite
from a Spark StructType schema — no manual spec needed.

Great for bootstrapping a new pipeline's test suite quickly.
"""

from typing import Optional

from pyspark.sql.types import (
    ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType,
    StructField, StructType,
)

from .expectation_builder import ColumnExpectationSpec, ExpectationSuiteBuilder

# Type-safe bounds. DoubleType has no practical bound; Decimal/string/date
# columns only get the NOT NULL contract.
_NUMERIC_BOUNDS = {
    ByteType:    (-128,              127),
    ShortType:   (-32_768,           32_767),
    IntegerType: (-2_147_483_648,    2_147_483_647),
    LongType:    (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    FloatType:   (-3.4028235e38,     3.4028235e38),   # float32 max
}


def auto_generate_suite(
    schema:       StructType,
    suite_name:   str = "auto_generated_suite",
    min_rows:     int = 1,
):
    """
    Generates a GX suite with sensible defaults derived
    purely from the schema's type and nullable information.

    What gets generated automatically:
      - NOT NULL expectations for nullable=False columns
      - Numeric range bounds (type-safe min/max) for integer and float columns
      - Row count minimum

    Use as a starting point, then refine with domain knowledge
    (value sets, regexes, business ranges).
    """
    builder = ExpectationSuiteBuilder(suite_name).add_row_count(min_rows=min_rows)

    for struct_field in schema.fields:
        spec = _field_to_spec(struct_field)
        if spec:
            builder.add_column(spec)

    return builder.build()


def _field_to_spec(f: StructField) -> Optional[ColumnExpectationSpec]:
    """Derives expectations from a single StructField."""
    spec = ColumnExpectationSpec(column=f.name)

    if not f.nullable:
        spec.not_null = True

    for num_type, (lo, hi) in _NUMERIC_BOUNDS.items():
        if isinstance(f.dataType, num_type):
            spec.min_value = lo
            spec.max_value = hi
            break

    return spec
