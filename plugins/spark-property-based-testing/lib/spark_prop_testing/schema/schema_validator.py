# spark_prop_testing/schema/schema_validator.py

"""
The main SchemaValidator class.
Validates DataFrames against an expected schema with configurable strictness.
"""

from typing import Optional, Set
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField

from .type_compatibility  import check_type_compatibility, Compatibility
from .validation_result   import SchemaValidationResult, Severity


class SchemaValidator:
    """
    Validates a DataFrame's schema against an expected StructType.

    Checks performed:
      1. Missing required columns
      2. Unexpected extra columns
      3. Type compatibility (exact, compatible, coercible, incompatible)
      4. Nullable contract violations
      5. Column order (optional)

    Args:
        expected_schema:    The schema you designed/expect
        strict_types:       If True, any non-exact type (COMPATIBLE or COERCIBLE) is an error
        allow_extra_cols:   If True, extra columns are INFO not WARNING
        check_column_order: If True, column ordering is validated
        schema_name:        Label for error messages
    """

    def __init__(
        self,
        expected_schema:    StructType,
        strict_types:       bool = False,
        allow_extra_cols:   bool = True,
        check_column_order: bool = False,
        schema_name:        str  = "schema",
    ):
        self.expected_schema    = expected_schema
        self.strict_types       = strict_types
        self.allow_extra_cols   = allow_extra_cols
        self.check_column_order = check_column_order
        self.schema_name        = schema_name

        # Pre-index expected fields by name for O(1) lookup
        self._expected_fields = {
            f.name: f for f in expected_schema.fields
        }

    def validate(self, df: DataFrame) -> SchemaValidationResult:
        """
        Run all schema checks against the given DataFrame.
        Returns a SchemaValidationResult — never raises directly.
        """
        result = SchemaValidationResult(schema_name=self.schema_name)
        actual_fields = {f.name: f for f in df.schema.fields}

        self._check_missing_columns(result, actual_fields)
        self._check_extra_columns(result, actual_fields)
        self._check_type_compatibility(result, actual_fields)
        self._check_nullable_contracts(result, actual_fields)

        if self.check_column_order:
            self._check_column_order(result, df.schema)

        return result

    # ── Check 1: Missing required columns ────────────────────────────────────
    def _check_missing_columns(
        self,
        result:        SchemaValidationResult,
        actual_fields: dict,
    ):
        for col_name, expected_field in self._expected_fields.items():
            if col_name not in actual_fields:
                result.add_error(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': required column is missing from DataFrame"
                    ),
                    suggestion=(
                        f"Add column '{col_name}' of type "
                        f"{expected_field.dataType} to the source"
                    ),
                )

    # ── Check 2: Extra/unexpected columns ─────────────────────────────────────
    def _check_extra_columns(
        self,
        result:        SchemaValidationResult,
        actual_fields: dict,
    ):
        extra_cols = set(actual_fields.keys()) - set(self._expected_fields.keys())

        for col_name in sorted(extra_cols):
            if self.allow_extra_cols:
                result.add_info(
                    column=col_name,
                    message=f"Column '{col_name}': extra column not in expected schema",
                )
            else:
                result.add_warning(
                    column=col_name,
                    message=f"Column '{col_name}': unexpected column (strict mode)",
                    suggestion=f"Remove '{col_name}' or update expected schema",
                )

    # ── Check 3: Type compatibility ────────────────────────────────────────────
    def _check_type_compatibility(
        self,
        result:        SchemaValidationResult,
        actual_fields: dict,
    ):
        for col_name, expected_field in self._expected_fields.items():
            if col_name not in actual_fields:
                continue  # already reported as missing

            actual_field = actual_fields[col_name]
            compat, msg  = check_type_compatibility(
                source=actual_field.dataType,
                target=expected_field.dataType,
            )

            if compat == Compatibility.EXACT:
                pass  # perfect

            elif compat == Compatibility.COMPATIBLE:
                if self.strict_types:
                    result.add_error(
                        column=col_name,
                        message=(
                            f"Column '{col_name}': type mismatch (strict mode): "
                            f"got {actual_field.dataType}, "
                            f"expected {expected_field.dataType}. {msg}"
                        ),
                    )
                else:
                    result.add_warning(
                        column=col_name,
                        message=(
                            f"Column '{col_name}': compatible but non-exact type: "
                            f"got {actual_field.dataType}, "
                            f"expected {expected_field.dataType}. {msg}"
                        ),
                        suggestion=f"Cast to {expected_field.dataType} explicitly",
                    )

            elif compat == Compatibility.COERCIBLE and self.strict_types:
                result.add_error(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': lossy type conversion (strict mode): "
                        f"{actual_field.dataType} → {expected_field.dataType}. {msg}"
                    ),
                    suggestion="Validate data range before casting",
                )

            elif compat == Compatibility.COERCIBLE:
                result.add_warning(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': lossy type conversion required: "
                        f"{actual_field.dataType} → {expected_field.dataType}. "
                        f"{msg}"
                    ),
                    suggestion="Validate data range before casting",
                )

            elif compat == Compatibility.INCOMPATIBLE:
                result.add_error(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': incompatible types: "
                        f"got {actual_field.dataType}, "
                        f"expected {expected_field.dataType}. {msg}"
                    ),
                    suggestion=(
                        f"Source system changed column type. "
                        f"Manual migration required."
                    ),
                )

    # ── Check 4: Nullable contract ─────────────────────────────────────────────
    def _check_nullable_contracts(
        self,
        result:        SchemaValidationResult,
        actual_fields: dict,
    ):
        for col_name, expected_field in self._expected_fields.items():
            if col_name not in actual_fields:
                continue

            actual_field = actual_fields[col_name]

            # Expected NOT NULL but actual is NULLABLE → danger
            if not expected_field.nullable and actual_field.nullable:
                result.add_warning(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': nullable contract weakened: expected NOT NULL, "
                        f"got NULLABLE"
                    ),
                    suggestion=(
                        f"Add .filter(F.col('{col_name}').isNotNull()) "
                        f"or add a NOT NULL constraint at source"
                    ),
                )

            # Expected NULLABLE but actual is NOT NULL → fine (stricter)
            elif expected_field.nullable and not actual_field.nullable:
                result.add_info(
                    column=col_name,
                    message=(
                        f"Column '{col_name}': stricter than expected "
                        f"(NOT NULL vs expected NULLABLE)"
                    ),
                )

    # ── Check 5: Column order ──────────────────────────────────────────────────
    def _check_column_order(
        self,
        result: SchemaValidationResult,
        actual_schema: StructType,
    ):
        expected_names = [f.name for f in self.expected_schema.fields]
        actual_names   = [
            f.name for f in actual_schema.fields
            if f.name in self._expected_fields
        ]

        if expected_names != actual_names:
            result.add_warning(
                column=None,
                message=(
                    f"Column order mismatch. "
                    f"Expected: {expected_names}. "
                    f"Got: {actual_names}"
                ),
                suggestion="Reorder with df.select(*expected_column_order)",
            )