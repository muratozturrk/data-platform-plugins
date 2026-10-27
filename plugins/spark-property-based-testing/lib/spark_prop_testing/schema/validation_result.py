# spark_prop_testing/schema/validation_result.py

"""
Immutable result object returned by all schema validators.
Uses the 'returns' library for functional error handling.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional
from returns.result import Result, Success, Failure


class Severity(Enum):
    ERROR   = auto()   # pipeline will break
    WARNING = auto()   # pipeline might produce wrong results
    INFO    = auto()   # informational, no action needed


@dataclass(frozen=True)
class ValidationIssue:
    """A single schema validation finding."""
    severity:    Severity
    column:      Optional[str]
    message:     str
    suggestion:  Optional[str] = None

    def __str__(self) -> str:
        col_part  = f"[{self.column}] " if self.column else ""
        sug_part  = f" → Suggestion: {self.suggestion}" if self.suggestion else ""
        return f"{self.severity.name}: {col_part}{self.message}{sug_part}"


@dataclass
class SchemaValidationResult:
    """
    Aggregated result of a schema validation run.

    Usage:
        result = validator.validate(df)
        if result.is_valid:
            proceed()
        else:
            print(result.summary())
    """
    issues:      List[ValidationIssue] = field(default_factory=list)
    schema_name: str = "unnamed"

    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def is_valid(self) -> bool:
        """Valid = no ERROR-level issues."""
        return len(self.errors) == 0

    def add_error(self, column: str, message: str, suggestion: str = None):
        self.issues.append(ValidationIssue(
            severity=Severity.ERROR,
            column=column,
            message=message,
            suggestion=suggestion,
        ))

    def add_warning(self, column: str, message: str, suggestion: str = None):
        self.issues.append(ValidationIssue(
            severity=Severity.WARNING,
            column=column,
            message=message,
            suggestion=suggestion,
        ))

    def add_info(self, column: str, message: str):
        self.issues.append(ValidationIssue(
            severity=Severity.INFO,
            column=column,
            message=message,
        ))

    def to_result(self) -> Result["SchemaValidationResult", List[ValidationIssue]]:
        """
        Returns a functional Result type (from 'returns' library).
        Success if valid, Failure containing errors if not.
        """
        if self.is_valid:
            return Success(self)
        return Failure(self.errors)

    def summary(self) -> str:
        lines = [
            f"Schema Validation: '{self.schema_name}'",
            f"  Errors:   {len(self.errors)}",
            f"  Warnings: {len(self.warnings)}",
            "─" * 50,
        ]
        for issue in self.issues:
            lines.append(f"  {issue}")
        return "\n".join(lines)

    def raise_if_invalid(self):
        """Raises AssertionError with full summary if validation failed."""
        if not self.is_valid:
            raise AssertionError(
                f"Schema validation failed:\n{self.summary()}"
            )