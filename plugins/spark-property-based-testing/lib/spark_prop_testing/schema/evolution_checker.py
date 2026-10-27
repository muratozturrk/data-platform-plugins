# spark_prop_testing/schema/evolution_checker.py

"""
Checks whether a schema change is a SAFE EVOLUTION or a BREAKING CHANGE.

Safe evolutions (backward compatible):
  - Adding a nullable column
  - Widening a numeric type (INT → LONG)

Breaking changes:
  - Removing a column
  - Narrowing a type (LONG → INT)
  - Changing nullable=True → nullable=False on existing column
  - Renaming a column
"""

from dataclasses import dataclass, field
from typing import List, Tuple
from pyspark.sql.types import StructType

from .type_compatibility import check_type_compatibility, Compatibility


@dataclass
class EvolutionIssue:
    is_breaking: bool
    description: str

    def __str__(self):
        tag = "BREAKING" if self.is_breaking else "SAFE"
        return f"[{tag}] {self.description}"


@dataclass
class EvolutionReport:
    old_version: str
    new_version: str
    issues:      List[EvolutionIssue] = field(default_factory=list)

    @property
    def has_breaking_changes(self) -> bool:
        return any(i.is_breaking for i in self.issues)

    @property
    def breaking_changes(self) -> List[EvolutionIssue]:
        return [i for i in self.issues if i.is_breaking]

    @property
    def safe_changes(self) -> List[EvolutionIssue]:
        return [i for i in self.issues if not i.is_breaking]

    def summary(self) -> str:
        status = "BREAKING" if self.has_breaking_changes else "SAFE"
        lines  = [
            f"Schema Evolution: '{self.old_version}' → '{self.new_version}'",
            f"Status: {status}",
            f"Breaking changes: {len(self.breaking_changes)}",
            f"Safe changes:     {len(self.safe_changes)}",
            "─" * 50,
        ]
        for issue in self.issues:
            lines.append(f"  {issue}")
        return "\n".join(lines)


class SchemaEvolutionChecker:
    """
    Compares two schema versions and classifies changes.

    Usage:
        checker = SchemaEvolutionChecker()
        report  = checker.check(old_schema, new_schema)
        if report.has_breaking_changes:
            raise PipelineBreakingSchemaChange(report.summary())
    """

    def check(
        self,
        old_schema:  StructType,
        new_schema:  StructType,
        old_name:    str = "v_old",
        new_name:    str = "v_new",
    ) -> EvolutionReport:

        report     = EvolutionReport(old_version=old_name, new_version=new_name)
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        # ── Removed columns → always breaking ────────────────────────────────
        for col_name in old_fields:
            if col_name not in new_fields:
                report.issues.append(EvolutionIssue(
                    is_breaking=True,
                    description=(
                        f"Column '{col_name}' was REMOVED "
                        f"(downstream consumers will break)"
                    ),
                ))

        # ── Added columns → safe only if nullable ─────────────────────────────
        for col_name in new_fields:
            if col_name not in old_fields:
                new_field   = new_fields[col_name]
                is_breaking = not new_field.nullable
                report.issues.append(EvolutionIssue(
                    is_breaking=is_breaking,
                    description=(
                        f"Column '{col_name}' ({new_field.dataType}) "
                        f"was ADDED — "
                        + ("NOT NULL: breaking for existing writers"
                           if is_breaking
                           else "nullable: safe addition")
                    ),
                ))

        # ── Changed columns ───────────────────────────────────────────────────
        for col_name in old_fields:
            if col_name not in new_fields:
                continue  # already reported as removed

            old_f = old_fields[col_name]
            new_f = new_fields[col_name]

            # Type change — compare full types, so decimal precision and
            # nested element/field changes are seen too.
            if old_f.dataType.simpleString() != new_f.dataType.simpleString():
                compat, msg = check_type_compatibility(
                    old_f.dataType, new_f.dataType
                )
                is_breaking = compat in (
                    Compatibility.INCOMPATIBLE, Compatibility.COERCIBLE
                )
                report.issues.append(EvolutionIssue(
                    is_breaking=is_breaking,
                    description=(
                        f"Column '{col_name}': type changed "
                        f"{old_f.dataType.simpleString()} → "
                        f"{new_f.dataType.simpleString()}. {msg}"
                    ),
                ))

            # Nullable contract change
            if old_f.nullable and not new_f.nullable:
                report.issues.append(EvolutionIssue(
                    is_breaking=True,
                    description=(
                        f"Column '{col_name}': nullable → NOT NULL "
                        f"(existing NULL values will cause write failures)"
                    ),
                ))
            elif not old_f.nullable and new_f.nullable:
                report.issues.append(EvolutionIssue(
                    is_breaking=False,
                    description=(
                        f"Column '{col_name}': NOT NULL → nullable "
                        f"(relaxed constraint, safe)"
                    ),
                ))

        return report