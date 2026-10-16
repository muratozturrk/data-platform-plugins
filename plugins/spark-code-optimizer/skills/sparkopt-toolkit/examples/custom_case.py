"""Custom sparkopt rule: warn on high-null join keys and emit the fix.

Run (no Spark needed):
    python custom_case.py
It registers the rule, builds a profile by hand, and checks the case and the
composition operators with asserts.
"""
from __future__ import annotations

import os
import sys

# <plugin>/scripts on sys.path so `import sparkopt` resolves to the bundled copy
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts"))

from sparkopt import CaseRegistry, OptimizationCase, Priority  # noqa: E402
from sparkopt.cases import AdaptiveCase, BroadcastJoinCase, SkewJoinCase  # noqa: E402
from sparkopt.codegen.context import CodegenContext, CodeRenderer  # noqa: E402
from sparkopt.core.analyzer import ColumnProfile, DataProfile, PartitionProfile  # noqa: E402


class NullJoinKeyCase(OptimizationCase):
    """Null join keys never match and all hash to one partition (a skew source)."""

    name = "null_join_key"
    priority = Priority.HIGH

    def __init__(self, col: str, threshold: float = 0.1):
        self.col, self.threshold = col, threshold

    def matches(self, profile: DataProfile) -> bool:
        cp = profile.col(self.col)
        return cp is not None and cp.null_ratio > self.threshold

    def apply(self, ctx: CodegenContext) -> None:
        ratio = ctx.profile.col(self.col).null_ratio
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_warning(f"'{self.col}' null ratio {ratio:.1%}: null keys pile up in one join partition")
        ctx.add_hint(self.name, f'''
# Join null keys separately (inner join drops them anyway; left join keeps them unmatched)
keyed   = left_df.filter(F.col("{self.col}").isNotNull())
no_key  = left_df.filter(F.col("{self.col}").isNull())
result  = keyed.join(right_df, on="{self.col}", how="left").unionByName(no_key, allowMissingColumns=True)
'''.strip())

    def explain(self) -> str:
        return f"'{self.col}' null ratio above {self.threshold:.0%}: split null keys before the join."


def _profile(null_ratio: float, skew: float, mb: int) -> DataProfile:
    return DataProfile(
        row_count=20_000_000, estimated_bytes=mb * 1024 ** 2,
        partition=PartitionProfile(count=200, skew_ratio=skew),
        columns={"user_id": ColumnProfile("user_id", "long", null_ratio=null_ratio)},
    )


if __name__ == "__main__":
    case = NullJoinKeyCase("user_id", threshold=0.1)
    assert case.matches(_profile(0.3, 1.0, 5000))
    assert not case.matches(_profile(0.01, 1.0, 5000))

    ctx = CodegenContext(profile=_profile(0.3, 1.0, 5000), join_cols=["user_id"])
    case.apply(ctx)
    code = CodeRenderer(ctx).render()
    assert "isNull()" in code and "null ratio" in code

    # "small table -> broadcast; OR skewed -> salt; AQE in both cases"
    strategy = ((~SkewJoinCase() & BroadcastJoinCase()) | SkewJoinCase()) & AdaptiveCase()
    assert strategy.matches(_profile(0.0, 1.0, 50))        # small, not skewed
    assert strategy.matches(_profile(0.0, 6.0, 50_000))    # big but skewed
    assert not strategy.matches(_profile(0.0, 1.0, 50_000))  # big, balanced

    CaseRegistry.instance().register(case)  # now also picked up by SparkOptimizer.build()
    print("custom_case: ok")
