# spark_prop_testing/transformations/standard_properties.py

"""
A library of reusable TransformationProperty functions
covering the most common invariant categories.

Each function has signature: (input_df, output_df) -> None
and raises AssertionError on violation.
"""

import math
from collections import Counter
from typing import Callable, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ══════════════════════════════════════════════════════════════
# CATEGORY 1: ROW COUNT PROPERTIES
# ══════════════════════════════════════════════════════════════

def row_count_non_increasing(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: Filtering/aggregating transformations must not ADD rows.
    Fails if output has MORE rows than input.
    """
    in_count  = input_df.count()
    out_count = output_df.count()

    assert out_count <= in_count, (
        f"row_count_non_increasing violated: "
        f"input={in_count}, output={out_count} "
        f"(output has {out_count - in_count} extra rows)"
    )


def row_count_non_decreasing(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: Enrichment/join transformations must not LOSE rows.
    Fails if output has FEWER rows than input.
    """
    in_count  = input_df.count()
    out_count = output_df.count()

    assert out_count >= in_count, (
        f"row_count_non_decreasing violated: "
        f"input={in_count}, output={out_count} "
        f"(output lost {in_count - out_count} rows)"
    )


def row_count_exactly_preserved(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: Pure column transformations (withColumn, cast, rename)
    must preserve exact row count.
    """
    in_count  = input_df.count()
    out_count = output_df.count()

    assert out_count == in_count, (
        f"row_count_exactly_preserved violated: "
        f"input={in_count}, output={out_count}"
    )


def output_is_non_empty_when_input_is_non_empty(
    input_df: DataFrame,
    output_df: DataFrame,
):
    """
    PROPERTY: A transformation on non-empty data must produce non-empty output.
    Catches transformations that accidentally drop all rows.
    """
    if input_df.count() > 0:
        assert output_df.count() > 0, (
            "Transformation produced empty output from non-empty input"
        )


# ══════════════════════════════════════════════════════════════
# CATEGORY 2: COLUMN PRESERVATION PROPERTIES
# ══════════════════════════════════════════════════════════════

def input_columns_preserved(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: All input columns must still exist in the output.
    Catches transformations that accidentally drop columns via select().
    """
    input_cols  = set(input_df.columns)
    output_cols = set(output_df.columns)
    missing     = input_cols - output_cols

    assert not missing, (
        f"input_columns_preserved violated: "
        f"columns dropped by transformation: {sorted(missing)}"
    )


def no_new_nulls_introduced(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: A transformation must not introduce nulls into columns
    that had no nulls in the input.
    Catches silent null introduction from bad joins or UDFs.
    """
    shared = [c for c in input_df.columns if c in output_df.columns]
    if not shared:
        return
    # One aggregation per side instead of two jobs per column.
    in_nulls  = _null_counts(input_df, shared)
    out_nulls = _null_counts(output_df, shared)

    for col_name in shared:
        if in_nulls[col_name] == 0:
            assert out_nulls[col_name] == 0, (
                f"no_new_nulls_introduced violated on column '{col_name}': "
                f"input had 0 nulls, output has {out_nulls[col_name]}"
            )


def _null_counts(df: DataFrame, columns: List[str]) -> dict:
    row = df.agg(*[
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ]).collect()[0]
    return {c: (row[c] or 0) for c in columns}


def column_types_preserved(input_df: DataFrame, output_df: DataFrame):
    """
    PROPERTY: Transformation must not silently change column types.
    """
    input_types  = {f.name: f.dataType for f in input_df.schema.fields}
    output_types = {f.name: f.dataType for f in output_df.schema.fields}

    for col_name, in_type in input_types.items():
        if col_name in output_types:
            out_type = output_types[col_name]
            # Compare the full type, so decimal(10,2) → decimal(38,2) or
            # array<int> → array<bigint> also count as drift.
            assert in_type.simpleString() == out_type.simpleString(), (
                f"column_types_preserved violated on '{col_name}': "
                f"type changed {in_type.simpleString()} → {out_type.simpleString()}"
            )


# ══════════════════════════════════════════════════════════════
# CATEGORY 3: AGGREGATION PROPERTIES
# ══════════════════════════════════════════════════════════════

Transformation = Callable[[DataFrame], DataFrame]


def _sum(df: DataFrame, column: str) -> float:
    return float(df.agg(F.sum(column)).collect()[0][0] or 0.0)


def _close(a: float, b: float, rel_tol: float, abs_tol: float) -> bool:
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)


def make_sum_linearity_property(
    sum_column:    str,
    group_columns: Optional[List[str]] = None,
    transform:     Optional[Transformation] = None,
    rel_tol:       float = 1e-9,
    abs_tol:       float = 1e-6,
) -> "TransformationProperty":
    """
    FACTORY: SUM linearity (metamorphic) or SUM conservation.

    With ``transform`` (the function under test):
        sum(f(A ∪ B)) == sum(f(A)) + sum(f(B))    for a split A/B of the input
      i.e. the aggregation has no cross-row dependencies.

    Without ``transform``:
        sum(output[sum_column]) == sum(input[sum_column])
      i.e. the transformation neither creates nor loses the measure
      (re-grouping, re-shaping, joins that must not fan out).

    Floats are compared with math.isclose(rel_tol, abs_tol); an exact
    comparison of floating-point sums fails on summation order alone.
    ``group_columns`` is accepted for backwards compatibility and unused.
    """

    def sum_linearity(input_df: DataFrame, output_df: DataFrame):
        if transform is None:
            in_sum, out_sum = _sum(input_df, sum_column), _sum(output_df, sum_column)
            assert _close(in_sum, out_sum, rel_tol, abs_tol), (
                f"sum conservation violated on '{sum_column}': "
                f"input={in_sum}, output={out_sum}"
            )
            return

        if input_df.count() < 2:
            return
        # Deterministic split on the data itself (not on partition-dependent ids).
        bucket = F.abs(F.hash(*input_df.columns)) % 2
        part_a = input_df.filter(bucket == 0)
        part_b = input_df.filter(bucket == 1)
        whole  = _sum(output_df, sum_column)
        parts  = _sum(transform(part_a), sum_column) + _sum(transform(part_b), sum_column)
        assert _close(whole, parts, rel_tol, abs_tol), (
            f"sum_linearity violated on '{sum_column}': "
            f"sum(f(A∪B))={whole}, sum(f(A))+sum(f(B))={parts}"
        )

    sum_linearity.__name__ = (
        f"sum_linearity[{sum_column}]" if transform else f"sum_conservation[{sum_column}]"
    )
    return sum_linearity


def make_aggregation_monotonicity_property(
    agg_column: str,
    transform:  Transformation,
    agg_fn:     str = "sum",     # "sum" (non-negative values), "count", "max", "min"
) -> "TransformationProperty":
    """
    FACTORY: monotonicity of an aggregate under adding input rows.

    PROPERTY: for input X and a superset X ∪ {r} (r = a copy of an existing
    row), agg(f(X ∪ {r})) >= agg(f(X)) for count/max and for sum over
    non-negative values, and <= for min.
    """
    if agg_fn not in {"sum", "count", "max", "min"}:
        raise ValueError(f"agg_fn must be sum/count/max/min, got {agg_fn!r}")
    agg_func = getattr(F, agg_fn)

    def aggregation_monotonicity(input_df: DataFrame, output_df: DataFrame):
        if input_df.count() == 0:
            return
        before = output_df.agg(agg_func(agg_column)).collect()[0][0]
        after  = transform(input_df.unionByName(input_df.limit(1))).agg(
            agg_func(agg_column)).collect()[0][0]
        if before is None or after is None:
            return
        if agg_fn == "min":
            assert after <= before, (
                f"aggregation_monotonicity [min] violated on '{agg_column}': "
                f"adding a row increased min from {before} to {after}"
            )
        else:
            assert after >= before, (
                f"aggregation_monotonicity [{agg_fn}] violated on '{agg_column}': "
                f"adding a row decreased {agg_fn} from {before} to {after}"
            )

    aggregation_monotonicity.__name__ = f"aggregation_monotonicity[{agg_fn}({agg_column})]"
    return aggregation_monotonicity


# ══════════════════════════════════════════════════════════════
# CATEGORY 4: IDEMPOTENCY PROPERTIES
# ══════════════════════════════════════════════════════════════

def make_idempotency_property(
    key_columns: List[str],
    transform:   Optional[Transformation] = None,
) -> "TransformationProperty":
    """
    FACTORY: idempotency of a deduplicating / normalizing transformation.

    Always checks: the output has no duplicate rows for ``key_columns``.
    With ``transform``: additionally f(f(x)) == f(x) — same row count and the
    same multiset of rows (compared with exceptAll in both directions).
    """

    def idempotency(input_df: DataFrame, output_df: DataFrame):
        total    = output_df.count()
        distinct = output_df.select(*key_columns).distinct().count()
        assert total == distinct, (
            f"idempotency violated: output has duplicate keys in {key_columns} "
            f"({distinct} unique vs {total} rows)"
        )
        if transform is None:
            return
        twice = transform(output_df)
        assert twice.count() == total, (
            f"idempotency violated: f(f(x)) has {twice.count()} rows, f(x) has {total}"
        )
        common = [c for c in output_df.columns if c in twice.columns]
        # ponytail: multiset compare on the driver (exceptAll hits a Spark 3.5
        # analyzer bug after subset dropDuplicates); fine for test-sized data.
        once_rows  = Counter(tuple(r) for r in output_df.select(*common).collect())
        twice_rows = Counter(tuple(r) for r in twice.select(*common).collect())
        extra   = sum((twice_rows - once_rows).values())
        missing = sum((once_rows - twice_rows).values())
        assert extra == 0 and missing == 0, (
            f"idempotency violated: f(f(x)) differs from f(x) "
            f"({extra} rows only in f(f(x)), {missing} rows only in f(x))"
        )

    idempotency.__name__ = f"idempotency[{','.join(key_columns)}]"
    return idempotency


# ══════════════════════════════════════════════════════════════
# CATEGORY 5: JOIN PROPERTIES
# ══════════════════════════════════════════════════════════════

def make_left_join_preserves_left_keys(
    join_key: str,
) -> "TransformationProperty":
    """
    FACTORY: Returns a property verifying LEFT JOIN key preservation.

    PROPERTY: Every non-null key from the left table must appear in the join
    result. Catches accidental INNER JOIN masquerading as LEFT JOIN.
    """
    def left_join_key_preservation(input_df: DataFrame, output_df: DataFrame):
        input_keys  = input_df.select(join_key).where(F.col(join_key).isNotNull()).distinct()
        output_keys = output_df.select(join_key).distinct()
        missing     = input_keys.subtract(output_keys)
        n_missing   = missing.count()
        assert n_missing == 0, (
            f"left_join_key_preservation violated: "
            f"{n_missing} keys from left table missing in output. "
            f"Sample missing: {[r[0] for r in missing.limit(5).collect()]}"
        )

    left_join_key_preservation.__name__ = (
        f"left_join_key_preservation[{join_key}]"
    )
    return left_join_key_preservation


# ══════════════════════════════════════════════════════════════
# CATEGORY 6: WINDOW FUNCTION PROPERTIES
# ══════════════════════════════════════════════════════════════

def make_window_rank_property(
    partition_col: str,
    rank_col:      str,
    dense:         bool = False,
) -> "TransformationProperty":
    """
    FACTORY: Returns a property verifying window rank correctness.

    PROPERTY: Within each partition, ranks are
      1. >= 1
      2. starting from 1
      3. contiguous (``dense=True``, for dense_rank / row_number):
         max(rank) == number of distinct ranks
    """
    def window_rank_validity(input_df: DataFrame, output_df: DataFrame):
        if rank_col not in output_df.columns:
            return  # rank column not yet added — skip

        negative_ranks = output_df.filter(F.col(rank_col) < 1).count()
        assert negative_ranks == 0, (
            f"window_rank_validity: found {negative_ranks} "
            f"ranks < 1 in column '{rank_col}'"
        )

        stats = output_df.groupBy(partition_col).agg(
            F.min(rank_col).alias("min_rank"),
            F.max(rank_col).alias("max_rank"),
            F.countDistinct(rank_col).alias("n_ranks"),
        )
        not_from_one = stats.filter(F.col("min_rank") != 1).count()
        assert not_from_one == 0, (
            f"window_rank_validity: {not_from_one} partitions "
            f"don't start at rank 1 in '{partition_col}'"
        )
        if dense:
            gaps = stats.filter(F.col("max_rank") != F.col("n_ranks")).count()
            assert gaps == 0, (
                f"window_rank_validity: {gaps} partitions have gaps in '{rank_col}'"
            )

    window_rank_validity.__name__ = (
        f"window_rank_validity[partition={partition_col}, rank={rank_col}]"
    )
    return window_rank_validity
