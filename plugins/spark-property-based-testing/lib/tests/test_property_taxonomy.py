"""
CATEGORY 1 — INVARIANTS
  Properties that hold regardless of input.
  Example: "row count never increases after a filter"

CATEGORY 2 — ROUND-TRIP / IDEMPOTENCY
  Applying an operation twice = applying it once.
  Example: dedup(dedup(df)) == dedup(df)

CATEGORY 3 — METAMORPHIC RELATIONS
  Relationship between outputs when inputs change in a known way.
  Example: if you double the input rows, aggregation results double too.
"""

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pyspark.sql import functions as F


# `spark` fixture: provided by the spark_prop_testing pytest plugin
class TestPropertyTaxonomy:

    # CATEGORY 2: Idempotency of dropDuplicates
    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
    @given(
        rows=st.lists(
            st.fixed_dictionaries({"id": st.integers(min_value=1, max_value=10)}),
            min_size=1, max_size=50
        )
    )
    def test_dedup_is_idempotent(self, spark, rows):
        df = spark.createDataFrame(rows)

        once  = df.dropDuplicates().count()
        twice = df.dropDuplicates().dropDuplicates().count()

        assert once == twice, "dropDuplicates is not idempotent!"

    # CATEGORY 3: Metamorphic — doubling data doubles sum
    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
    @given(
        rows=st.lists(
            st.fixed_dictionaries({
                "amount": st.integers(min_value=0, max_value=1000)
            }),
            min_size=1, max_size=50
        )
    )
    def test_sum_metamorphic_scaling(self, spark, rows):
        df       = spark.createDataFrame(rows)
        doubled  = df.unionAll(df)

        sum_once   = df.agg(F.sum("amount")).collect()[0][0] or 0
        sum_double = doubled.agg(F.sum("amount")).collect()[0][0] or 0

        assert sum_double == 2 * sum_once, (
            f"Doubling data should double sum. Got {sum_double} ≠ 2×{sum_once}"
        )
