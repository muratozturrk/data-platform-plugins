
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

# ── Shared session fixture (pytest scope)
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Ch1Tests")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

# ── Property 1: Row count is preserved after a pure projection ───────────────
class TestRowCountPreservation:
    """
    PROPERTY: Selecting a subset of columns never changes row count.
    This seems obvious — but projection bugs DO occur with explode(),
    lateral views, and certain UDFs. Testing it explicitly catches regressions.
    """

    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
    @given(
        rows=st.lists(
            st.fixed_dictionaries({
                "id":    st.integers(min_value=1, max_value=10_000),
                "name":  st.text(min_size=1, max_size=50),
                "score": st.floats(min_value=0.0, max_value=100.0,
                                   allow_nan=False, allow_infinity=False),
            }),
            min_size=1,
            max_size=200,
        )
    )
    def test_projection_preserves_row_count(self, spark, rows):
        schema = StructType([
            StructField("id",    IntegerType(), nullable=False),
            StructField("name",  StringType(),  nullable=True),
            StructField("score", DoubleType(),  nullable=True),
        ])

        df = spark.createDataFrame(rows, schema=schema)
        original_count = df.count()

        projected_df = df.select("id", "name")   # drop 'score'
        assert projected_df.count() == original_count, (
            f"Projection changed row count: {original_count} → {projected_df.count()}"
        )

# ── Property 2: Filter is a subset operation ─────────────────────────────────
class TestFilterSubsetProperty:
    """
    PROPERTY: A filtered DataFrame is always a subset of the original.
    Formally: len(filter(df, pred)) <= len(df)
    AND every row in filtered_df exists in original df.
    """

    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
    @given(
        rows=st.lists(
            st.fixed_dictionaries({
                "id":    st.integers(min_value=1, max_value=500),
                "value": st.integers(min_value=0, max_value=100),
            }),
            min_size=1,
            max_size=100,
        )
    )
    def test_filter_is_subset(self, spark, rows):
        df = spark.createDataFrame(rows)

        filtered_df = df.filter(F.col("value") > 50)
        filtered_count = filtered_df.count()
        original_count = df.count()

        # ── Assertion 1: count constraint ────────────────────────────────────
        assert filtered_count <= original_count

        # ── Assertion 2: all filtered rows satisfy the predicate ─────────────
        # If any row has value <= 50, the filter is broken
        violations = filtered_df.filter(F.col("value") <= 50).count()
        assert violations == 0, (
            f"Filter let through {violations} rows that violate the predicate!"
        )

# ── Property 3: Union row count additivity ───────────────────────────────────
class TestUnionAdditivity:
    """
    PROPERTY: UNION ALL row count = sum of individual counts.
    Catches bugs in deduplication logic accidentally applied to unions.
    """

    @settings(suppress_health_check=[HealthCheck.too_slow], max_examples=15)
    @given(
        rows_a=st.lists(
            st.fixed_dictionaries({"id": st.integers(min_value=-2**31, max_value=2**31 - 1), "val": st.text()}),
            min_size=0, max_size=50
        ),
        rows_b=st.lists(
            st.fixed_dictionaries({"id": st.integers(min_value=-2**31, max_value=2**31 - 1), "val": st.text()}),
            min_size=0, max_size=50
        ),
    )
    def test_union_all_count_additivity(self, spark, rows_a, rows_b):
        schema = StructType([
            StructField("id",  IntegerType(), True),
            StructField("val", StringType(),  True),
        ])

        df_a = spark.createDataFrame(rows_a, schema=schema)
        df_b = spark.createDataFrame(rows_b, schema=schema)

        union_df    = df_a.unionAll(df_b)
        union_count = union_df.count()
        expected    = df_a.count() + df_b.count()

        assert union_count == expected, (
            f"Union count {union_count} ≠ {df_a.count()} + {df_b.count()} = {expected}"
        )

