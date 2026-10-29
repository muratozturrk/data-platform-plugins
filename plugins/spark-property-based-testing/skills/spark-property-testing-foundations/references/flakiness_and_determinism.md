# Flakiness and determinism

## Concept

A property test is flaky when the same generated input can pass or fail. Hypothesis notices
this during replay and raises `Flaky`. In Spark the cause is almost always an assertion
that depends on something outside the DataFrame's contract:

| Cause | Why it varies | Stable assertion |
|---|---|---|
| Row order | DataFrames are unordered; order follows partitioning, shuffles, join strategy | compare multisets: sorted `collect()`, `exceptAll` both ways, or keyed dicts |
| `first()`, `collect()[0]`, `limit(1)` without total order | picks whatever row comes first | `orderBy(<unique key>)` first, or assert on all rows |
| Floating point | `(a+b)+c != a+(b+c)`; sum order depends on partitions | `math.isclose(spark_sum, math.fsum(...), rel_tol, abs_tol)`; prefer decimals/integers for money |
| Time zones | collected `TimestampType` values are naive datetimes in the Python process' local zone; `date_format`/`hour` use the session zone | compare epoch seconds (`cast("long")`), set `spark.sql.session.timeZone=UTC` |
| DST | wall-clock times that don't exist or occur twice | keep test data in UTC, compare instants |
| Partition-dependent ids | `monotonically_increasing_id()` = partition index << 33 + row index | assert only uniqueness; use `row_number()` over a total order or a business key |
| Ties in windows | `row_number().over(orderBy(non_unique))` breaks ties arbitrarily | add a tie-breaker column |
| `dropDuplicates(subset)` | keeps an arbitrary row per key | aggregate deterministically (`max_by`, window + tie-breaker) |
| Randomness / current time | `rand()`, `current_timestamp()`, `uuid()` | inject the seed/clock as a parameter |
| Test timing | Hypothesis deadline | `deadline=None` |
| Shared session state | temp views, changed confs, cached tables from another test | unique view names, restore confs, `unpersist` |

Determinism is also a **property** to test: `f(df.repartition(n)) == f(df)` for several `n`.
If it fails, the transformation itself is non-deterministic - a production bug, not a test bug.

## API

No library API is needed; use Spark and the standard library:

```python
sorted(a.collect()) == sorted(b.collect())                     # Row is a tuple: sortable if
                                                                # columns are comparable and non-null
a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0     # multiset equality in Spark
math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-6)
F.col("ts").cast("long")                                        # epoch seconds, no Python datetime
F.row_number().over(Window.partitionBy("k").orderBy("ts", "id"))   # total order
```

The library's own properties already follow these rules: `make_sum_linearity_property` uses
`math.isclose` (`rel_tol=1e-9`, `abs_tol=1e-6`), `no_new_nulls_introduced` aggregates once per
side, and `make_window_rank_property(..., dense=True)` checks contiguous ranks.

`pyspark.testing.assertDataFrameEqual` (3.5) is order-insensitive with float tolerance, but
importing `pyspark.testing` in 3.5 requires pandas and pyarrow.

## Worked example

`examples/test_flakiness.py`:

```python
def test_row_order_compare_as_multisets(spark):
    """Row order: a repartitioned DataFrame has the same rows, compared order-free."""
    ...
        shuffled = df.repartition(3)
        assert sorted(shuffled.collect()) == sorted(df.collect())
        assert shuffled.exceptAll(df).count() == 0 and df.exceptAll(shuffled).count() == 0

def test_float_sum_compared_with_tolerance(spark):
    ...
        spark_sum = df.repartition(2).agg(F.sum("x")).first()[0]
        exact = math.fsum(r["x"] for r in df.collect())
        assert math.isclose(spark_sum, exact, rel_tol=1e-9, abs_tol=1e-6)

def test_timestamps_compared_as_epoch_seconds(spark):
    """Time zones: the instant is zone-independent, the wall-clock text is not."""
    # same df, session zone UTC vs America/New_York:
    # cast("long") identical, date_format("ts", "HH") different

def test_monotonically_increasing_id_depends_on_partitioning(spark):
    df = spark.range(4).repartition(2)
    ids = sorted(r[0] for r in df.select(F.monotonically_increasing_id()).collect())
    assert max(ids) >= 1 << 33            # second partition's ids start at 2**33
```

Generate floats bounded (`st.floats(-1e6, 1e6, allow_nan=False)`): the default `DoubleType`
strategy spans the full double range, so sums overflow to `inf` and `isclose(inf, inf)` hides
real differences. NaN is excluded by the library's float strategies.

### Windows-specific timestamp trap

PySpark 3.5 converts collected `TimestampType` values with `datetime.fromtimestamp()`, which
raises `OSError: [Errno 22]` on Windows for instants before 1970 (that is why the library's
timestamp strategy starts at 1970-01-02 UTC). If a test needs older instants via a
`custom_strategy`, compare in Spark (`cast("long")`, `date_format`) instead of collecting raw
timestamps - as `test_timestamps_compared_as_epoch_seconds` does with a 1900-2100 range.

## Pitfalls / troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Flaky: Hypothesis ... produces unreliable results` | assertion depends on order/time/partitions | find the non-determinism; never just retry |
| Passes locally, fails in CI | different host time zone, cores, Spark version | UTC session zone, fixed `local[n]`, pin versions |
| `sorted(collect())` raises `TypeError` | `None` mixed with values, or map columns | sort with a key: `key=lambda r: tuple((v is None, v) for v in r)`, or `exceptAll` |
| `exceptAll` raises on map columns | maps not comparable in set operations | compare collected rows / `map_entries` sorted |
| Off by 0.0000001 | float tolerance missing | `math.isclose` with `abs_tol` for sums near zero |
| Hour differs by 1-3 | session/host time zone | UTC, compare epoch seconds |
| `OSError [Errno 22]` on collect (Windows) | pre-1970 timestamp (custom strategy) → `fromtimestamp` | compare in Spark or keep ≥ 1970 |
| Duplicate-key result differs between runs | `dropDuplicates`/window ties | deterministic tie-breaker |

## Related topics

- `property_taxonomy.md` - determinism as a property.
- `test_session_setup.md` - session settings that remove host dependence.
- Skill `spark-dataframe-strategies` - bounding floats, timestamps and unique keys.
