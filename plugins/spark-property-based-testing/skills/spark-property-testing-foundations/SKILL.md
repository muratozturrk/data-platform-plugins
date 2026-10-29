---
name: spark-property-testing-foundations
description: "Foundations of property-based testing for PySpark with Hypothesis and the bundled spark_prop_testing library: property taxonomy (invariants, idempotency, metamorphic, oracle, determinism), generation/shrinking/replay, Spark-safe Hypothesis settings and profiles, a deterministic test SparkSession, and flakiness causes. Use when starting property tests for a Spark pipeline, choosing which properties to write, or debugging slow, flaky or unreproducible Hypothesis + Spark tests."
---

# Spark Property-Testing Foundations

An example-based test checks one hand-picked input: `assert dedupe(df_3_rows).count() == 2`.
A property-based test states a rule that must hold for **every** valid input and lets
Hypothesis search for an input that breaks it: "for any orders DataFrame, `dedupe` never
increases the row count and applying it twice changes nothing". When the rule fails,
Hypothesis **shrinks** the input to a minimal counterexample (often 1-2 rows, a null, an
empty string) and stores it so the next run replays it first.

The bundled library (`lib/spark_prop_testing`) supplies DataFrame strategies, standard
transformation properties and Spark-safe Hypothesis settings. This skill covers the concepts
and the test harness; `spark-dataframe-strategies` covers generating the data.

## When to Use

- Writing the first property tests for a transformation, aggregation, join or dedupe step.
- Deciding *what* to assert when there is no single expected output.
- A Spark property test fails with `DeadlineExceeded`, `Flaky`, `FailedHealthCheck`.
- A failure cannot be reproduced, or a test passes locally and fails in CI.
- Assertions that break on row order, float sums, time zones or generated ids.

## Decision table

| Testing need | Technique / API | Reference |
|---|---|---|
| A rule that holds for every input (counts, ranges, keys, schema) | invariant | `references/property_taxonomy.md` |
| Re-running the step must be safe (dedupe, upsert, normalisation) | idempotency `f(f(x)) == f(x)`, round-trip `decode(encode(x)) == x` | `property_taxonomy.md` |
| No oracle, but a known input change has a known output change | metamorphic relation (`totals(df ∪ df) == 2·totals(df)`) | `property_taxonomy.md` |
| A simple/slow/legacy implementation exists | oracle / differential test | `property_taxonomy.md` |
| Result must not depend on partitioning or order | determinism (`f(df.repartition(n)) == f(df)`) | `property_taxonomy.md`, `flakiness_and_determinism.md` |
| Spark tests hit deadlines / health checks | `spark` profile, `deadline=None`, `SPARK_HEALTH_CHECKS` | `references/hypothesis_for_spark.md` |
| Reproduce a failure / reproducible CI | example database, `@seed`, `print_blob`, `spark-ci` profile | `hypothesis_for_spark.md` |
| Fast, deterministic test session | `local[1]`, 1 shuffle partition, UI off, AQE off, UTC | `references/test_session_setup.md` |
| Intermittent failures | sort/multiset compare, `math.isclose`, epoch comparisons, explicit ordering | `references/flakiness_and_determinism.md` |

## Property taxonomy

Five families cover most data-pipeline properties:

- **Invariant**: true for every output, e.g. `clean(df).count() <= df.count()`, no negative
  amounts, keys unique, schema unchanged.
- **Idempotency / round-trip**: `f(f(df)) == f(df)`; `parse(format(x)) == x`.
- **Metamorphic**: relate two runs: duplicating rows doubles sums; filtering more never
  increases a count; adding a row with an unknown key leaves other groups unchanged.
- **Oracle / differential**: compare with a model (plain Python on `df.collect()`,
  the previous implementation, SQL vs DataFrame API).
- **Determinism**: the same input gives the same output regardless of partitioning,
  input order or re-execution.

```python
FAST = settings(settings.get_profile("spark"), max_examples=3)

def test_metamorphic_duplicating_input_doubles_totals(spark):
    """Metamorphic: totals(df ∪ df) == 2 × totals(df) per customer."""
    @FAST
    @given(df=spark_dataframes(spark, ORDERS, CONFIG))
    def check(df):
        single = as_dict(totals(df))
        assert as_dict(totals(df.unionAll(df))) == {k: 2 * v for k, v in single.items()}
    check()
```

Gotchas: a property that restates the implementation proves nothing; pick a relation the
code does not obviously encode. Combine weak properties (count + sum + keys) rather than one
"perfect" one. Bound generated numbers or the property fails on overflow, not on logic.

## Generation, shrinking and replay

`@given` draws inputs from strategies; on failure Hypothesis shrinks toward simpler values
(fewer rows, 0, `""`, `None`), saves the failing choice sequence in the example database
(`.hypothesis/examples`) and replays it first next time. `@seed(n)` fixes the random
generation, `derandomize=True` derives it from the test name, and `print_blob=True` prints an
`@reproduce_failure('<version>', b'...')` line you can paste onto the test.

Gotcha: `derandomize=True` implies `database=None`; the `spark-ci` profile therefore never
saves examples. Commit nothing under `.hypothesis/` and don't share it between Hypothesis versions.

## Hypothesis settings for Spark

Spark actions take hundreds of milliseconds, so the 200 ms default deadline gives
`DeadlineExceeded`/`Flaky` instead of findings. `spark_prop_testing._hypothesis`:

- `register_spark_profiles()` registers `spark` (`deadline=None`, `max_examples=20`,
  `suppress_health_check=[too_slow, data_too_large, large_base_example]`) and `spark-ci`
  (parent `spark`, `max_examples=100`, `derandomize=True`, `print_blob=True`).
- The pytest plugin (`spark_prop_testing.plugin`) registers both and loads `spark` unless
  `--hypothesis-profile` is given.
- `spark_settings(max_examples)` is what `@spark_property` and the library runners use;
  `SPARK_PROP_MAX_EXAMPLES` / `--spark-prop-max-examples` / CLI `-n` override their count.

**Pitfall:** `@settings(suppress_health_check=[HealthCheck.too_slow])` *replaces* the
profile's list (other keys are inherited, this one is not merged). Extend it:
`suppress_health_check=[*SPARK_HEALTH_CHECKS, HealthCheck.filter_too_much]`.

## Test SparkSession

```python
SparkSession.builder.master("local[1]") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.session.timeZone", "UTC").getOrCreate()
```

Session-scoped fixture, created once. The plugin's own `spark` fixture uses
`SPARK_PROP_MASTER` (default `local[2]`), 4 shuffle partitions and 2g driver memory: set
`SPARK_PROP_MASTER=local[1]` on small machines, or define your own `spark` fixture in the test
module or `conftest.py` for full control (UTC, 1 shuffle partition).

## Flakiness

Row order is not part of a DataFrame's contract: compare sorted rows or `exceptAll` both ways.
Float sums depend on partitioning: `math.isclose`/`fsum`. Collected timestamps are naive local
datetimes of the Python process: compare `cast("long")` epoch seconds or fix the session time
zone. `monotonically_increasing_id()` encodes the partition: never assert on its values.
See `references/flakiness_and_determinism.md`.

## When NOT to use property tests

- One fixed business rule with a few known cases (a mapping table): examples are clearer.
- No property beyond "it equals the implementation": use golden/example tests.
- Performance, cluster configuration, I/O connectors, cloud permissions: integration tests.
- The per-example Spark cost makes 20+ examples too slow: test the pure-Python row logic
  with Hypothesis without Spark and keep a few Spark examples.
- Non-deterministic sources (current time, random, external services) not injectable.

Use both: examples document known regressions (add each shrunk counterexample as an example
test, or `@example(...)`), properties hunt for unknown ones.

## Run it

```bash
# PYTHONPATH must contain <plugin>/lib (or pip install -e "<plugin>/lib")
PYSPARK_PYTHON=$(which python) PYTHONPATH=<plugin>/lib \
  python -m pytest -q skills/spark-property-testing-foundations/examples/test_property_taxonomy.py
python -m pytest -p spark_prop_testing.plugin tests/ --hypothesis-profile=spark-ci  # CI
python -m pytest tests/ --spark-prop-max-examples=5                                # quick
```

`-p spark_prop_testing.plugin` is only needed when the library is not pip-installed (the
installed package registers the plugin through the `pytest11` entry point). The examples
register the profiles themselves and run without the plugin. Run one Spark test file at a
time on small machines; on Windows use Python 3.11 with PySpark 3.5.

## Anti-patterns

- `@settings(suppress_health_check=[HealthCheck.too_slow])` alone: drops the other two.
- Leaving the 200 ms deadline (no profile, plain `@settings(max_examples=...)` without plugin).
- `collect()[0]` / `first()` assertions that assume an order.
- Exact `==` on aggregated doubles.
- Unbounded `st.integers()` for an `IntegerType` column, sums of unbounded doubles.
- Creating a SparkSession per example or per test.
- Deleting `.hypothesis/` to make a failure go away instead of reading the replayed example.
- Retrying a flaky test until green instead of removing the non-determinism.

## References

- `references/property_taxonomy.md` - the five families with Spark examples.
- `references/hypothesis_for_spark.md` - generation, shrinking, database, seeds, profiles.
- `references/test_session_setup.md` - session config and fixtures.
- `references/flakiness_and_determinism.md` - causes and stable assertions.
- Examples: `examples/test_property_taxonomy.py`, `test_hypothesis_settings.py`,
  `test_session_setup.py`, `test_flakiness.py`.
