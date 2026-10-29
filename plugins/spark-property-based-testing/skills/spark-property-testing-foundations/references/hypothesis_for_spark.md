# Hypothesis for Spark: generation, shrinking, replay, settings

## Concept

`@given(df=strategy)` turns a test into a search. Hypothesis draws a choice sequence (a stream
of bounded integers/floats/booleans), builds inputs from it through strategies, and runs the
test up to `max_examples` times. Generation is biased toward boundaries (0, -1, max, empty,
`""`, duplicates) rather than uniform.

On failure:

1. **Shrink** - mutate the choice sequence toward "simpler" (shorter lists, smaller numbers,
   earlier `one_of` branches, `None` in the library's null strategies) and keep each mutation
   that still fails. For a DataFrame this means fewer rows and minimal column values.
2. **Save** - store the shrunk sequence in the example database (`.hypothesis/examples/`,
   keyed by the test's identity).
3. **Report** - `Falsifying example: check(df=...)` plus your assertion error. A DataFrame
   prints as `DataFrame[col: type, ...]`, so print/`note()` the rows you need:
   `hypothesis.note(df.collect())` is shown only for the final failing example.
4. **Replay** - next run replays saved failures first, so a fixed bug is confirmed instantly
   and an unfixed one fails immediately.

## API

| Tool | Effect |
|---|---|
| `@settings(max_examples=n)` | examples per test (Spark: 3-20 locally, more in CI) |
| `@settings(deadline=None)` | no per-example time limit (required for Spark) |
| `@settings(suppress_health_check=[...])` | silence named health checks (replaces the inherited list) |
| `@settings(database=None)` | no saving/replay (scratch runs, demos) |
| `@settings(derandomize=True)` | deterministic generation from the test name; implies `database=None` |
| `@settings(print_blob=True)` | failure prints `@reproduce_failure('6.x', b'...')` |
| `@seed(1234)` | fixed random seed for one test |
| `@reproduce_failure(version, blob)` | replay exactly one failing example (temporary, version-bound) |
| `@example(...)` | always run this explicit input (regression anchor) |
| `note(x)`, `event(label)` | annotate the failing example / collect statistics (`--hypothesis-show-statistics`) |
| `find(strategy, predicate)` | return the minimal value satisfying predicate (exploration) |

Library (`spark_prop_testing._hypothesis`):

```python
SPARK_HEALTH_CHECKS = [HealthCheck.too_slow, HealthCheck.data_too_large, HealthCheck.large_base_example]
MAX_EXAMPLES_ENV = "SPARK_PROP_MAX_EXAMPLES"

register_spark_profiles() -> None
#   "spark":    deadline=None, suppress_health_check=SPARK_HEALTH_CHECKS, max_examples=20
#   "spark-ci": parent=spark, max_examples=100, derandomize=True, print_blob=True
effective_max_examples(requested: Optional[int]) -> int   # env var wins, else requested, else 20
spark_settings(max_examples: Optional[int] = None) -> settings
```

The pytest plugin calls `register_spark_profiles()` in `pytest_configure` and loads `spark`
unless `--hypothesis-profile` names another. `--spark-prop-max-examples N` sets the env var,
which affects the **library runners** (`@spark_property`, `TransformationPropertyTester`,
`DataQualityPropertySuite`), not hand-written `@settings(max_examples=...)`; for those, use the
`prop_max_examples` fixture or a profile.

## Why the defaults break on Spark

| Default | Spark effect | Setting |
|---|---|---|
| `deadline=200ms` | first action after JVM start takes seconds; timing varies → `DeadlineExceeded`, then `Flaky` when the replay is fast | `deadline=None` |
| `too_slow` health check | data generation + DataFrame creation is slow | suppressed |
| `data_too_large` | DataFrames with many rows/columns consume a lot of choice data | suppressed |
| `large_base_example` | `min_rows` in the hundreds makes the smallest example large | suppressed |
| `max_examples=100` | 100 × Spark actions = minutes | 20 locally, 100 in CI |

## Worked example

```python
from hypothesis import HealthCheck, given, settings
from spark_prop_testing._hypothesis import SPARK_HEALTH_CHECKS, register_spark_profiles

register_spark_profiles()                       # idempotent; not needed with the plugin loaded
FAST = settings(settings.get_profile("spark"), max_examples=3)   # parent + one override

# WRONG: drops data_too_large and large_base_example
bad = settings(settings.get_profile("spark"), suppress_health_check=[HealthCheck.filter_too_much])
# RIGHT
ok = settings(settings.get_profile("spark"),
              suppress_health_check=[*SPARK_HEALTH_CHECKS, HealthCheck.filter_too_much])
```

`settings(...)` without an explicit parent inherits from the profile that is *loaded at
decoration time*; with the plugin active that is `spark`. Passing the profile as parent makes
the test independent of whether the plugin was loaded.

Reproducing a CI failure locally:

1. CI uses `--hypothesis-profile=spark-ci` → `print_blob=True` prints `@reproduce_failure(...)`.
2. Paste that decorator on the test (same Hypothesis version), run, fix, remove it.
3. Add the shrunk input as a permanent `@example(...)` or a plain example test.

`examples/test_hypothesis_settings.py` asserts all of the above (profiles, inheritance,
the replacement pitfall, `find` shrinking `sum(xs) > 100` to `[101]`, `@seed` replay,
the reproduce blob in the failure notes) and shows `@spark_property`.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `DeadlineExceeded` / `Flaky: ... unreliable test timings` | default deadline active | load `spark` profile or `deadline=None` |
| `FailedHealthCheck: large_base_example` despite profile | explicit `suppress_health_check` replaced the list | `[*SPARK_HEALTH_CHECKS, ...]` |
| `FailedHealthCheck: function_scoped_fixture` | `@given` on a test using function-scoped fixtures | session-scoped `spark`, or `@given` on an inner function |
| Failure gone on rerun in CI | `derandomize` → no database; or different seed | use `print_blob` output; add `@example` |
| Old failure keeps replaying after a schema change | stale database entry | it's discarded when it no longer fits; delete `.hypothesis/` only if needed |
| `Unsatisfiable` / `filter_too_much` | `.filter()`/`assume()` rejects most data | constrain the strategy instead (bounds, `sampled_from`, `flatmap`) |
| Tests take minutes | `max_examples × rows × actions` | fewer examples locally, pure-Python tests for row logic |
| `fixture 'df' not found` | old decorator behaviour | the bundled `@spark_property` hides `df` from pytest |

## Related topics

- `test_session_setup.md` - fixture scope and session configuration.
- `flakiness_and_determinism.md` - when the *test* is non-deterministic, not Hypothesis.
- Skill `spark-dataframe-strategies` - shrinking behaviour of null and skew strategies.
