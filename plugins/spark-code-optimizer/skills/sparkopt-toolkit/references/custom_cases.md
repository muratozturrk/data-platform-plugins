# Writing custom sparkopt cases

## Contract

```python
from sparkopt import OptimizationCase, Priority

class MyCase(OptimizationCase):
    name = "my_case"              # unique; also the --exclude / add_case replacement key
    priority = Priority.HIGH      # LOW=10, MEDIUM=50, HIGH=90, CRITICAL=100

    def matches(self, profile) -> bool: ...   # pure function of DataProfile
    def apply(self, ctx) -> None: ...         # write into CodegenContext
    def explain(self) -> str: ...             # one sentence, no profile access
```

`CodegenContext` writers:

| Method | Rendered as |
| --- | --- |
| `add_import(stmt)` | import block (deduplicated) |
| `add_config(key, value)` | `spark.conf.set(...)`; launch-time keys (`spark.memory.*`, `spark.serializer`, `spark.executor.*`, `spark.driver.*`…) are rendered as `--conf` comments |
| `add_transformation(key, code)` | "DataFrame dönüşümleri" section |
| `add_hint(key, code)` | "Optimizasyon önerileri" section |
| `add_note(key, text)` | notes section |
| `add_warning(msg)` | warnings section |

The keys are dict keys: two cases writing the same key overwrite each other. That is intended for
shared concerns, but choose a unique key otherwise.

## Rules of thumb

- `matches()` must not touch Spark. It runs offline in the CLI and in tests.
- `explain()` can't see the profile. Don't print profile numbers there (original sparkopt's
  skew_join and fanout_cache did and printed wrong values; the bundled copy fixes both). Put numbers
  in `apply()`, which has `ctx.profile`.
- Generated code must be valid PySpark. Use only PySpark StorageLevels, and remember that
  `localCheckpoint` needs no checkpoint dir.
- If the template is an f-string, escape literal braces as `{{ }}`.
- Every threshold you add is a heuristic: name it, and make it a constructor parameter.

## Composition

| Expression | matches | apply | priority |
| --- | --- | --- | --- |
| `a & b` | both | both | max(a, b) |
| `a \| b` | either | whichever sides match | max(a, b) |
| `~a` | not a | nothing (filter only) | a |

```python
from sparkopt.cases import BroadcastJoinCase, SkewJoinCase, AdaptiveCase
join_strategy = ((~SkewJoinCase() & BroadcastJoinCase()) | SkewJoinCase()) & AdaptiveCase()
```

Composite names are `and_case` / `or_case` / `not_case`, so `--exclude` can't target them individually.

## Registering

```python
from sparkopt import CaseRegistry
CaseRegistry.instance().register(MyCase())       # global: picked up by SparkOptimizer.build()
SparkOptimizer(df, spark).add_case(MyCase())     # one run only
```

The CLI evaluates the built-in list (narrowed to the named built-ins when `--case <builtin>` is
given) plus any `--case <complex>` opt-ins. To use a custom case offline, import
`sparkopt_cli` and pass a profile to your case directly (see `examples/custom_case.py`), or add it to
`OPT_IN_CASES` in a local copy.

## Testing

Build a `DataProfile` by hand and assert on `matches()` and on the rendered code. pyspark is not
needed:

```python
p = DataProfile(row_count=20_000_000, estimated_bytes=5 * 1024**3,
                partition=PartitionProfile(count=200, skew_ratio=6.0))
assert MyCase().matches(p)
ctx = CodegenContext(profile=p); MyCase().apply(ctx)
assert "expected snippet" in CodeRenderer(ctx).render()
```
