# spark_dataframes, DataFrameConfig, ColumnConfig, profiles

## Concept

`spark_dataframes(spark, schema, config)` builds a strategy in three steps:

1. **Row strategy** - `st.fixed_dictionaries({col: strategy})`, one entry per schema field:
   the column's `custom_strategy` or `strategy_for_type(dtype)`; nullable columns with
   `null_rate == 1.0` become `st.none()`.
2. **Rows** - `st.lists(row, min_size=min_rows, max_size=max_rows, unique_by=<unique columns>)`;
   then fractional null rates (per DataFrame, seeded `Random`) and `always_include_null`.
3. **DataFrame** - `spark.createDataFrame(rows, schema=schema)` (empty list → empty DataFrame
   with the schema).

Shrinking acts on step 2: fewer rows, simpler values, nulls. The DataFrame is created once per
example (and again for every shrink attempt), which is where the time goes.

## API (from `strategies/dataframe_strategy.py`)

```python
@dataclass
class ColumnConfig:
    null_config:     NullConfig = NullConfig()            # default: no nulls
    custom_strategy: Optional[st.SearchStrategy] = None   # replaces the type strategy
    unique:          bool = False                         # no duplicate values (keys)

@dataclass
class DataFrameConfig:
    min_rows:       int = 1
    max_rows:       int = 100
    column_configs: Dict[str, ColumnConfig] = {}          # keyed by column name
    default_null_config: Optional[NullConfig] = None      # nulls for unconfigured nullable columns

    def column(self, name: str) -> ColumnConfig           # effective config for a column

def spark_dataframes(spark: SparkSession, schema: StructType,
                     config: DataFrameConfig = None) -> st.SearchStrategy[DataFrame]
```

Rules:

- Keys of `column_configs` that are not in the schema are silently ignored - check spelling.
- `unique=True` on several columns: each column is unique **independently**, not the
  combination. For a composite key, generate rows yourself (see `composing_related_data.md`).
- `unique=True` + nullable + null rate → at most one null in that column.
- `NullConfig` on a `nullable=False` field has no effect.
- `default_null_config` applies only to nullable columns **without** a `column_configs` entry;
  `config.column(name)` returns `column_configs[name]`, else
  `ColumnConfig(null_config=default_null_config)`, else `ColumnConfig()`.
- Numeric bounds, value sets and formats go through `custom_strategy`.
- `@spark_property(schema, config=None, max_examples=20, spark=None)` uses
  `DataFrameConfig(min_rows=1, max_rows=100)` when `config` is omitted.

## Profiles (`strategies/profiles.py`)

| Function | Returns | Notes |
|---|---|---|
| `edge_case_profile()` | `DataFrameConfig(min_rows=1, max_rows=10)` | tiny frames incl. a single row; boundary *values* still need column strategies |
| `high_null_profile(null_rate=0.4)` | 50-500 rows, `default_null_config=NullConfig(null_rate)` | every nullable column without its own config gets the rate |
| `production_like_profile(id_cardinality=1000, id_columns=(), null_rate=0.02)` | 100-1000 rows; `column_configs` = `realistic_id_strategy(max_id=id_cardinality)` per id column; `default_null_config=NullConfig(null_rate)` | repeated keys for joins/group-bys, background nulls elsewhere (id columns get none) |
| `stress_test_profile()` | `DataFrameConfig(min_rows=1_000, max_rows=10_000)` | seconds per example; volume is better served by `spark.range` |

Override or add columns on the returned config; an explicit entry replaces the default nulls
for that column:

```python
from dataclasses import replace
from spark_prop_testing.strategies.profiles import production_like_profile

cfg = production_like_profile(id_cardinality=20, id_columns=["user_id"], null_rate=0.05)
cfg.column_configs["amount"] = ColumnConfig(custom_strategy=st.integers(0, 1_000),
                                            null_config=NullConfig(0.05))
cfg = replace(cfg, min_rows=1, max_rows=40)       # smaller frames for logic tests
cfg.column("user_id")      # effective ColumnConfig (custom id strategy, no nulls)
cfg.column("comment")      # ColumnConfig(null_config=NullConfig(0.05)) from the default
```

## Worked example

`examples/test_dataframe_config.py`:

```python
SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("qty",      IntegerType(), nullable=False),
])
CONFIG = DataFrameConfig(min_rows=2, max_rows=15, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 20), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["new", "paid", "shipped"])),
    "qty":      ColumnConfig(custom_strategy=st.integers(1, 99),
                             null_config=NullConfig(null_rate=1.0)),   # no effect: NOT NULL
})

def test_config_is_honoured(spark):
    @FAST
    @given(df=spark_dataframes(spark, SCHEMA, CONFIG))
    def check(df):
        rows = df.collect()
        assert 2 <= len(rows) <= 15
        ids = [r.order_id for r in rows]
        assert len(ids) == len(set(ids))
    check()
```

Note the domain: 15 unique ids drawn from 1..20 works; `unique=True` with a domain smaller than
`min_rows` makes generation impossible (`Unsatisfiable`/health-check failure).

## Performance

| Lever | Effect |
|---|---|
| `max_rows` | linear in generation + conversion cost; 10-50 rows find logic bugs, shrinking reduces to 1-3 |
| `min_rows` | large minimums slow every example *and* every shrink step; keep 0-1 unless the property needs volume |
| `max_examples` | 3-5 while developing, 20 (profile `spark`) locally, 100 (`spark-ci`) in CI |
| Spark actions per example | each `count()`/`collect()` is a job; aggregate several checks in one `agg` |
| Columns | each column adds a strategy draw per row; omit irrelevant columns from the test schema |
| Pure-Python layer | test row-level functions with Hypothesis directly (thousands of examples/s) |

Measure with `pytest --durations=10` and `--hypothesis-show-statistics` (shows time spent
generating vs running).

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Duplicate keys break a join/uniqueness check | key not `unique=True` | add `unique=True` |
| `Unsatisfiable` / `FailedHealthCheck` with `unique=True` | domain smaller than needed rows | widen the domain or lower `min_rows` |
| Column config has no effect | name typo, or field not nullable (nulls) | check `schema.fieldNames()` |
| `fixture 'df' not found` | hand-rolled decorator | `@spark_property` or inner `@given` function |
| `large_base_example` health check | `min_rows` in the hundreds | `spark` profile suppresses it; keep the profile's list when overriding |
| Very slow tests | stress/production profiles in logic tests | `edge_case_profile()` or small `max_rows` |

## Related topics

- `type_strategies.md` - default per-type strategies.
- `nulls_and_edge_cases.md` - `NullConfig` and distribution helpers.
- `composing_related_data.md` - cross-column and cross-DataFrame constraints.
