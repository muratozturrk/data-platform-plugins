# The spark-prop-test CLI, YAML suites and the HTML report

## Concept

`spark-prop-test` runs data-quality property suites declared in YAML, without Python test code:
for every suite it generates DataFrames, optionally passes them through the transformation under
test, validates the result (schema + GX expectations) with
`DataQualityPropertySuite.run_property_loop()`, prints a table per suite, writes an HTML report
and exits non-zero on failure. `validate` checks an existing data file against a schema.

## Commands

```text
spark-prop-test init     [-o/--output spark_prop_test.yaml]
spark-prop-test run      [-c/--config spark_prop_test.yaml] [-n/--max-examples N] [-s/--suite NAME]
                         [-r/--report] [--report-path spark_prop_test_report.html] [--no-shrink]
spark-prop-test validate FILE --schema-module MOD --schema-name NAME [--format parquet|csv|json]
                         [--strict-types]
spark-prop-test --version        # reads package metadata: only works when pip-installed
python -m spark_prop_testing.cli run ...   # same commands without the console script
```

| Command | Does | Exit code |
|---|---|---|
| `init` | writes the template; if the file exists asks "Overwrite?" | 0; 1 if you answer no |
| `run` | cwd → `sys.path`, load YAML, Spark on `SPARK_PROP_MASTER` (default `local[2]`, 4 shuffle partitions), one `run_property_loop()` per suite, `spark.stop()` | 0 all suites valid; 1 any suite invalid, a suite without resolvable schema, config file missing, unknown `--suite`, or an exception (import error, bad `transform`) |
| `validate` | cwd → `sys.path`, import the schema, read FILE (csv: `header` + `inferSchema`) on `local[1]`, `SchemaValidator(schema, strict_types=--strict-types).validate` | 0 no ERROR (WARNINGs allowed); 1 errors |

- `-n` sets every suite's `max_examples` **and** `SPARK_PROP_MAX_EXAMPLES` in the process.
- A suite whose schema cannot be resolved (no `schema_module`/`schema_name`) prints
  "No schema loaded ... suite not run" and makes the run exit 1; an empty `suites:` block exits 0.
- GX is imported only for suites with `ge_expectations`; schema-only suites run without it.
- `--no-shrink` runs each suite with phases explicit/reuse/generate: the reported failure is the
  first one found, not the minimal one.
- `validate --strict-types`: COMPATIBLE and COERCIBLE type differences (int → long, double data
  for an int column) become errors → exit 1.
- `run` does not read `@prop` annotations (YAML suites only); use `click.testing.CliRunner` in tests.

## YAML schema

```yaml
suites:
  <suite_name>:
    schema_module: myproject.schemas      # importable from the cwd
    schema_name: ORDERS                   # schema of the validated (output) DataFrame
    input_schema_name: RAW_ORDERS         # optional: generate inputs from this schema
    input_schema_module: myproject.raw    # optional, default schema_module
    transform: myproject.pipeline:clean   # optional "module:function", DataFrame -> DataFrame
    max_examples: 20                      # -n overrides
    min_rows: 1
    max_rows: 100
    null_rates: {amount: 0.05}            # NullConfig(null_rate) per column
    ge_expectations:
      row_count_min: 1
      row_count_max: 1000
      columns:
        order_id: {not_null: true, unique: true, min_value: 1}
        status:   {value_set: [new, paid, shipped]}
        email:    {regex_pattern: '^[^@]+@[^@]+$', max_null_fraction: 0.1}
        code:     {min_length: 3, max_length: 3}
```

Alignment: **without** `transform`, generated data is validated directly, so the generator
follows `ge_expectations.columns` - `unique` → unique strategy, `min_value`/`max_value` →
bounded ints/floats, `value_set` → `sampled_from`, `not_null` → no nulls. **With** `transform`,
inputs are generated from `input_schema_name` unaligned: the transform must turn *any* input
into valid output. A malformed `transform` (no colon) raises `ValueError`. The YAML is read as
UTF-8. Expectation semantics are in skill `spark-data-quality-property-testing`.

## Worked example (as in `examples/test_cli.py`)

```python
MODULE = '''
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
ORDERS = StructType([StructField("order_id", IntegerType(), False),
                     StructField("status", StringType(), False),
                     StructField("amount", IntegerType(), False)])
RAW_ORDERS = StructType([StructField("order_id", IntegerType(), False),
                         StructField("status", StringType(), False),
                         StructField("amount_cents", IntegerType(), False)])
def clean(df):
    return (df.dropDuplicates(["order_id"])
              .select("order_id", "status",
                      (F.greatest("amount_cents", F.lit(0)) / 100).cast("int").alias("amount")))
def clean_buggy(df):        # forgets the rename
    return df.select("order_id", "status", (F.col("amount_cents") / 100).cast("int").alias("amount_eur"))
'''

def test_run(tmp_path, monkeypatch):
    (tmp_path / "cli_pipeline.py").write_text(MODULE, encoding="utf-8")
    (tmp_path / "spark_prop_test.yaml").write_text(YAML, encoding="utf-8")  # suites orders_clean / orders_buggy
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SPARK_PROP_MASTER", "local[1]")
    monkeypatch.delenv("SPARK_PROP_MAX_EXAMPLES", raising=False)   # -n sets it in-process
    ok = CliRunner().invoke(main, ["run", "-s", "orders_clean", "-n", "2", "--report"])
    assert ok.exit_code == 0, ok.output                   # "✓ All suites passed"
    bad = CliRunner().invoke(main, ["run", "-s", "orders_buggy", "-n", "2"])
    assert bad.exit_code == 1                             # schema: Column 'amount' missing
```

In-process quirks: the JVM keeps the working directory it started in, so pass absolute paths
to `validate`; pass `--no-shrink` when a test expects a failing run, to keep it short.

## The HTML report

`HTMLReporter().generate(report, output_path)` (used by `run --report` and the plugin) renders one
self-contained file (inline CSS, Jinja2 with **autoescape**): generated strings such as
`<script>` or `'` appear as `&lt;script&gt;` / `&#39;` text, never as markup.

| Part | Content |
|---|---|
| Header | suite name ("Full Run" from the CLI, "Full Test Run" from pytest), PASSED/FAILED badge, timestamp, library version |
| Summary cards | overall ✓/✗; schema issues (= number of invalid schema results); GX passed/total expectations; property-failure count |
| Schema Validation | each `SchemaValidationResult.summary()` (errors, warnings, "Column '<name>': ..." lines) |
| Content Validation (Great Expectations) | per expectation: status, column (or "(table)"), expectation type, unexpected count and % for failures |
| Property Failures | full failure text, including the minimal failing example's per-layer summary |

The merged report holds the per-layer results of the **last example of each suite** (the minimal
failing one on failure); suite names appear inside the texts. Missing parent directories of
the report path are created.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: myproject` | run from another directory | `cd` to the project root (cwd is added to `sys.path`) |
| Exit 1, "No schema loaded" | suite without resolvable `schema_module`/`schema_name` | fix the suite entry |
| Exit 0, nothing tested | empty `suites:` block | add a suite |
| Template suite fails at once | `transform` set but rules not guaranteed by it | make the transform enforce them, or drop `transform` to use alignment |
| `ImportError ... great_expectations` on `run` | a suite has `ge_expectations`, GX not installed | `pip install -e "<plugin>/lib[ge]"` |
| `validate` exits 0 for double data in an int column | double → int is COERCIBLE: WARNING | `validate --strict-types` |
| `validate` csv: every column string | (fixed) csv uses header + inferSchema | - |
| Slow failing `run` | shrinking with GX per step | `--no-shrink`, fewer rows (`max_rows: 5`), `-n 2` while iterating |
| Unicode error printing results on Windows | console code page | `PYTHONUTF8=1` |

## Related topics

- Skill `spark-data-quality-property-testing` (`quality_suite.md`): `run_property_loop`,
  `ge_expectations` semantics.
- Skill `spark-schema-contract-testing`: what `validate` reports.
- `ci_and_troubleshooting.md`: running `spark-prop-test` in CI.
