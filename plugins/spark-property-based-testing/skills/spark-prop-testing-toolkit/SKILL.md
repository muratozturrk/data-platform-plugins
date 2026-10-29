---
name: spark-prop-testing-toolkit
description: "Tooling around the bundled spark_prop_testing library: installing it (pip -e lib[ge,dev] or PYTHONPATH), @spark_property (pytest-visible signature, spark and other fixtures, method style) and @prop, the pytest plugin (spark fixture, --spark-prop-report, --spark-prop-report-path, --spark-prop-max-examples, --spark-prop-config, quality_report_collector, the auto-loaded 'spark' Hypothesis profile and --hypothesis-profile=spark-ci), SPARK_PROP_MASTER and SPARK_PROP_MAX_EXAMPLES, the spark-prop-test CLI (init, run --no-shrink, validate --strict-types; YAML with transform/input_schema_name/alignment; exit codes; autoescaped HTML report), a GitHub Actions recipe, Windows notes and troubleshooting. Use when wiring Spark property tests into pytest, the command line or CI."
---

# Spark Property-Testing Toolkit

The library ships the ergonomics around its layers: two decorators, a pytest plugin, a CLI with
YAML suites and an HTML report. This skill covers how to install, run and automate them; what
the tests check is in the sibling skills: `spark-property-testing-foundations` (Hypothesis
settings, session, flakiness), `spark-dataframe-strategies`, `spark-schema-contract-testing`,
`spark-data-quality-property-testing` (`DataQualityPropertySuite`, YAML `ge_expectations`) and
`spark-transformation-property-testing` (`TransformationPropertyTester`, standard properties).

## When to Use

- Writing pytest property tests with as little Hypothesis boilerplate as possible.
- Running suites from the command line or a scheduler with a YAML file and an HTML report.
- Setting up CI: deterministic examples, more examples than locally, report as an artifact.
- Spark property tests that fail on Windows or slow machines for environmental reasons.

## Decision table

| Need | Tool | Reference |
|---|---|---|
| Property test over a generated `df` in one decorator | `@spark_property(schema, config, max_examples)` | `references/decorators.md` |
| Declare a transform's properties | `@prop(p1, p2, max_examples=)` + `TransformationPropertyTester.from_prop` | `decorators.md` |
| Shared SparkSession in tests | plugin fixture `spark` (`SPARK_PROP_MASTER`) | `references/pytest_plugin.md` |
| More/fewer examples without editing tests | `--spark-prop-max-examples N` / `SPARK_PROP_MAX_EXAMPLES` | `pytest_plugin.md` |
| HTML report of quality suites from pytest | `quality_report_collector` + `--spark-prop-report` | `pytest_plugin.md` |
| CI profile | `--hypothesis-profile=spark-ci --spark-prop-max-examples 100` | `pytest_plugin.md`, `references/ci_and_troubleshooting.md` |
| Suites without Python test code | `spark-prop-test init` / `run` + YAML | `references/cli_and_yaml.md` |
| Check a data file against a schema | `spark-prop-test validate FILE --schema-module --schema-name [--strict-types]` | `cli_and_yaml.md` |
| Fast failing CLI run | `spark-prop-test run --no-shrink` | `cli_and_yaml.md` |
| GitHub Actions | Java + Python 3.11, cached `.hypothesis`, report artifact | `ci_and_troubleshooting.md` |
| "Python worker exited unexpectedly" / "Accept timed out" | Python 3.11, `SPARK_PROP_MASTER=local[1]` | `ci_and_troubleshooting.md` |

## Install

```bash
pip install -e "<plugin>/lib[ge,dev]"     # pytest11 plugin + spark-prop-test script; drop ge without GX
# or, from a checkout without installing:
export PYTHONPATH=<plugin>/lib
python -m pytest -p spark_prop_testing.plugin tests/     # load the plugin explicitly
```

Requirements: Python 3.9+ (3.11 on Windows), PySpark 3.3+, Java 8/11/17, `PYSPARK_PYTHON`
pointing at the test interpreter. Great Expectations is needed only for suites with
`ge_expectations` (GX is imported lazily). `spark-prop-test --version` needs a pip install
(package metadata); everything else also works as `python -m spark_prop_testing.cli`.

## @spark_property and @prop

```python
@spark_property(schema=ORDERS, config=SMALL, max_examples=10)
def test_filter_never_adds_rows(spark, df):
    assert df.filter(F.col("status") == "paid").count() <= df.count()
```

`df` is generated and removed from the visible signature, so pytest resolves only the other
parameters (`spark`, any fixture). Styles: `(spark, df)`, `(df, other_fixture)` (spark = active
session, else a new one on `SPARK_PROP_MASTER`), `(self, spark, df)` in a class. A test without a
`df` parameter raises `TypeError` at decoration. `max_examples` (default 20) is read at call
time and overridden by `SPARK_PROP_MAX_EXAMPLES`. `@prop(*properties, max_examples=20)` only
annotates a transformation; `TransformationPropertyTester.from_prop` runs it (the CLI does not).

## The pytest plugin

| Item | Behaviour |
|---|---|
| `spark` fixture | session scope; master `SPARK_PROP_MASTER` (default `local[2]`), AQE off, 4 shuffle partitions, UI off, driver 2g; stopped at session end |
| Hypothesis profile | registers `spark` (no deadline, Spark health checks off, 20 examples) and `spark-ci` (100, derandomize, print_blob); loads `spark` unless `--hypothesis-profile` is given |
| `--spark-prop-max-examples N` | sets `SPARK_PROP_MAX_EXAMPLES`: honoured by `@spark_property`, `TransformationPropertyTester`, `DataQualityPropertySuite.run_property_loop`; not by raw `@given` (use the `prop_max_examples` fixture or the profile) |
| `--spark-prop-report`, `--spark-prop-report-path` (`spark_prop_test_report.html`) | merges every report passed to `quality_report_collector` into one HTML file ("Full Test Run"); nothing collected → no file |
| `--spark-prop-config` (`spark_prop_test.yaml`) | the `spark_prop_config` fixture loads it with `ConfigLoader` (dict of `SuiteConfig`) or returns None |

The plugin registers both profiles at import, so `--hypothesis-profile=spark-ci` works without a
conftest. Caveats that still apply:

- `spark-ci`'s `max_examples=100` applies to raw `@given` tests; library runners pass their own
  `max_examples` and take only `derandomize`/`print_blob` from it, so add
  `--spark-prop-max-examples 100` in CI.
- `derandomize=True` also means no example database.
- All library runners (`TransformationPropertyTester`, `DataQualityPropertySuite`) share one
  Hypothesis database key, so a saved failure can replay in an unrelated runner test, and under
  `derandomize` they all get the same seed. Clear `.hypothesis/` if a replay looks foreign.

## The CLI

```bash
spark-prop-test init [-o spark_prop_test.yaml]          # template; asks before overwriting
spark-prop-test run -c spark_prop_test.yaml -n 5 -s orders --report --report-path report.html
spark-prop-test validate data.csv --format csv --schema-module myproject.schemas --schema-name ORDERS
```

`run` (`--no-shrink` skips shrinking: faster, non-minimal failures) loads every suite (`schema_module`/`schema_name`, optional `transform: module:function`,
`input_schema_name`, `max_examples`, `min_rows`/`max_rows`, `null_rates`, `ge_expectations`),
puts the cwd on `sys.path`, starts Spark on `SPARK_PROP_MASTER` and runs
`DataQualityPropertySuite.run_property_loop()` per suite. Without `transform` the generator is
aligned with `ge_expectations.columns`. Exit codes: `run` 0 = all suites valid, 1 = a suite
failed, a suite has no resolvable schema, config missing or `--suite` unknown; `validate` 0 = no
ERROR (warnings allowed), 1 = errors. `validate --strict-types` turns COMPATIBLE/COERCIBLE type
differences (e.g. double data for an int column) into errors.

## CI and Windows (short)

CI: Java 17 + Python 3.11, cache `.hypothesis/`, `--hypothesis-profile=spark-ci
--spark-prop-max-examples 100`, upload the HTML report with
`if: always()` (full workflow in `ci_and_troubleshooting.md`). Windows: Python 3.11 (3.12 worker
crash), `SPARK_PROP_MASTER=local[1]` when worker start-up is slow, `PYTHONUTF8=1` for the
report/summary symbols, `winutils.exe` only for writing files.

## Run it

```bash
SPARK_PROP_MASTER=local[1] PYTHONPATH=<plugin>/lib \
  python -m pytest -q -p spark_prop_testing.plugin skills/spark-prop-testing-toolkit/examples/test_decorators.py
GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib \
  python -m pytest -q skills/spark-prop-testing-toolkit/examples/test_cli.py
```

## Anti-patterns

- `@given(df=...)` + hand-written `@settings` in every test instead of `@spark_property`.
- `local[*]` and 200 shuffle partitions in unit tests.
- Expecting `--hypothesis-profile=spark-ci` alone to raise library runners to 100 examples.
- Relying on `validate` without `--strict-types` when lossy type drift must fail the job.
- Debugging a runner failure whose rows came from another test's saved example (clear `.hypothesis/`).

## References

- `references/decorators.md`, `references/pytest_plugin.md`, `references/cli_and_yaml.md`,
  `references/ci_and_troubleshooting.md`.
- Examples: `examples/test_decorators.py`, `test_pytest_plugin.py`, `test_cli.py`.
