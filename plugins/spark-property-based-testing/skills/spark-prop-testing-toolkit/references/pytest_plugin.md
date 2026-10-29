# The pytest plugin (spark_prop_testing.plugin)

## Concept

The plugin gives every test run the same Spark-friendly defaults: one SparkSession per session,
a Hypothesis profile without deadlines, a switch for the number of examples, and an optional
HTML report of data-quality results. It is registered through the `pytest11` entry point when
the library is pip-installed; from a source checkout load it with `-p spark_prop_testing.plugin`
or `pytest_plugins = ["spark_prop_testing.plugin"]` in the **root** `conftest.py` (the bundled
`lib/conftest.py` does that only when no entry point exists).

## API reference

| Name | Kind | Behaviour |
|---|---|---|
| `spark` | fixture, session | `SparkSession.builder.master($SPARK_PROP_MASTER or "local[2]")`, app `spark-prop-testing`, `spark.sql.adaptive.enabled=false`, `spark.sql.shuffle.partitions=4`, `spark.ui.enabled=false`, `spark.driver.memory=2g`; `stop()` at the end |
| `spark_prop_config` | fixture, session | `ConfigLoader(--spark-prop-config).load()` → `dict[str, SuiteConfig]`, or `None` if the file does not exist (importing schema/transform modules as it loads) |
| `prop_max_examples` | fixture, session | `--spark-prop-max-examples` value or 20, for raw `@given` tests that build settings inside the test |
| `quality_report_collector` | fixture, function | returns `collect(report)`; appends a `UnifiedQualityReport` to the session list |
| `--spark-prop-report` | flag | at session end, merge the collected reports into one HTML file |
| `--spark-prop-report-path` | option | default `spark_prop_test_report.html`; missing parent directories are created |
| `--spark-prop-max-examples N` | option, int | `os.environ["SPARK_PROP_MAX_EXAMPLES"] = N` in `pytest_configure` |
| `--spark-prop-config PATH` | option | default `spark_prop_test.yaml`, read by `spark_prop_config` |

The module registers the profiles at import (before Hypothesis' plugin reads
`--hypothesis-profile`), and `pytest_configure` calls
`settings.load_profile(--hypothesis-profile or "spark")`:

| Profile | max_examples | deadline | health checks suppressed | derandomize | print_blob | database |
|---|---|---|---|---|---|---|
| `spark` | 20 | None | too_slow, data_too_large, large_base_example | False | False | `.hypothesis/` |
| `spark-ci` (parent `spark`) | 100 | None | same | True | True | None (implied by derandomize) |

Who honours `--spark-prop-max-examples` / `SPARK_PROP_MAX_EXAMPLES`: `@spark_property`,
`TransformationPropertyTester.run`, `DataQualityPropertySuite.run_property_loop` and the CLI
(`-n` sets the same variable). Raw `@given` tests do not; they follow the profile or explicit
`@settings`. Conversely the profile's `max_examples` does not reach the library runners (they
pass their own, default 20), but `derandomize`, `print_blob`, `database` and (unless the call
passes `phases=`) `phases` do. All library runners share one database key (the digest of their
internal test function): a failure saved by one runner test can replay in another, and with
`derandomize` they all get the same seed.

## Worked example

```python
# test_quality.py (no conftest needed when the package is installed)
def test_orders_contract(spark, quality_report_collector):
    report = DataQualityPropertySuite(spark, CONFIG).run_property_loop()
    quality_report_collector(report)    # ends up in the HTML report
    report.assert_valid()
```

```bash
pytest tests/                                             # 'spark' profile, local[2]
SPARK_PROP_MASTER=local[1] pytest tests/ --spark-prop-max-examples 3      # quick local loop
mkdir -p reports && pytest tests/ --hypothesis-profile=spark-ci --spark-prop-max-examples 100 \
    --spark-prop-report --spark-prop-report-path reports/spark_prop_report.html
```

The merged report is named "Full Test Run" and contains the schema summaries, GX tables and
property failures of every collected report (see `cli_and_yaml.md` for the layout). Reports
of `TransformationPropertyTester` or `@spark_property` failures are not collected: they show up
in pytest's own output.

## Profile selection

Hypothesis' own pytest plugin loads `--hypothesis-profile` in its `pytest_configure`, which can
run before this plugin's. The plugin therefore registers `spark` and `spark-ci` when the module
is imported, so `-p spark_prop_testing.plugin --hypothesis-profile=spark-ci` works without a
conftest. `test_pytest_plugin.py` checks that the inner session runs under `spark-ci`.

## Testing the plugin itself

`pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", ...)` runs an inner session in
a fresh interpreter, so profiles and `SPARK_PROP_MAX_EXAMPLES` do not leak into the outer run;
inner tests that do not touch Spark run in a few seconds. Add `pytest_plugins = ["pytester"]`
to the test module.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `fixture 'spark' not found` | plugin not loaded (source checkout) | `-p spark_prop_testing.plugin` or root `conftest.py` |
| `Plugin already registered under a different name` | `-p` (or `pytest_plugins`) plus the installed entry point load the module twice | drop `-p` when installed; guard `pytest_plugins` like `lib/conftest.py` |
| A runner test fails on rows from another test | shared database key across library runners | delete `.hypothesis/` |
| CI still runs 20 examples per library test | profile `max_examples` not used by library runners | `--spark-prop-max-examples 100` |
| `.hypothesis` cache never helps in CI | `spark-ci` derandomizes → no database | randomized job with the `spark` profile for replay |
| No HTML file | nothing passed to `quality_report_collector`, or flag missing | collect reports; pass `--spark-prop-report` |
| Session uses `local[2]` on a slow box | default master | `SPARK_PROP_MASTER=local[1]` |
| Your own `spark` fixture ignored / duplicated | a conftest fixture named `spark` overrides the plugin's | keep one; the closest definition wins |
| Example counts leak between tests | `SPARK_PROP_MAX_EXAMPLES` set by an in-process CLI call | `monkeypatch.delenv(..., raising=False)` |

## Related topics

- `decorators.md`: `@spark_property` uses the fixture and the override.
- `ci_and_troubleshooting.md`: the full CI workflow.
- Skill `spark-property-testing-foundations` (`hypothesis_for_spark.md`, `test_session_setup.md`).
- Skill `spark-data-quality-property-testing`: `UnifiedQualityReport` contents.
