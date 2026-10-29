# CI recipe, Windows notes and troubleshooting

## Concept

Property tests in CI have different goals than locally: more examples, reproducible results on
every pull request, failures that can be replayed, and a report people can open. Locally the
goal is a fast loop. The toolkit maps these to two Hypothesis profiles (`spark` for local and
randomized runs, `spark-ci` for derandomized PR runs), the `--spark-prop-max-examples` override,
the example database in `.hypothesis/`, and the HTML report.

| Run | Profile | Examples | Database | Purpose |
|---|---|---|---|---|
| Local | `spark` (auto) | `--spark-prop-max-examples 3-5` | `.hypothesis/` | fast feedback, replays your last failure |
| Pull request | `spark-ci` | `--spark-prop-max-examples 100` | none (derandomize) | same examples on every run: no "it passed on retry" |
| Nightly | `spark` | 200-500 | cached `.hypothesis/` | new random examples; replays earlier failures first |

## GitHub Actions workflow

```yaml
name: spark-property-tests
on:
  pull_request:
  schedule: [{cron: "0 3 * * *"}]

jobs:
  property-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      GX_ANALYTICS_ENABLED: "false"
      SPARK_PROP_MASTER: "local[2]"
      PYTHONUTF8: "1"
      PROFILE: ${{ github.event_name == 'schedule' && 'spark' || 'spark-ci' }}
      EXAMPLES: ${{ github.event_name == 'schedule' && '300' || '100' }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: {distribution: temurin, java-version: "17"}
      - uses: actions/setup-python@v5
        with: {python-version: "3.11", cache: pip}
      - run: pip install -e "plugins/spark-property-based-testing/lib[ge,dev]"
      - uses: actions/cache@v4                  # example database (used by the 'spark' profile)
        with:
          path: .hypothesis
          key: hypothesis-${{ github.ref_name }}-${{ github.run_id }}
          restore-keys: |
            hypothesis-${{ github.ref_name }}-
            hypothesis-
      - run: >
          pytest tests/ -q
          --hypothesis-profile=$PROFILE
          --spark-prop-max-examples $EXAMPLES
          --spark-prop-report --spark-prop-report-path reports/spark_prop_report.html
      - run: spark-prop-test run -c spark_prop_test.yaml -n 20 --report --report-path reports/cli_report.html
      - if: always()
        uses: actions/upload-artifact@v4
        with: {name: spark-prop-reports, path: reports/, if-no-files-found: ignore}
```

Notes:

- The plugin registers `spark`/`spark-ci` at import, so `--hypothesis-profile=spark-ci` needs no
  conftest; the reporter creates `reports/` itself.
- `spark-ci` sets `derandomize=True`, which implies `database=None`: the cache only matters for
  the randomized (`spark`) job. Keep it anyway if both jobs share the workflow.
- `--spark-prop-max-examples` is what raises the library runners; the profile's
  `max_examples=100` reaches only raw `@given` tests.
- Library runners share one database key: a failure saved by one runner test can replay in
  another, and under `derandomize` all runners use the same seed. Scope the cache per branch.
- `print_blob=True` in `spark-ci` prints `@reproduce_failure(...)` for failing raw `@given`
  tests; for library runners the report's minimal failing rows are the reproduction.
- PySpark 3.5 supports Java 8/11/17; with pip-installed PySpark no Spark download is needed.
- Use the matrix sparingly: each Spark job needs ~2 GB of memory; the plugin's driver uses 2g.

## Windows notes

| Problem | Symptom | Fix |
|---|---|---|
| Python 3.12 + PySpark 3.5 | `Python worker exited unexpectedly (crashed)` on the first action | Python 3.11 venv; `PYSPARK_PYTHON` = that interpreter |
| Slow worker start-up (antivirus scanning the venv) | `Python worker failed to connect back`, `Accept timed out` (Spark waits 10 s) | `SPARK_PROP_MASTER=local[1]`, few shuffle partitions, fewer parallel jobs; rerun once to confirm |
| Wrong interpreter for workers | import errors in workers, version mismatch | `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the venv's `python.exe` |
| Missing `winutils.exe` | `WARN Shell: Did not find winutils.exe` | harmless for reading and in-memory tests; writing files needs `HADOOP_HOME` with winutils |
| Timestamps before 1970 | `OSError` in `collect()` | the default TimestampType strategy starts at 1970-01-02 UTC (skill `spark-dataframe-strategies`) |
| Console code page | `UnicodeEncodeError` on `→`, `✓` in summaries | `PYTHONUTF8=1` or `PYTHONIOENCODING=utf-8` |
| Leftover `java.exe` after Ctrl-C | memory not freed, next run slow | stop the processes of the venv before rerunning |

On a slow laptop expect seconds per example: keep `max_examples` at 2-5 and rows ≤ 20 locally, run
one test file at a time, and let CI carry the volume.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `DeadlineExceeded` / `Flaky` | Hypothesis' 200 ms deadline outside the `spark` profile | `@spark_property`, the plugin, or `settings(deadline=None)` |
| `FailedHealthCheck: too_slow / data_too_large / large_base_example` | explicit `@settings(suppress_health_check=[...])` **replaces** the profile list | include all three in your list |
| A failing test runs for minutes | shrinking reruns Spark tens of times | smaller inputs; `tester.run(phases=(Phase.explicit, Phase.generate))`, `QualitySuiteConfig(phases=...)`, CLI `--no-shrink` |
| `fixture 'spark' not found` | plugin not loaded | `-p spark_prop_testing.plugin` / install the package |
| CI examples differ between runs | randomized profile on PRs | `--hypothesis-profile=spark-ci` |
| CI green, nightly red | new random inputs found a bug | reproduce locally with the failing rows from the report; add an `@example` |
| `spark-prop-test run` exits 1 with "No schema loaded" | `schema_module`/`schema_name` missing or wrong | fix the suite; it is a failure, not a skip |
| A runner test fails on rows that look like another test's data | shared database key across library runners | delete `.hypothesis/` (or the cached CI DB) |
| `--version` raises | package metadata missing (PYTHONPATH install) | `pip install -e` |
| Out of memory with parallel jobs | several SparkSessions | one Spark process per machine slot; `local[1]` |

## Related topics

- `pytest_plugin.md`: profiles and options in detail.
- `cli_and_yaml.md`: exit codes and the report.
- Skill `spark-property-testing-foundations` (`flakiness_and_determinism.md`,
  `test_session_setup.md`): deterministic sessions and flakiness causes.
