# A SparkSession for property tests

## Concept

A property test runs the same pipeline on many tiny DataFrames. The cost that matters is the
fixed overhead per Spark job (task scheduling, shuffle stages, Python worker start-up), not
data volume. The test session is tuned to make each job small, cheap and reproducible:

| Setting | Value | Why |
|---|---|---|
| `master` | `local[1]` (or `local[2]`) | in-process, no cluster; one core avoids parallel worker spawns and keeps memory low |
| `spark.sql.shuffle.partitions` | `1` (≤ 4) | default 200 → 200 near-empty tasks per groupBy/join |
| `spark.default.parallelism` | `1` | RDD/`createDataFrame` partitions follow the core count anyway |
| `spark.ui.enabled` | `false` | no port binding (parallel test runs), faster start |
| `spark.sql.adaptive.enabled` | `false` | AQE re-plans at runtime (coalesces partitions, switches join strategy): plans and partition counts become data-dependent |
| `spark.sql.session.timeZone` | `UTC` | `date_format`, `to_date`, `hour()` etc. independent of the host zone |
| `spark.driver.memory` | `1g`-`2g` | enough for tests; only takes effect before the JVM starts |

Scope: **session**. Starting the JVM takes seconds; a session per test or per example makes
property tests unusable. Generated DataFrames are small, so sharing one session is safe as long
as tests don't leave temp views or changed confs behind (restore confs in `finally`).

## API

The bundled pytest plugin (`spark_prop_testing.plugin`) provides:

```python
@pytest.fixture(scope="session")
def spark(request) -> SparkSession:
    # master = $SPARK_PROP_MASTER (default local[2]), appName "spark-prop-testing",
    # AQE off, shuffle.partitions=4, UI off, driver.memory=2g; session.stop() at the end

@pytest.fixture(scope="session")
def spark_prop_config(request)    # YAML from --spark-prop-config (default spark_prop_test.yaml) or None

@pytest.fixture(scope="session")
def prop_max_examples(request) -> int   # --spark-prop-max-examples or 20
```

`@spark_property` takes `spark` from the test's `spark` argument (the fixture) when the test
asks for it, otherwise from `SparkSession.getActiveSession()`, otherwise it creates a session on
`SPARK_PROP_MASTER` (default `local[2]`) with AQE off, 4 shuffle partitions and UI off.

`SPARK_PROP_MASTER=local[1]` is the quickest fix on slow machines where parallel Python worker
start-up exceeds Spark's 10 s accept timeout ("Accept timed out").

Fixture override: a fixture named `spark` in the test module or a closer `conftest.py` wins
over the plugin's fixture. Use that when you also want UTC or 1 shuffle partition.

## Worked example

`examples/test_session_setup.py`:

```python
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("property-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()

def test_aqe_off_gives_a_static_plan(spark):
    """With AQE off the physical plan has no AdaptiveSparkPlan node."""
    df = spark.range(10).groupBy((F.col("id") % 3).alias("k")).count()
    assert "AdaptiveSparkPlan" not in df._jdf.queryExecution().executedPlan().toString()
```

Changing a conf inside one test (e.g. a different session time zone) must be undone:

```python
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
try:
    ...
finally:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
```

### Environment notes

- `PYSPARK_PYTHON` must point to the same interpreter as the driver (venv python), otherwise
  Python workers start with another interpreter/version.
- Windows: PySpark 3.5 Python workers crash on Python 3.12 ("Python worker exited
  unexpectedly"); use Python 3.11. Slow worker start-up (antivirus scanning the venv) can exceed
  Spark's 10 s accept timeout → "Python worker failed to connect back / Accept timed out";
  `local[1]` avoids parallel spawns. Plain DataFrame operations and `createDataFrame` from a
  Python list don't start Python workers; UDFs, `rdd.map` and pandas UDFs do.
- `getOrCreate()` returns an existing session: `.master()`/`.config()` of a later builder do
  not change static confs (master, driver memory) of a running JVM.
- Run one Spark test process at a time on small machines; `pytest-xdist` with `-n 4` starts
  four JVMs.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Each test takes 200 tasks per shuffle | default `shuffle.partitions=200` | set to 1-4 |
| Partition-count assertions differ between runs | AQE coalescing | `spark.sql.adaptive.enabled=false` |
| Hour/date results depend on the machine | host time zone | `spark.sql.session.timeZone=UTC` |
| Config change has no effect | session already created by another fixture/test | set it before the first `getOrCreate`, or use runtime confs (`spark.conf.set`) |
| `Address already in use` for 4040 | UI on, parallel runs | `spark.ui.enabled=false` |
| `function_scoped_fixture` health check | `spark` fixture function-scoped with `@given` | session scope |
| OOM / machine swaps | multiple JVMs or `driver.memory` too large | one test process, 1-2g |
| "Python worker exited unexpectedly" (Windows) | Python 3.12 + PySpark 3.5 | Python 3.11 |

## Related topics

- `hypothesis_for_spark.md` - the settings that go with this session.
- `flakiness_and_determinism.md` - what the session config cannot make deterministic.
