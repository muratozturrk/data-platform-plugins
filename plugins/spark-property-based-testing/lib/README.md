# spark-prop-testing

Property-based testing for PySpark pipelines, built on Hypothesis.

Layers (each usable on its own):

| Layer | Main API | Checks |
| --- | --- | --- |
| Strategies | `spark_dataframes`, `DataFrameConfig`, `ColumnConfig`, `NullConfig`, `skewed_integers`, `edge_case_strings` | generates DataFrames for any `StructType`, with nulls, skew and edge cases |
| Schema | `SchemaValidator`, `SchemaEvolutionChecker` | missing/extra columns, type drift (incl. nested types), nullable contracts, breaking changes |
| Content (optional, GX 1.x) | `ExpectationSuiteBuilder`, `GEPropertyValidator`, `auto_generate_suite`, `DataQualityPropertySuite` | Great Expectations suites run against generated or real DataFrames |
| Transformations | `TransformationPropertyTester`, standard properties (`row_count_*`, `no_new_nulls_introduced`, `make_sum_linearity_property`, `make_idempotency_property`, ...) | invariants of your DataFrame → DataFrame functions |
| Tooling | `@spark_property`, `@prop`, pytest plugin, `spark-prop-test` CLI, HTML report | ergonomics, CI |

## Install

```bash
pip install -e ".[ge,dev]"      # drop "ge" if you don't need Great Expectations
```

Requirements: Python 3.9+, PySpark 3.3+, Java 8/11/17. On Windows, PySpark 3.5 workers crash on
Python 3.12 ("Python worker exited unexpectedly"); use Python 3.11 there.

## Quick start

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from spark_prop_testing import spark_property

SCHEMA = StructType([StructField("id", IntegerType(), False),
                     StructField("amount", DoubleType(), True)])

@spark_property(schema=SCHEMA, max_examples=25)
def test_filter_never_adds_rows(spark, df):
    assert df.filter(F.col("amount") > 0).count() <= df.count()
```

```bash
pytest tests/                                   # 'spark' Hypothesis profile: no deadline
pytest tests/ --hypothesis-profile=spark-ci     # 100 examples, derandomized
pytest tests/ --spark-prop-max-examples 50 --spark-prop-report
spark-prop-test init && spark-prop-test run --report
```
