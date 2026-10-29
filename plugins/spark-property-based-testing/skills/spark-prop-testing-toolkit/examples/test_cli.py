"""
The spark-prop-test CLI (init | run | validate) driven with click.testing.CliRunner: a temp YAML,
schema module and transform in tmp_path, exit code 0 for a passing and 1 for a failing transform
or a suite without schema, --no-shrink, the HTML report, validate (--strict-types) on CSV files,
and `python -m spark_prop_testing.cli`.

Run:
    GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib python -m pytest -q test_cli.py
"""

import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import subprocess
import sys
import textwrap

import pytest
from click.testing import CliRunner

from spark_prop_testing.cli import main

MODULE = """
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
RAW_ORDERS = StructType([
    StructField("order_id",     IntegerType(), nullable=False),
    StructField("status",       StringType(),  nullable=False),
    StructField("amount_cents", IntegerType(), nullable=False),
])

def clean(df):
    return (df.dropDuplicates(["order_id"])
              .select("order_id", "status",
                      (F.greatest("amount_cents", F.lit(0)) / 100).cast("int").alias("amount")))

def clean_buggy(df):          # forgets the rename: the output has amount_eur, not amount
    return df.select("order_id", "status", (F.col("amount_cents") / 100).cast("int").alias("amount_eur"))
"""

YAML = """\
suites:
  orders_clean:
    schema_module: cli_pipeline
    schema_name: ORDERS
    input_schema_name: RAW_ORDERS
    transform: cli_pipeline:clean
    max_examples: 20            # -n 2 overrides it
    min_rows: 1
    max_rows: 5
    ge_expectations:
      columns:
        order_id: {not_null: true, unique: true}
        amount:   {min_value: 0}
  orders_buggy:
    schema_module: cli_pipeline
    schema_name: ORDERS
    input_schema_name: RAW_ORDERS
    transform: cli_pipeline:clean_buggy
    min_rows: 1
    max_rows: 5
  orders_no_schema:             # schema_module/schema_name missing
    max_rows: 5
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A tiny project: schema module + transforms + YAML; cwd = project root (as in CI)."""
    (tmp_path / "cli_pipeline.py").write_text(textwrap.dedent(MODULE), encoding="utf-8")
    (tmp_path / "spark_prop_test.yaml").write_text(YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)                  # run/validate put the cwd on sys.path
    monkeypatch.setenv("SPARK_PROP_MASTER", "local[1]")
    monkeypatch.delenv("SPARK_PROP_MAX_EXAMPLES", raising=False)   # `run -n` sets it in-process
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    sys.modules.pop("cli_pipeline", None)


def test_init_writes_a_template(tmp_path):
    """init writes a YAML template and asks before overwriting."""
    runner = CliRunner()
    target = tmp_path / "suite.yaml"
    result = runner.invoke(main, ["init", "-o", str(target)])
    assert result.exit_code == 0 and "suites:" in target.read_text(encoding="utf-8")
    again = runner.invoke(main, ["init", "-o", str(target)], input="n\n")
    assert again.exit_code == 1 and "Overwrite?" in again.output


def test_config_errors_exit_1(project):
    """A missing config file or an unknown --suite exits 1 before Spark starts."""
    runner = CliRunner()
    missing = runner.invoke(main, ["run", "-c", "nope.yaml"])
    assert missing.exit_code == 1 and "Config file not found" in missing.output
    unknown = runner.invoke(main, ["run", "--suite", "nope"])
    assert unknown.exit_code == 1 and "Suite 'nope' not found" in unknown.output


def test_run_passing_transform_with_report(project):
    """run -s orders_clean -n 2 --report: exit 0 and an HTML report with the per-layer results."""
    result = CliRunner().invoke(main, ["run", "-s", "orders_clean", "-n", "2",
                                       "--report", "--report-path", "report.html"])
    assert result.exit_code == 0, result.output
    assert "All suites passed" in result.output
    html = (project / "report.html").read_text(encoding="utf-8")
    assert "PASSED" in html and "Schema Validation" in html and "Great Expectations" in html


def test_run_failing_transform_exits_1(project):
    """run -s orders_buggy: the output misses 'amount' → schema layer FAIL, exit code 1."""
    result = CliRunner().invoke(main, ["run", "-s", "orders_buggy", "-n", "2", "--no-shrink",
                                       "--report", "--report-path", "reports/report.html"])
    assert result.exit_code == 1, result.output
    assert "Some suites failed" in result.output
    html = (project / "reports" / "report.html").read_text(encoding="utf-8")   # dir created
    assert "FAILED" in html and "Property Failures" in html
    assert "Column &#39;amount&#39;" in html                  # escaped schema message


def test_suite_without_schema_fails(project):
    """A suite whose schema cannot be resolved is a failure (exit 1), not a silent skip."""
    result = CliRunner().invoke(main, ["run", "-s", "orders_no_schema"])
    assert result.exit_code == 1, result.output
    assert "No schema loaded" in result.output and "Some suites failed" in result.output


def test_validate_a_csv_file(project):
    """validate reads csv with header + inferSchema; exit 0 on a match or a warning, 1 on an ERROR."""
    (project / "orders.csv").write_text("order_id,status,amount\n1,paid,10\n2,new,0\n", encoding="utf-8")
    # amount 10.5 → double: double → int is COERCIBLE, a WARNING without strict_types → still exit 0
    (project / "coerced.csv").write_text("order_id,status,amount\n1,paid,10.5\n", encoding="utf-8")
    (project / "drift.csv").write_text("order_id,status\n1,paid\n", encoding="utf-8")   # amount missing
    runner = CliRunner()
    # absolute paths: in-process, the JVM keeps the working directory it was started in
    ok = runner.invoke(main, ["validate", str(project / "orders.csv"), "--format", "csv",
                              "--schema-module", "cli_pipeline", "--schema-name", "ORDERS"])
    assert ok.exit_code == 0, ok.output
    coerced = runner.invoke(main, ["validate", str(project / "coerced.csv"), "--format", "csv",
                                   "--schema-module", "cli_pipeline", "--schema-name", "ORDERS"])
    # 4 warnings: the coercion + 3 × "nullable contract weakened" (CSV columns are always nullable)
    assert coerced.exit_code == 0 and "Errors:   0" in coerced.output and "Warnings: 4" in coerced.output
    strict = runner.invoke(main, ["validate", str(project / "coerced.csv"), "--format", "csv",
                                  "--schema-module", "cli_pipeline", "--schema-name", "ORDERS",
                                  "--strict-types"])
    assert strict.exit_code == 1, strict.output                # --strict-types: coercion is an ERROR
    drift =runner.invoke(main, ["validate", str(project / "drift.csv"), "--format", "csv",
                                 "--schema-module", "cli_pipeline", "--schema-name", "ORDERS"])
    assert drift.exit_code == 1 and "amount" in drift.output


def test_python_dash_m_runs_the_cli():
    """`python -m spark_prop_testing.cli --help` prints the command list."""
    out = subprocess.run([sys.executable, "-m", "spark_prop_testing.cli", "--help"],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0 and "validate" in out.stdout and "run" in out.stdout
