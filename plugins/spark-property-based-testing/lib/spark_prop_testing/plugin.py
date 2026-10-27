# spark_prop_testing/plugin.py

"""
pytest plugin for spark-prop-testing.

Provides:
  1. --spark-prop-report flag → generates HTML report after test run
  2. spark fixture (session-scoped, auto-configured)
  3. spark_prop_config fixture (loads from YAML)
  4. Automatic HTML report generation on test failure
"""

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ._hypothesis import MAX_EXAMPLES_ENV, register_spark_profiles


def pytest_configure(config):
    """Registers the 'spark' / 'spark-ci' Hypothesis profiles and loads 'spark'
    unless the user picked one with --hypothesis-profile."""
    register_spark_profiles()
    from hypothesis import settings
    chosen = None
    try:
        chosen = config.getoption("--hypothesis-profile")
    except ValueError:  # hypothesis' own pytest plugin not active
        pass
    settings.load_profile(chosen or "spark")
    override = config.getoption("--spark-prop-max-examples", default=None)
    if override is not None:
        os.environ[MAX_EXAMPLES_ENV] = str(override)


# ── CLI options ───────────────────────────────────────────────────────────────
def pytest_addoption(parser):
    group = parser.getgroup("spark_prop_testing")
    group.addoption(
        "--spark-prop-report",
        action="store_true",
        default=False,
        help="Generate HTML report after test run",
    )
    group.addoption(
        "--spark-prop-report-path",
        default="spark_prop_test_report.html",
        help="Path for HTML report output",
    )
    group.addoption(
        "--spark-prop-max-examples",
        type=int,
        default=None,
        help="Override max_examples in the library runners (spark_property, "
             "TransformationPropertyTester, DataQualityPropertySuite)",
    )
    group.addoption(
        "--spark-prop-config",
        default="spark_prop_test.yaml",
        help="Path to YAML config file",
    )


# ── Session-scoped SparkSession fixture ───────────────────────────────────────
@pytest.fixture(scope="session")
def spark(request) -> SparkSession:
    """
    Auto-configured SparkSession for property testing.
    Injected automatically into all test functions with a 'spark' parameter.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("spark-prop-testing")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── Config fixture ─────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark_prop_config(request):
    """Loads YAML config if it exists, returns None otherwise."""
    config_path = request.config.getoption("--spark-prop-config")

    if not Path(config_path).exists():
        return None

    from .config import ConfigLoader
    return ConfigLoader(config_path).load()


# ── max_examples override fixture ─────────────────────────────────────────────
@pytest.fixture(scope="session")
def prop_max_examples(request) -> int:
    override = request.config.getoption("--spark-prop-max-examples")
    return override if override is not None else 20  # for raw @settings(max_examples=...)


# ── Report collection ─────────────────────────────────────────────────────────
_collected_reports = []


@pytest.fixture
def quality_report_collector():
    """
    Fixture that collects UnifiedQualityReports during the test session
    for inclusion in the final HTML report.
    """
    def collect(report):
        _collected_reports.append(report)
    return collect


def pytest_sessionfinish(session, exitstatus):
    """Hook: after all tests, generate HTML report if requested."""
    if not session.config.getoption("--spark-prop-report", default=False):
        return

    if not _collected_reports:
        return

    report_path = session.config.getoption(
        "--spark-prop-report-path",
        default="spark_prop_test_report.html",
    )

    from .data_quality_suite import UnifiedQualityReport
    from .reporting.html_reporter import HTMLReporter

    # Merge all collected reports into one
    merged = UnifiedQualityReport(suite_name="Full Test Run")
    for r in _collected_reports:
        merged.schema_results.extend(r.schema_results)
        merged.schema_valid.extend(r.schema_valid)
        merged.ge_results.extend(r.ge_results)
        merged.property_failures.extend(r.property_failures)

    path = HTMLReporter().generate(merged, output_path=report_path)

    # Print to terminal using rich if available
    try:
        from rich.console import Console
        from rich.panel import Panel
        Console().print(Panel(
            f"[bold]HTML Report:[/bold] {path}",
            title="spark-prop-testing",
            style="cyan",
        ))
    except ImportError:
        print(f"\nspark-prop-testing: HTML report written to {path}")