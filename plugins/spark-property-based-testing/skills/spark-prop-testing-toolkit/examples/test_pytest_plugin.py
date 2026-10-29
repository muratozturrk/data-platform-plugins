"""
The pytest plugin (spark_prop_testing.plugin): command-line options, the auto-loaded 'spark'
Hypothesis profile vs --hypothesis-profile=spark-ci, --spark-prop-max-examples, and the HTML
report from quality_report_collector. Each check runs an inner pytest session in a subprocess
(pytester); the inner tests need no Spark, so this file is fast.

Run:
    GX_ANALYTICS_ENABLED=false PYTHONPATH=<plugin>/lib python -m pytest -q test_pytest_plugin.py
"""

import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import pytest

pytest_plugins = ["pytester"]

INNER_SETTINGS = """
import os
from hypothesis import settings

def test_settings(prop_max_examples):
    s = settings.default
    print("PROFILE", settings.get_current_profile_name(), s.max_examples, s.derandomize,
          s.deadline, s.database is None)
    print("ENV", os.environ.get("SPARK_PROP_MAX_EXAMPLES"), "FIXTURE", prop_max_examples)
"""

INNER_REPORT = """
from spark_prop_testing import UnifiedQualityReport

def test_collect(quality_report_collector):
    report = UnifiedQualityReport(suite_name="orders")
    report.schema_results.append("Schema 'orders': VALID")
    report.schema_valid.append(True)
    report.property_failures.append("status was <script>alert(1)</script>")
    quality_report_collector(report)
"""


def test_options_are_registered(pytester):
    """The plugin adds four --spark-prop-* options to pytest."""
    result = pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", "--help")
    help_text = result.stdout.str()
    for option in ("--spark-prop-report", "--spark-prop-report-path",
                   "--spark-prop-max-examples", "--spark-prop-config"):
        assert option in help_text


def test_spark_profile_is_loaded_by_default(pytester):
    """Without --hypothesis-profile the plugin loads 'spark': no deadline, 20 examples."""
    pytester.makepyfile(test_inner=INNER_SETTINGS)
    result = pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", "-s",
                                           "--spark-prop-max-examples", "7")
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*PROFILE spark 20 False None False*"])
    # the override reaches library runners through the environment variable
    result.stdout.fnmatch_lines(["*ENV 7 FIXTURE 7*"])


def test_spark_ci_profile(pytester):
    """--hypothesis-profile=spark-ci works with the plugin alone: 100 examples, derandomized, no database."""
    pytester.makepyfile(test_inner=INNER_SETTINGS)          # no conftest registration needed
    result = pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", "-s",
                                           "--hypothesis-profile=spark-ci")
    assert "is not registered" not in result.stdout.str() + result.stderr.str()
    result.assert_outcomes(passed=1)
    # the profile's 100 does not reach library runners: CI also needs --spark-prop-max-examples
    result.stdout.fnmatch_lines(["*PROFILE spark-ci 100 True None True*", "*ENV None FIXTURE 20*"])


def test_html_report_from_collected_reports(pytester):
    """--spark-prop-report writes the collected reports to --spark-prop-report-path, HTML-escaped."""
    pytester.makepyfile(test_inner=INNER_REPORT)
    result = pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", "--spark-prop-report",
                                           "--spark-prop-report-path", "out/report.html")
    # the reporter created the missing out/ directory
    result.assert_outcomes(passed=1)
    html = (pytester.path / "out" / "report.html").read_text(encoding="utf-8")
    assert "Full Test Run" in html and "FAILED" in html          # one property failure
    assert "Schema &#39;orders&#39;: VALID" in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html and "<script>" not in html


def test_no_report_without_collected_reports(pytester):
    """Nothing collected → no file, even with --spark-prop-report."""
    pytester.makepyfile(test_inner="def test_nothing():\n    pass\n")
    (pytester.path / "out").mkdir()
    result = pytester.runpytest_subprocess("-p", "spark_prop_testing.plugin", "--spark-prop-report",
                                           "--spark-prop-report-path", "out/report.html")
    result.assert_outcomes(passed=1)
    assert not (pytester.path / "out" / "report.html").exists()
