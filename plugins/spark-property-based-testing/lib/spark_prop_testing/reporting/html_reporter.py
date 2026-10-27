# spark_prop_testing/reporting/html_reporter.py

"""
Generates a self-contained HTML report from a UnifiedQualityReport.
No external CSS dependencies — everything inline.
"""

from datetime import datetime
from pathlib import Path
from typing import List

from jinja2 import Template

from .. import __version__
from ..data_quality_suite import UnifiedQualityReport


_REPORT_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>spark-prop-testing: {{ report.suite_name }}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0f1117; color: #e2e8f0;
    padding: 2rem;
  }
  .header {
    border-bottom: 2px solid #2d3748;
    padding-bottom: 1rem; margin-bottom: 2rem;
  }
  .header h1 { font-size: 1.8rem; color: #90cdf4; }
  .header .meta { color: #718096; margin-top: 0.3rem; font-size: 0.9rem; }
  .badge {
    display: inline-block; padding: 0.2rem 0.8rem;
    border-radius: 9999px; font-weight: 600; font-size: 0.85rem;
  }
  .badge-pass { background: #1a4731; color: #68d391; }
  .badge-fail { background: #4a1c1c; color: #fc8181; }
  .badge-warn { background: #44321a; color: #f6ad55; }
  .section {
    background: #1a202c; border-radius: 8px;
    padding: 1.5rem; margin-bottom: 1.5rem;
    border: 1px solid #2d3748;
  }
  .section h2 {
    font-size: 1.1rem; color: #90cdf4;
    margin-bottom: 1rem; padding-bottom: 0.5rem;
    border-bottom: 1px solid #2d3748;
  }
  table {
    width: 100%; border-collapse: collapse; font-size: 0.88rem;
  }
  th {
    background: #2d3748; color: #a0aec0;
    padding: 0.6rem 1rem; text-align: left;
    font-weight: 600; text-transform: uppercase;
    font-size: 0.75rem; letter-spacing: 0.05em;
  }
  td { padding: 0.6rem 1rem; border-bottom: 1px solid #2d3748; }
  tr:hover td { background: #2d3748; }
  .summary-grid {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 1rem; margin-bottom: 2rem;
  }
  .summary-card {
    background: #1a202c; border-radius: 8px;
    padding: 1.2rem; text-align: center;
    border: 1px solid #2d3748;
  }
  .summary-card .number {
    font-size: 2.5rem; font-weight: 700; line-height: 1;
  }
  .summary-card .label {
    font-size: 0.8rem; color: #718096;
    margin-top: 0.4rem; text-transform: uppercase; letter-spacing: 0.05em;
  }
  .number-pass { color: #68d391; }
  .number-fail { color: #fc8181; }
  .number-warn { color: #f6ad55; }
  .number-info { color: #90cdf4; }
  pre {
    background: #0f1117; padding: 1rem;
    border-radius: 4px; overflow-x: auto;
    font-size: 0.82rem; color: #a0aec0;
    border: 1px solid #2d3748;
  }
</style>
</head>
<body>

<div class="header">
  <h1>
    spark-prop-testing Report:
    <span style="color:#e2e8f0">{{ report.suite_name }}</span>
    <span class="badge {{ 'badge-pass' if report.is_valid else 'badge-fail' }}">
      {{ 'PASSED' if report.is_valid else 'FAILED' }}
    </span>
  </h1>
  <div class="meta">
    Generated: {{ generated_at }} &nbsp;|&nbsp;
    spark-prop-testing v{{ version }}
  </div>
</div>

<!-- Summary cards -->
<div class="summary-grid">
  <div class="summary-card">
    <div class="number {{ 'number-pass' if report.is_valid else 'number-fail' }}">
      {{ '✓' if report.is_valid else '✗' }}
    </div>
    <div class="label">Overall</div>
  </div>
  <div class="summary-card">
    <div class="number number-info">{{ schema_issue_count }}</div>
    <div class="label">Schema Issues</div>
  </div>
  <div class="summary-card">
    <div class="number {{ 'number-pass' if ge_pass_count == ge_total else 'number-fail' }}">
      {{ ge_pass_count }}/{{ ge_total }}
    </div>
    <div class="label">GE Expectations</div>
  </div>
  <div class="summary-card">
    <div class="number {{ 'number-fail' if prop_fail_count > 0 else 'number-pass' }}">
      {{ prop_fail_count }}
    </div>
    <div class="label">Property Failures</div>
  </div>
</div>

<!-- Schema Validation -->
{% if report.schema_results %}
<div class="section">
  <h2>Schema Validation</h2>
  {% for schema_result in report.schema_results %}
  <pre>{{ schema_result }}</pre>
  {% endfor %}
</div>
{% endif %}

<!-- GE Results -->
{% if report.ge_results %}
<div class="section">
  <h2>Content Validation (Great Expectations)</h2>
  {% for ge_result in report.ge_results %}
  <table>
    <thead>
      <tr>
        <th>Status</th>
        <th>Column</th>
        <th>Expectation</th>
        <th>Violations</th>
        <th>Violation %</th>
      </tr>
    </thead>
    <tbody>
      {% for r in ge_result.results %}
      <tr>
        <td>
          <span class="badge {{ 'badge-pass' if r.success else 'badge-fail' }}">
            {{ 'PASS' if r.success else 'FAIL' }}
          </span>
        </td>
        <td style="color: #90cdf4; font-family: monospace;">
          {{ r.column or '(table)' }}
        </td>
        <td style="font-family: monospace; font-size: 0.82rem; color: #a0aec0;">
          {{ r.expectation_type }}
        </td>
        <td>{{ r.unexpected_count if not r.success else '—' }}</td>
        <td>{{ '%.1f%%'|format(r.unexpected_pct * 100) if not r.success else '—' }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% endfor %}
</div>
{% endif %}

<!-- Property Failures -->
{% if report.property_failures %}
<div class="section">
  <h2>Property Failures</h2>
  {% for failure in report.property_failures %}
  <pre style="border-left: 3px solid #fc8181;">{{ failure }}</pre>
  {% endfor %}
</div>
{% endif %}

</body>
</html>
"""


class HTMLReporter:
    """Generates a self-contained HTML report from a UnifiedQualityReport."""

    def generate(
        self,
        report:      UnifiedQualityReport,
        output_path: str = "spark_prop_test_report.html",
    ) -> str:
        """Renders the report and writes to output_path. Returns the path."""

        # ── Count GE expectations ─────────────────────────────────────────────
        ge_total     = sum(len(r.results) for r in report.ge_results)
        ge_pass_count = sum(
            sum(1 for e in r.results if e.success)
            for r in report.ge_results
        )
        schema_issue_count = sum(1 for ok in report.schema_valid if not ok)

        # autoescape: failure messages contain generated data (edge-case
        # strings such as "<script>"), which must not become live HTML.
        html = Template(_REPORT_TEMPLATE, autoescape=True).render(
            report=report,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            version=__version__,
            ge_total=ge_total,
            ge_pass_count=ge_pass_count,
            schema_issue_count=schema_issue_count,
            prop_fail_count=len(report.property_failures),
        )

        Path(output_path).write_text(html, encoding="utf-8")
        return output_path