# spark_prop_testing/cli.py

"""
CLI entry point: spark-prop-test

Commands:
  spark-prop-test run        → run all suites from YAML config
  spark-prop-test init       → create a config template
  spark-prop-test validate   → validate a parquet/csv/json file against a schema

`run --report` writes the HTML report.
"""

import os
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

console = Console()


def _no_shrink():
    from hypothesis import Phase
    return (Phase.explicit, Phase.reuse, Phase.generate)


@click.group()
@click.version_option(package_name="spark-prop-testing", prog_name="spark-prop-test")
def main():
    """spark-prop-testing: Property-based testing for PySpark pipelines."""
    pass


@main.command()
@click.option(
    "--config", "-c",
    default="spark_prop_test.yaml",
    help="Path to YAML config file",
    show_default=True,
)
@click.option(
    "--max-examples", "-n",
    default=None, type=int,
    help="Override max_examples for all suites",
)
@click.option(
    "--report", "-r",
    is_flag=True, default=False,
    help="Generate HTML report",
)
@click.option(
    "--report-path",
    default="spark_prop_test_report.html",
    show_default=True,
)
@click.option(
    "--suite", "-s",
    default=None,
    help="Run only this suite (by name)",
)
@click.option(
    "--no-shrink",
    is_flag=True, default=False,
    help="Skip shrinking (faster failures, non-minimal examples)",
)
def run(config, max_examples, report, report_path, suite, no_shrink):
    """Run property test suites from a YAML config file."""

    from .config import ConfigLoader
    from .data_quality_suite import DataQualityPropertySuite, QualitySuiteConfig
    from pyspark.sql import SparkSession

    _cwd_on_path()

    console.print(Panel(
        "[bold cyan]spark-prop-testing[/bold cyan] — starting run",
        style="cyan",
    ))

    # ── Load config ───────────────────────────────────────────────────────────
    try:
        suites = ConfigLoader(config).load()
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    if suite:
        if suite not in suites:
            console.print(f"[red]Suite '{suite}' not found in config[/red]")
            sys.exit(1)
        suites = {suite: suites[suite]}

    # ── Start Spark ───────────────────────────────────────────────────────────
    console.print("Starting SparkSession...", style="dim")
    spark = (
        SparkSession.builder
        .master(os.environ.get("SPARK_PROP_MASTER", "local[2]"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # ── Run each suite ────────────────────────────────────────────────────────
    all_reports = []
    overall_passed = True

    for suite_name, suite_config in suites.items():
        console.print(f"\nRunning suite: [bold]{suite_name}[/bold]")

        if suite_config.schema is None:
            console.print(
                f"  [red]✗ No schema loaded (schema_module/schema_name) — suite not run[/red]"
            )
            overall_passed = False
            continue

        if max_examples is not None:
            suite_config.max_examples = max_examples
            os.environ["SPARK_PROP_MAX_EXAMPLES"] = str(max_examples)

        quality_config = QualitySuiteConfig(
            suite_name=suite_name,
            expected_schema=suite_config.schema,
            ge_suite=suite_config.ge_suite,
            df_config=suite_config.df_config,
            max_examples=suite_config.max_examples,
            transform=suite_config.transform,
            input_schema=suite_config.input_schema,
            phases=_no_shrink() if no_shrink else None,
        )

        runner  = DataQualityPropertySuite(spark, quality_config)
        result  = runner.run_property_loop()
        all_reports.append(result)

        _print_suite_result(result)

        if not result.is_valid:
            overall_passed = False

    spark.stop()

    # ── HTML Report ───────────────────────────────────────────────────────────
    if report and all_reports:
        from .data_quality_suite import UnifiedQualityReport
        from .reporting.html_reporter import HTMLReporter

        merged = UnifiedQualityReport(suite_name="Full Run")
        for r in all_reports:
            merged.schema_results.extend(r.schema_results)
            merged.schema_valid.extend(r.schema_valid)
            merged.ge_results.extend(r.ge_results)
            merged.property_failures.extend(r.property_failures)

        path = HTMLReporter().generate(merged, output_path=report_path)
        console.print(f"\n[cyan]HTML report:[/cyan] {path}")

    # ── Exit code ─────────────────────────────────────────────────────────────
    console.print(
        f"\n[bold]{'✓ All suites passed' if overall_passed else '✗ Some suites failed'}[/bold]",
        style="green" if overall_passed else "red",
    )
    sys.exit(0 if overall_passed else 1)


@main.command()
@click.option("--output", "-o", default="spark_prop_test.yaml")
def init(output):
    """Create a config file template."""
    from .config import generate_config_template

    if Path(output).exists():
        click.confirm(f"{output} already exists. Overwrite?", abort=True)

    Path(output).write_text(generate_config_template())
    console.print(f"[green]✓[/green] Config template written to [bold]{output}[/bold]")
    console.print("Edit it to point to your schemas, then run:")
    console.print(f"  [bold cyan]spark-prop-test run --config {output}[/bold cyan]")


@main.command()
@click.argument("file_path")
@click.option("--schema-module", required=True)
@click.option("--schema-name",   required=True)
@click.option("--format", "file_format", default="parquet",
              type=click.Choice(["parquet", "csv", "json"]))
@click.option("--strict-types", is_flag=True, default=False,
              help="Treat lossless/lossy type differences as errors too")
def validate(file_path, schema_module, schema_name, file_format, strict_types):
    """Validate an existing data file against a schema."""
    import importlib
    from .schema.schema_validator import SchemaValidator
    from pyspark.sql import SparkSession

    _cwd_on_path()
    module = importlib.import_module(schema_module)
    schema = getattr(module, schema_name)

    spark  = SparkSession.builder.master("local[1]").getOrCreate()
    reader = spark.read.format(file_format)
    if file_format == "csv":
        # Without these every column reads as string and every type "drifts".
        reader = reader.option("header", "true").option("inferSchema", "true")
    df = reader.load(file_path)

    validator = SchemaValidator(schema, strict_types=strict_types, schema_name=schema_name)
    result    = validator.validate(df)

    console.print(result.summary())
    spark.stop()
    sys.exit(0 if result.is_valid else 1)


def _cwd_on_path():
    """Console scripts don't put the working directory on sys.path, so
    `--schema-module myproject.schemas` would not import from the project."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def _print_suite_result(result):
    """Prints a rich table summarising one suite result."""
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Layer",   style="dim")
    table.add_column("Status")
    table.add_column("Details")

    schema_ok = all(result.schema_valid)
    table.add_row(
        "Schema",
        "[green]PASS[/green]" if schema_ok else "[red]FAIL[/red]",
        f"{len(result.schema_results)} checks",
    )

    ge_ok = all(r.success for r in result.ge_results)
    ge_total = sum(len(r.results) for r in result.ge_results)
    table.add_row(
        "Content (GE)",
        "[green]PASS[/green]" if ge_ok else "[red]FAIL[/red]",
        f"{ge_total} expectations",
    )

    props_ok = len(result.property_failures) == 0
    table.add_row(
        "Properties",
        "[green]PASS[/green]" if props_ok else "[red]FAIL[/red]",
        f"{len(result.property_failures)} failures",
    )

    console.print(table)

if __name__ == "__main__":
    main()
