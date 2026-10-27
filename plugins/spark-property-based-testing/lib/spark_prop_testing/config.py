# spark_prop_testing/config.py

"""
YAML and environment-variable based configuration.
Allows teams to define test suites declaratively.

Example spark_prop_test.yaml:
    suites:
      orders_pipeline:
        schema_module: myproject.schemas
        schema_name: ORDER_SCHEMA
        max_examples: 30
        null_rates:
          amount: 0.05
          status: 0.0
        ge_expectations:
          row_count_min: 1
          columns:
            order_id:
              not_null: true
              unique: true
              min_value: 1
            status:
              value_set: [pending, shipped, delivered, cancelled]
"""

import os
import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pyspark.sql.types import StructType

from .strategies.dataframe_strategy import DataFrameConfig, ColumnConfig
from .strategies.null_strategy       import NullConfig


@dataclass
class SuiteConfig:
    """Parsed configuration for one test suite."""
    name:         str
    schema:       Optional[StructType]   = None
    df_config:    DataFrameConfig        = field(default_factory=DataFrameConfig)
    ge_suite:     Any                    = None   # ExpectationSuite
    max_examples: int                    = 20


class ConfigLoader:
    """
    Loads suite configurations from YAML files or environment variables.
    Supports CI/CD pipelines where config is injected via env vars.
    """

    def __init__(self, config_path: str = "spark_prop_test.yaml"):
        self._path = Path(config_path)

    def load(self) -> Dict[str, SuiteConfig]:
        """Load all suite configs from YAML file."""
        if not self._path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self._path}\n"
                f"Run 'spark-prop-test init' to create a template."
            )

        with open(self._path, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        suites = {}
        for suite_name, suite_raw in raw.get("suites", {}).items():
            suites[suite_name] = self._parse_suite(suite_name, suite_raw)

        return suites

    def _parse_suite(self, name: str, raw: dict) -> SuiteConfig:
        """Parse one suite definition from YAML."""

        # ── Load schema from module reference ────────────────────────────────
        schema = None
        if "schema_module" in raw and "schema_name" in raw:
            module = importlib.import_module(raw["schema_module"])
            schema = getattr(module, raw["schema_name"])

        # ── Build DataFrameConfig from null_rates ─────────────────────────────
        column_configs = {}
        for col_name, null_rate in raw.get("null_rates", {}).items():
            column_configs[col_name] = ColumnConfig(
                null_config=NullConfig(null_rate=float(null_rate))
            )

        df_config = DataFrameConfig(
            min_rows=raw.get("min_rows", 1),
            max_rows=raw.get("max_rows", 100),
            column_configs=column_configs,
        )

        # ── Build GE suite from expectations block ────────────────────────────
        ge_suite = None
        if "ge_expectations" in raw:
            ge_suite = self._parse_ge_expectations(name, raw["ge_expectations"])

        return SuiteConfig(
            name=name,
            schema=schema,
            df_config=df_config,
            ge_suite=ge_suite,
            max_examples=raw.get("max_examples", 20),
        )

    def _parse_ge_expectations(
        self,
        suite_name: str,
        raw: dict,
    ):
        # Imported here: suites without ge_expectations must not need GX.
        from .ge_integration.expectation_builder import (
            ColumnExpectationSpec, ExpectationSuiteBuilder,
        )
        builder = ExpectationSuiteBuilder(f"{suite_name}_suite")

        if "row_count_min" in raw or "row_count_max" in raw:
            builder.add_row_count(
                min_rows=raw.get("row_count_min"),
                max_rows=raw.get("row_count_max"),
            )

        for col_name, col_raw in raw.get("columns", {}).items():
            builder.add_column(ColumnExpectationSpec(
                column=col_name,
                not_null=col_raw.get("not_null", False),
                unique=col_raw.get("unique", False),
                min_value=col_raw.get("min_value"),
                max_value=col_raw.get("max_value"),
                value_set=col_raw.get("value_set"),
                regex_pattern=col_raw.get("regex_pattern"),
                min_length=col_raw.get("min_length"),
                max_length=col_raw.get("max_length"),
                max_null_fraction=col_raw.get("max_null_fraction"),
            ))

        return builder.build()


def generate_config_template() -> str:
    """Returns a YAML config template string."""
    return """\
suites:
  my_pipeline:
    schema_module: myproject.schemas
    schema_name: MY_SCHEMA
    max_examples: 20
    min_rows: 1
    max_rows: 100
    null_rates:
      amount: 0.05
    ge_expectations:
      row_count_min: 1
      columns:
        id:
          not_null: true
          unique: true
          min_value: 1
        status:
          value_set: [active, inactive]
"""