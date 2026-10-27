# spark_prop_testing/ge_integration/context_factory.py

"""
Bootstraps a Great Expectations (GX 1.x) context configured for PySpark.
Uses the ephemeral (in-memory) context — nothing is written to disk, so it is
safe for parallel test runs.

GX 1.x flow: context → Spark data source → DataFrame asset → whole-DataFrame
batch definition → batch (the DataFrame is passed as a batch parameter).
"""

import logging
import os

import great_expectations as gx
from pyspark.sql import DataFrame

# No usage-statistics network calls from test runs.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
logging.getLogger("great_expectations").setLevel(logging.WARNING)


def build_ge_context():
    """Creates an ephemeral (in-memory) GX context."""
    return gx.get_context(mode="ephemeral")


def _get_or_add_batch_definition(context, datasource_name: str, asset_name: str):
    try:
        datasource = context.data_sources.get(datasource_name)
    except Exception:  # GX raises different KeyError subclasses across 1.x versions
        datasource = context.data_sources.add_spark(name=datasource_name)
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_dataframe_asset(name=asset_name)
    try:
        return asset.get_batch_definition("whole_dataframe")
    except Exception:
        return asset.add_batch_definition_whole_dataframe("whole_dataframe")


def df_to_ge_batch(
    context,
    df: DataFrame,
    datasource_name: str = "spark_datasource",
    asset_name: str = "test_asset",
):
    """
    Wraps a PySpark DataFrame in a GX Batch so it can be validated against an
    expectation suite (``batch.validate(suite)``). The data source, asset and
    batch definition are created once per context and reused.
    """
    batch_definition = _get_or_add_batch_definition(context, datasource_name, asset_name)
    return batch_definition.get_batch(batch_parameters={"dataframe": df})
