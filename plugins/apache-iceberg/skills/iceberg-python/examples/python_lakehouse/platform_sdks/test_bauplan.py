"""Bauplan: cloud service only, so the documented client calls are checked against the SDK offline."""
import inspect

import bauplan
import pytest

# Every documented Bauplan call of the branch-import-merge workflow, with its arguments.
DOCUMENTED_CALLS = {
    "create_branch": dict(branch="dev", from_ref="main"),
    "create_namespace": dict(namespace="default", branch="dev"),
    "create_table": dict(table="trips", search_uri="s3://data-lake/trips/*.parquet",
                         namespace="default", branch="dev", replace=True),
    "import_data": dict(table="trips", search_uri="s3://data-lake/trips/*.parquet",
                        namespace="default", branch="dev"),
    "scan": dict(table="trips", ref="dev", namespace="default",
                 columns=["passenger_count", "total_amount"]),
    "merge_branch": dict(source_ref="dev", into_branch="main"),
    "delete_branch": dict(branch="dev"),
}


def test_client_constructs_offline():
    client = bauplan.Client(api_key="my_api_key")       # no network call until an operation runs
    assert isinstance(client, bauplan.Client)


@pytest.mark.parametrize("method", sorted(DOCUMENTED_CALLS))
def test_documented_call_matches_sdk_signature(method):
    sig = inspect.signature(getattr(bauplan.Client, method))
    sig.bind(None, **DOCUMENTED_CALLS[method])                # raises TypeError on a mismatch


def test_documented_update_schema_does_not_exist():
    assert not hasattr(bauplan.Client, "update_schema")
