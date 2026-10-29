"""Spec-conformant request bodies for calls that are often written incorrectly.

Sources: spec/polaris-management-service.yml (apache/polaris) and
open-api/rest-catalog-open-api.yaml (apache/iceberg). Each function is the corrected form of one
naive example and is exercised by the lab tests.
"""
from __future__ import annotations

import time

from common.polaris import S3_ENDPOINT


def create_catalog(name: str, base_location: str) -> dict:
    """Naive body omits storageConfigInfo, which is required (400 'must not be null')."""
    return {"catalog": {"type": "INTERNAL", "name": name,
                        "properties": {"default-base-location": base_location},
                        "storageConfigInfo": {"storageType": "S3", "allowedLocations": [base_location],
                                              "region": "us-east-1", "endpoint": S3_ENDPOINT,
                                              "pathStyleAccess": True, "stsUnavailable": True}}}


def revoke_grant(privilege: str) -> tuple[str, dict]:
    """Naive: DELETE .../grants?cascade=true. Spec: POST .../grants (cascade not implemented)."""
    return "POST", {"grant": {"type": "catalog", "privilege": privilege}}


def create_table(name: str) -> dict:
    """Naive body uses 'spec'; the spec field is 'partition-spec' (with explicit field ids)."""
    return {"name": name,
            "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "customer_id", "required": True, "type": "long"},
                {"id": 2, "name": "email", "required": False, "type": "string"},
                {"id": 3, "name": "created_at", "required": True, "type": "timestamp"}]},
            "partition-spec": {"spec-id": 0, "fields": [
                {"source-id": 3, "field-id": 1000, "transform": "day", "name": "day_created"}]},
            "stage-create": False}


def commit_properties(table_uuid: str, updates: dict) -> dict:
    """Naive body sends {'updates': {'metadata-location', 'base'}}. A REST commit sends requirements and
    a list of metadata updates; the server writes the new metadata.json itself."""
    return {"requirements": [{"type": "assert-table-uuid", "uuid": table_uuid}],
            "updates": [{"action": "set-properties", "updates": updates}]}


def transaction(changes: list[tuple[list[str], str, str, dict]]) -> dict:
    """Naive body sends {'updates': [...]}; the spec field is 'table-changes'."""
    return {"table-changes": [
        {"identifier": {"namespace": ns, "name": name},
         "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
         "updates": [{"action": "set-properties", "updates": props}]}
        for ns, name, uuid, props in changes]}


def commit_report(table: str, snapshot_id: int, sequence_number: int) -> dict:
    """Naive body sends free-form {'metrics': {...}}; the spec accepts Iceberg ScanReport/CommitReport."""
    return {"report-type": "commit-report", "table-name": table, "snapshot-id": snapshot_id,
            "sequence-number": sequence_number, "operation": "append",
            "metrics": {"total-duration": {"count": 1, "time-unit": "nanoseconds", "total-duration": 231_000_000}}}


def create_view(name: str, namespace: list[str], sql: str, schema_fields: list[dict]) -> dict:
    """Naive body sends {'identifier', 'view-version': {'version-id': 'v1', 'sql'}}. The spec needs name,
    schema, and a view-version with an integer id, timestamp, summary and SQL representations."""
    return {"name": name,
            "schema": {"type": "struct", "schema-id": 0, "fields": schema_fields},
            "view-version": {"version-id": 1, "schema-id": 0, "timestamp-ms": int(time.time() * 1000),
                             "summary": {"engine-name": "lab"}, "default-namespace": namespace,
                             "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}]},
            "properties": {}}


def replace_view(schema_id: int, version_id: int, namespace: list[str], sql: str) -> dict:
    """Replacing a view is a commit on /views/{view}, not a second POST to /views."""
    return {"requirements": [],
            "updates": [{"action": "add-view-version", "view-version": {
                "version-id": version_id, "schema-id": schema_id, "timestamp-ms": int(time.time() * 1000),
                "summary": {"engine-name": "lab"}, "default-namespace": namespace,
                "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}]}},
                {"action": "set-current-view-version", "view-version-id": -1}]}


def rename(source: tuple[list[str], str], destination: tuple[list[str], str]) -> dict:
    """Tables and views: {'source', 'destination'} (not 'from'/'to' for views)."""
    return {"source": {"namespace": source[0], "name": source[1]},
            "destination": {"namespace": destination[0], "name": destination[1]}}
