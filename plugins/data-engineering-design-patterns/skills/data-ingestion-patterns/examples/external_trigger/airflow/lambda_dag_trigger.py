"""External Trigger: AWS Lambda handler that triggers an Airflow DAG per S3 object.

Push semantics: S3 invokes the function only when a new object lands, so nothing polls an empty bus.
The handler:
  1. subscribes (S3 event notification -> this Lambda, configured in infrastructure);
  2. reacts: keeps only object-created records under the expected prefix;
  3. triggers ``devices-loader`` through the Airflow stable REST API with a rich ``conf``: the raw event,
     the trigger identity (function name/version, request id) and the file to load.

Idempotency: ``dag_run_id`` is derived from the object key + version/etag, so an S3 redelivery or a Lambda
retry produces HTTP 409 (run already exists), which is treated as success instead of a duplicate run.
Error management: any other failure raises, Lambda retries and then sends the event to its on-failure
destination (SQS/SNS) so no notification is lost.

Credentials are never in code: AIRFLOW_URL, AIRFLOW_USER and AIRFLOW_PASSWORD come from the function's
environment, which should itself be filled from AWS Secrets Manager. Only the standard library is used,
so the function needs no deployment package.

Test locally: python lambda_dag_trigger.py --self-test
"""
import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "devices-loader")
EXPECTED_PREFIX = os.environ.get("EXPECTED_PREFIX", "devices/")


def build_payload(record: dict, event: dict, ctx) -> dict:
    s3 = record["s3"]
    key = urllib.parse.unquote_plus(s3["object"]["key"], encoding="utf-8")
    object_version = s3["object"].get("versionId") or s3["object"].get("eTag", "")
    run_suffix = hashlib.sha1(f"{s3['bucket']['name']}/{key}@{object_version}".encode()).hexdigest()[:16]
    return {
        "dag_run_id": f"External-{run_suffix}",
        "conf": {
            "event": json.dumps(event),
            "trigger": {"function_name": ctx.function_name, "function_version": ctx.function_version,
                        "lambda_request_id": ctx.aws_request_id},
            "file_to_load": f"s3://{s3['bucket']['name']}/{key}",
            "event_time": record.get("eventTime"),
        },
    }


def is_relevant(record: dict) -> bool:
    return (record.get("eventSource") == "aws:s3" and record.get("eventName", "").startswith("ObjectCreated")
            and urllib.parse.unquote_plus(record["s3"]["object"]["key"]).startswith(EXPECTED_PREFIX))


def trigger_dag(payload: dict) -> str:
    url = f"{os.environ['AIRFLOW_URL'].rstrip('/')}/api/v1/dags/{DAG_ID}/dagRuns"
    token = base64.b64encode(f"{os.environ['AIRFLOW_USER']}:{os.environ['AIRFLOW_PASSWORD']}".encode()).decode()
    request = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST", headers={
        "Content-Type": "application/json", "Authorization": f"Basic {token}"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return f"triggered ({response.status})"
    except urllib.error.HTTPError as err:
        if err.code == 409:
            return "already triggered"                  # duplicate notification, nothing to do
        raise RuntimeError(f"Couldn't trigger {DAG_ID}: {err.code} {err.read()[:500]!r} for {payload}") from err


def lambda_handler(event, ctx):
    results = []
    for record in event.get("Records", []):
        if not is_relevant(record):
            continue
        payload = build_payload(record, event, ctx)
        results.append((payload["dag_run_id"], trigger_dag(payload)))
    print(json.dumps({"request_id": ctx.aws_request_id, "results": results}))
    return {"triggered": results}


def _self_check() -> None:
    class Ctx:
        function_name, function_version, aws_request_id = "devices-trigger", "7", "req-1"

    record = {"eventSource": "aws:s3", "eventName": "ObjectCreated:Put", "eventTime": "2024-01-01T10:00:00Z",
              "s3": {"bucket": {"name": "in"}, "object": {"key": "devices/2024+01+01.csv", "eTag": "abc"}}}
    event = {"Records": [record]}
    p1, p2 = build_payload(record, event, Ctx), build_payload(record, event, Ctx)
    assert p1["dag_run_id"] == p2["dag_run_id"], "redelivery must map to the same run id"
    assert p1["conf"]["file_to_load"] == "s3://in/devices/2024 01 01.csv"
    assert p1["conf"]["trigger"]["lambda_request_id"] == "req-1"
    assert is_relevant(record)
    assert not is_relevant({**record, "eventName": "ObjectRemoved:Delete"})
    print("self-test OK")


if __name__ == "__main__" and "--self-test" in sys.argv:
    _self_check()
