import pytest

from requirements_evaluator.evaluator import SCENARIOS, Requirements, evaluate


def rec(key):
    return evaluate(SCENARIOS[key])


def test_low_latency_fintech_gets_streaming_at_the_commit_floor():
    r = rec("fintech fraud monitoring")
    assert (r.model, r.commit_interval_s) == ("streaming", 5)
    assert r.tools[:2] == ["Apache Flink", "Spark Structured Streaming"]
    assert any("aggressive" in w for w in r.warnings)
    assert r.delivery.startswith("exactly-once")


def test_high_throughput_media_gets_bulk_tools_and_file_guardrails():
    r = rec("media playback events")
    assert r.model == "micro-batch" and "Qlik (Upsolver)" in r.tools
    assert any("size files" in g for g in r.guardrails)


def test_complex_transformations_exclude_managed_connectors():
    r = rec("healthcare device streams")
    assert r.tools == ["Apache Flink", "Spark Structured Streaming"]
    assert any("checkpointing" in g for g in r.guardrails)


def test_frequent_schema_change_adds_registry_and_dlq():
    r = rec("mobile product analytics")
    assert "Schema Registry (Confluent or Redpanda)" in r.tools
    assert any("dead-letter" in g for g in r.guardrails)


def test_small_team_saas_gets_managed_batch_ingestion():
    r = rec("SaaS marketing and sales")
    assert r.model == "batch" and r.tools == ["Fivetran", "Airbyte"]
    assert r.write_pattern.startswith("overwritePartitions")


def test_aws_team_gets_cloud_native_tools():
    r = rec("AWS-only data team")
    assert {"AWS Glue", "Amazon EMR"} <= set(r.tools) and "Amazon Data Firehose" not in r.tools


def test_sub_second_freshness_is_redirected_to_an_oltp_store():
    r = evaluate(Requirements("ticker", freshness_s=0.5, events_per_day=10**6, source="kafka"))
    assert r.commit_interval_s == 5
    assert any("OLTP" in w for w in r.warnings)


def test_streaming_cdc_upserts_use_merge_on_read_and_exclude_tableflow():
    r = evaluate(Requirements("cdc", freshness_s=30, events_per_day=10**7, source="kafka",
                              needs_upserts=True, cloud="aws"))
    assert "merge-on-read" in r.write_pattern and "Confluent Tableflow" not in r.tools
    appends = evaluate(Requirements("logs", freshness_s=30, events_per_day=10**7, source="kafka",
                                    cloud="aws"))
    assert "Confluent Tableflow" in appends.tools and appends.write_pattern == "append"


def test_non_replayable_source_cannot_be_exactly_once():
    r = evaluate(Requirements("webhook", freshness_s=600, events_per_day=10**5, source="api",
                              replayable_source=False))
    assert r.delivery == "at-least-once" and any("idempotent" in w for w in r.warnings)


def test_invalid_input_is_rejected():
    with pytest.raises(ValueError, match="source"):
        Requirements("x", freshness_s=60, events_per_day=1, source="ftp")
