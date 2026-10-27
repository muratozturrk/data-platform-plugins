"""Ingestion requirements -> recommended model, tools and guardrails.

The output is a starting point for an architecture review, not a final decision:
requirements trade off against each other.
"""
from dataclasses import dataclass, field

# Commits faster than a few seconds are discouraged; even 5-second commits are aggressive.
MIN_PRACTICAL_COMMIT_S = 5
HIGH_THROUGHPUT_EVENTS_PER_DAY = 1_000_000_000      # high-volume event streams: billions per day


@dataclass(frozen=True)
class Requirements:
    name: str
    freshness_s: float                     # max acceptable delay from event to queryable
    events_per_day: int
    source: str                            # kafka | database | saas | files | api
    transformations: str = "light"         # none | light | complex (joins, stateful)
    schema_changes: str = "rare"           # rare | frequent
    team: str = "platform"                 # small | platform (operational capacity)
    cloud: str | None = None               # aws | azure | gcp | None
    needs_upserts: bool = False
    replayable_source: bool = True

    def __post_init__(self):
        checks = {"source": ("kafka", "database", "saas", "files", "api"),
                  "transformations": ("none", "light", "complex"),
                  "schema_changes": ("rare", "frequent"), "team": ("small", "platform"),
                  "cloud": ("aws", "azure", "gcp", None)}
        for attr, allowed in checks.items():
            if getattr(self, attr) not in allowed:
                raise ValueError(f"{attr}={getattr(self, attr)!r} not in {allowed}")
        if self.freshness_s <= 0 or self.events_per_day < 0:
            raise ValueError("freshness_s must be > 0 and events_per_day >= 0")


@dataclass
class Recommendation:
    model: str                             # batch | micro-batch | streaming
    commit_interval_s: int
    tools: list[str]
    write_pattern: str
    delivery: str
    guardrails: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _model(r: Requirements) -> tuple[str, int]:
    if r.freshness_s <= 60:  # seconds
        return "streaming", max(MIN_PRACTICAL_COMMIT_S, int(r.freshness_s // 2))
    if r.freshness_s <= 3600:  # seconds to minutes
        return "micro-batch", int(min(r.freshness_s // 2, 900))
    return "batch", int(min(r.freshness_s, 86_400))  # hourly, daily, weekly


def evaluate(r: Requirements) -> Recommendation:
    model, interval = _model(r)
    tools: list[str] = []
    guard: list[str] = []
    warn: list[str] = []

    # Tools (selection considerations: model, transformations, team, cloud).
    managed_ok = r.transformations != "complex"
    if r.source == "kafka":
        tools += ["Apache Flink", "Spark Structured Streaming"] if model != "batch" else ["Apache Spark"]
        if managed_ok and model != "batch":
            tools.append("Redpanda Iceberg topics")
            if not r.needs_upserts and r.cloud == "aws":
                tools.append("Confluent Tableflow")  # append-only, AWS only
    elif r.source in ("saas", "database") and r.team == "small" and managed_ok:
        tools += ["Fivetran", "Airbyte"]
    elif r.transformations == "complex" or r.events_per_day >= HIGH_THROUGHPUT_EVENTS_PER_DAY:
        tools += ["Apache Flink", "Apache Spark"]
    else:
        tools += ["Apache Spark"] + (["Airbyte"] if r.source in ("saas", "database", "api") else [])
    if r.events_per_day >= HIGH_THROUGHPUT_EVENTS_PER_DAY and managed_ok:
        tools.append("Qlik (Upsolver)")
    cloud_tools = {"aws": ["AWS Glue", "Amazon EMR"] + (["Amazon Data Firehose"] if model != "batch" else []),
                   "azure": ["Azure Data Factory"],
                   "gcp": ["Google Cloud Dataflow (lands Parquet, register with Spark/Trino)"]}
    if r.cloud and managed_ok:
        tools += cloud_tools[r.cloud]
    if r.schema_changes == "frequent" and r.source == "kafka":
        tools.append("Schema Registry (Confluent or Redpanda)")

    # Write pattern.
    if r.needs_upserts:
        write = ("MERGE INTO per micro-batch, merge-on-read (Spark: position deletes, "
                 "Flink: equality deletes)" if model != "batch" else "MERGE INTO, copy-on-write")
    elif model == "batch":
        write = "overwritePartitions on a bounded input (idempotent rerun)"
    else:
        write = "append"

    # Delivery guarantee.
    if r.replayable_source:
        delivery = "exactly-once (replayable source + checkpointed offsets + atomic commits)"
    else:
        delivery = "at-least-once"
        warn.append("source cannot replay: exactly-once is impossible, make the consumer idempotent "
                    "(MERGE on a business key) or deduplicate downstream")

    # Guardrails.
    if r.freshness_s < MIN_PRACTICAL_COMMIT_S:
        warn.append(f"freshness {r.freshness_s:g}s is below the {MIN_PRACTICAL_COMMIT_S}s practical "
                    "commit floor: serve it from an OLTP store or stream state, keep Iceberg as the "
                    "analytical sink")
    elif interval <= 10:
        warn.append(f"{interval}s commits are aggressive: monitor metadata growth closely")
    if model != "batch":
        guard.append("schedule compaction of closed partitions and snapshot expiration")
    if r.needs_upserts and model != "batch":
        guard.append("compact delete files regularly; equality deletes are costly to read")
    if r.schema_changes == "frequent":
        guard.append("validate incoming schemas, alert on incompatible changes, route bad records "
                     "to a dead-letter queue")
    if r.transformations == "complex":
        guard.append("tune state size and checkpointing; budget for testing")
    if model == "streaming":
        guard.append("monitor lag, commit status and retry counts continuously")
    if r.events_per_day >= HIGH_THROUGHPUT_EVENTS_PER_DAY:
        guard.append("batch commits and size files to limit snapshot growth")

    return Recommendation(model, interval, list(dict.fromkeys(tools)), write, delivery, guard, warn)


# Six illustrative scenarios, one per dominant requirement. Input values are assumptions.
SCENARIOS = {
    "fintech fraud monitoring": Requirements(
        "fintech", freshness_s=5, events_per_day=50_000_000, source="kafka"),
    "media playback events": Requirements(
        "media", freshness_s=300, events_per_day=100_000_000_000, source="kafka"),
    "healthcare device streams": Requirements(
        "healthcare", freshness_s=60, events_per_day=200_000_000, source="kafka",
        transformations="complex"),
    "mobile product analytics": Requirements(
        "product", freshness_s=900, events_per_day=500_000_000, source="kafka",
        schema_changes="frequent"),
    "SaaS marketing and sales": Requirements(
        "saas", freshness_s=4 * 3600, events_per_day=2_000_000, source="saas", team="small"),
    "AWS-only data team": Requirements(
        "aws", freshness_s=24 * 3600, events_per_day=20_000_000, source="database", cloud="aws"),
}

if __name__ == "__main__":
    for title, req in SCENARIOS.items():
        rec = evaluate(req)
        print(f"{title}: {rec.model} every {rec.commit_interval_s}s via {', '.join(rec.tools)}")
