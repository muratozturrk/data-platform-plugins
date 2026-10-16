# API Gateway

Category: Streaming Data Ingestion.

## Problem
The product team needs a near real-time sessionization pipeline. In a hurry, you created a
Kafka topic and asked backend engineers to send all user actions to it directly.
It worked for a month, then the system scaled up:
- unexpected maintenance on the growing Kafka cluster made the topic unreachable, and events
  were **dropped** because nobody had planned the failure scenario;
- a bad backend release wrote **malformed** data that consumers could not read.

You must remove the tight coupling between producers and Kafka, and the resulting downstream
data quality problems.

## Solution / Mechanics
Put an intermediary service between data producers (backends, mobile apps, games) and the
streaming broker. Producers talk to the gateway; only the gateway
talks to Kafka.

```
 before:  web/mobile/backend --------------------------------> Kafka topic
 after :  web/mobile/backend --HTTP(S)/gRPC--> [LB + DNS] --> API gateway --> Kafka topic
                                                                 |  ^
                                         undelivered records     v  | replay / drain
                                                       dead-letter or buffer storage
```

Coordinate five criteria between producers and the gateway:
1. **Connectivity**: widely supported protocols, typically HTTP(S) or gRPC.
2. **Fault tolerance**: broker unavailability must be transparent to producers:
   - *dead-letter storage*: write undelivered records to a secondary store; a **dedicated
     replay pipeline** integrates them later;
   - *buffer storage*: keep undelivered records in a local buffer (for example a queue); the
     **gateway itself** drains it once the broker is reachable again.
   The difference is locality: who replays the stored records.
3. **Scalability**: elastic scaling for traffic spikes; otherwise the gateway becomes a
   bottleneck for the user-facing application.
4. **Availability**: deployments must never impact producers. A DNS name pointing to a load
   balancer separates both layers so they evolve independently.
5. **Data validation**: protect internal data with schema (technical) validation and
   semantic (business rule) validation of each record. Semantic validation follows the
   shift-left paradigm: validate once, as close to the source as possible, for all consumers.

Request flow in a Flask example:
1. `POST /ingestion/ingest/<topic>/<event_key>` receives a JSON array.
2. Build headers (`source=api`, `referral` from the `Referer` header).
3. Produce each record with a delivery callback, then `flush(timeout=20)`.
4. Records not acknowledged in 20 s are written to fallback storage
   (`/fallback/topic=<topic>/<file>`).
5. A Spark Structured Streaming job with `availableNow` and a checkpoint replays new fallback
   files to Kafka, sorted per key and time, with `src=replay` in the headers;
   `cleanSource=delete` removes processed files.

## Consequences & trade-offs
Three points of friction:
- **Business logic**: semantic validation belongs in the gateway, but not all business rules.
  The gateway must guard against critical quality issues while still scaling. Rules have a
  scope risk too: an approximation such as reducing event frequency from 10 s to 1 min breaks
  every consumer that expects the original data.
- **Extra security concerns**: besides the data security patterns, the gateway is an application
  exposed to the outside, with application security concerns such as DDoS protection.
  Managed cloud offerings bring that protection natively.
- **Data producers and resilience**: even a fault-tolerant, fully managed gateway can be
  unavailable (no service guarantees 100% uptime). Producers must still buffer data locally
  during temporary gateway unavailability.
- Replayed records arrive late and out of order compared with real-time ones, and a record
  that timed out may still have been delivered, so the replay can duplicate it. Consumers must
  be idempotent and late-data aware.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Fault tolerance | dead-letter storage + replay pipeline, local buffer drained by the gateway | dead-letter survives gateway crashes; local buffers die with the instance unless persistent |
| Fallback store | object store, durable queue, local disk (demo only) | needs a high SLA and its own monitoring |
| Response on fallback | 202 accepted (transparent), 503 (producer retries) | 202 only if the fallback write is durable |
| Validation depth | schema only, schema + critical semantic rules | keep heavy business logic downstream |
| Topic routing | path parameter with allow-list, fixed topic per endpoint | never let callers write to arbitrary topics |
| Hosting | managed API gateway + serverless/containers, self-hosted | managed brings DDoS protection and autoscaling |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Many producers (web, mobile, partners) send events | Producers are internal services already using Kafka clients with retries and schema registry | direct produce + Schema Compatibility Enforcer |
| Broker outages must not drop events | Data already lives in a database | Change Data Capture |
| Payloads must be validated before entering the platform | You need to copy an existing topic elsewhere | Passthrough Replicator |

## Gotchas & best practices
- Treat "not acknowledged within the flush timeout" as failed, but remember the record may
  still land: replays create duplicates, so key the records and dedupe downstream.
- Track delivery per request (per-request callback state), not in a global list shared by
  concurrent requests.
- Bound the request size and reject invalid payloads with 400 before producing anything.
- Use `acks=all` and `enable.idempotence=true` on the gateway producer.
- Alert on the fallback backlog (file count, age). A growing backlog means the gateway still
  cannot reach the broker.
- Mark origin in headers (`api` vs `replay`) so consumers can distinguish replayed data.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Events lost during broker maintenance | no fallback, or fallback on ephemeral disk | durable fallback store + replay pipeline |
| Duplicates after replay | timed-out records delivered late | idempotent consumers (keyed upsert, dedup window) |
| Gateway latency spikes | synchronous flush per request under load | autoscale, smaller batches, async ack with durable buffer |
| Replay job re-sends old files | checkpoint deleted or not configured | stable `checkpointLocation`, `cleanSource` |
| Consumers break after a gateway release | a semantic rule modified data | keep rules to validation; transformations go downstream |

## Implementation notes per stack
**Kafka (gateway)** - `examples/api_gateway/kafka/gateway_app.py`, Flask + confluent-kafka
, with a topic allow-list, schema + one semantic rule, and
per-request delivery tracking:

```python
for i, record in enumerate(records):
    producer.produce(topic=topic, key=event_key.encode("utf-8"),
                     value=json.dumps(record).encode("utf-8"), headers=headers,
                     on_delivery=lambda err, msg, i=i: on_delivery(err, msg, i))
producer.flush(timeout=20)
failed = [r for r, ok in zip(records, delivered) if not ok]
if failed:   # dead-letter storage, replayed by the Spark job
    (FALLBACK_DIR / f"topic={topic}" / f"{uuid.uuid4()}.json").write_text(...)
```

**Spark (replay)** - `examples/api_gateway/spark/fallback_replay_job.py`:

```python
input_df = spark.readStream.schema(SCHEMA).option("cleanSource", "delete").json(FALLBACK_DIR)
(input_df.writeStream.option("checkpointLocation", CHECKPOINT_DIR)
 .trigger(availableNow=True).foreachBatch(write_sorted_data_to_kafka).start())
```
`write_sorted_data_to_kafka` calls `sortWithinPartitions("key", "payload.event_time")` and
adds `source=replay` / `referral` headers.

**Airflow** - `examples/api_gateway/airflow/fallback_replay_dag.py`: every 15 minutes a
`@task.short_circuit` checks for fallback files and alerts above a backlog threshold, then a
`SparkSubmitOperator` runs the replay job.

## Related patterns
- [Dead-Letter](../../error-management-patterns/references/dead_letter.md): the fallback storage is a dead-letter store for undeliverable records.
- [Constraints Enforcer](../../data-quality-patterns/references/constraints_enforcer.md): the validation step; protovalidate-style rules can live in the gateway.
- [Checkpointer](../../error-management-patterns/references/checkpointer.md): lets the replay job read each fallback file once.
- [Metadata Decorator](../../data-value-patterns/references/metadata_decorator.md): `source`/`referral` headers distinguish real-time and replayed records.
- [Zero-ETL Synchronizer](zero_etl_synchronizer.md): the next step, landing the gateway's topic in the lakehouse.
- [Keyed Idempotency](../../idempotency-patterns/references/keyed_idempotency.md): handles duplicates created by replays.
