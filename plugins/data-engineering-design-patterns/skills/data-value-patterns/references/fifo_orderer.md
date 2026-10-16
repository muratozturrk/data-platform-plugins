# FIFO Orderer

> Category: data ordering, per-record (or one-request-at-a-time) delivery. Mostly streaming.

## Problem

A streaming job runs on the visits dataset. It detects a subset of events and forwards them to
another stream **in processing order**. Each record must be delivered as soon as possible, so
buffering to save network traffic is not an option. Bin packing is overkill for
this: the volume is moderate and latency matters more than throughput.

## Solution / Mechanics

There is no sorting algorithm. Records go out first in, first out: the job detects a record and
issues its delivery request. The key rule is to **get the delivery acknowledgment for each
record before sending the next one**. Otherwise data can end up out of order or lost.

Two implementations:

1. **One record at a time**: Kinesis `PutRecord`, or Kafka `send()`/`produce()` followed by a
   synchronous `flush()`.
2. **Bulk API with concurrency 1**, only for stores with **full commit** semantics (all or
   nothing per request). Keep a single request in flight. In Kafka, set
   `max.in.flight.requests.per.connection=1`, or use the **idempotent producer**, which accepts
   up to five concurrent requests and still guarantees order (KIP-98).

```
 consumer --> filter --> produce(r1) --ack--> produce(r2) --ack--> produce(r3) ...   (single)

 producer buffer [r1 r2 r3] --req A--> ack --> [r4 r5] --req B--> ack                (1 in flight)

 in-flight hazard without idempotence:
   req A [r1 r2] --X fails          req B [r3 r4] --> ok
   retry A --> ok                   sink order: r3 r4 r1 r2   (broken)
 idempotent producer: the broker sees sequence numbers per partition, rejects
   out-of-sequence batches, and the client re-sends in order (<= 5 in flight)
```

Store-specific mechanisms:

- **Kinesis** `PutRecord` with `SequenceNumberForOrdering` set to the previous put's sequence
  number per partition key. Without it, Kinesis keeps only a rough order.
- **GCP Pub/Sub** `ordering_key`: FIFO per key, only for one publisher in one region
. The client needs `enable_message_ordering=True`.

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| I/O overhead and latency | One request per record instead of one per batch. Both producer and store handle every request, and the backlog grows at high volume | Monitor delivered vs pending records per minute; use bulk with 1 in flight or the idempotent producer where the store allows it |
| Multithreading | Producers in separate processes do not know about each other, so the global order is lost | **Ordering scopes**: send all records of one entity through the same process, which delivers them one by one |
| FIFO is not exactly once | `send()` succeeds, then `ack()` / offset commit fails. After a restart the producer sends the record again | Idempotency patterns, deterministic ids, or Kafka transactions |
| Simplicity | No bins, no custom sort | The reason to choose it over the Bin Pack Orderer when volume allows |

## Key design decisions

- **Delivery mode**, slowest to fastest. Each one keeps the order:
  1. single record + flush;
  2. bulk with 1 in flight;
  3. idempotent producer with up to 5 in flight.
- **Ordering scope**: global (one thread) or per entity (keyed partitions or threads). The
  source key should become the target key.
- **Ack handling**: block on the future or `flush()`. A non-retriable error means the order for
  that key is already compromised, so fail rather than skip.
- **Offset commit**: only after the forwarded records are acknowledged.
- **Ordering across batch runs**: `max_active_runs=1` and `depends_on_past=True`.
- **Order type**: FIFO is processing order. If event-time order matters, sort first (or use the
  Bin Pack Orderer).

## When to use / when not

| Situation | FIFO Orderer? | Alternative |
| --- | --- | --- |
| Moderate volume, each record must go out immediately | Yes | none |
| Kafka sink, need throughput and order | Yes, idempotent producer | none |
| Partial-commit bulk API (Kinesis `PutRecords`, ES bulk) at high volume | No | Bin Pack Orderer |
| Kinesis at low volume | Yes, `PutRecord` + `SequenceNumberForOrdering` | Bin Pack Orderer for volume |
| Exactly-once delivery required | Not alone | Transactional producer or idempotent writer |
| Many entities with independent order | Yes, per-entity scopes | none |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Records reordered after broker errors | `retries > 0`, `max.in.flight > 1`, no idempotence | `enable.idempotence=true`, or `max.in.flight.requests.per.connection=1` |
| Records lost on crash | Async `produce()` without `flush()`/callback, offsets auto-committed | Delivery callback that raises, flush, then manual commit |
| Duplicates after restart | Ack or commit failed after a successful send | Dedup on `event_id`, idempotent sink, transactions |
| Kinesis order only approximate | Missing `SequenceNumberForOrdering`, or using `PutRecords` | Chain the previous `SequenceNumber` per key; single `PutRecord` |
| Pub/Sub key stops publishing | A failed publish pauses the ordering key | `publisher.resume_publish(topic, key)` before the retry |
| Order mixed after scaling | Key changed or data repartitioned mid-stream | Keep the source key, repartition only by the ordering entity |
| Throughput collapses at peak | One request per record | Bulk + 1 in flight, idempotent producer, or Bin Pack Orderer |

## Implementation notes per stack

**Kafka**: `examples/fifo_orderer/kafka/kafka_fifo_orderer.py`. Three `FIFO_MODE`s map to the
three producer configurations. Offsets are committed after the acks:

```python
PRODUCER_CONFIGS = {
    "single": {"acks": "all"},                                  # produce() + flush()
    "inflight1": {"acks": "all", "max.in.flight.requests.per.connection": 1,
                  "queue.buffering.max.ms": 1000},
    "idempotent": {"enable.idempotence": True, "acks": "all",
                   "max.in.flight.requests.per.connection": 5, "queue.buffering.max.ms": 2000},
}
producer.flush(); consumer.commit(asynchronous=False)          # acked before offsets move
```

**Spark**: `examples/fifo_orderer/spark/spark_streaming_fifo_orderer_kinesis.py`. `foreachBatch`
repartitions by `visit_id`, so one task is the ordering scope of an entity. Each record goes
through `PutRecord`, chaining the sequence number:

```python
if key in last_seq:
    kwargs["SequenceNumberForOrdering"] = last_seq[key]
last_seq[key] = client.put_record(**kwargs)["SequenceNumber"]
```

To a Kafka sink, use Spark's Kafka writer with `kafka.enable.idempotence=true` and a stable key.

**Airflow**: `examples/fifo_orderer/airflow/fifo_orderer_dag.py`. An hourly publish to Pub/Sub
with `enable_message_ordering` and `ordering_key=visit_id`. It waits on `future.result()` for
each message and calls `resume_publish` on failure. Runs are serialized.

## Related patterns

- **Bin Pack Orderer**: the throughput-oriented alternative for partial-commit bulk APIs.
- **Idempotency patterns**, e.g. Keyed Idempotency and Transactional Writer: they close the
  exactly-once gap.
- **Checkpointer**: commit offsets only after the acks, so a restart resumes in order.
- **Filter Interceptor**: the "detect a subset of events" step in front of the orderer.
