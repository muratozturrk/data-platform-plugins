# External Trigger

Category: event-driven ingestion. One-liner: start the pipeline when the **producer announces** new data
(push semantics), instead of a clock.

## Problem
The backend team releases new features at most once a week, between Monday and Thursday. Each release enriches
the reference dataset of features on the website. The refresh job runs once a day, reloading data even when
nothing changed: wasted compute. The backend publishes a notification event to a central message bus for each
new feature, so you want to run the pipeline **only when there is something new**.

## Solution / Mechanics
Unpredictable data generation usually comes from event-driven data. A frequent time-scheduled job that copies the
whole dataset or checks for news wastes compute and adds operational burden; the External Trigger reacts to
events instead. Readiness Marker = pull (consumer checks); External Trigger = push (producer
notifies). If there's no job to trigger, the notification handler can ingest directly (*event-driven data
ingestion*). Three actions:
1. **Subscribe** to a notification channel: connect event producers with your pipelines.
2. **React** to notifications: the handler analyses each event and decides whether to trigger something; filter by
   event type/table if the bus can't.
3. **Trigger** the ingestion: start a workflow in the orchestration layer or a job in the processing layer. One event
   usually triggers one pipeline, but several workloads may depend on the same dataset.

```
 producers ──► message bus / storage events (S3, SQS, Kafka, Pub/Sub)
                    │ push (invocation per event)
                    ▼
            notification handler (e.g. AWS Lambda)
              - filter: ObjectCreated under devices/ ?
              - enrich: event, trigger version, request id, event time
              - idempotent run id (object key + etag)
         ┌──────────┴──────────────────┐
         ▼                             ▼
  orchestration layer            processing layer
  POST /api/v1/dags/             start job directly
  devices-loader/dagRuns         ("Write data to table orders")
  (schedule=None DAG)
         failures ──► retries ──► failed-event destination / dead-letter queue
```

## Consequences & trade-offs
- **Push versus pull**. A pull-based trigger is a long-running process checking at short intervals
  for new events: valid but not optimal, it may spend most of its time pulling nothing. Push is better: the data
  source informs the endpoint; each notification starts a consumer instance that ends after reacting.
- **Execution context**. A trigger that's just a ping to the orchestrator loses context: why was it
  triggered and what should it process? Enrich the call with metadata: trigger job version, the notification
  envelope, processing time, event time. Essential for day-to-day monitoring and failure investigation.
- **Error management**. Events are the only input; without them nothing runs. Design for failure so
  events are kept whatever happens, typically with the Dead-Letter pattern. AWS Lambda provides
  *failed-event destinations* for failed invocations, plus batch size and concurrency settings.
- At-least-once delivery is the norm: the same event can arrive twice, so triggers and jobs must be idempotent.
- Burst control: many events at once mean many concurrent runs (`max_active_runs`, Lambda concurrency).

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Semantics | push (serverless function, webhook) · pull (poller on queue/table) | Push; pull only when the source can't call you |
| Target | orchestrator API · processing job · direct ingestion in handler | Orchestrator for multi-step pipelines (retries, lineage); direct for tiny loads |
| Payload | bare ping · rich `conf` | Rich: event envelope, file/partition, trigger identity and version, event time |
| Deduplication | none · deterministic `dag_run_id` · dedup table | Deterministic id from event identity; 409 = already done |
| Failure handling | retries · DLQ / failed-event destination | Both; never ack/delete an event before the trigger succeeded |
| Concurrency | unlimited · `max_active_runs` · function concurrency | Bound it to what the target and source can sustain |
| Credentials | in code · env from secret store | Secret store; hardcode them only in throwaway examples |

## When to use / when not
| Situation | Use External Trigger? | Alternative |
|---|---|---|
| Irregular, unpredictable arrivals; idle schedule wastes compute | Yes | - |
| Producer already emits events (storage notifications, bus messages) | Yes | - |
| Data arrives on a reliable fixed schedule | No | Scheduled pipeline + Readiness Marker |
| Consumers belong to other teams and shouldn't be called by you | No | Readiness Marker (they pull) |
| Very high event rate (thousands/s) | Not per event | Micro-batch streaming job, or batch events per trigger |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Same file processed twice | Redelivered notification, retry after timeout | Deterministic run id (409 handling) and idempotent jobs |
| Events lost during an orchestrator outage | Handler swallowed errors / acked early | Raise on failure; failed-event destination or DLQ; ack after success |
| Triggered run doesn't know what to load | Ping without context | Pass `file_to_load`, event and trigger metadata in `conf` |
| DAG also runs at midnight | DAG has a schedule | `schedule=None` |
| Thundering herd of runs | Bulk upload → one run per object | `max_active_runs`, function concurrency, or batch events |
| Keys with spaces/`+` not found | S3 keys are URL-encoded in events | `urllib.parse.unquote_plus(key)` |
| Credentials leaked in function code | Hardcoded basic auth | Env vars from Secrets Manager, or a secretless connector |
| Malformed events retried forever | Retrying non-retryable errors | Validate early, `AirflowFailException`, send to DLQ |

## Implementation notes per stack

### Airflow + AWS Lambda (push)
`examples/external_trigger/airflow/lambda_dag_trigger.py` (stdlib only):
```python
payload = {"dag_run_id": f"External-{run_suffix}",          # hash of bucket/key@etag
           "conf": {"event": json.dumps(event),
                    "trigger": {"function_name": ctx.function_name,
                                "function_version": ctx.function_version,
                                "lambda_request_id": ctx.aws_request_id},
                    "file_to_load": f"s3://{bucket}/{key}"}}
request = urllib.request.Request(f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns",
                                 data=json.dumps(payload).encode(), method="POST", headers=...)
# HTTPError 409 -> "already triggered"; anything else raises -> Lambda retry -> failed-event destination
```
`examples/external_trigger/airflow/devices_loader_dag.py`: `schedule=None`,
`max_active_runs=5`, reads `dag_run.conf`, `AirflowFailException` on malformed conf, stores the trigger metadata
next to the copied file. `external_trigger_dag.py` adds a receiver DAG that validates and fans out triggers.

### Spark (pull variants)
`examples/external_trigger/spark/spark_s3_file_trigger.py`: SQS long polling of S3 notifications, dedup table,
message deleted only after all its files succeed. `spark_external_trigger_processor.py`: DB trigger queue with
`SELECT ... FOR UPDATE SKIP LOCKED` and status transitions. Both are pull-based: acceptable when you can't receive
pushes, at the cost of an always-on poller.

### Kafka
`examples/external_trigger/kafka/kafka_pipeline_trigger.py`: producers publish completion events; a consumer maps
each dataset to a handler, retries with exponential backoff, sends failures to a DLQ topic and commits the offset
only after success or DLQ (no lost events).

## Related patterns
- **Readiness Marker** - the pull counterpart; often combined (trigger on marker creation).
- **Dead-Letter** - keeps failed trigger events for replay.
- **Full / Incremental Loader** - what the trigger usually starts.
- **Secrets Pointer / Secretless Connector** - remove credentials from trigger code.
