# Isolated Sequencer

Category: Sequence · Scope: two or more physically separated pipelines (often different teams)

## Problem
Your team cleans and enriches raw datasets that feed views in a data visualization tool. In a
technical meeting both teams agree that the dashboard transformations should not live in your
data preparation pipeline because your team won't own them. You must provide only the cleansed,
enriched dataset; the visualization team runs its own transformation.

## Solution / Mechanics
Two pipelines where one provides data to another, which organizational separation prevents
from merging. The Isolated Sequencer combines physically isolated pipelines.

Step by step:
1. **Define boundaries**: split by provider/consumer or by team. If you are both provider and
   consumer inside your scope, split only when one combined pipeline would be unreadable;
   otherwise keep a Local Sequencer.
2. **Choose the trigger strategy**:
   - *data based* -- the producer writes the dataset and a readiness marker (Readiness Marker
     pattern); the consumer waits for the marker and starts as soon as it appears;
   - *task based* -- no marker; the producer directly triggers (or is awaited by) the
     consumer's pipeline.
3. Publish the contract: location, marker name, schedule, schema, who to notify on change.
4. Make the consumer's first task a **dependency enforcer** that fails loudly (timeout,
   `failed_states`) when the provider doesn't deliver as agreed.

```
 data based (loose coupling)
 [producer: load] --writes--> s3://.../date=D/dataset + _SUCCESS <--waits-- [consumer: sensor -> load -> aggregate]

 task based (tight coupling)
 [producer: load -> ExternalTaskMarker] ...........> [consumer: ExternalTaskSensor -> load -> aggregate]
 [producer: load -> TriggerDagRun] ===triggers===> [consumer (schedule=None): notify users]
```

Coupling and ownership:
| Aspect | Data based | Task based |
|---|---|---|
| Coupling | Loose; only the marker contract | Tight; physically separate but can't live alone |
| Consumer rename | Invisible to producer | Breaks the producer's trigger/marker |
| Consumer freedom | Can switch datasets without telling the producer | Can't skip; producer triggers directly |
| Dependency visibility | Implicit (needs lineage) | Explicit in code |
| Fits | Shared dataset | No shared dataset (e.g. consumer only sends notifications) |

## Consequences & trade-offs
- **Keeping pipelines in sync** is the biggest challenge; the logical isolation adds an
  operational constraint.
- **Scheduling**: task-based dependencies ideally share one schedule. Otherwise
  the producer must skip triggering on some runs, or the consumer must adapt its schedule and
  only process when needed -- extra complexity on one side.
- **Communication**: pipelines owned by different teams break when the
  culture doesn't communicate changes. Technical checks on the other side's conditions are
  costly and may be impossible (no incoming connections for security reasons). The resilient
  fix is organizational: each team knows its inputs/outputs and announces breaking changes.
- **Implicitness**: with data-based dependencies nothing shows the two pipelines are
  interdependent; use lineage (OpenLineage, Dataset Tracker).
- **Backfills**: task-based `ExternalTaskMarker` propagates producer backfills to the
  consumer; data-based consumers must detect re-delivered partitions themselves.

## Key design decisions
1. Boundary: team ownership, readability, deployment cadence.
2. Data-based vs task-based (shared dataset? who may change what?).
3. Marker format: empty `_SUCCESS` vs manifest (row count, schema version, completeness).
4. Same Airflow instance (Datasets, ExternalTaskSensor) or cross-tool (files, events)?
5. Timeout and failure behavior of the consumer's waiter.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Producer and consumer owned by different teams | Yes | -- |
| Same team, combined pipeline still readable | No | Local Sequencer |
| Many consumers of one dataset, unknown to producer | Yes, data based | Kafka readiness events |
| Consumer must react to producer even without shared data | Yes, task based | TriggerDagRunOperator |
| Need low latency, record-level hand-off | No | Streaming topic between jobs |
| Producer is an external system you can't change | Partly (sensor on its output) | External Trigger |

## Gotchas & best practices
- Write the marker **last**, after the data commit; consumers never see half-written data.
- `ExternalTaskSensor` matches the same logical date: different schedules wait forever unless
  you set `execution_delta`/`execution_date_fn`; always set `failed_states` and a timeout.
- Treat DAG/task ids used by other teams as a public API; version or announce renames.
- Airflow `Dataset` scheduling works within one Airflow deployment only.
- Prefer a manifest marker: lets the consumer validate the contract (schema version, row count).
- Document the contract next to the data (catalog entry, README in the bucket, schema registry).

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Consumer sensor waits until timeout every day | Logical dates differ between DAGs | Align schedules or `execution_delta` |
| Producer DAG fails after consumer refactor | Task-based trigger to renamed task | Data-based dependency, or coordinate renames |
| Consumer processed a partial partition | Marker written before data commit | Marker as the last write |
| Nobody knows who reads a dataset | Implicit data dependency | Lineage (OpenLineage), catalog ownership |
| Backfilled producer, stale consumer output | Data-based consumer didn't re-run | `ExternalTaskMarker` or re-delivery events |
| Consumer breaks on new column | Unannounced schema change | Schema version in manifest, contract checks |

## Implementation notes per stack
**Airflow, data based** -- `examples/isolated_sequencer/airflow/data_based_dependency_dags.py`: producer copies the file then writes `_SUCCESS`; consumer waits on it. A
second consumer uses `schedule=[DEVICES]` (Airflow Datasets) instead of polling.
```python
input_data_sensor = FileSensor(task_id="input_data_sensor",
    filepath=DEVICES_DIR + "/{{ ds_nodash }}/_SUCCESS", mode="reschedule", timeout=6 * 3600)
input_data_sensor >> load_data_to_table() >> refresh_aggregates()
```
**Airflow, task based** -- `examples/isolated_sequencer/airflow/task_based_dependency_dags.py`: `ExternalTaskMarker` in the producer (clears the consumer on recursive
clears/backfills), `ExternalTaskSensor` in the consumer; plus a `TriggerDagRunOperator` push.
```python
parent_dag_sensor = ExternalTaskSensor(task_id="downstream_trigger_sensor",
    external_dag_id="devices_loader_tb", external_task_id="trigger_downstream_consumers",
    allowed_states=["success"], failed_states=["failed", "skipped"], mode="reschedule")
```
**Spark** -- `examples/isolated_sequencer/spark/producer_consumer_jobs.py`: producer writes a
`_READY.json` manifest after the data; consumer refuses to run without it and validates
schema version and row count.
```python
if manifest["schema_version"] != SCHEMA_VERSION:
    sys.exit(f"Contract broken: schema_version={manifest['schema_version']}")
```
**Kafka** -- `examples/isolated_sequencer/kafka/readiness_events.py`: provider publishes a
keyed readiness event per partition; each consumer team uses its own consumer group and commits
only after processing.

## Related patterns
- Local Sequencer -- same sequencing inside one pipeline; preferred when one team owns both.
- Readiness Marker -- the mechanism behind the data-based strategy.
- External Trigger -- event-driven start when the producer is outside your orchestrator.
- Parallel Split -- move a slow branch into its own pipeline linked by an Isolated Sequencer.
- Dataset Tracker / Fine-Grained Tracker -- make implicit data dependencies visible.
