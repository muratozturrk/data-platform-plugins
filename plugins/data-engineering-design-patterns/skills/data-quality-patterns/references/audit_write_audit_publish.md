# Audit-Write-Audit-Publish (AWAP)

> Category: quality enforcement, pipeline side (you write the rules). Batch and streaming.

## Problem

A daily batch ETL job computes statistics over user visits. For a week the number of unique
visitors "dropped" by 50%, and the product team launched a marketing campaign to bring visitors
back. While working on a new feature you find that the unique-visitors aggregation is simply
wrong. The campaign is stopped and you are asked to make sure this cannot happen again. Nothing in the pipeline asserted that its output made sense, so wrong data was
published and acted on.

## Solution / Mechanics

AWAP evolves Write-Audit-Publish (WAP), which Michelle Ufford presented at DataWorks Summit 2017
("Whoops, the Numbers Are Wrong! Scaling Data Quality @ Netflix"). AWAP adds an audit of the
**input** before the transformation. The audits work like unit-test
assertions running on real data inside the pipeline.

```
 sensor --> AUDIT #1 (input) --> TRANSFORM --> WRITE (staging, invisible) --> AUDIT #2 (output) --> PUBLISH
              |  cheap: format,                                  |  data rules: NULLs,          |  atomic
              |  size, header,                                   |  volume, distinct,          |  swap /
              |  schema                                          |  NULL ratio                 |  COPY
              v                                                  v
            fail                                       fail | dispatch | nonblocking (annotate)
```

1. **Audit the input**. Keep it to fast operations: file format, file or table size,
   line count, schema/header checks. Example: verifying a CSV has columns `a, b, c` needs only its
   first line. A full validation here means reading the dataset twice.
2. **Transform and write** to a place consumers cannot see yet (staging file or table, a Delta
   staging table, an Iceberg/Nessie branch).
3. **Audit the output**. This extends your unit tests to the real dataset: the
   checks focus on data, e.g. transformed columns must never be NULL.
4. **Publish** atomically: rename/swap, `INSERT ... SELECT`, `COPY`, branch merge.

**Same rule, different intent**. A NULL check on the input validates your
*provider*; on the output it validates *your transformation*. If running the rule twice is too
expensive, put it in the **most exhaustive place**, the output audit, which catches NULLs from
both sources.

**Scope**: record level (attributes of a row) or dataset level (volume,
distinctness of a column, proportion of NULLs).

**Outcomes** besides failing:

| Outcome | What happens | Use when |
| --- | --- | --- |
| Fail | Stop the pipeline, publish nothing | Rule breach makes the whole dataset untrustworthy |
| Data dispatching | Valid rows published, invalid rows kept in separate storage | Only part of the output is bad. Like Dead-Letter, but driven by explicit rules, not runtime errors |
| Nonblocking audit | Publish anyway, write a data-quality summary (table/file) listing the issues | Imperfections are tolerable for some consumers (e.g. one column has more NULLs) |

**Streaming** follows classical WAP: data flows continuously, so the
input audit is dropped and the exhaustive output audit stays.
- **Window based**: the job buffers a processing-time window. When the window closes it audits the
  buffer and applies fail, dispatch or ignore.
- **Staging based**: the processing job is unchanged and writes to a staging layer. A second job
  audits staging and promotes valid data to the final output.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Compute cost | Metadata checks are cheap. Row-based checks cost a pass over the data | Push row rules to the output audit only. Use Constraints Enforcer where the store can do it |
| Rules coverage | Today's rules may not cover tomorrow's dataset, so AWAP is never 100% reliable | Run an Offline/Online Observer to discover missing or outdated rules |
| Streaming latency | A window-based audit delays delivery by the accumulation period | Use staging-based audits or shorter triggers. Accept the latency for the rules that need a window |
| An issue may not be an issue | Volume rules fire on real success (e.g. the blog quoted on social media) | Let volume/anomaly rules alert for investigation instead of failing |

## Key design decisions

- **Rule placement**: input (cheap, provider contract) vs output (exhaustive, your logic).
- **Outcome per rule**: fail, dispatch or annotate. Don't use one global policy.
- **Thresholds**: absolute (min 100 lines) vs relative to previous runs (volume +-50%).
- **Staging mechanism and publish primitive** for your store: temp table + swap, Delta staging
  table, branch merge, `COPY` from staged files.
- **Streaming flavour**: window (one job, adds latency) vs staging (two jobs, extra storage).

## When to use / when not

| Situation | Use AWAP? | Alternative |
| --- | --- | --- |
| Wrong output would trigger business decisions (KPIs, billing) | Yes | - |
| Rules are simple per-column NULL/range/FK checks on a typed store | Maybe not | Constraints Enforcer (declarative, no code) |
| Constraint-less formats (CSV, JSON lines) | Yes, you must check NULLs/types yourself | - |
| Rule logic needs history or cross-dataset comparisons | Yes, the store cannot express it | - |
| You only want to learn how data drifts, never block | No | Offline / Online Observer |
| Runtime parsing errors on individual records | No | Dead-Letter (error management patterns) |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Error message shows only the first broken rule | Raising on the first failure | Collect all errors, raise once |
| Input read twice, job much slower | Full validation in audit #1 | Limit audit #1 to metadata and the first lines |
| Rerun after fixing the audit duplicates rows | Publish not idempotent | Delete+insert per partition in one transaction, `txnAppId/txnVersion` in Delta |
| Consumers saw half-published data | Publish is not atomic | Swap/rename or single-transaction `COPY`; never write straight to the final table |
| Streaming job "stuck" after an audit failure | Fail strategy raises, checkpoint not committed | Intended: fix the rule or the data, restart. Batch is retried from the same offsets |
| Pipeline failed on a traffic spike | Volume rule treated as fatal | Downgrade to an alert / nonblocking audit |

## Implementation notes per stack

**Airflow** - `examples/audit_write_audit_publish/airflow/awap_dag.py`:
FileSensor, input audit (size, lines, JSON validity), JSON to CSV in a staging dir, output audit
with pandas, publish in one PostgreSQL transaction.
```python
def audit_input_file(path, min_size=MIN_SIZE_BYTES, min_lines=MIN_LINES):
    errors = []
    if os.path.getsize(path) < min_size:
        errors.append(f"File is too small. ...")
    ...
    if errors:
        raise ValueError("Audit failed for the input file:\n-" + "\n-".join(errors))

with conn, conn.cursor() as cur, open(staged_path) as f:   # publish: one transaction
    cur.execute("DELETE FROM dp.visits WHERE event_time::date = %s", (ds,))
    cur.copy_expert("COPY dp.visits FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)", f)
```

**Spark** - `examples/audit_write_audit_publish/spark/spark_streaming_awap_staging.py`: staging-based. Job 1 writes Kafka to a Delta staging table, job 2 streams
staging with an `is_valid` column, fails the batch above an invalid ratio, otherwise dispatches.
```python
invalid = batch.filter(~F.col("is_valid")).count()
if invalid / total > MAX_INVALID_RATIO:
    raise RuntimeError(...)                      # fail: batch retried after the fix
for path, cond in ((OUTPUT, F.col("is_valid")), (ERRORS, ~F.col("is_valid"))):
    (batch.filter(cond).drop("is_valid").write.format("delta").mode("append")
     .option("txnAppId", f"awap-{os.path.basename(path)}").option("txnVersion", batch_id).save(path))
```

**Kafka** - `examples/audit_write_audit_publish/kafka/kafka_window_awap.py`: window-based. The
consumer buffers 15 s or 500 records, audits at record and dataset level, produces to
`visits_audited` / `visits_audit_errors`, then commits offsets (at-least-once).
`--self-check` runs the audit asserts without a broker.
```python
valid, invalid = audit_window(buffer)   # raises -> fail strategy, offsets not committed
for topic, rows in (("visits_audited", valid), ("visits_audit_errors", invalid)):
    for row in rows:
        producer.produce(topic, json.dumps(row).encode("utf-8"))
producer.flush()
consumer.commit(asynchronous=False)
```

## Related patterns

- **Constraints Enforcer**: declarative alternative for record-level rules, enforced by the store.
- **Dead-Letter** (error management patterns): dispatches records that fail at runtime. AWAP dispatch is rule-driven.
- **Offline / Online Observer**: non-blocking counterpart that finds rules AWAP is missing.
- **Local Sequencer** (data flow patterns): the audit -> transform -> audit -> publish chain is one.
- **Data Overwrite / Transactional Writer** (idempotency patterns): make the publish step idempotent and atomic.
- **Flow Interruption Detector** (data observability patterns): catches the case where the audited pipeline stops producing.
