# Anonymizer

Category: Data Protection · Layer: data sharing / export

## Problem
Your organization contracted an external analytics company to analyze customer behavior. The
dataset holds many PII attributes and some users didn't agree to share them with third parties.
You must build a pipeline that makes the shared dataset privacy compliant. PII is
the usual case, but PHI and intellectual-property data need the same treatment.

## Solution / Mechanics
Remove or transform sensitive data so each row becomes anonymous: the consumer can't identify
the user. Three implementations:

1. **Data removal**: take the columns out. Omit them from `SELECT` when you keep few columns;
   `drop()` them when you keep many.
2. **Data perturbation**: add noise so the value means something else, e.g. random digits in an
   IP: `123.456.789.012` -> `1823.456.7809.012`.
3. **Synthetic replacement**: substitute values produced by a generator that understands the
   column type, e.g. `Portugal` -> `Croatia`. Ideally an ML model from the data science team;
   a lower-quality fallback is a per-column random generator such as Faker.

Removal and perturbation are column transformations; replace one column with `withColumn`,
many with a row function like `mapInPandas`.

```
 private zone                         anonymizer                         partner share
 +--------------------+     +-------------------------------+     +------------------+
 | user_id, email,    | --> | 1 drop: birthday, personal_id | --> | email (fake),    |
 | full_name,birthday,|     | 2 perturb: ip, age +/- 3      |     | ip (noisy),      |
 | ip, age, country   |     | 3 synth: email, name (Faker)  |     | age (shifted),   |
 +--------------------+     | 4 validate: no PII col/value  |     | country (coarse) |
                            +-------------------------------+     +------------------+
                 irreversible: nothing kept that maps output rows back to users
```

## Consequences & trade-offs
- **Information loss**: removed or replaced columns are useless for analysts and
  data scientists. That can produce wrong prediction models and incorrect insights. Agree on the
  column list with consumers before building.
- Strongest protection of the data protection patterns: combined with other datasets the data
  stays unidentifiable, unlike pseudo-anonymized data.
- Synthetic data needs effort (a model) to keep distributions realistic.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Sharing with third parties without user consent | Yes | - |
| Consumers need joins on user, distinct counts | No | Pseudo-Anonymizer (keyed hash / tokens) |
| Internal team must not see some columns | Often overkill | Fine-Grained Accessor for Tables (masks) |
| Need to re-identify later (support case) | No | Tokenization (Pseudo-Anonymizer) |
| Test/dev data generation | Yes (synthetic) | - |

## Key design decisions
- Per column: remove, perturb, synthesize or generalize; document the choice with consumers.
- **Allowlist** output fields rather than denylist PII: a new upstream column is dropped by default.
- Quasi-identifiers (zip + birth date + gender) re-identify people: generalize or drop them too.
- Noise level for perturbation: enough to break identification, small enough for aggregates.
- Validation gate before publishing: no forbidden column, no original value present.

## Gotchas & best practices
- Removing `user_id` but keeping a stable event ID that's also in shared logs re-links rows.
- Faker instances per Python worker: create inside the pandas UDF, not on the driver.
- `random` vs `SystemRandom`: perturbation shouldn't be reproducible by a seed known to others.
- Clean staging copies of the raw extract after the export (they are PII).
- Coarsen timestamps (to month/day) if exact times + location identify a person.
- Re-run the validation on every schema change of the source.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| New PII column reached partner | Denylist of columns | Output allowlist |
| Partner re-identified users via zip+birthday | Quasi-identifiers kept | Generalize/drop quasi-identifiers |
| Analysts' models broke | Information loss not agreed | Agree on columns; consider Pseudo-Anonymizer |
| Fake emails collide with real ones | Generator domain overlaps real data | Validate against originals; reserved domains |
| UDF slow | Row-by-row Python UDF | Vectorized `pandas_udf` |
| Raw extract left on disk | Staging not cleaned | Delete staging in the publish step |

## Implementation notes per stack
**Spark** — `examples/anonymizer/spark/anonymizer_job.py`:
```python
@pandas_udf(StringType())
def replace_email(emails: pandas.Series) -> pandas.Series:
    faker = Faker()
    return emails.apply(lambda _: faker.email())

def anonymize(users: DataFrame) -> DataFrame:
    return (users.drop(*COLUMNS_TO_DROP)
            .withColumn("email", replace_email("email"))
            .withColumn("full_name", replace_name("full_name"))
            .withColumn("ip", perturb_ip_udf("ip"))
            .withColumn("age", F.col("age") + (F.floor(F.rand() * 7) - 3).cast("int")))
```
`perturb_ip` inserts a random digit per octet.

**Kafka** — `examples/anonymizer/kafka/anonymizing_relay.py`: consume `visits_raw`, keep
`ALLOWED_FIELDS`, zero the IP host part, Faker email, produce to `visits_shared`, commit offsets
after delivery. `python anonymizing_relay.py test` runs the offline self-check.

**Airflow** — `examples/anonymizer/airflow/anonymized_export_dag.py`:
`extract >> anonymize >> validate >> publish`; `validate` fails on forbidden columns or any
original email in the output, `publish` removes the raw staging copy.

## Verification
Run these before every publication and after every source schema change:
- Output columns are a subset of the agreed allowlist; no column from the PII inventory present.
- No original value of a replaced column appears in the output (`isin` against the source).
- Perturbed values differ from originals for every row (no no-op noise).
- k-anonymity check on quasi-identifiers: each combination (country, age band, ...) occurs at
  least k times (k >= 5 is a common floor); smaller groups get generalized further or dropped.
- Row count matches the source (removal of columns, not rows, unless agreed).
- The raw extract and staging files no longer exist after the publish task.
- A sample is reviewed by someone outside the team before the first delivery.
- Partner contract states that re-identification attempts are forbidden.

## Related patterns
- Pseudo-Anonymizer: keeps business meaning, weaker guarantee.
- Fine-Grained Accessor for Tables: query-time masking for internal readers.
- Vertical Partitioner: PII isolated in one store is easier to exclude from exports.
- Audit-Write-Audit-Publish: the validate-before-publish gate.
