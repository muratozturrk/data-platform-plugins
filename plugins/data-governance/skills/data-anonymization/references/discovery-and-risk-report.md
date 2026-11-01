# Discovery, Triage and Exact Measurement

How to find direct identifiers and quasi-identifiers automatically, how far database statistics
can take you before reading data, and how to measure k exactly on relational and lakehouse
engines. ARX paths are relative to `src/main/org/deidentifier/arx/` at commit `4e0a5f5`.
Tags: **K** code, **D** official docs, **L** literature, **(derived)** this skill's reasoning,
**(not verified)** searched without a confirming source.

## 1. Direct-identifier detection

### 1.1 Two matcher families (K)

ARX ships a HIPAA Safe Harbor detector built from two families:

| Family | Mechanism | Evidence |
|---|---|---|
| **Column name** | Lower-cased, trimmed name compared with a pattern under a Levenshtein tolerance — *"Implements the levenshtein distance for fuzzy detection"* | `risk/HIPAAMatcherAttributeName.java:20-24,38,106` |
| **Column value** | Abstract value matcher with subtypes (age, city, date, e-mail, first/last name, IBAN, IP, SSN, state, URL, ZIP); regex (`HIPAAMatcherString`) or dictionary lookup | `risk/HIPAAMatcherAttributeValue.java` |
| **Pattern + validation** | Only the IBAN matcher validates after the regex: `super.matches()` then `IBANCheckDigit.isValid` | `HIPAAMatcherAttributeValue.java:199-206` |

Pattern + validation is the strongest value signal and should be the default for any identifier
that carries a check digit (derived).

### 1.2 Locale: US only (K)

`HIPAAConstants` is abstract with one implementation, `HIPAAConstantsUS`, reached through the
factory `getUSData()` (`risk/HIPAAConstants.java:40-50`). Dictionaries live only in
`risk/resources/us/`: `lastnames.csv` 88,798 lines, `zipcodes.csv` 29,466, `cities.csv` 16,697,
`firstnames.csv` 10,141, `states.csv` 153, plus `labels.properties` — about 145,000 lines, one locale.

### 1.3 What a Turkish locale needs

| Category | Detection method | Note |
|---|---|---|
| First / last names | Dictionary + model | Turkish name lists; many names are also common words |
| Province, district, neighbourhood, postal code | Dictionary | Official administrative lists |
| **T.C. Kimlik No (national ID, 11 digits)** | Pattern + **check-digit validation** | No ARX category exists |
| **VKN (tax ID, 10 digits)** | Pattern + check-digit validation | No ARX category exists |
| **IBAN-TR** | `TR` prefix + length + ISO 7064 mod-97 | ARX's generic IBAN matcher covers the checksum |
| Phone, e-mail, IP, URL | Pattern | Standard |
| Social-security, land-registry, case/file numbers | Configurable pattern | Institution-specific formats |

Take check-digit algorithms from the issuing authority's official publication, not from
circulating snippets — the research did not verify them (not verified).

Turkish-specific traps (derived):

- **Case folding.** ARX lower-cases with the JVM default locale (`toLowerCase()` with no `Locale`,
  `HIPAAMatcherAttributeName.java:38,106`). Under a Turkish locale `"TITLE"` becomes `"tıtle"`
  (dotless ı); under any other locale `"İL"` becomes `"i̇l"` (i + combining dot). A name pattern can
  silently fail in either direction — fold with an explicit locale and also try an ASCII-folded form.
- **ASCII-fied column names.** `dogum_tarihi`, `dgm_trh`, `tc_kimlik`, `tcKimlikNo` — fuzzy name
  matching must run on ASCII-folded text as well as on the original.
- **Agglutination.** Values and headers carry suffixes (`adresi`, `adresleri`); exact dictionary
  lookup misses them, stemming or prefix matching helps.

Structured matchers work on column names and column values; an ID typed into a free-text column is
not found this way — that is the document problem (`document-anonymization.md`).

## 2. Quasi-identifier discovery

### 2.1 The measures (K, L)

`risk/RiskModelAttributes.java:38-41` cites Motwani & Xu, *Efficient algorithms for masking and
finding quasi-identifiers*, VLDB 2007. Definitions (L, §1.1):

- **α-distinct QI**: a column subset that becomes a key after removing at most a 1−α fraction of
  rows. ARX: `numClasses / numRecords` (`RiskModelAttributes.java:246-257`) — one
  `COUNT(DISTINCT …)/COUNT(*)` query.
- **α-separation QI**: a subset that separates at least α of all record pairs (two records are
  separated if they differ in at least one column). ARX derives it from the class-size histogram
  (`:259-273`).

The two diverge. For the same 0.5-distinct QI on 100 rows (L, §1.1):

| Distribution | Separation |
|---|---|
| 50 values × 2 rows each | ≈ 0.99 |
| 49 unique values + 51 rows sharing one value | 0.75 |

Report both; separation alone makes risk look lower. The gap tells you how the risk is distributed.

### 2.2 ARX enumerates the power set (K)

`RiskModelAttributes.java:176-187` builds `getPowerSet(identifiers)` (`:317`) and runs a grouping
per non-empty subset: 2ⁿ − 1 aggregations for n candidate columns.

| Candidate columns | Subsets | Verdict (derived) |
|---|---|---|
| 10 | 1,023 | Feasible |
| 20 | 1,048,575 | Borderline |
| 30 | 1,073,741,823 | Infeasible |

### 2.3 Sampled greedy search (L)

Motwani & Xu show minimum-key finding is NP-hard and replace exactness with sampling.
**(ε,δ)-separation minimum key** (§2.3.2): sample `k = log_{1/(1−ε)}(2m/δ)` record **pairs**, map
each column to the pairs it separates, run greedy set cover.

> Theorem 5: *"With probability 1−δ, the above algorithm outputs a (1−ε)-separation
> quasi-identifier whose size is at most (1 + ln log_{1/(1−ε)} 2m/δ)|I\*|"* … *"The space
> requirement of the above algorithm is mk = O(m²)"* (m = number of columns).

Space depends on columns, not rows: discovery cost is the same for a thousand or a billion rows.
The set-cover reduction does **not** hold for α-distinct; the paper gives a separate sampling
algorithm with a lowered threshold (Theorem 12, §2.4).

Measured (L, §5; 2007 hardware, largest set 10 M rows × 41 columns): on `california` (233,687 rows)
0.5-distinct masking took 80 min on the full table vs 10 min on a 30,000-row sample, with utility
degrading "very slowly"; δ = 0.01 and *"the privacy constraint are satisfied in all experiments"*.
Transfer the scaling behaviour, not the absolute times.

### 2.4 Discovery is not proof

| Stage | Method | Statement it supports |
|---|---|---|
| Discovery | Sampled greedy (probabilistic, 1−δ) | "This combination **looks** risky" |
| Risk report | Class-size histogram over the full data | "Risk measures for this dataset are …" |
| Proof | Exact `GROUP BY … HAVING count(*) < k` | "Minimum k in this table **is** …" |

## 3. Triage from catalog statistics (no data read)

### 3.1 The one-sided test (derived)

With N rows, D distinct QI tuples and D ≥ maxᵢ NDV(qiᵢ):

> **maxᵢ NDV(qiᵢ) > N/k ⇒ average class size < k ⇒ the table is certainly not k-anonymous.**

Use the estimator's lower bound: with relative standard error s and a 3s margin,
`NDV_est × (1 − 3s) > N/k` (s = 0.05 → `NDV_est > 1.18 × N/k`). The converse is **never** valid:
k-anonymity depends on the frequency-of-frequencies profile (φ_j = 0 for all j < k) and NDV is only
Σ φ_j — millions of large classes can hide one singleton. Per-column NDVs cannot yield the tuple
NDV; independence-based estimates are routinely off by orders of magnitude, and theta sketches have
no Cartesian operator. Label the result "prioritisation heuristic", never "risk measurement".

Direction of error: an **under**-estimated NDV only loses efficiency (the exact step still runs); an
**over**-estimated NDV produces a false "certainly violates" — never publish a triage verdict
without the exact measurement.

### 3.2 Where each engine keeps its statistics (D)

| Engine | Source | Trap |
|---|---|---|
| PostgreSQL | `pg_stats.n_distinct` | **Negative values are ratios**: `-1` = unique column. Read as absolute, the riskiest column looks safest. True NDV = `|n_distinct| × reltuples` when negative. View shows only tables the user may read |
| MySQL 8.0 | `INFORMATION_SCHEMA.COLUMN_STATISTICS` histogram JSON (`buckets`, `last-updated`, `sampling-rate`, `histogram-type`) | Histograms update **only on demand** — record their age. Singleton: buckets = NDV exactly; equi-height: 4th bucket field = NDV per bucket. No stats for geometry/JSON → "no data", not "no risk" |
| Oracle | `USER_TAB_COL_STATISTICS.NUM_DISTINCT` | Read `NUM_DISTINCT`, not histogram endpoints (cumulative frequencies). Default 254 buckets; frequency / top-frequency / height-balanced / hybrid types |
| SQL Server | Statistics object: header, histogram, **density vector** (`DBCC SHOW_STATISTICS`, `sys.dm_db_stats_histogram`) | NDV = `1 / All Density`; histogram max 200 steps. Not available on tables with a row-level security policy; dynamic data masking needs `UNMASK`. `Rows Sampled < Rows` ⇒ estimate |
| Hive | Metastore `TAB_COL_STATS` / `PART_COL_STATS`: `numDVs`, `bitVectors`, `LAST_ANALYZED` | Column stats are not generated by default before Hive 2.3; `numDVs` method and error bound undocumented (not verified) — treat as estimate |
| Spark catalog | `ANALYZE TABLE … FOR COLUMNS` → `distinctCount` | A **different** source from Iceberg Puffin NDV — record which one was used |
| Iceberg | `.partitions.record_count`; Puffin `apache-datasketches-theta-v1` NDV via `compute_table_stats` | `record_count` **ignores deletes** (groups look larger — unsafe direction); string bounds `truncate(16)`; metrics only for the first 100 columns by default; equality deletes make `total_record_count` NULL |

Multi-column NDV exists only opportunistically: SQL Server's density vector carries every column
**prefix** of a multi-column statistics object ((A), (A,B), (A,B,C)); Oracle column-group extended
statistics (`DBMS_STATS.CREATE_EXTENDED_STATS`, up to 20 per table) expose a group `NUM_DISTINCT`.
Someone must have created them; a read-only scanner cannot. Still estimates — they sharpen triage,
they do not prove.

Rule: every triage verdict carries the **age, sampling rate and source** of the statistic it used.
Rich statistics everywhere depend on a maintenance command (`ANALYZE`, `DBMS_STATS`, `UPDATE
HISTOGRAM`, `FOR COLUMNS`) — if none exist, say "no data".

## 4. Exact measurement

```sql
SELECT qi_1, ..., qi_m, count(*) AS k_observed
FROM   t
GROUP BY qi_1, ..., qi_m
HAVING count(*) < :k;
```

Use `count(*)` only — collecting group members (`collect_list`, UDAFs) pushes Spark to
`ObjectHashAggregateExec`, which falls back to sort-based after 128 rows. Iceberg's Spark aggregate
pushdown refuses `GROUP BY` (*"group by aggregation push down is not supported"*); metadata answers
`SELECT COUNT(*)`, never the k query.

k-anonymity is the worst case for an engine: the riskier the table, the closer the group count is
to the row count, and map-side partial aggregation reduces nothing.

| Engine | Parallel `GROUP BY`? | Notes (D unless marked) |
|---|---|---|
| PostgreSQL | **Effectively no** | Planner avoids parallel aggregation when the finalize step may see as many groups as input rows. `work_mem` default 4 MB per operation → hash spills; PG13+ needed for hash-agg spill. Ceiling estimate ~1 TB overnight, 10 TB fragile (estimate, not measured) |
| MySQL | No | `innodb_parallel_read_threads` covers clustered-index scans only |
| Oracle | Yes | `GROUP BY` is listed as parallelisable; depends on `PARALLEL_DEGREE_POLICY` and object settings |
| SQL Server | Yes | Depends on `cost threshold for parallelism` and `MAXDOP` (= 1 disables) |
| Spark on Iceberg | Yes, with shuffle | Shuffle can exceed local disk at very large row counts — see `arx-and-scaling.md` |
| Flink | Yes | Must run with `RuntimeExecutionMode.BATCH`; in streaming mode aggregates are incremental/retractable and an intermediate result can be mistaken for final |
| Citus | Poorly | QI set changes per query, so no fixed distribution column; falls back to coordinator aggregation |

In every engine the job still "works" when parallelism silently disappears. Read the actual plan
(`EXPLAIN (ANALYZE)`, physical plan, execution mode) and store it with the result; fail the run if
the expected plan shape is absent.

## 5. Showing violating rows

1. Run under the requester's own connection and permissions, not a service account.
2. Stream results from memory; do not write them to disk, cache or logs. Strip parameters from
   logged SQL — `WHERE district = '…' AND age = 43` is the record itself.
3. Default cap (e.g. 10 rows). "Dump all violating rows" is an export with its own authorisation.
4. Log who, when, which table, how many rows and why — never the row contents or the QI values of
   violating groups (a size-1 group's values describe exactly one person).
