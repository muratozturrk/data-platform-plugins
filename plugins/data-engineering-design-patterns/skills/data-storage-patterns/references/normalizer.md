# Normalizer

> Category: data representation. Transactional (normal forms) and analytical (snowflake schema) models.

## Problem

The visits model from the case study stores event attributes (visit time, visited page) next to
immutable ones (device name, OS name and version). The immutable attributes repeat on every visit
row, which inflates storage and makes updates slow whenever they change. You must propose a model
without the repetition and the slow updates.

## Solution / Mechanics

Represent each piece of information **once** and link entities through keys. Both implementations,
**normal forms** and the **snowflake schema**, share one design process:

1. **Define the business entities**: visits, devices, browsers, link referrals.
2. **Describe each entity**: a browser has a name and a version.
3. **Define relationships**: a visit depends on a browser, and a browser on an operating system.

**Normal forms (NF)** suit fast, write-heavy transactional workloads. Fewer duplicates mean fewer
quality issues and less data to write:

| Form | Rule | Broken example | Fix |
| --- | --- | --- | --- |
| 1NF | Atomic, non-repeating values; PK per row | `games.comments = ["...", "..."]` | `game_comments` table |
| 2NF | Non-key columns depend on the *whole* composite PK | `platform_language` depends only on `platform` in PK(name, platform) | `platforms(platform, language)` |
| 3NF | No transitive dependency between non-key columns | `studio_country` depends on `studio`, not game | `studios(studio, country)` |

Higher forms exist, but these three are enough for the pattern.

**Snowflake schema**: a dimensional model (fact table plus dimension tables) where dimensions are
described by further dimensions. Date splits into month and quarter, page into category.

```
                   dim_date_month   dim_date_quarter
                          \             /
 dim_page_category --- dim_page     dim_date
                           \         /
                            fact_visit (visit_id, dim_page_id, dim_date_id, event_time)
 NF variant: visits -> visits_context -> {browsers, devices, ads}; visits -> users, pages -> categories
```

The goal is **consistency over performance**. An update lands in one place and is reflected
everywhere at once.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Query cost | Getting the full picture takes many JOINs, which mean network exchange in distributed engines | Colocate small dimension/entity tables with big ones (node-local joins); broadcast small tables |
| Broadcasting big tables | A dimension is too big to broadcast by default | Filter it first; raise `spark.sql.autoBroadcastJoinThreshold` |
| Archival | Entities change over time (product price by year); point-in-time queries break | SCD techniques from the Static Joiner pattern (Type 2 validity ranges) |
| Complexity | Verbose join chains; fully normalized data is hard to query and slow on big data | Build a Denormalizer read model on top |
| Integrity enforcement | FK constraints protect relational DBs; lakehouse formats rarely enforce them | Constraints Enforcer where supported; AWAP checks otherwise |

## Key design decisions

- **NF vs snowflake vs straight Denormalizer**: OLTP and write-heavy work gets NF. Analytics that
  need consistency get a snowflake. Read-heavy analytics get a star schema or OBT.
- **Keys**: natural or surrogate. Deterministic surrogate keys (`HASH(page)`) keep lakehouse reruns
  idempotent.
- **Dimension history**: SCD Type 1 (overwrite) or Type 2 (validity ranges).
- **Visibility**: a public model, or a private reference model that feeds denormalized tables.
- **Load order**: parents before children so FKs hold.

## When to use / when not

| Situation | Use Normalizer? | Alternative |
| --- | --- | --- |
| Write-heavy transactional workload, frequent updates of shared attributes | Yes (3NF) | - |
| Analytics needing consistent, reusable dimensions | Yes (snowflake) | Star schema if sub-dimensions are small |
| 80% of queries join the same many tables and latency matters | Not alone | Denormalizer on top of it |
| Immutable attributes repeated per row, but only one consumer reads them | Maybe | Vertical Partitioner |
| Event payloads in a broker | Partially (entity topics) | Stream-table enrichment downstream |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| FK violation during load | Child table loaded before parent | Load entities first (DAG ordering) or use deferred constraints |
| Facts disappear in reports | INNER join on a dimension with missing rows | Read with LEFT joins (as in the examples) and monitor orphan keys |
| Join shuffles dominate runtime | Big-to-big joins on normalized tables | Broadcast small dims; Bucket both sides on the join key |
| Broadcast join not chosen | Key type mismatch (string vs int) or table above threshold | Align types; filter; tune threshold |
| Historical report shows today's value | Type 1 dimension | SCD Type 2 + as-of join |
| Duplicate dimension rows after rerun | Non-deterministic surrogate keys | Hash-based keys + upsert (`ON CONFLICT`) / overwrite |

## Implementation notes per stack

**Spark** - `examples/normalizer/spark/spark_normalizer.py`: turns a flat visits feed into a Delta
snowflake schema (fact_visit, dim_page -> dim_page_category, dim_date -> month/quarter) with
hash surrogate keys. It then reads it back through the join chain, broadcasting the small dims.

```python
save(flat.selectExpr("HASH(category) AS page_category_id", "category").distinct(), "dim_page_category")
save(flat.selectExpr("HASH(page) AS page_id", "page", "HASH(category) AS dim_page_category_id")
     .distinct(), "dim_page")
save(flat.selectExpr("visit_id", "HASH(page) AS dim_page_id", "HASH(TO_DATE(event_time)) AS dim_date_id",
                     "event_time"), "fact_visit")
page_w_category = t["dim_page"].join(
    F.broadcast(t["dim_page_category"]),
    t["dim_page"].dim_page_category_id == t["dim_page_category"].page_category_id, "left_outer")
```

**Kafka** - `examples/normalizer/kafka/README.md` (no code). The broker has no tables, joins or FKs.
The normalized equivalent is one compacted topic per entity (`pages`, `devices`, `users`) keyed by
id, events carrying only foreign keys, and Schema Registry schema references to reuse entity
schemas. Consumers recombine with stream-table joins on co-partitioned topics.

**Airflow** - `examples/normalizer/airflow/normalizer_dag.py`: a PostgreSQL 3NF model that fixes the
three broken games tables (1NF, 2NF, 3NF violations), with FKs and an FK-ordered, upsert-based (rerunnable) load.

```python
DDL = """
CREATE TABLE IF NOT EXISTS dp.studios (name TEXT PRIMARY KEY, country TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dp.platforms (name TEXT PRIMARY KEY, language TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS dp.games (
    name TEXT PRIMARY KEY, studio TEXT NOT NULL REFERENCES dp.studios(name));
CREATE TABLE IF NOT EXISTS dp.game_releases (
    game TEXT REFERENCES dp.games(name), platform TEXT REFERENCES dp.platforms(name),
    release_year INT NOT NULL, PRIMARY KEY (game, platform));
CREATE TABLE IF NOT EXISTS dp.game_comments (
    game TEXT REFERENCES dp.games(name), comment TEXT NOT NULL);
"""
create_schema() >> [load_studios(), load_platforms()] >> load_games() >> load_releases_and_comments()
```

## Related patterns

- **Denormalizer**: the read-optimized counterpart, often built on top of a normalized model.
- **Vertical Partitioner**: also splits rows by attribute group, but for storage/retention, not normal forms.
- **Constraints Enforcer**: integrity (FK) constraints are typical of normalized models.
- **Static Joiner**: its SCD techniques handle time-sensitive dimensions.
- **Bucket**: identical bucketing of fact and entity tables removes join shuffles.
- **Dataset Materializer**: materialize the expensive join chain for frequent readers.
- **Local Sequencer**: orders entity loads before dependent tables.
