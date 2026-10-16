# Plan complexity: depth, window/agg/filter/subquery counts, is_complex, lineage_break_needed

## Reading the tree shape

Spark prints plans as trees, root first. Each nesting level adds a 3-character prefix:

- `+- ` marks the **last** (or only) child; below it the column continues with 3 spaces;
- `:- ` marks a **non-last** child (the left side of a join/union); below it the column continues
  with `:  `.

```
+- SortMergeJoin [id#0L], [id#48L], Inner                <- level 1
   :- Project [...]                                       <- level 2 (left child)
   :  +- SortMergeJoin [id#0L], [id#37L], Inner          <- level 3
   :     :- ...                                           <- level 4
   +- Sort [id#48L ASC NULLS FIRST], false, 0             <- level 2 (right child)
      +- Exchange hashpartitioning(id#48L, 200)           <- level 3
```

Deep plans come from long chains of transformations (each `withColumn`, `filter`, `join` and `union`
adds nodes) and, above all, from **loops** that keep building on the previous DataFrame. The optimizer
collapses many of them (`CollapseProject`, `CombineFilters`, `PushDownPredicates`), so the **physical**
plan is usually much shallower than the logical one.

Why depth matters: the driver analyzes and optimizes the whole tree on every action (planning time
can grow faster than linearly with depth), a lost partition is recomputed through the whole lineage,
and very deep trees end in `StackOverflowError` during analysis. Breaking the lineage
(`localCheckpoint()`, `checkpoint()`, writing and re-reading) resets it.

## What sparkopt extracts

```python
_INDENT_RE = re.compile(r'^(\s*)[+\-\:!\|]')
for line in lines:
    m = _INDENT_RE.match(line)
    if m: max_depth = max(max_depth, len(m.group(1)) // 3)    # "Spark usually uses 3 spaces"
features.depth = max_depth
```

When the text has a `== Physical Plan ==` section, only that section is parsed (depth and every
counter below). The CLI and `analyze()` copy `depth` into `lineage.depth` unless the profile sets it.

### Limits of depth = indent // 3 (measured, Spark 3.5.3)

1. **Only leading whitespace counts.** The regex stops at the first `:` or `+`, so every line under a
   **left** child (`:  ` prefix) gets the depth of the shallowest left branch. Left-deep join chains,
   which are what `df.join(a).join(b).join(c)` produces, are almost invisible:

   | Query | real max level | sparkopt `depth` |
   |---|---|---|
   | 5 chained sort-merge joins (left-deep) | 13 | **5** |
   | 3 nested joins built right-deep | 9 | 9 |
   | 1 `groupBy().agg()` | 4 | 4 |

2. **Only the physical section counts.** `queryExecution().toString()` also holds parsed, analyzed and
   optimized logical plans, which keep every `Project`/`Filter`/`Aggregate`; the bundled inspector skips
   them. For a loop of 12 `withColumn + filter + groupBy` steps, `df.explain()`, `explain("formatted")`
   and `toString()` all give **5**. (Original sparkopt parsed the whole text and reported **36** for the
   `toString()` capture; the bundled copy fixes it.)
3. Cached subtrees under `InMemoryRelation` are counted (they will not re-run once materialized).
4. Subqueries and the AQE `== Initial Plan ==` section add their own nesting.
5. `depth` is a **plan** depth, not the RDD lineage. A DataFrame read back from a checkpoint has depth
   about 1 regardless of history, and an RDD-API loop doesn't appear in a DataFrame plan at all.

Treat `depth` as "at least this deep". For loop-built DataFrames, set `lineage.depth` (or
`has_iterative_loop: true`) in the profile from what the code does.

## Counters

```python
_WINDOW_RE = re.compile(r'\bWindow\b', re.I)
_AGG_RE    = re.compile(r'\b(?:Hash|Sort|ObjectHash)?Aggregate\b', re.I)
_FILTER_RE = re.compile(r'\bFilter\b', re.I)
_SUBQ_RE   = re.compile(r'\bSubquery\b|\bScalarSubquery\b', re.I)
```

All of them are word counts over the physical section, case-insensitive. Values for single
operations (measured on Spark 3.5.3 plan text, recomputed for the bundled regexes):

| Query | Capture | window | agg | subquery |
|---|---|---|---|---|
| one `groupBy().agg(sum)` | simple / full / formatted | - | 2 / 2 / 6 | - |
| one window `row_number().over(...)` | simple / full / formatted | 1 / 1 / 2 | - | - |
| one `groupBy().agg(collect_list)` | simple / full / formatted | - | 2 / 2 / 6 | - |
| one scalar subquery `WHERE v > (SELECT avg(v) ...)` | simple / full / formatted | - | 2 / 2 / 6 | **4** / 4 / 5 |

What this shows:

- `agg_count` counts **operators, not aggregations**: every `groupBy` is a partial + final
  `HashAggregate` (or `ObjectHashAggregate`) pair, so 2 per `groupBy`.
- `ObjectHashAggregate` (`collect_list`, `collect_set`, `percentile_approx`, typed aggregators) counts
  like `HashAggregate`. Original sparkopt counted it as 0 and also counted logical `Aggregate`
  nodes in full captures; the bundled copy fixes both.
- `formatted` still doubles everything: the numbered detail blocks (`(3) HashAggregate`,
  `Aggregate Attributes ...`) are inside the physical section.
- `subquery_count` matches both `Subquery` and the expression id `subquery#138`, so **one scalar
  subquery counts 4** in the simple plan. That alone crosses the `is_complex` threshold (`> 2`).
- `re.I` means column or alias names such as `window`, `filter` or `aggregate` are counted too.
- `filter_count` includes the `isnotnull(...)` filters that Spark adds automatically for joins.
- Capture with plain `df.explain()` (simple mode) for `--plan`; `formatted` is for humans only.

**Nothing consumes these counters.** No sparkopt case reads `window_count`, `agg_count`, `filter_count`
or `subquery_count`. The aggregation cases (`partial_agg` > 1M rows, `approx_distinct` > 5M,
`window_opt` > 500k, `repartition_before_agg` skewed or > 10M) fire on `row_count` and skew alone. Use
the counters to decide **which** of those cases is relevant, by hand.

## Derived properties

```python
is_complex           = depth > 15 or exchange_count > 3 or plan_size_chars > 5000 or subquery_count > 2
lineage_break_needed = depth > 20 or exchange_count > 5
recommended_depth_label: <=5 shallow, <=15 moderate, <=30 deep, else very_deep
```

All thresholds are **sparkopt heuristics**, not Spark rules. Caveats:

- `plan_size_chars > 5000` is the length of the physical section. A 5-join query's simple capture was
  2.3k characters (`False`); the full text was 7.5k, which original sparkopt measured as a whole
  (`True`), while the bundled copy measures only its physical section. Wide schemas (hundreds of
  columns in every `Project [...]`) make almost any plan "complex".
- One scalar subquery makes `is_complex` true (see above).
- `lineage_break_needed` fires on 6 shuffles (a 5-way join chain) even when the plan is shallow and
  cheap to recompute.
- Neither property is read by a case. The CLI prints them in its `plan` section. The case that
  actually breaks lineage, `lineage_break`, fires on `lineage.depth > 20` only, and
  `persistence_strategy` uses `depth > 10`.

## Signal to case

| Signal | Interpretation | sparkopt case | Skill |
|---|---|---|---|
| `depth > 20` (profile `lineage.depth`) | very long lineage | `lineage_break` (reliable `checkpoint()`; needs `setCheckpointDir` on HDFS/S3/ABFS) | spark-persistence-strategy |
| `depth > 10` with reuse >= 2 | medium lineage, reused | `persistence_strategy` uses `localCheckpoint()` (no checkpoint dir needed) | spark-persistence-strategy |
| depth grows with each loop iteration | iterative lineage | set `has_iterative_loop: true`: `iterative_loop` | spark-persistence-strategy |
| `depth > 8` or `exchange_count >= 3` | multi-stage pipeline | `staged_materialization` (opt-in `--case`) | spark-complex-workloads |
| `lineage_break_needed` true | check depth vs shuffles first | `lineage_break` if depth, `staged_materialization` if shuffles | spark-persistence-strategy |
| `window_count >= 1` | sort + shuffle per window spec | `window_opt` (row_count > 500k) | spark-aggregation-optimization |
| `agg_count` high, `HashAggregate` partial/final pairs | shuffle aggregations | `partial_agg`, `approx_distinct`, `repartition_before_agg` | spark-aggregation-optimization |
| `subquery_count` > 0 | each scalar subquery is a separate job | rewrite as join / broadcast the result | spark-join-optimization |
| `is_complex` true | look at which term fired | none directly | this skill |
