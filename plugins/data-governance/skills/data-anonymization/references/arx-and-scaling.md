# ARX Internals and Distributed Anonymisation

ARX is the reference open-source anonymiser (Apache 2.0 core; some `package-info` files carry
GPLv3 headers). This file explains how its search works, why it is single-machine, which parts
parallelise, and what the distributed literature has — and has not — proven. Code: `arx`
commit `4e0a5f5`, paths relative to `src/main/org/deidentifier/arx/`. Tags: **K** code,
**L** literature (full text checked by the research unless marked "citation only"),
**D** docs/repository facts, **(est.)** order-of-magnitude estimate, never measured.

## 1. How ARX anonymises (K)

Full-domain global recoding + record suppression:

1. **Dictionary-encode** every column: string → int code (`framework/data/Dictionary.java:34-46`,
   `String[][] mapping`); the whole table is one flat row-major `int[]`
   (`framework/data/DataMatrix.java:28-40`).
2. Each QI has a **generalisation hierarchy** stored as `int[value][level]`
   (`framework/data/GeneralizationHierarchy.java:36`) — data-independent.
3. The choice of one level per QI defines a **lattice**. Size = product over QIs of the number of
   levels (`framework/lattice/SolutionSpace.java:78-85`): independent of row count, exponential in
   QI count — 9 QIs × 5 levels ≈ 1.95 M nodes, 15 × 5 ≈ 30 billion. The lattice is virtual, backed
   by the bundled JHPL jar (`lib/ant/jhpl/jhpl-0.0.1.jar`, source not in the repo).
4. For a node: transform rows, group them into equivalence classes with a chained hash table
   (`framework/check/groupify/HashGroupify.java:44`, load factor 0.75, initial capacity 1 % of
   rows `framework/check/TransformationChecker.java:121`), check privacy models per class, compute
   information loss.
5. The search algorithm walks the lattice for the privacy-satisfying node with the lowest loss.

### 1.1 Algorithms (`ARXAnonymizer.java:650-711`)

| Algorithm | Class | Optimal? |
|---|---|---|
| **Flash** (`OPTIMAL`) | `algorithm/FLASHAlgorithmImpl.java:54` | Yes — global optimum for the chosen metric |
| Flash, limited (`BEST_EFFORT_BINARY`) | same | No |
| Lightning bottom-up / top-down | `algorithm/LIGHTNINGAlgorithm.java:44`, `LIGHTNINGTopDownAlgorithm.java` | No |
| Genetic | `algorithm/GeneticAlgorithm.java:44` | No |
| Data-dependent DP search | `algorithm/DataDependentEDDPAlgorithm.java:45` | No (exponential mechanism) |

Incognito is not implemented at this commit. Flash's own paper (Kohlmayer et al. 2012) is not
cited in the code; `FLASHAlgorithmImpl.java` javadoc lists only authors.

**Flash** interleaves two phases (`algorithm/FLASHConfiguration.java:23`): a binary search along a
greedy bottom-to-top path (`FLASHAlgorithmImpl.java:179-212`, `checkPath :258-310`) and a recursive
depth-first linear phase (`linearSearch :389-416`). Six monotonicity cases choose the phases
(`algorithm/FLASHAlgorithm.java:49-102`). `FLASHStrategy` imposes a total order on the lattice
"to achieve stable execution times".

**Optimality is conditional.** `OPTIMAL` refuses lattices above `heuristicSearchThreshold`,
default **100,000 nodes** (`ARXConfiguration.java:387`; check `ARXAnonymizer.java:654-658`:
*"Solution space is too large to execute the optimal algorithm"*), and any lattice above 2³¹−1
(`FLASHAlgorithmImpl.java:92-94`). Heuristic defaults: 30,000 ms time limit
(`ARXConfiguration.java:390`), unlimited steps (`:393`). The global optimum disappears with scale
even inside ARX.

### 1.2 Pruning

| Mechanism | Evidence |
|---|---|
| Predictive tagging: "k-anonymous" propagates **up**, "not k-anonymous" **down** the lattice; "anonymous" direction set at runtime (`NONE` if not monotone) | `framework/lattice/SolutionSpace.java:87-118,374-381` |
| Lower-bound utility pruning: if the best known loss ≤ a node's monotone lower bound, prune it and its successors | `FLASHAlgorithmImpl.java:424-491` (`:457-459`); `metric/Metric.java:1372-1398` |
| Roll-up / snapshot reuse (cost per node, not search space) | `framework/check/TransformationCheckerStateMachine.java:56-66` |

### 1.3 The real speed-up: roll-up and snapshots (K)

Each check is classified as `ROLLUP` (regroup the previous node's classes — no raw rows read),
`SNAPSHOT` (regroup a cached intermediate) or `UNOPTIMIZED` (*"all rows need to be transformed"*)
(`TransformationCheckerStateMachine.java:56-66`; dispatch `TransformationChecker.java:204-213`).
Snapshots live in an MRU cache (`framework/check/history/History.java:51`), default 200 entries
(`ARXAnonymizer.java:154`), taken only when classes ≤ 20 % of rows and the snapshot ≤ 80 % of its
parent (`ARXAnonymizer.java:160-163`). The cost of node N depends on **which node was checked just
before it** — the deepest obstacle to parallelism.

## 2. Memory model and single-machine limits (K)

| # | Limit | Evidence |
|---|---|---|
| D1 | One `int[]` for the whole table; `Math.multiplyExact(rows, columns)` → *"Not more then 2^31-1 cells supported"* (≈ 214 M rows at 10 columns) | `DataMatrix.java:57-66` |
| D1b | 3–4 copies: two projections allocate new matrices (`DataManager.java:181-188`, `Data.java:54-86`) plus an output buffer (`framework/check/Transformer.java:107-108`); subsets are index views over the full matrix (`DataMatrixSubset.java:24-44`) | — |
| D2 | One mutable `currentGroupify`/`lastGroupify` pair swapped on every check; state machine remembers the last node | `TransformationChecker.java:200-201`, `TransformationCheckerStateMachine.java:69-81` |
| D3 | Lattice-sized arrays: `sortedSuccessors = new int[solutionSpaceSize][]`, `FLASHStrategy.cache` | `FLASHAlgorithmImpl.java:99`, `FLASHStrategy.java:83` |
| D4 | Snapshot MRU cache shared across nodes | `History.java:51,63` |
| D5 | Global dictionary of all distinct values — high-cardinality columns fill the heap | `Dictionary.java:40` |
| D6 | Sequential global counters with early exit (`currentNumOutliers > suppressionLimit`) | `HashGroupify.java:674-676` |
| D7 | Recursive DFS — stack depth on deep lattices | `FLASHAlgorithmImpl.java:389-416` |
| D8 | Local recoding re-runs the whole engine on the previous run's outliers | `ARXResult.java:585-700` (`optimizeFast`), `:779-820` |
| D9 | Hand-unrolled code: `Transformer01`…`Transformer15`, `equalsIgnore`/`hashCode` switch-cases up to 20 columns | `Transformer.java`, `DataMatrix.java:129-234,280-333` |

**No streaming mode.** `Data.create(Iterator)` exists, but `DataHandleInput` consumes the iterator
into a `DataMatrix` (`DataHandleInput.java:96-140`; unknown length → an `ArrayList<int[]>` first,
temporarily doubling memory).

**No multithreading in the engine.** `grep -rlE "ExecutorService|Executors\.|new Thread\(|ForkJoinPool|parallelStream" src/main/`
returns nothing; thread use is GUI-only. A trace of abandoned data parallelism remains:
`AbstractTransformer implements Callable<HashGroupify>` (`framework/check/transformer/AbstractTransformer.java:37`)
with start/stop indices, yet `Transformer.applyInternal` always passes the full range and calls
`transformer.call()` once (`Transformer.java:405`). No Spark/Hadoop integration.

**Tested scale.** Largest dataset in the repo: `data/adult.csv`, 30,162 records. Larger test sets
(`atus`, `cup`, `fars`, `ihis`) are git-ignored. No benchmark project; `AbstractTestExecutionTime`
averages 10 runs without thresholds. `readme.md` claims "very large datasets on commodity hardware"
with no number.

## 3. What parallelises (K + derived)

| Stage | Parallel? | Why |
|---|---|---|
| Dictionary encoding | ⚠️ rewrite | Per-partition dictionaries + global merge |
| Hierarchies | ✅ | Data-independent, small, broadcast |
| Global pre-tables (cardinalities, sensitive distributions) | ✅ map-reduce | Frequency counts (`metric/v2/Cardinalities.java:62-81`, `DataManager.java:376-393`) |
| Transform + group for one node | ✅ hash-partition by QI | This is SQL `GROUP BY` |
| Class-local privacy checks | ✅ | `isAnonymous(node, entry)` sees one class |
| Sample-based criteria (average risk, uniqueness) | ❌ as ARX does it | Binary-search suppression over a global histogram — but cheap once the histogram exists |
| Decomposable loss metrics | ✅ reduce | See `privacy-models-and-risk.md` §5 |
| NUE, KL-divergence, classification | ❌ | Global tables per node |
| Lattice traversal | ⚠️ task-parallel in theory | Breaks roll-up (each node falls to `UNOPTIMIZED` = full scan), shared pruning writes (`FLASHAlgorithmImpl.java:415,459,481,533`), one shared `globalOptimum` (`algorithm/AbstractAlgorithm.java:39-42`) |
| Applying the chosen transformation | ✅ embarrassingly parallel | Row-independent |

Verdict: ARX cannot be "run on a cluster"; scaling it means re-implementing the algorithm. Reuse
its **semantics** (model definitions, hierarchy structure, decomposable metric formulas, requirement
flags), not its code. A practical hybrid: search on a sample or a small QI set (e.g. Lightning-style
heuristic), then apply the one chosen transformation to all data in parallel, then verify k exactly.

## 4. Distributed literature

| Algorithm | Needs global view? | Parallelism | Largest verified scale | Source |
|---|---|---|---|---|
| **Mondrian** (multidimensional top-down partitioning) | Medians per cut (sampling can replace) | Very high — sub-partitions are disjoint after a cut | Not verified for the centralised version | LeFevre, DeWitt, Ramakrishnan 2006 (citation only) |
| **Distributed Mondrian** (sample-based coordinator) | **No** — workers see only their part; coordinator sees a sample | Very high, **zero data exchange** between workers | **1.0 M** (Poker Hand) and **1.5 M** (ACS PUMS 2019) records; 2–12 workers on one machine (Docker) + AWS t2.medium; 28–98 % / 45–98 % time saved vs centralised; sampling 0.1–0.001 % had "very limited" loss impact | De Capitani di Vimercati et al. 2023, IEEE TBD 9(3) |
| Subtree generalisation on Spark RDD (TDS/BUG family) | Global score per iteration (broadcast) | High; linear in records | **2–30 GB, 0.1–1.2 × 10⁹ records**, 6 data nodes, Spark 2.1 | Bazai, Jang-Jaccard, Alavizadeh 2021 |
| TDS / BUG on MapReduce, MRMondrian | Global per iteration | Medium | Not verified (paywalled) | Zhang et al. 2014 ×2; Zhang et al. MRMondrian (citation only) |
| Datafly, Incognito, Flash | Yes — global counts every step | Low | — | Sweeney; LeFevre 2005; Kohlmayer 2012 |
| **P4** (ARX-based lexicographic partitioning) | No (per chunk) | **Multi-core only**; distribution claimed "straight-forward", untested | **85 M records / 6.5 GB**, 64 cores, 512 GB RAM, one machine; utility loss 0.54–14.4 % at 12 threads | Halilovic, Meurers, Otte, Prasser 2025, BMC MIDM |
| **SKALD** (merge per-chunk sufficient statistics) | No | Conceptually high; single machine | 125 M synthetic records (125 × 1 M chunks) | Reddy et al. 2025, arXiv:2505.03529 |
| λ-diversity clustering on Spark | — | — | 1,025,010 records | Ashkouti & Khamforoosh 2023, PLOS ONE |

Multidimensional partitioning in distributed Mondrian overloads some workers unless the worker
count is a power of two; quantile-based partitioning has no such constraint. Proven ways to cut
shuffle: broadcast small global state, or pre-partition so workers never exchange data — only the
second scales with volume. Skew is real: one dense QI value cannot be split across workers.

**No distributed ARX exists**: GitHub search `arx+anonymization+spark` and
`arx+deidentifier+distributed` returned 0 results (2026-09-08); none of the 19 `arx-deidentifier`
repositories is distributed. Independent confirmation: ARX *"can only operate on a per-chunk basis
if the dataset is too large"* (Reddy et al. 2025).

## 5. Approximate methods: triage only (L)

| Method | What it estimates | Why it cannot prove k |
|---|---|---|
| HyperLogLog | Cardinality (number of distinct groups), ~2–4 % error | k is a **minimum** group size; the minimum is not a mergeable sketch statistic and the error matters most in the tail |
| KHyperLogLog | The whole uniqueness distribution (*"keep the entire distribution of k"*) | Measurement, not enforcement; benchmark table to 1 TB; Google's "petabytes" claim is an unreproduced production statement — the **only** PB-scale claim in the literature |
| Count-Min sketch | Frequencies, **over**-estimates only | A real group of 2 can read as 7 → missed violation (false negative). Separating k = 5 at N = 10¹³ would need ε < 1/N — a sketch larger than the data (est.) |
| Row sampling | Group sizes | A size-k group appears as ~Binomial(k, f) in an f-sample — small groups vanish; distinct-count estimation needs nearly the whole table (Charikar et al. 2000; Harmouch & Naumann 2018) |

Safe use of sampling: to **compute split conditions** (distributed Mondrian), with every worker then
checking k exactly on its full partition. Cardinality sketches are as sensitive as raw data
(Desfontaines, Lochbihler, Basin 2019): store them under raw-data access control.

## 6. Differential privacy tools and streaming (D)

| Tool | Distributed backend | Maturity (as recorded 2026-09) |
|---|---|---|
| Tumult Analytics | PySpark | Production users publicly documented (US Census Bureau, IRS, Wikimedia); moved to OpenDP Oct 2025 |
| PipelineDP | Spark, Beam | Repo: *"we don't recommend its usage in production systems"* |
| PipelineDP4j | Beam (Spark "coming soon" at announcement, Oct 2024) | Published on Maven Central |
| OpenDP core | None (single process) | Mature library |

Budget accounting must be central and transactional (a single-writer ledger); computation may be
distributed, budget deduction cannot. Job-level tools do not solve cross-job accounting
(Nanayakkara et al., arXiv:2211.03898).

**Streaming k-anonymity is a prototype field.** CASTLE (Cao et al. 2011) is centralised; Prink on
Flink (Groneberg et al. 2025) was tested on 321,728 tuples on 2 × t3.large, with mean end-to-end
delay up to thousands of seconds, and states that keying by data subject *"can limit scalability if
the number of data subjects is small"*. No Spark Streaming k-anonymity implementation was found.
For streams, DP aggregation is the more defensible option.

## 7. Petabyte scale: what is and is not proven

- **Not proven:** k-anonymity enforcement at 1 PB. Largest verified: 30 GB / 1.2 × 10⁹ records —
  roughly 4–5 orders of magnitude short. Extrapolation is unsupported: QI skew, class-count growth
  and shuffle do not scale linearly. The only direct scaling observation ("linear in records")
  holds up to 30 GB.
- **Not measured anywhere:** the shuffle share of runtime.
- **Generic engineering facts (est.)** at ~10¹³ rows (1 PB compressed, ~100 B/row): a naive one-stage
  `GROUP BY` shuffles 400–600 TB and fails rather than slowing down if that exceeds local disk;
  average shuffle block ≈ 16 bytes at 64 MB target partitions. Shuffle can be removed by writing the
  table bucketed on a derived `qi_hash` and grouping by `qi_1..qi_m, qi_hash` (Spark storage-partitioned
  join path), but only with `spark.sql.sources.v2.bucketing.enabled` and
  `spark.sql.iceberg.planning.preserve-data-grouping` set to true (both default false), no partition
  spec evolution, and never `…allowJoinKeysSubsetOfPartitionKeys.enabled` (reported data-correctness
  bug). Accept the run only if the physical plan has no `ShuffleExchangeExec`.
- **Design implication:** measure incrementally (new snapshots), run full-corpus scans rarely, and
  calibrate on 1–10 TB of your own data before promising anything.
