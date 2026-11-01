# Privacy Models, Re-identification Risk and Information Loss

What each technique guarantees, what it does not, how its check is computed, and how the
re-identification risk report and information-loss metrics behave when the data is split across
machines. ARX paths are relative to `src/main/org/deidentifier/arx/` at commit `4e0a5f5`.
Tags: **K** code, **D** official docs, **L** peer-reviewed literature (full text checked by the
research), **(derived)** = this skill's reasoning from those facts.

## 1. Attribute roles

| Role | What it is | Typical treatment | What goes wrong if misclassified |
|---|---|---|---|
| **Direct identifier** | Identifies a person on its own (national ID, name, e-mail, phone, IBAN) | Remove, or replace with a pseudonym/token | Left in place: the release is trivially re-identifiable |
| **Quasi-identifier (QI)** | Harmless alone, identifying in combination (age, district, dates, occupation, device or product attributes) | Generalise and/or suppress until every QI combination is shared by enough records | A **missing** QI makes measured k look **larger** than it is — the error is in the unsafe direction |
| **Sensitive attribute** | The value an attacker wants to learn (diagnosis, salary, special-category data) | Keep, but control its distribution per group (ℓ-diversity, t-closeness) | Homogeneous group → value disclosed even though k holds |
| **Insensitive** | None of the above | Keep | — |

Masking hides a value; anonymisation is a property of the **whole released dataset**. Deleting the
national ID column does not help when "43 years old, lives in district X, did Y on a Tuesday" still
points to one person.

## 2. What each model guarantees

| Model | Guarantees | Does not guarantee | Check is computed from |
|---|---|---|---|
| **Masking / tokenisation / hashing / encryption** | The masked field is not readable | Anything about groups; records stay linkable through QIs. Managed services that list these techniques (e.g. AWS Glue DataBrew substitution, hashing, deterministic encryption, nulling, shuffling) do **not** offer k-anonymity (D) | Each record alone |
| **Pseudonymisation** | Direct identifiers replaced by a consistent key | Unlinkability: the same pseudonym joins records; with the key it is reversible | Each record alone |
| **k-anonymity** | Every QI combination occurs in ≥ k records (prosecutor re-identification ≤ 1/k) | Attribute disclosure (all k share the sensitive value); background-knowledge attacks; WP216 does **not** treat it as sufficient alone (L) | Class size `count(*)` per QI group — local to one class |
| **ℓ-diversity** (distinct, recursive (c,ℓ), entropy) | ≥ ℓ "well-represented" sensitive values per class | Skewness/similarity attacks (ℓ values that mean the same) | Value distribution inside one class |
| **t-closeness** (equal, ordered, hierarchical distance) | Each class's sensitive distribution is within t (EMD) of the **global** distribution | Utility; needs a global pre-table | Class distribution + global distribution |
| **δ-presence** | Bounds the probability that a person is **in** the dataset, relative to a population table | Needs a public superset/population table | Class count + population count |
| **k-map** | Each class matches ≥ k people in the **population** (not just in the sample) | Needs population data or an estimator | Class count + population model |
| **Differential privacy (ε,δ)** | Output distribution changes by at most e^ε (+δ) whether or not one person is included — a property of the **mechanism**, not of a table | Utility of small groups; repeated queries consume budget | Noise/sampling mechanism + a central budget ledger |

k-anonymity protects a published **table**; differential privacy protects published **query
results** or a randomised release. They answer different questions and are best run as separate
capabilities rather than one pipeline (derived).

## 3. ARX's 21 criteria: locality and monotonicity (K)

The abstract check `PrivacyCriterion.isAnonymous(Transformation<?> node, HashGroupifyEntry entry)`
(`criteria/PrivacyCriterion.java:181`) sees **one equivalence class**, not the whole table. That
signature is why most models can be decided per partition. Exceptions are "sample-based" criteria
(`criteria/SampleBasedCriterion.java:55-56` `enforce(HashGroupifyDistribution, …)`; `isAnonymous`
throws `UnsupportedOperationException`, `:64-66`). k-anonymity bypasses `isAnonymous` entirely —
`KAnonymity.isAnonymous` throws `"This should never be called!"` (`criteria/KAnonymity.java:103-106`);
the engine uses a minimal-class-size shortcut instead.

Per-class requirements are bit flags (`ARXConfiguration.java:288-294`): `COUNTER` (class count),
`SECONDARY_COUNTER` (population/public-table count), `DISTRIBUTION` (sensitive values per class).

| Model | Class (`criteria/`) | Needs | Local or global | Partition-wise? |
|---|---|---|---|---|
| k-anonymity | `KAnonymity.java:35` | COUNTER | Local (`count ≥ k`) | **Yes** |
| Distinct / recursive / entropy ℓ-diversity | `DistinctLDiversity.java:30`, `RecursiveCLDiversity.java:33`, `EntropyLDiversity.java:35` | DISTRIBUTION | Local | **Yes** |
| t-closeness (equal / ordered / hierarchical) | `EqualDistanceTCloseness.java:32` (`manager.getDistribution` `:61`), `OrderedDistanceTCloseness.java:35`, `HierarchicalDistanceTCloseness.java:33` | DISTRIBUTION | Global **pre-table** | Partly — if the global distribution is computed first and broadcast |
| δ-disclosure, basic/enhanced β-likeness | `DDisclosurePrivacy.java:36`, `BasicBLikeness.java:35`, `EnhancedBLikeness.java:35` | DISTRIBUTION | Global pre-table | Partly |
| δ-presence | `DPresence.java:37` | COUNTER + SECONDARY | Local, population-filled counter | Partly |
| k-map | `KMap.java:42` | COUNTER (+SECONDARY) | Local + population model | Yes (population parameter is a constant) |
| Profitability (prosecutor/journalist, ± no-attack) | `ProfitabilityProsecutor.java:41` etc. | COUNTER (+SECONDARY) | Local | Yes |
| (ε,δ)-DP via SafePub | `EDDifferentialPrivacy.java:51` | COUNTER + SECONDARY | **Global random sampling** first | **No** |
| Average re-identification risk | `AverageReidentificationRisk.java:30` | COUNTER | **Global** — `1/averageClassSize ≤ threshold` (`:165`) | **No** |
| Sample / population uniqueness | `SampleUniqueness.java:28`, `PopulationUniqueness.java:34` | COUNTER | **Global** histogram (+ population model) | **No** |

Global criteria are enforced by binary-search suppression over the whole class histogram
(`HashGroupifyDistribution.suppressWhileNotFulfilledBinary`,
`framework/check/groupify/HashGroupifyDistribution.java:230`) — O(log n) full passes for a single
lattice node. (b,t)-privacy was not found in the code.

**Monotonicity decides pruning** (constructor flags, `PrivacyCriterion.java:54-58`):

| Model | Monotone with suppression | Monotone with generalisation |
|---|---|---|
| k-anonymity, distinct ℓ-diversity, k-map, average risk, sample uniqueness | ✅ | ✅ |
| recursive/entropy ℓ-diversity, t-closeness, β-likeness, δ-presence | ❌ | ✅ |
| δ-disclosure, (ε,δ)-DP, profitability (with attack) | ❌ | **❌** |
| population uniqueness | ❌ | only with the Zayatz model |

Not monotone with generalisation ⇒ the lattice property "anonymous" gets `Direction.NONE`
(`framework/lattice/SolutionSpace.java:374-381`) and the strongest pruning is off — the search
approaches brute force. Practical reading: masking + k-anonymity is the cheapest, most
partitionable starting point; ℓ/t add a global pre-table and lose suppression monotonicity; DP is a
different paradigm (derived).

### Differential privacy in ARX is not noise addition (K)

`EDDifferentialPrivacy` is *"(e,d)-Differential Privacy implemented with SafePub"* (`:36-49`):
random sub-sampling with probability β, then k-anonymity. Sampling uses
`new Random(0xDEADBEEF)` in deterministic mode, else `SecureRandom` (`:216`). (ε,δ) is converted to
(k,β) in `dp/ParameterCalculation.java`. With DP, optimal search, time-limited heuristic search and
local recoding are all unsupported (`:259-272`); the data-dependent search uses the exponential
mechanism (`dp/ExponentialMechanism.java`) with the ε budget split across search steps.

## 4. The re-identification risk report

### 4.1 Three attacker models, four measures (K)

`risk/RiskModelSampleSummary.java:30-36` implements *"risk measures as proposed by El Emam in
'Guide to the De-Identification of Personal Health Information', 'Measuring the Probability of
Re-Identification'"*. Each attacker model stores `t, rA, rB, rC, rM` (`:128-136`):

| Field | Meaning (javadoc) |
|---|---|
| `t` | User-supplied risk threshold |
| `rA` | Proportion of records with risk above the threshold |
| `rB` | Maximum probability of re-identification |
| `rC` | Proportion of records that can be re-identified on average (success rate) |
| `rM` | Minimum probability of re-identification |

| Attacker | Assumption | Measures (K) |
|---|---|---|
| **Prosecutor** (`ProsecutorRisk extends RiskSummary`, `:90`) | Knows the target **is** in the data | All four |
| **Journalist** (`JournalistRisk extends RiskSummary`, `:44`) | Target is in a larger population; the release is a sample | All four |
| **Marketer** (`MarketerRisk`, `:64`, does not extend `RiskSummary`) | Wants to match as many records as possible | **Only `rC`** — "highest risk" has no meaning here |

Two report rules follow:

- **Show both thresholds.** `getEffectiveRiskThreshold()` returns *"the effective threshold, which
  may differ from user-specified parameters due to rounding issues"* (`:147-151`). A user who typed
  0.05 must see the threshold actually applied.
- **Population measures need outside input.** Population uniqueness uses the Pitman, Zayatz or SNB
  estimators (`risk/ModelPitman.java`, `ModelZayatz.java`, `ModelSNB.java`; references Dankar,
  Zayatz, Pannekoek) and needs the population size or sampling fraction. If nobody supplied it,
  print **"not computable"** — never 0 and never a default.

`null`/"not computable" ≠ 0: ARX itself returns `0d` for an **empty** histogram
(`risk/RiskModelSampleRisks.java:55-59`, `:128-133`). A report that copies those zeros would
present "no data" as "no risk".

### 4.2 Computing it on large data (derived from K)

Every sample-based measure is a function of one object: the **class-size histogram**
φ_j = number of QI groups of exactly size j. ARX computes average risk as `1 / avgClassSize`
(`RiskModelSampleRisks.java:59`) and highest risk as `1 / smallestClassSize` (`:133`). The
histogram costs one aggregation over the table plus one over its result:

```sql
SELECT class_size, count(*) AS classes            -- φ_j
FROM  (SELECT count(*) AS class_size
       FROM   t GROUP BY qi_1, ..., qi_m) g
GROUP BY class_size;
```

| Measure (prosecutor) | From φ_j (N = Σ j·φ_j, C = Σ φ_j) |
|---|---|
| min k | smallest j with φ_j > 0 |
| highest risk `rB` | 1 / min k |
| average risk / success rate `rC` | C / N (= 1 / average class size) |
| records at risk `rA` | Σ_{j : 1/j > t} j·φ_j / N |
| sample uniqueness | φ_1 / N |
| k-anonymous? | φ_j = 0 for all j < k |

The inner `GROUP BY` is the expensive part and parallelises in any engine that parallelises
high-cardinality aggregation (see `discovery-and-risk-report.md`). The histogram itself has at most
as many rows as distinct class sizes — tiny, so the "global" risk measures that ARX cannot
distribute become cheap once the histogram exists. Journalist and population measures add the
population model on top of the same histogram.

## 5. Information-loss metrics: which ones decompose (K)

A metric is **decomposable** if, with data split into QI-disjoint partitions, each partition can
return a sufficient statistic that a sum/weighted mean turns into the global value (global
constants such as N and hierarchy domain sizes are published once). Paths under `metric/v2/`.

| Metric | Class | Sufficient statistic | Decomposable? |
|---|---|---|---|
| Discernibility DM* | `MetricSDDiscernability.java:33` | Σ nᵢ² | ✅ pure sum, no constants |
| Discernibility DM | `MetricSDNMDiscernability.java:41` | (Σ nᵢ², Σ suppressed nᵢ) | ✅ needs N |
| Precision (monotone / non-monotone) | `MetricMDPrecision.java:37`, `MetricMDNMPrecision.java:47` | none / (n_kept, n_suppressed) | ✅ |
| Height, Static | `MetricMDHeight.java:40`, `MetricMDStatic.java:44` | none (data-independent) | ✅ trivial |
| **Loss / NDS** | `MetricMDNMLoss.java:46` | per-dimension raw Σ nᵢ·share(v, level) | ✅ shares come from the **hierarchy**, not data frequencies (`DomainShareMaterialized.java:75,108`); normalisation `(aggregate − min)/(max − min)` with global N (`MetricMDNMLoss.java:427-434`) |
| Ambiguity, entropy-based IL, publisher payout | `MetricSDNMAmbiguity.java:38` etc. | raw sums | ✅ |
| AECS | `MetricSDAECS.java:37` | (N, number of classes) | ⚠️ the pair adds up, the AECS **values** cannot be averaged; "suppressed = one class" rule applied once globally |
| **Non-uniform entropy** (3 variants) | `MetricMDNUEntropyPrecomputed.java:53` | — | ❌ the `HashGroupify g` argument is never read in `getInformationLossInternalRaw` (`:256-287`); the value comes from global marginal frequency tables built over all rows (`Cardinalities.java:62-81`); the non-monotone variant adds a whole-table suppression term per node |
| KL-divergence | `MetricSDNMKLDivergence.java:44` | — | ❌ loops over original rows, global normalisation |
| Classification accuracy (QI targets) | `MetricSDClassification.java:48` | — | ❌ regroups across partitions |

Cross-QI aggregate functions (`SUM`, `MAXIMUM`, `ARITHMETIC_MEAN`, `GEOMETRIC_MEAN`) combine fine;
`RANK` must be applied after the per-dimension values are summed (`ILMultiDimensionalRank.java`).
Always merge partitions at the **raw** level, then normalise and then combine dimensions.

Choice for distributed work: **Loss/NDS as primary, DM\* as secondary**; avoid NUE, KL-divergence
and classification accuracy unless the data fits one machine. Keep hierarchies data-independent —
deriving bins from data quantiles turns Loss/NDS into a metric that needs another global pass
(derived).

No lower bound means no utility pruning: `MetricSDClassification` returns `null` with *"TODO: Can a
lower bound be calculated for this model?"* (`MetricSDClassification.java:430-439`), and
`MetricSDNMKLDivergence` gives none either, so `FLASHAlgorithmImpl.prune` returns early.

## 6. What an anonymisation claim can prove

### 6.1 KVKK link (not legal advice)

| Provision | Content | Technical consequence |
|---|---|---|
| Destruction Regulation Art. 4/1(c), Art. 10 | Destruction = erasure, destruction **or anonymisation** | "We anonymised it" is a legal destruction statement; record the method as its own field |
| Destruction Regulation Art. 7/3 | All destruction operations are recorded; records kept **≥ 3 years** | Measurement and destruction records must be immutable and retained |
| Destruction Regulation Art. 11/2 | Periodic destruction interval ≤ 6 months | Scheduled job with an overdue alert |
| Law Art. 28/1(b) | Processing for research, planning and statistics **anonymised through official statistics** is outside the Law entirely | The anonymisation measurement is the technical precondition for treating an activity as out of scope — the highest-leverage and highest-risk claim |
| Law Art. 4/2(ç); Board Principle Decision 2026/1301 | Minimum data sharing; masking/anonymisation for public-law controllers publishing online | Record which method was applied to each published dataset |

The Law's definition of anonymisation (Art. 3/1(b): data that can no longer be associated with an
identified or identifiable person **even when matched with other data**) and the Board's guide on
erasure, destruction and anonymisation were not part of the research's verified table — read both
in the official text before relying on them (not verified). Applying any of this is counsel's job.

Two technical consequences (derived):

- **The source keeps being personal data.** Write anonymised output to a **new** table/document; the
  source needs its own, separately recorded destruction. One `INSERT OVERWRITE` that does both makes
  the destruction record impossible.
- **Snapshots and tags interact with destruction.** Lakehouse snapshots used as evidence expire with
  `expire_snapshots`; an audit tag with infinite `max-ref-age` keeps personal data alive. Store the
  measurement result with the snapshot ID outside the table, and never pin a snapshot that must be
  destroyed.

### 6.2 The evidence ledger

An append-only record per measurement or transformation:

| Field | Required |
|---|---|
| Data source ID + snapshot/version reference | ✅ |
| Policy version (QI set, hierarchies, k/ℓ/t, thresholds) and effective date | ✅ |
| QI set **and who approved it** | ✅ |
| Engine name and version; query text with parameters stripped | ✅ |
| Plan check result (parallelism present, no unexpected shuffle, BATCH mode) | ✅ |
| Measured min k, **number** of violating groups, affected record count | ✅ |
| Information-loss value and metric | ✅ |
| Duration | ✅ |
| Documents: model + version, threshold, gold-set size/date, recall/precision, OCR quality | ✅ |
| Sample-row views: who, when, table, row count, reason | ✅ |

Never written: raw rows, QI values of violating groups, document content or detected identifier
values, unstripped query text. A record with a missing required field is not created.

Limits — put them on the report cover:

1. It proves a **measurement** ("k = 7 was measured"), not a legal qualification ("this data is
   anonymous").
2. k-anonymity alone is not sufficient under WP216 (singling out, linkability, inference).
3. A database the operator controls is tamperable (a DBA can edit rows; lakehouse snapshot chains
   are integers, not signatures) — export to WORM storage or a signed external log; without it the
   audit value is limited.
4. Document recall cannot be proven, only estimated.
5. No supervisory guidance or case law on whether sampled or sketched k estimates count as
   evidence was found — use exact measurement for evidence.
6. Rolled-back commits appear in table history; filter them or a state that never existed may be
   cited.
