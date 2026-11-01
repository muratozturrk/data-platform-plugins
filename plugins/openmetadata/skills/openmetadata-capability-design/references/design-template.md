# Scoring Worksheet and Technical Design Template

The research specified a weighted scoring step (T8) and deep designs for the top 3-5 candidates
but did **not** execute them. What follows is the method and the templates, ready to run. Do not
publish a ranking without filling the rationale column - a number without a reason is not a score.
Product facts: OpenMetadata 1.13.3 (`1.13.3-release`, `255f6694913b`). Short paths: `spec/` =
`openmetadata-spec/src/main/resources/json/schema/`, `svc/` =
`openmetadata-service/src/main/java/org/openmetadata/service/`. Not legal advice.

## 1. T8 Scoring

| Criterion | Weight | What it measures | Anchor for 5 / 1 |
|---|---|---|---|
| Impact | 25% | Size of the rubric / DMBOK gap closed | 5: closes several high-impact gaps or unlocks others · 1: cosmetic |
| **KVKK & regulatory** | **18%** | (a) eases a concrete KVKK obligation (inventory, destruction, data-subject request, breach notification, cross-border transfer); (b) runs on-prem; (c) does it create new personal-data risk itself (e.g. raw data to an LLM) | 5: directly eases an obligation, on-prem, no new risk · 1: no obligation, or new unmitigated risk |
| Novelty | 17% | From the novelty verdict | new = 5 · partial = 3 · planned = 2 · exists = eliminated, not scored |
| Platform fit | 17% | Integration ease with the target stack (Kafka, Flink, Iceberg, ClickHouse, Trino, Ranger, Vault, Airflow, dbt) | 5: talks to existing components, no new infra · 1: needs a new heavy component |
| Fit to OpenMetadata | 13% | Fit with the entity model and ingestion framework; fork required? | 5: plug-in on an existing hook · 1: fork or core-schema rewrite |
| Effort | 10% | **Inverse** scale | 5: very little effort · 1: very large |

KVKK is deliberately second: when a platform processes personal data in Türkiye, compliance is a
precondition of deployment, not a nice-to-have. Formula, each rating an integer 1-5:

```
score = 0.25·impact + 0.18·kvkk + 0.17·novelty + 0.17·platform + 0.13·openmetadata + 0.10·effort
        # range 1.0 - 5.0; eliminated candidates excluded; missing criterion = error
```

```python
WEIGHTS = {"impact": .25, "kvkk": .18, "novelty": .17, "platform": .17, "openmetadata": .13, "effort": .10}

def weighted_score(r: dict[str, int]) -> float:
    for k in WEIGHTS:
        if not 1 <= r[k] <= 5:            # KeyError if missing, ValueError if out of range
            raise ValueError(f"{k}: {r[k]} not in 1..5")
    return round(sum(r[k] * w for k, w in WEIGHTS.items()), 3)

assert weighted_score({k: 5 for k in WEIGHTS}) == 5.0
assert weighted_score({k: 1 for k in WEIGHTS}) == 1.0
```

### Worksheet (one row per non-eliminated candidate; fill both columns)

| ID | Impact | KVKK | Novelty | Platform | OM fit | Effort | Score | Rationale (one line per criterion) |
|---|---|---|---|---|---|---|---|---|
| C01 | | | 3 | | | | | impact: … · kvkk: … · novelty: partial (drift audit unique) · platform: … · om: … · effort: … |
| C13 | | | 5 | | | | | … novelty: new … |
| C14 | | | 5 | | | | | … kvkk must reflect highest new risk (document text to LLM) … |
| … | | | | | | | | |

Inputs that already exist per candidate (see [candidate-pool.md](candidate-pool.md)): gap IDs
(impact), KVKK note incl. new-risk statement (kvkk), verdict (novelty), platform integration and
on-prem rationale (platform), extension point and its limits (OM fit), risks and prerequisites
(effort - e.g. C02 needs an Iceberg connector first, C19 needs a Spark lineage producer).

Scoring hygiene: rate all candidates criterion by criterion (not candidate by candidate) to keep
scales consistent; have a second person cross-check; record ties instead of breaking them
silently; show eliminated ideas in a separate list with their evidence. Present the ranking to the
decision-maker and let them choose the 3-5 to design - the ranking informs, it does not decide.

## 2. Technical Design Template

Write one document per selected capability. **Rule: every OpenMetadata class, schema file or API
path named in the design must exist in the pinned source tree.** Unverified names are not used.

```markdown
# <Capability name>

## 1. Problem
Gap(s) closed (IDs from the gap catalog), who benefits, what fails today (with path:line).

## 2. Solution summary
2-3 paragraphs. State the surviving differentiator from the novelty check.

## 3. Integration with OpenMetadata
### 3.1 Data model
New entity type? (Not possible via API in 1.13.3 - say "core schema change" if you need one.)
Field on an existing entity, custom property, glossary/classification, doc-store Document?
JSON Schema draft. Files to change/add, each verified in the tree.
### 3.2 API surface
New REST endpoints, request/response bodies; reuse of existing resources.
### 3.3 Ingestion / workflow
New connector or workflow type, plug-in to an existing processor/validator, new workflow node,
native scheduled app, lifecycle handler, event-subscription destination.
### 3.4 Storage
Is MySQL/PostgreSQL + Elasticsearch/OpenSearch enough? Any new component (triple store,
GPU server, Kafka topic) with its operational cost stated explicitly.
### 3.5 UI
Screens and interactions; what can ship API-first.

## 4. Platform integration
Which stack components (Kafka, Flink, Iceberg, ClickHouse, Trino, Airflow, dbt,
Great Expectations, Ranger, Vault, MinIO) and how: direction, protocol, failure behaviour.

## 5. LLM design (if any)
- Model class and size; serving (vLLM / Ollama); quantisation; GPUs
- Context window needed and why (what goes into the prompt)
- Turkish performance expectation and mitigation
- Prompt structure; versioning (promptTemplate entity)
- Output validation: what if the model hallucinates; where the human approves
- Resource need and latency; fallback when the model under-performs

## 6. KVKK impact assessment          (mandatory)
(see checklist below)

## 7. Risks and limitations

## 8. Validation plan
Metric, test scenario, acceptance criterion - how we prove it works.

## 9. Effort estimate
Rough work items and order of magnitude.
```

### Where things plug in (verified in 1.13.3, `K`)

| Design need | Hook | Path |
|---|---|---|
| NLQ provider | Implement `NLQService`, set `NATURAL_LANGUAGE_SEARCH_PROVIDER_CLASS` | `svc/search/nlq/NLQService.java:12`, `NLQServiceFactory.java:15-29`, `conf/openmetadata.yaml:494-499` |
| Embeddings on-prem | `EMBEDDING_PROVIDER=djl`, `DJL_EMBEDDING_MODEL=<multilingual model>` (provider set is an enum; a new provider needs factory code) | `svc/search/vector/client/EmbeddingClient.java:14`, `DjlEmbeddingClient.java:37-46`, `conf/openmetadata.yaml:497,523-524` |
| LLM service / run records | `llmService` + `ollamaConnection` / `customLLMConnection` (vLLM via custom); `promptTemplate`, `agentExecution` entities with REST | `spec/entity/services/connections/llm/`, `spec/entity/ai/`, `svc/resources/ai/` |
| New workflow node | Add subtype to enum + case in factory switch | `spec/governance/workflows/elements/nodeSubType.json`, `svc/governance/workflows/elements/NodeFactory.java:44` |
| Propose → approve → apply | Recognizer-feedback approval nodes | `createRecognizerFeedbackApprovalTask`, `applyRecognizerFeedbackTask`, `rejectRecognizerFeedbackTask` in `nodeSubType.json` |
| Scheduled job | Native app extending `AbstractNativeApplication`, Quartz `startApp` | `svc/apps/bundles/dataRetention/DataRetention.java:39,78` |
| React to entity changes | `EntityLifecycleEventHandler` + `registerHandler` | `svc/events/lifecycle/EntityLifecycleEventHandler.java:29`, `EntityLifecycleEventDispatcher.java:73` |
| Periodic measurement as a test | Validator file at the dynamic-import path | `ingestion/src/metadata/utils/importer.py:247,266-270` |
| LLM in PII pipeline | New branch in processor factory | `ingestion/src/metadata/pii/processor_factory.py:14` |
| Git export sink | Schema exists, write a `SinkProvider` and register it | `.../automatedTask/sinkConfig/gitSinkConfig.json`, `.../sink/SinkProviderRegistry.java:52` |
| Ontology inference / SHACL | CUSTOM reasoner rules, shapes file (enable `RDF_ENABLED`) | `svc/rdf/reasoning/InferenceEngine.java`, `rdf/shapes/openmetadata-shapes.ttl` |
| New document kinds | Doc store `Document` free `data` | `spec/entity/docStore/document.json:35,39,75` |

Known walls: no new entity types via API (`svc/resources/types/TypeResource.java:78-85`); no
"value required" on custom properties (`spec/type/customProperty.json:43,69-70`); Kafka is not an
event-subscription destination (`spec/events/eventSubscription.json:98-110`, enum change needed);
deny-precedence is fixed in policy evaluation (category rules can only tighten).

## 3. LLM Sizing Assumptions (research, not benchmarks)

| Task shape | Model class | Serving | Context | Turkish | Candidates |
|---|---|---|---|---|---|
| Short-label similarity | In-process sentence embedding (MiniLM default is English-heavy → multilingual model by config) | DJL in the server | n/a | Short labels fine, domain abbreviations weak | C05, C06 |
| Template filling from structured facts | ~14B instruct (e.g. Qwen2.5-14B) | vLLM, AWQ 4-bit, 1×A100-80GB | 16k | High for formal text; legal review anyway | C10, C19 |
| Metadata classification | ~14B, escalate to ~32B on conflicts | vLLM, AWQ 4-bit, 1×A100-80GB | 16k | Must expand Turkish abbreviations | C08 |
| Translation of terms/definitions | ~14B or ~70B (Llama-3.3-70B, 4-bit, 2×A100-80GB) | vLLM | 8k | Core of the task; 7-8B insufficient | C16 |
| NL → query with schema + glossary context | ~32B (Qwen2.5-32B / Qwen3-32B), AWQ/GPTQ 4-bit | 2×A100-80GB or 4×L40S | 32k | Turkish question ↔ English schema mapping | C07 |
| Document page scan for residual PII | ~14B, 4-bit | 1×A100-80GB | 32k | Very high (names, titles, partial masking) | C14 |

Every LLM design must state its fallback: what still works when the model is switched off. In the
pool, the core value survives without the model for C05, C06, C07 (hybrid search), C10, C16, C19;
C08 degrades to manual inventory; C14 degrades to a flagger for humans.

## 4. KVKK Impact Assessment Checklist

Answer each item explicitly; "not applicable" needs a reason.

| # | Question | Typical answers from the pool |
|---|---|---|
| 1 | Is personal data processed? Which categories (incl. special categories, Art. 6)? | Metadata only (C01, C03, C11) · statistics from raw data (C13) · real document text (C14) |
| 2 | Legal basis for this processing (KVKK Art. 5, and Art. 6 for special categories) | State the basis you rely on and have legal counsel confirm it |
| 3 | Data minimisation: what goes **to the model**, what never does | Column names, types, paths, docs - **no sample rows, no values** (C07, C08); counts and categories, not person lists (C10) |
| 4 | Where is it processed? | On-prem GPU / in-process; no external API |
| 5 | Logging and retention of prompts, responses, results | Do not persist prompts/responses with personal data (C14); store labels/page numbers, not excerpts; justification text is itself retention-bound (C17) |
| 6 | Cross-border transfer risk | None on-prem - **state it explicitly** anyway |
| 7 | Identifiers in evidence records | Request number + affected row count, never the identifier (C02, C09) |
| 8 | Can the feature escalate privilege? | Generated queries run under the caller's identity (C07) |
| 9 | Fine-tuning on personal data? | No |
| 10 | Does an automated output change access or legal state? | Then approval workflow, no auto-apply (C08 via C01); no auto-takedown (C14) |
| 11 | New risk introduced and its mitigation | Name the residual risk and the control enforcing it in code |

Obligation-level mapping (which KVKK article / secondary regulation each capability serves) lives
in `openmetadata-kvkk-compliance`; verify every legal reference against the official text in force.

## 5. Path Verification Before Review

```bash
OM=path/to/openmetadata   # checked out at 1.13.3-release
for f in designs/*.md; do
  echo "--- $f"
  grep -oE '(openmetadata-spec|openmetadata-service|ingestion)/[A-Za-z0-9_/.-]+' "$f" | sort -u |
  while read -r p; do [ -e "$OM/$p" ] && echo "  OK   $p" || echo "  MISSING $p"; done
done
```

Any `MISSING` line is fixed or removed before the design goes out. Short forms (`spec/`, `svc/`)
should be expanded for this check.

## 6. Risks to Watch in Every Design

- On-prem small models may not do the job → size explicitly, keep the fallback (research risk table).
- Operational leanness: OpenMetadata is often chosen for its light footprint; a triple store or
  GPU cluster must be justified, not slipped in.
- Version drift: several gaps close on `main`/2.0 (policy agent, task due dates); re-check before build.
- License boundary: code in the Apache-2.0 core vs plug-in vs features the vendor keeps commercial.
