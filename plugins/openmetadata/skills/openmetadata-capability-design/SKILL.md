---
name: openmetadata-capability-design
description: "Designing new governance capabilities on OpenMetadata 1.13.3 under an on-prem, open-weight-LLM-only constraint: turning measured gaps into LLM, agentic and ontology candidates; the researched pool C01-C19 (Ranger policy derivation, KVKK lifecycle engine, transfer ontology, ontology-backed NLQ, LLM classification to a VERBİS inventory, deadline/escalation, breach-scope agent, Kafka event backbone, governance-as-code, anonymisation evidence and more) with recorded novelty verdicts; the novelty-check method; the weighted scoring method (Impact 25, KVKK 18, Novelty 17, Platform fit 17, OpenMetadata fit 13, Effort 10 inverse); and a design template wired to real extension points with a mandatory KVKK impact assessment. Use when proposing an OpenMetadata feature or plug-in, checking whether an idea already exists, shortlisting candidates, sizing an on-prem LLM, or writing a design doc that must pass a privacy review."
---

# OpenMetadata Capability Design

This skill goes one step past gap-closing: from gaps that no product closes well to **new**
capabilities built on OpenMetadata, in three themes - **LLM**, **agentic** (scheduled, stateful,
acting workflows) and **ontology/semantic**. Every fact about the product is pinned to
**OpenMetadata 1.13.3** (`1.13.3-release`, commit `255f6694913b`); later `main`/2.0 changes are
flagged as such. The hard constraint throughout: **on-prem, open-weight models only** (vLLM/Ollama
serving Qwen/Llama/Mistral-class models), no external API calls, no new heavy infrastructure
without its cost written down. The research produced 19 candidates and checked each for novelty;
the weighted scoring (T8) and the deep designs were specified but **not executed** - this skill
gives the method and templates, not a ranking.

Evidence types: `K` = verified in source code, `D` = official docs, `İ` = issue/PR/RFC/roadmap,
`B` = vendor claim. Short paths: `spec/` = `openmetadata-spec/src/main/resources/json/schema/`,
`svc/` = `openmetadata-service/src/main/java/org/openmetadata/service/`. Not legal advice.

## When to Use This Skill

- Someone proposes "let's add X to OpenMetadata" and you need to know if X already exists (in OSS,
  in the commercial edition, in a competitor, in the literature).
- Picking which LLM/agentic/ontology idea to build first and defending the choice.
- Mapping an idea onto OpenMetadata's real extension points instead of forking.
- Sizing an on-prem model (parameters, context, GPUs, Turkish quality) and its fallback.
- Writing a design document that includes a KVKK (Law No. 6698 on the Protection of Personal
  Data) impact assessment: what goes to the model, where it runs, what is logged.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| See the 19 researched candidates, gaps, extension points, risks | Candidate pool C01-C19 | [candidate-pool.md](references/candidate-pool.md) |
| Decide whether an idea is new | 4-source search, verdict `new / partial / planned / exists` | [novelty-method.md](references/novelty-method.md) |
| Understand why an extension point is empty | "Reserved for commercial edition" pattern | [novelty-method.md](references/novelty-method.md) |
| Rank candidates | T8 weights, 1-5 ratings with written rationale | [design-template.md](references/design-template.md) |
| Write the design | 9-section template + KVKK impact assessment | [design-template.md](references/design-template.md) |
| Size an on-prem LLM | Per-candidate model/context/Turkish assumptions + fallback | [candidate-pool.md](references/candidate-pool.md), [design-template.md](references/design-template.md) |
| Where ideas come from | Class 1 gaps (no product reaches level 2) | [novelty-method.md](references/novelty-method.md) |

## Workflow: Gap → Candidate → Verdict → Score → Design

1. **Start from gaps, not features.** Every candidate names the gap IDs it closes (rubric IDs
   such as `D14.3`, or platform-specific gaps). The richest space is *Class 1*: capabilities where
   none of OpenMetadata, DataHub v1.7.0, Atlas 2.5.0 reaches rubric level 2. After issue/roadmap
   verification only four are "absent and unplanned everywhere": anonymisation (`D6.8`), golden
   record (`D8.4`), MDM integration (`D8.3`), multilingual user content (`D12.5`). Gap catalog and
   rubric: `openmetadata-gap-closing`, `openmetadata-governance-assessment`.
2. **Bind to a theme and a stack component.** At least one of LLM / agentic / ontology, plus
   which platform component it talks to (Ranger, Kafka, Airflow, Iceberg, Trino, Vault ...).
3. **Find the extension point in the source before writing a word of design.** Name a file and
   line in the 1.13.3 tree or do not name it. The common anchors are listed below.
4. **Run the novelty check** (≥3 sources; the research used four). Record what you searched even
   when nothing is found. `exists` → drop (keep it under "evaluated and eliminated"); otherwise
   sharpen the claim to the differentiator that survived.
5. **Score** the survivors with T8 (each rating needs a one-line rationale).
6. **Design** the top 3-5 with the template; every class, schema file and API path in the design
   must exist in the source tree.

## OpenMetadata 1.13.3 Extension Points Worth Knowing (verified, `K`)

| Hook | What it gives you | Evidence |
|---|---|---|
| NLQ provider | Interface + reflection-loaded provider class; default returns `null` | `svc/search/nlq/NLQService.java:12`, `NLQServiceFactory.java:15-29`, `NoOpNLQService.java:23`, `conf/openmetadata.yaml:494-499` |
| Local embeddings | DJL runs `all-MiniLM-L6-v2` in-process; default provider is `bedrock` - set `EMBEDDING_PROVIDER=djl` | `svc/search/vector/client/DjlEmbeddingClient.java:37-46`, `conf/openmetadata.yaml:497,523-524` |
| AI record layer | Schemas + REST for `agentExecution`, `promptTemplate`, `llmModel`, `aiApplication`; LLM service with `ollamaConnection` / `customLLMConnection` - **no generation engine** | `spec/entity/ai/`, `svc/resources/ai/`, `spec/entity/services/connections/llm/` |
| Workflow nodes | Flowable-based governance workflows; 17 node subtypes; add new ones in the factory switch | `svc/governance/workflows/elements/NodeFactory.java:44`, `spec/governance/workflows/elements/nodeSubType.json` |
| Scheduled native app | Quartz-triggered app pattern (copy the pattern, not the scope - it cleans OM's own data) | `svc/apps/bundles/dataRetention/DataRetention.java:39,78` |
| Lifecycle hooks | Default-empty handler interface + `registerHandler` | `svc/events/lifecycle/EntityLifecycleEventHandler.java:29`, `EntityLifecycleEventDispatcher.java:73` |
| Data-quality validators | Dynamic import by module path - drop a file, no registration | `ingestion/src/metadata/utils/importer.py:247,266-270` |
| PII processors | Factory with two branches; add an LLM branch here | `ingestion/src/metadata/pii/processor_factory.py:14`, `base_processor.py:40` |
| RDF / reasoning | Jena reasoner RDFS/OWL_LITE/OWL_DL/CUSTOM, SHACL shapes; **off by default**, needs Fuseki/QLever | `svc/rdf/reasoning/InferenceEngine.java`, `rdf/shapes/openmetadata-shapes.ttl`, `conf/openmetadata.yaml:716-717` (`RDF_ENABLED=false`) |
| Doc store | `Document` with free-form `data` object - new document kinds without schema change | `spec/entity/docStore/document.json:35,39,75` |

Hard limits: no new entity types through the API (custom properties use existing types only,
`svc/resources/types/TypeResource.java:78-85`); custom properties have no "value required" flag
and `additionalProperties:false` blocks adding one without a schema change
(`spec/type/customProperty.json:43,69-70`); event-subscription destinations are a closed enum
without Kafka (`spec/events/eventSubscription.json:98-110`).

## The Cross-Cutting Finding: Empty Extension Points Are Not Forgotten

The research found the same pattern four times: an extension point defined in OSS but empty is
usually **already implemented in the commercial edition (Collate)**. Ranger
(`spec/metadataIngestion/securityServiceMetadataPipeline.json` is a one-field stub; connector is
"Collate Only", `D`), the AI record layer (engine ships under the Collate Community License,
`D`/`İ`), `NoOpNLQService` (working counterpart is commercial), `slaValidation.json` (result
schema with no evaluator; zero Java/Python references, `K`). Treat every empty hook as both a
**risk** (you compete with the vendor) and an **opportunity** (no on-prem, Apache-2.0,
external-API-free equivalent). Check the license boundary before choosing where code lives -
see `openmetadata-architecture`.

## Novelty Verdicts at a Glance (as recorded)

19 candidates: **new 2** (C13 anonymisation evidence layer, C14 publication lifecycle) ·
**partial 16** · **planned 0** · **exists 1 → eliminated** (C17 time-bound access requests, shipped
in the commercial 2.0 line). 18 remain for scoring. Full table: [candidate-pool.md](references/candidate-pool.md).

## Scoring (T8) in One Line

`score = 0.25·Impact + 0.18·KVKK + 0.17·Novelty + 0.17·PlatformFit + 0.13·OMFit + 0.10·Effort`,
each rated 1-5 (Effort inverse: 5 = least effort), result 1.0-5.0. Novelty maps from the verdict:
new = 5, partial = 3, planned = 2, exists = eliminated. KVKK rates three things: does it ease a
concrete obligation, does it run on-prem, does it create new personal-data risk (e.g. raw data
to an LLM). No rating without a written rationale.

## Design Rules That Kept Candidates Realistic

- **LLM proposes, human approves.** No model output is written to the catalog directly; route it
  through an approval workflow (the recognizer-feedback approval nodes are the ready pattern).
- **Facts never come from the model.** For generated text (breach notice, model card), the model
  only phrases structured facts computed elsewhere.
- **Metadata only to the model.** No sample rows, no column values - enforce it in code, not policy.
- **Every LLM feature has a no-LLM fallback** that keeps the core value (C05, C06, C07, C10, C16,
  C19 all state theirs).
- **One-way sync + drift read-back**, never bidirectional (C01, C12).
- **Catalog declares and proves; the platform executes** (C02: catalog is not a deletion engine).
- **7-8B models degrade badly on Turkish domain terms** (research assumption); plan 14B-32B
  class and say so.

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| "No product has X" after one search | Weak claim; often it is commercial or on `main` | ≥3 sources, record misses, label `D`/`İ` |
| Designing on an empty hook without asking why it is empty | You rebuild the vendor's paid feature or hit a license wall | Check Collate docs / license boundary first |
| Proposing a new entity type | Not possible via API in 1.13.3 | Custom properties, glossary terms, doc store, or a core-schema change stated as such |
| Sending sample data to the LLM | New personal-data processing, KVKK risk | Metadata only; sampling stays in Presidio/spaCy |
| Auto-applying LLM classifications | Classifications drive access policy once synced to Ranger | Approval workflow, confidence threshold |
| Retention/deadline rules inside Airflow DAGs | Stewards cannot see them; "which assets have no retention?" becomes unanswerable | Rule in catalog, execution in platform |
| Fabricating a top-5 | Scoring was never run | Fill the worksheet, publish rationale |
| Citing a class or path not in the 1.13.3 tree | Design fails review | grep the tag; drop unverified names |

## References

- [references/candidate-pool.md](references/candidate-pool.md): C01-C19 - theme, gaps, extension point, verdict and differentiator, LLM sizing, KVKK relevance, key risk.
- [references/novelty-method.md](references/novelty-method.md): four-source search, evidence rules, verdict categories, the empty-extension-point finding, Class 1 opportunity space.
- [references/design-template.md](references/design-template.md): T8 worksheet and formula, 9-section technical design template, KVKK impact assessment checklist, path verification.
- [references/sources.md](references/sources.md)
- Related skills: `openmetadata-gap-closing` (the gap catalog these candidates start from), `openmetadata-kvkk-compliance` (obligation matrix behind the KVKK criterion), `openmetadata-architecture` (entity model, extension points, licensing), `openmetadata-platform-integration` (Ranger, Kafka, Iceberg connectors), `openmetadata-governance-assessment` (rubric and DataHub/Atlas comparison).
