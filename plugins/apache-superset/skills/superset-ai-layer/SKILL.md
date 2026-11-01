---
name: superset-ai-layer
description: "Safe AI assistance on top of Superset: governed MCP gateway in front of the Superset MCP service, NL2SQL over an allow-listed semantic layer, sqlglot query validation (SELECT only, allow-listed tables/columns, mandatory LIMIT), schema-and-metadata-only model context, local LLM serving with vLLM versus hosted models, NL2SQL evaluation pipeline with an incorrect-but-confident metric, audit and observability with Langfuse-style tracing, prompt-injection and data-leak boundaries, human approval for writes, and strict separation of development tooling from product runtime. Includes a decision table and anti-pattern list. Use when designing, reviewing or hardening natural-language query, AI chart or dashboard generation, MCP exposure, or any LLM integration with Superset."
---

# Superset AI Layer

MCP service mechanics are in `superset-extensions-mcp`; roles/RLS in `superset-security-model`; data-copy controls in `superset-privacy-compliance`. This skill is the architecture and guardrails around them (research baseline Superset 6.1, commit c83fb2bb1dcf).

## Reference architecture
User question (UI panel or extension, carrying the IdP session token) -> **AI service** (separate process, not a Superset fork) -> **MCP gateway** (rule-based, no AI inside) -> Superset MCP service (JWT per tool) -> query runs **as the end user** against the engine, whose policies apply. Result rows go to the user's screen, **not back to the model**.

| Component | Role | Rule |
|---|---|---|
| AI service (e.g. FastAPI + a typed-output agent library) | Assemble permitted context, call model, validate output | Model output is a typed object (metric, dimensions, filters, time range) or a clarification question, never free SQL |
| MCP gateway | Identity/scope exchange, per-role tool allow-list, row separation, approval, quota | Only network path to the MCP service; run redundantly (single point of failure) |
| Superset MCP service | Structured tools (list datasets, chart preview...) | Tools that can return rows (SQL execution, chart data) disabled by default and behind a separate permission |
| sqlglot validator | Parse generated SQL | SELECT only, allow-listed tables and columns, mandatory LIMIT, no multi-statement; the rule has the final word, not the model |
| Model server | vLLM (OpenAI-compatible API) with an open-weight model | Swapping the model is a config change only |
| Audit + tracing | Append-only audit table/SIEM plus LLM tracing (Langfuse-style) | Questions may contain identifiers: treat logs as personal-data stores with retention |

## Context rules (data-leak boundary)
- Model sees: schema, metric definitions, descriptions, catalog metadata, column names/types, row counts. Never row values or sample rows. Masked columns enter marked "masked"; unauthorised columns not at all.
- Context is built per user from what that user may see (ask the gateway), then enriched from catalog/metric definitions (OpenMetadata, dbt).
- Text sent to a model for insights or anomaly narration uses only aggregated, small-cell-suppressed numbers; same suppression library as the chart path.
- Show generated SQL and a confidence signal to the user before running; confidence can come from sampling several generations and comparing them.

## Prompt injection
Any text from datasets, comments, descriptions, documents or chart titles is untrusted model input. Defences are structural, not prompt wording: no row-returning tool for the model, typed outputs only, allow-list validation after generation, write tools need explicit user approval *and* a second gateway-side rejection of unapproved writes (two independent locks), per-user quotas, scheduled agents emit reports/suggestions or pull requests and never self-apply changes.

## Decision table
| Question | Choose | When |
|---|---|---|
| Model location | Local vLLM + open-weight model | Data residency, no public cloud allowed, sensitive schemas |
| | Hosted API | Only with metadata-only context, a contract/DPA and legal sign-off; still gated by the gateway |
| NL2SQL form | Semantic query (metric/dimension/filter) compiled by rules | Default; governed and testable |
| | Free SQL from the model | Only in SQL Lab for analysts, validated and shown first |
| Enforcement point | Engine policy + impersonation | Always the authority; the AI layer adds, never replaces |
| Writes | Preview then approve | Dashboard/chart creation; never unattended |
| Batch agents | Rule first, model second | Rules detect; the model only phrases or classifies |
| Observability | Separate dev and prod tracing stacks | Always |

## Evaluation pipeline
NL2SQL is not a solved problem; published benchmark numbers do not transfer to your data and language. Build a question set from your own domain and language with expected semantic queries; run every candidate model; report accuracy **and the incorrect-but-confident rate**; gate model swaps on thresholds; rerun on schema or prompt change. Do this before shipping other LLM features.

## Dev tooling vs product runtime
Two separate stacks. Coding agents, dev-time tracing, prompt experiments and memory files live on the dev side with no real data or credentials. Only code, schemas and tests cross via Git; real data, query results, real schema/host names and credentials never cross. The product runtime has its own tracing instance, audit table and secrets. Never point a developer agent or MCP client at a production Superset.

## Anti-patterns
- MCP service exposed directly to an LLM client or the internet.
- Returning query rows to the model "to explain them".
- Service-account queries that bypass the end user's engine policy.
- Trusting a prompt instruction ("only SELECT") instead of parsing.
- Hosted-model calls with sample rows in the prompt.
- One tracing instance shared by dev experiments and production.
- Auto-applying agent suggestions; skipping the evaluation set; no per-user quota.

Sources: `references/sources.md`.
