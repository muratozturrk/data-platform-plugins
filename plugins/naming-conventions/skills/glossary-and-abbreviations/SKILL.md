---
name: glossary-and-abbreviations
description: Build and maintain the controlled vocabulary behind all names: canonical terms per concept, banned synonyms, the approved abbreviation list, pluralisation and casing rules, and how to resolve a naming dispute. Use when two names compete for one concept, when a new domain term appears, or when an abbreviation is requested.
---

# Glossary and Abbreviations

Feitelson's finding — developers rarely pick the same name independently — means vocabulary must be decided explicitly. A naming scheme without a glossary produces consistent *shapes* with inconsistent *words*.

## When to Use This Skill
- A new business concept enters the platform
- Two teams use different words for the same thing (`customer`/`client`/`account`)
- Someone wants to abbreviate

## Core Concepts

### Glossary entry
```yaml
customer:
  definition: A legal entity that buys from the tenant; one row per contract signer
  canonical: customer
  plural: customers
  banned_synonyms: [client, account, buyer, cust]
  id_column: customer_id
  owner: sales-domain
  related: [order, invoice]
```
Terms are owned by the domain (data mesh) and stored with the data catalog glossary (OpenMetadata glossary terms) so names and documentation share one source.

### Abbreviation policy
- Default: **no abbreviations**. Full words outperform abbreviations and single letters for comprehension (Lawrie et al.).
- Allowed only from the list, and only where a hard length limit forces it (Azure storage accounts, 63-char labels). The list is small and global:
  `id, url, uri, api, db, cfg (config files only), env, prod/stg/dev (environments only), weu/neu (regions), rg/aks/kv/st (Azure CAF), pk/fk/uq/ix (constraints), dlq, cdc, utc, ms, iso`.
- Industry acronyms allowed when universal in the domain (`iban`, `vat`, `sku`); listed with expansion.
- Never create new abbreviations in a PR; propose them to the glossary first.

### Casing by context (never mix inside one system)
| Context | Casing |
|---|---|
| Python, SQL, dbt, JSON, columns, Airflow ids | `snake_case` |
| Kubernetes, Helm, URLs, CLI flags, manifest names, topics (with `.` hierarchy) | `kebab-case` |
| Python classes, TypeScript types, proto messages | `PascalCase` |
| Constants, enums, env vars | `UPPER_SNAKE` |
| TypeScript variables/functions, protobuf JSON mapping | `camelCase` |

### Singular/plural rule
Entity names singular in compound names (`customer_id`, `order.created`); collections plural where the ecosystem expects it (REST collections, dbt tables). Never both forms for the same entity in the same system.

### Dispute resolution
1. Is it the same concept? (If definitions differ, it is two terms.)
2. Which word does the business use in contracts/UI? Prefer it.
3. Which is already most widespread in published assets? Weigh migration cost.
4. Decide, record in the glossary with the banned synonyms, and add a fitness function that greps for the banned words in new code and DDL.

## Anti-patterns
- Glossary in a wiki nobody reads; keep it machine-readable (`glossary.yaml`) and in the catalog
- Abbreviating to fit a limit that could be solved by dropping a redundant component
- "Legacy name kept for compatibility" without a dated deprecation
