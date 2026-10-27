---
name: identifier-naming-principles
description: Research-backed principles for naming variables, functions, classes, modules and parameters in any language (Python, SQL, PySpark, TypeScript, Scala). Use when choosing or reviewing a code identifier, resolving a naming dispute, or writing the naming section of a style guide.
---

# Identifier Naming Principles

## When to Use This Skill
- Choosing a name and the obvious one feels wrong or vague
- Reviewing a PR for naming quality
- Explaining *why* a name is bad, with a citation rather than taste

## Core Concepts

### What the research says
- **Agreement is rare, understanding is common.** Feitelson et al. (334 developers): median 6.9% chance two developers pick the same name; but a chosen name is usually understood. Therefore conventions and glossaries are not bureaucracy, they are the only way to get consistency.
- **Bad names correlate with bugs.** Butler et al. found identifiers violating basic guidelines (e.g. capitalisation anomalies, number-only names, excessive length, non-dictionary words) statistically associated with FindBugs-reported defects.
- **Misleading beats vague.** Arnaoudova et al.'s linguistic antipatterns (`getX` that sets, `isX` that returns a number, `checkX` that changes state, plural name holding one item) actively mislead; Avidan & Feitelson showed misleading names slow comprehension more than meaningless ones.
- **Names are read more than written** (Martin, Boswell): optimise for the reader's time-to-understanding, not the writer's typing.

### The three-step naming procedure (Feitelson model; proven to produce better names than "just be more descriptive")
1. **Select the concepts** the name must carry: role (what it is), type/unit, scope/qualifier, action for functions.
2. **Choose the word for each concept** from the glossary; prefer specific over generic (`fetch_customer_orders` over `get_data`).
3. **Construct the name** in the language's casing with a consistent order: `{qualifier}_{noun}` for data, `{verb}_{object}` for functions, `is_/has_/can_` prefixes for booleans.

### Length
Code Complete: average identifier length of 10–16 characters minimises debugging effort; shorter for tight scopes (loop vars), longer for wider scopes and public APIs. Boswell: name length should scale with scope.

## Patterns

| Kind | Rule | Good | Bad |
|---|---|---|---|
| Variable | noun, singular; collections plural | `active_customer`, `pending_orders` | `data`, `list1`, `tmp` |
| Boolean | `is_/has_/can_/should_` + positive form | `is_active`, `has_discount` | `active`, `not_deleted`, `flag` |
| Function | verb + object; one verb one effect | `load_orders`, `compute_total_amount` | `process`, `handle_data`, `do_it` |
| Query vs command | `get/find/fetch/is/has` never mutate; `set/update/write/apply` never return data silently | `find_user()` pure | `get_user()` that creates one |
| Class | noun, role-revealing, no `Manager/Helper/Util/Data/Info` | `OrderRepository`, `RetryPolicy` | `OrderManager`, `DataHelper` |
| Constant | UPPER_SNAKE with unit | `MAX_RETRIES`, `TIMEOUT_MS` | `MAX`, `T` |
| Units/dir | encode unit or direction | `delay_ms`, `size_bytes`, `upstream_lag_seconds` | `delay`, `size` |
| Parameters | same name as the caller's variable for the same concept | `tenant_id` everywhere | `tid`, `tenant`, `tenantId` mixed |
| Modules/files | lowercase snake, what it contains | `order_ingest.py` | `utils.py`, `misc.py`, `new_version.py` |
| Scope | short names only in ≤ 5-line scopes | `for i in range(n)` | `x` as a module-level variable |

### Words to avoid
`data, info, value, item, object, thing, stuff, temp, tmp, result, output, helper, util, manager, handler, process, do, handle, misc, new, old, final, v2` (in identifiers; `v2` belongs in contract names only).

### Abbreviations
Only from the organisation's abbreviation list (see glossary-and-abbreviations). Never invent; never mix `cust` and `customer`.

## Anti-patterns
- Hungarian notation or type prefixes (`str_name`, `lstOrders`) in typed languages
- Names that differ only by number (`df1, df2`) in PySpark pipelines — name the stage (`orders_raw`, `orders_deduped`)
- Comments that explain a bad name instead of fixing it (Boswell)
- Encoding the author, date or ticket in a name

See `references/sources.md`.
