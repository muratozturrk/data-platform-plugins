---
name: documentation-standards
description: Documentation standard for code and the platform: docstrings, READMEs, ADRs, runbooks, API reference generation, changelogs, comments policy, Diátaxis structure for product docs. Use when writing or reviewing documentation or deciding what must be documented.
---

# Documentation Standards

## What must exist
| Artifact | Required for | Template |
|---|---|---|
| Module/function docstring (Google style) | every public symbol | Args / Returns / Raises / Example |
| README | every repo and plugin | purpose, quickstart (≤ 10 min), configuration, development, links |
| ADR (`docs/adr/NNNN-title.md`) | every non-obvious architectural or standards decision | Context / Decision / Consequences / Status |
| Runbook | every alert | symptom, impact, diagnosis steps, remediation, escalation, links to dashboards |
| CHANGELOG (Keep a Changelog) | every released package/plugin/API | Added / Changed / Deprecated / Removed / Fixed / Security |
| API reference | every public API/SDK/CLI | generated from OpenAPI/docstrings, never hand-written |
| Manifest schema docs | every manifest kind | generated from JSON Schema with examples |

## Structure (Diátaxis)
Product docs split into **tutorials** (learning), **how-to guides** (tasks), **reference** (facts, generated), **explanation** (why). Do not mix; a how-to never explains theory, reference never tutors.

## Rules
1. Docs live next to code and are reviewed in the same PR; CI builds them; broken links fail.
2. Quickstarts are executed in CI (stranger-installability fitness function).
3. Comments explain *why* and non-obvious constraints; no narration of the code; no commented-out code.
4. Every deprecation has: version introduced, replacement, removal version, migration note.
5. Diagrams as code (Mermaid/C4 in markdown) so they diff.
6. Audience first line: "This page is for plugin authors/operators/analysts."
7. Examples are real, tested snippets (doctest/pytest-examples), not pseudo-code.
8. Language: English in code and docs; glossary terms used verbatim.

## Anti-patterns
- README that is a feature list without a runnable quickstart
- Docstrings restating the signature
- Wiki pages that drift from the repo
- ADRs written after the fact to justify, or never superseded

Sources: Google developer documentation style guide; Diátaxis (https://diataxis.fr); Michael Nygard, "Documenting Architecture Decisions"; Keep a Changelog (https://keepachangelog.com); Write the Docs guides; PEP 257; Google Python Style Guide §3.8 Comments and Docstrings.
