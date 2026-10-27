---
name: python-standards
description: Python coding standard for platform code, plugins and SDK: PEP 8 + Google style as baseline, ruff/mypy/pytest tooling, typing, structure, imports, dataclasses/pydantic, async, packaging. Use when writing or reviewing Python, configuring ruff/mypy, or drafting the Python standard.
---

# Python Standards

Baseline: PEP 8 and the Google Python Style Guide. Deviations below are deliberate and enforced.

## When to Use This Skill
- Writing or reviewing Python in the control plane, plugins, SDK or pipelines
- Configuring ruff, mypy, pytest, pre-commit

## Core Rules

| # | Rule | Enforced by |
|---|---|---|
| 1 | Format with `ruff format`; line length 100; double quotes; trailing commas | ruff |
| 2 | Lint rule set: `E,F,W,I,N,UP,B,C4,SIM,RET,ARG,PTH,PL,RUF,T20,S` (T20 bans print; S = bandit) | ruff |
| 3 | Type hints on every public function/method and class attribute; `mypy --strict` for `platform.sdk` and `platform.core`, `--check-untyped-defs` elsewhere; no `Any` in public signatures | mypy |
| 4 | Imports: absolute; grouped stdlib / third-party / first-party; no wildcard; no import-time side effects | ruff I, F403 |
| 5 | Functions ≤ 40 lines, ≤ 5 parameters, nesting ≤ 3, complexity ≤ 10; keyword-only after the second positional | ruff PLR, C901 |
| 6 | Data structures: `@dataclass(frozen=True)` for value objects, `pydantic` models at boundaries (API, manifests), `TypedDict` for JSON shapes; never bare dicts across module boundaries | review |
| 7 | No mutable default arguments; no global mutable state; configuration injected, never read from `os.environ` outside the config module | ruff B006, review |
| 8 | Errors: custom exception hierarchy per package (`PlatformError` → `ManifestError` …); never bare `except`; re-raise with `from`; see error-handling skill | ruff BLE, B904 |
| 9 | Logging: `structlog`/`logging` with structured fields; no f-strings in log calls; no `print` | ruff T20, G |
| 10 | Tests: pytest, `tests/` mirrors `src/`, names `test_{unit}_{behaviour}`; fixtures over setup; property tests (hypothesis) for parsers/compilers; coverage gate 85% on core/sdk | pytest-cov |
| 11 | Packaging: `src/` layout, `pyproject.toml` only, pinned lockfile (`uv`/`pip-tools`), entry points for plugins | review |
| 12 | Async only at I/O boundaries; never mix sync blocking calls inside async without `to_thread` | ruff ASYNC |
| 13 | Docstrings Google style on public API; module docstring states purpose | ruff D (selected) |
| 14 | Comments explain why, not what; no commented-out code; TODO must carry a ticket id | ruff ERA, review |
| 15 | Secrets and tenant context never in defaults, logs or exceptions | review + fitness function |

## Patterns
- Pure core: functions that transform data take and return values; I/O lives in adapters/plugins (hexagonal).
- One public way: a module exposes a small `__all__`; private helpers prefixed `_`.
- Guard clauses over nested ifs; early return.
- `pathlib`, `datetime` with tz-aware UTC only, `decimal` for money.

## Anti-patterns
- `utils.py` dumping ground; God modules
- `from x import *`; circular imports solved with inline imports
- `try/except Exception: pass`
- Type hints `Optional` without default `None`; `dict[str, Any]` as an API

See `references/sources.md`.
