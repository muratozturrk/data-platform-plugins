# coding-standards – examples

Drop-in tooling configs. Adjust line length, dialect and paths; keep pre-commit and CI running the same tools.

- `pyproject.toml` – ruff (format + lint + pep8-naming), mypy, pytest, coverage
- `.sqlfluff` – dbt templater, Trino dialect, lowercase everything
- `.yamllint` – 2-space, 120 cols
- `.hadolint.yaml`, `.editorconfig`, `.prettierrc`, `eslint.config.js`
- `.pre-commit-config.yaml` – all hooks incl. gitleaks, dbt-checkpoint, commitlint
- `commitlint.config.js` – Conventional Commits with platform scopes
- `ci-standards.yml` – GitHub Actions running `pre-commit run --all-files`, mypy, tests, ratchet
- `ratchet.py` – baseline-based "no new violations" gate for legacy code
- `PULL_REQUEST_TEMPLATE.md`, `CODEOWNERS`
