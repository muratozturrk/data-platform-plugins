# coding-standards

Coding standards for the data-platform codebase, each rule mapped to the tool that enforces it.

| Kind | Name | Purpose |
|---|---|---|
| Agent | `standards-reviewer` | PR review for what linters cannot catch; turns lint-class findings into "add rule X" |
| Agent | `style-guide-author` | Writes ≤ 2-page example-driven standards and the configs/CI that enforce them |
| Command | `/standards-review` | Review a diff/repo; tooling gaps + findings + CI changes |
| Command | `/lint-setup` | Generate ruff/mypy/sqlfluff/yamllint/hadolint/eslint/pre-commit/commitlint/CI + legacy ratchet |
| Command | `/style-guide-draft` | Draft a standard for one language/tool with deviations table and enforcement map |
| Skill | `python-standards` | PEP 8 + Google baseline, ruff/mypy rule set, structure, typing, packaging |
| Skill | `sql-standards` | dbt + GitLab + Holywell baseline, sqlfluff, CTE structure, dbt tests |
| Skill | `pyspark-standards` | Palantir PySpark guide: expressions, naming, chaining, schemas, UDFs, joins |
| Skill | `yaml-config-standards` | Manifests, K8s/Helm, Airflow/dbt YAML, CI, Dockerfiles; pinning and secrets |
| Skill | `typescript-standards` | Google TS baseline, strict compiler, eslint/prettier, React rules |
| Skill | `error-handling-logging-standards` | Exception hierarchy, retries, structured logs with tenant/run context, OpenTelemetry |
| Skill | `documentation-standards` | Docstrings, README, ADR, runbooks, changelog, Diátaxis |
| Skill | `git-workflow-standards` | Branches, Conventional Commits, PR rules, CODEOWNERS, SemVer, releases |

`examples/`: drop-in configs (pyproject, .sqlfluff, .yamllint, .hadolint.yaml, .editorconfig, .prettierrc, eslint.config.js, .pre-commit-config.yaml, commitlint.config.js, CI workflow, ratchet script, PR template, CODEOWNERS).

## Sources
- Robert C. Martin, *Clean Code* (2008); Steve McConnell, *Code Complete* 2e (2004); Dustin Boswell & Trevor Foucher, *The Art of Readable Code* (2011); John Ousterhout, *A Philosophy of Software Design* 2e (2021); Brett Slatkin, *Effective Python* 2e; Luciano Ramalho, *Fluent Python* 2e; Michael Nygard, *Release It!* 2e; Google SRE Book
- PEP 8, PEP 257, Google Python Style Guide, Google TypeScript Style Guide, Google Engineering Practices (code review)
- dbt Labs "How we style our dbt projects" and "How we structure our dbt projects"; GitLab SQL Style Guide; Simon Holywell SQL Style Guide; Joe Celko, *SQL Programming Style*
- Palantir PySpark Style Guide; Spark tuning docs; chispa
- Ruff, mypy, pytest, sqlfluff, dbt-checkpoint, yamllint, hadolint, actionlint, kubeconform, typescript-eslint, Prettier, gitleaks, commitlint docs
- Conventional Commits 1.0.0; Semantic Versioning 2.0.0; Keep a Changelog; trunkbaseddevelopment.com; Chris Beams, "How to Write a Git Commit Message"
- Diátaxis; Google developer documentation style guide; Nygard, "Documenting Architecture Decisions"; OpenTelemetry semantic conventions; OWASP Logging Cheat Sheet
Related: the `naming-conventions` plugin owns all naming rules; this plugin references its `naming-scheme.yaml` in pre-commit.
