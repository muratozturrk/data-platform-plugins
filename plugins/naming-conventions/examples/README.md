# naming-conventions – examples

- `naming-scheme.yaml` – machine-readable scheme: one entry per asset type with regex, components, limits, examples
- `glossary.yaml` – controlled vocabulary with banned synonyms and the global abbreviation list
- `check_names.py` – fitness function: validates names from a JSON inventory against the scheme (CI exit code)
- `sqlfluff-naming.cfg` – sqlfluff settings enforcing lowercase snake_case identifiers in SQL/dbt
- `ruff-naming.toml` – ruff `N` (pep8-naming) rules for Python
- `kafka-topic-policy.rego` – OPA policy rejecting topic names outside the tenant pattern
