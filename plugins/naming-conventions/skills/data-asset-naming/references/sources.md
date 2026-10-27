# Sources

- dbt Labs. "How we style our dbt models / projects" (dbt style guide): `stg_`/`int_`/`fct_`/`dim_` prefixes, plural objects, `__` between source and entity, intermediate past-tense verbs. https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects and https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md
- dbt Labs. "How we structure our dbt projects" (staging → intermediate → marts). https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview
- GitLab Data Team. SQL Style Guide (naming of CTEs, columns, booleans, timestamps). https://handbook.gitlab.com/handbook/enterprise-data/platform/sql-style-guide/
- Simon Holywell. SQL Style Guide. https://www.sqlstyle.guide
- Ralph Kimball, Margy Ross. *The Data Warehouse Toolkit*, 3rd ed. Wiley, 2013 (fact/dimension naming, conformed dimensions, surrogate keys).
- Databricks. "What is the medallion lakehouse architecture?" (bronze/silver/gold). https://docs.databricks.com/lakehouse/medallion.html
- Apache Iceberg spec: identifiers, namespaces, partition transforms. https://iceberg.apache.org/spec/
- PostgreSQL identifier limits (63 bytes) and reserved words. https://www.postgresql.org/docs/current/sql-syntax-lexical.html
- sqlfluff rules `CP01–CP05` (capitalisation) and `RF06` (quoted identifiers), `AL*` aliasing. https://docs.sqlfluff.com/en/stable/reference/rules.html
