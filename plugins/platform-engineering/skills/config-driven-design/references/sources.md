# Sources

- Config-driven ETL frameworks as prior art for manifest shape, registries and layered config: Samara (config-driven ETL, engine abstracted away through configuration), dpone (YAML manifests with registry, owner, DAG inspection, validate/render CLI), Drune (declarative pipelines with inline data-quality rules), factl (declarative, Git-backed orchestration; environment differences in config files, not code). All on PyPI.
- Kubernetes resource model (`apiVersion`/`kind`/`metadata`/`spec`) as the de-facto standard for versioned declarative manifests.
- dbt: `schema.yml` tests and contracts as declarative quality and contract definitions.
- Zhamak Dehghani, *Data Mesh*: data product manifest components and the developer-experience plane.
