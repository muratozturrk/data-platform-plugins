"""Consumption-layer selection helper for ten reference scenarios.

Inputs (`Needs`) describe who consumes data and which audit requirements apply. Each rule
returns components with the reason and its source: "guideline" when the rule restates
established consumption-layer guidance, "lab assumption" when it is a judgment call added for
this helper. Components are categories, not products; product examples are listed per component.
"""
from dataclasses import dataclass, field

BI_PERSONAS = {"bi_analysts", "executives", "tenants", "operations", "public"}

EXAMPLES = {   # typical products for each component
    "notebooks": "JupyterLab, JupyterHub, Deepnote, Databricks Notebooks",
    "catalog_direct_readers": "PyIceberg, DuckDB, Spark",
    "experiment_tracking": "MLflow",
    "semantic_layer": "dbt Semantic Layer, AtScale, LookML",
    "federation_engine": "Dremio, Trino",
    "jdbc_odbc": "JDBC/ODBC drivers of the federation engine",
    "embedded_bi": "Metabase, Sigma, Superset",
    "enterprise_bi": "Tableau, Power BI, Looker, Excel",
    "live_dashboards": "Sigma, ThoughtSpot",
    "monitoring_alerting": "Grafana",
    "arrow_flight": "Arrow Flight / ADBC",
    "rest_api": "FastAPI over the federation engine",
    "mcp_server": "MCP server over the semantic layer",
    "public_views": "Preapproved views or scheduled extracts (Looker Studio)",
}


@dataclass(frozen=True)
class Needs:
    personas: frozenset[str]
    many_bi_tools: bool = False
    strict_governance: bool = False
    multi_tenant: bool = False
    embedded: bool = False
    real_time: bool = False
    alerting: bool = False
    public_access: bool = False
    data_residency: bool = False
    reproducibility: bool = False
    bulk_programmatic_access: bool = False
    minimal_ops: bool = False


@dataclass
class Recommendation:
    components: dict[str, list[str]] = field(default_factory=dict)   # component -> reasons

    def add(self, component: str, reason: str, source: str) -> None:
        self.components.setdefault(component, []).append(f"{reason} [{source}]")

    def __contains__(self, component: str) -> bool:
        return component in self.components


def recommend(n: Needs) -> Recommendation:
    r = Recommendation()
    bi = n.personas & BI_PERSONAS
    programmers = n.personas & {"data_science", "researchers", "external_partners"}

    if programmers:
        r.add("notebooks", "programmatic exploration and modeling", "guideline")
        r.add("catalog_direct_readers", "Arrow-backed reads without a server in between", "guideline")
    if n.reproducibility:
        r.add("snapshot_pinning", "training and research must bind to a snapshot", "guideline")
        if programmers:
            r.add("experiment_tracking", "record snapshot and outputs per run", "guideline")
    if bi and not (n.minimal_ops and not n.many_bi_tools):
        r.add("semantic_layer", "one definition per KPI for every dashboard", "guideline")
        r.add("federation_engine", "single governed SQL entry point for all tools", "guideline")
        r.add("jdbc_odbc", "the interface every BI tool supports", "guideline")
    if n.many_bi_tools:
        r.add("enterprise_bi", "keep each team's preferred tool", "guideline")
    if n.embedded:
        r.add("embedded_bi", "iframe/API embedding with white-label controls", "guideline")
        r.add("query_caching", "embedded dashboards need fast, repeated answers", "guideline")
    if n.strict_governance or n.multi_tenant:
        r.add("row_level_security", "users and tenants see only their rows", "guideline")
        r.add("audit_logging", "who accessed what, when and how", "guideline")
    if n.strict_governance:
        r.add("column_masking", "sensitive columns masked at the engine", "guideline")
    if n.multi_tenant or "external_partners" in n.personas or n.public_access:
        r.add("audit_logging", "external access must be traceable", "lab assumption")
    if n.real_time and "operations" in n.personas:
        r.add("live_dashboards", "operational monitoring on fresh data", "guideline")
        r.add("query_caching", "low-latency repeated dashboard queries", "guideline")
    if n.alerting:
        r.add("monitoring_alerting", "threshold alerts on sensor metrics", "guideline")
    if n.bulk_programmatic_access:
        r.add("arrow_flight", "columnar transfer of large results", "guideline")
    if n.public_access:
        r.add("public_views", "public queries only through preapproved views or extracts", "guideline")
    if n.public_access or (n.multi_tenant and n.bulk_programmatic_access):
        r.add("rest_api", "programmatic access for developers and clients", "guideline")
    if n.data_residency:
        r.add("region_bound_deployment", "tools and compute deployed per jurisdiction", "guideline")
    if "ai_agents" in n.personas:
        r.add("mcp_server", "agents call governed tools instead of writing SQL", "guideline")
    return r


# --- the ten scenarios: inputs restate each scenario's key requirements ----------------------

def _n(personas: str, **flags) -> Needs:
    return Needs(frozenset(personas.split()), **flags)


SCENARIOS: dict[str, Needs] = {
    "startup data science": _n("data_science", reproducibility=True, minimal_ops=True),
    "bank strict governance": _n("bi_analysts executives", many_bi_tools=True, strict_governance=True),
    "e-commerce embedded": _n("tenants", embedded=True, multi_tenant=True),
    "media self-service": _n("bi_analysts", many_bi_tools=True, strict_governance=True),
    "government transparency": _n("bi_analysts public researchers", public_access=True,
                                        strict_governance=True, reproducibility=True),
    "healthcare residency": _n("bi_analysts data_science", strict_governance=True, data_residency=True),
    "logistics real-time": _n("operations bi_analysts data_science", real_time=True,
                                    bulk_programmatic_access=True),
    "SaaS client access": _n("tenants", embedded=True, multi_tenant=True, bulk_programmatic_access=True),
    "nonprofit research": _n("researchers external_partners bi_analysts", reproducibility=True,
                                   bulk_programmatic_access=True),
    "manufacturing maintenance": _n("operations bi_analysts data_science", real_time=True, alerting=True),
}

# The reference tool choice of each scenario, mapped to component categories.
REFERENCE_PICKS: dict[str, set[str]] = {
    "startup data science": {"notebooks", "catalog_direct_readers", "snapshot_pinning", "experiment_tracking"},
    "bank strict governance": {"semantic_layer", "jdbc_odbc", "federation_engine", "enterprise_bi",
                                     "row_level_security", "column_masking", "audit_logging"},
    "e-commerce embedded": {"embedded_bi", "row_level_security", "federation_engine", "jdbc_odbc",
                                  "query_caching", "semantic_layer"},
    "media self-service": {"enterprise_bi", "semantic_layer", "federation_engine", "jdbc_odbc",
                                 "row_level_security", "audit_logging"},
    "government transparency": {"federation_engine", "public_views", "rest_api", "audit_logging",
                                      "snapshot_pinning"},
    "healthcare residency": {"region_bound_deployment", "notebooks", "federation_engine", "semantic_layer",
                                   "column_masking", "audit_logging"},
    "logistics real-time": {"live_dashboards", "semantic_layer", "arrow_flight", "catalog_direct_readers",
                                  "notebooks"},
    "SaaS client access": {"embedded_bi", "rest_api", "arrow_flight", "federation_engine",
                                 "row_level_security", "audit_logging"},
    "nonprofit research": {"notebooks", "arrow_flight", "catalog_direct_readers", "snapshot_pinning",
                                 "experiment_tracking", "audit_logging"},
    "manufacturing maintenance": {"monitoring_alerting", "notebooks", "federation_engine"},
}
