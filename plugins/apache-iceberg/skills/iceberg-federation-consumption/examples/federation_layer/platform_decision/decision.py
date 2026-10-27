"""Federation platform decision tool (deployment models + decision scenarios).

Capabilities below are only the documented core differentiators; anything else is left out on purpose.
The tool applies hard constraints first (an option that cannot work is excluded, with a reason),
then scores the remaining options against weighted requirements and explains the result.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# --- Capability matrix ---------------------------------------------------------------------------

DREMIO_CORE = {"semantic_layer", "ui", "reflections", "iceberg_native", "arrow_flight", "mcp"}
DREMIO_ENTERPRISE = DREMIO_CORE | {"fgac", "integrated_iceberg_catalog", "autonomous_maintenance",
                                   "autonomous_reflections", "elastic_scaling"}
TRINO_CORE = {"wide_connectors", "format_neutral"}


@dataclass(frozen=True)
class Option:
    name: str
    engine: str                      # "dremio" | "trino"
    managed: bool
    on_prem: bool                    # can run in your own data center
    clouds: frozenset[str] | None    # None: any cloud (no restriction)
    features: frozenset[str]
    formats: frozenset[str]
    requires_kubernetes: bool = False
    per_query_pricing: bool = False
    platform: str | None = None      # existing platform it is bundled with
    open_source: bool = False        # free core without a commercial license


ICEBERG_ONLY = frozenset({"iceberg", "parquet"})            # Dremio does not support Hudi
TRINO_FORMATS = frozenset({"iceberg", "delta", "hudi", "hive", "parquet"})   # format-neutral engine

OPTIONS: list[Option] = [
    Option("Dremio Community Edition", "dremio", False, True, None, frozenset(DREMIO_CORE), ICEBERG_ONLY,
           open_source=True),
    Option("Dremio Enterprise Edition", "dremio", False, True, None, frozenset(DREMIO_ENTERPRISE), ICEBERG_ONLY,
           requires_kubernetes=True),
    Option("Dremio Cloud", "dremio", True, False, frozenset({"aws", "azure"}),
           frozenset(DREMIO_ENTERPRISE | {"ai_agent"}), ICEBERG_ONLY),
    Option("Trino (open source, self-managed)", "trino", False, True, None, frozenset(TRINO_CORE), TRINO_FORMATS,
           open_source=True),
    Option("Amazon Athena", "trino", True, False, frozenset({"aws"}), frozenset(TRINO_CORE | {"serverless"}),
           TRINO_FORMATS, per_query_pricing=True),
    Option("Amazon EMR (Trino)", "trino", True, False, frozenset({"aws"}), frozenset(TRINO_CORE), TRINO_FORMATS),
    Option("Cloudera (managed Trino)", "trino", True, True, None, frozenset(TRINO_CORE | {"vendor_services"}),
           TRINO_FORMATS, platform="cloudera"),
    Option("Starburst Enterprise", "trino", False, True, None,
           frozenset(TRINO_CORE | {"governance", "observability"}), TRINO_FORMATS),
    Option("Starburst Galaxy", "trino", True, False, None,
           frozenset(TRINO_CORE | {"governance", "observability", "autoscaling"}), TRINO_FORMATS),
]


# --- Requirements ---------------------------------------------------------------------------------

@dataclass(frozen=True)
class Requirements:
    table_formats: frozenset[str] = frozenset({"iceberg"})
    iceberg_centric: bool = False            # Iceberg is the strategic core
    source_systems: int = 2                  # distinct system types to federate
    business_self_service: bool = False      # UI + governed semantic layer
    needs_fgac: bool = False
    location: frozenset[str] = frozenset({"aws"})   # "on_prem", "aws", "azure", "gcp"
    kubernetes_available: bool = True
    existing_platform: str | None = None     # e.g. "cloudera"
    occasional_queries: bool = False         # ad hoc, low frequency
    ops_capacity: str = "medium"             # "low" | "medium" | "high"
    managed_maintenance: bool = False        # wants table optimization handled by the platform
    prefer_open_source: bool = False         # start without a vendor license


@dataclass
class Scored:
    option: Option
    score: float
    reasons: list[str] = field(default_factory=list)


@dataclass
class Recommendation:
    best: Scored
    ranking: list[Scored]
    excluded: dict[str, str]

    @property
    def engine(self) -> str:
        return self.best.option.engine


def _exclusion(opt: Option, req: Requirements) -> str | None:
    missing_formats = req.table_formats - opt.formats
    if missing_formats:
        return f"does not support {', '.join(sorted(missing_formats))}"
    needs_on_prem = "on_prem" in req.location
    clouds = req.location - {"on_prem"}
    if needs_on_prem and not opt.on_prem:
        return "cannot run on premises"
    if clouds and opt.clouds is not None and not clouds <= opt.clouds and not (needs_on_prem and opt.on_prem):
        return f"not offered on {', '.join(sorted(clouds - opt.clouds))}"
    if not clouds and not opt.on_prem:
        return "cloud-only service"
    if opt.requires_kubernetes and not req.kubernetes_available:
        return "requires a Kubernetes cluster"
    if opt.platform and opt.platform != req.existing_platform:
        return f"only relevant on an existing {opt.platform} platform"
    if req.needs_fgac and opt.engine == "dremio" and "fgac" not in opt.features:
        return "no fine-grained access control in this edition"
    return None


def _score(opt: Option, req: Requirements) -> Scored:
    s = Scored(opt, 0.0)

    def add(points: float, why: str):
        s.score += points
        s.reasons.append(f"{points:+.0f} {why}")

    f = opt.features
    if req.iceberg_centric:
        if "iceberg_native" in f:
            add(3, "Iceberg-native engine")
        if "integrated_iceberg_catalog" in f:
            add(2, "integrated Iceberg catalog")
    if req.managed_maintenance and "autonomous_maintenance" in f:
        add(2, "autonomous table maintenance")
    if req.source_systems >= 5 and "wide_connectors" in f:
        add(3, f"broad connector ecosystem for {req.source_systems} source systems")
    if req.business_self_service:
        if "semantic_layer" in f:
            add(3, "built-in semantic layer")
        if "ui" in f:
            add(2, "UI for business users")
    if len(req.table_formats - {"iceberg", "parquet"}) and "format_neutral" in f:
        add(1, "format-neutral")
    if req.occasional_queries and opt.per_query_pricing:
        add(3, "pay per query for occasional use")
    if req.ops_capacity == "low":
        add(2 if opt.managed else -2, "managed service" if opt.managed else "self-managed needs ops skills")
    if req.ops_capacity == "high" and not opt.managed and opt.engine == "trino":
        add(1, "team can tune a self-managed cluster")
    if req.prefer_open_source and opt.open_source:
        add(1, "free open source core")
    if req.existing_platform and opt.platform == req.existing_platform:
        add(4, f"upgrade path inside existing {opt.platform} platform")
    if "on_prem" in req.location and len(req.location) > 1 and opt.on_prem:
        add(1, "can bridge on-premises and cloud (hybrid)")
    return s


def recommend(req: Requirements, options: list[Option] = OPTIONS) -> Recommendation:
    excluded, candidates = {}, []
    for opt in options:
        why = _exclusion(opt, req)
        if why:
            excluded[opt.name] = why
        else:
            candidates.append(_score(opt, req))
    if not candidates:
        raise ValueError(f"no option satisfies the hard constraints: {excluded}")
    ranking = sorted(candidates, key=lambda c: (-c.score, c.option.name))
    return Recommendation(ranking[0], ranking, excluded)


# --- Input from the running environment -----------------------------------------------------------

def connector_coverage(required: set[str]) -> dict[str, set[str]]:
    """Which required source types already have a configured Trino catalog."""
    from common.federation import run
    configured = {r[0] for r in run("SELECT DISTINCT connector_name FROM system.metadata.catalogs").rows}
    return {"configured": required & configured, "missing": required - configured}
