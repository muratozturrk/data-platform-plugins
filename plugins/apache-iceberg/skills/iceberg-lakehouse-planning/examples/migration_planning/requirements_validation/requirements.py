"""Requirements as data: outcome-focused, testable, grouped by component, prioritized,
and traceable to the audit. `validate()` enforces those rules so a requirements set can be gated in CI."""
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

COMPONENTS = ("storage", "ingestion", "catalog", "federation", "consumption")
PRIORITIES = ("critical", "nice-to-have", "future-facing")
STAKEHOLDERS = ("data_engineering", "analytics", "compliance", "business", "it_infrastructure")
OPS = {"<=": lambda v, t: v <= t, ">=": lambda v, t: v >= t, "==": lambda v, t: v == t}
# Common products. A requirement that names one prescribes a solution (rule:
# "don't lock in a technology or vendor"). Reported as a warning: it can be a deliberate constraint.
PRODUCTS = re.compile(r"\b(Apache Spark|Spark|Flink|Airflow|Kafka|Dremio|Trino|Presto|Snowflake|BigQuery|Polaris|"
                      r"Nessie|Glue|Tableau|Power BI|MinIO|NetApp|HDFS|Informatica|NiFi|Active Directory|Hive)\b")
DEFAULT_FILE = Path(__file__).with_name("requirements.yaml")


@dataclass(frozen=True)
class Requirement:
    id: str
    component: str
    statement: str
    priority: str
    source: str
    stakeholders: tuple[str, ...] = ()
    traces: tuple[str, ...] = ()
    acceptance: dict = field(default_factory=dict)

    @property
    def probe(self) -> str | None:
        return self.acceptance.get("probe")


@dataclass(frozen=True)
class Issue:
    level: str          # error | warning
    requirement: str    # requirement id, or "*" for set-level issues
    message: str


def load(path: Path = DEFAULT_FILE) -> list[Requirement]:
    raw = yaml.safe_load(Path(path).read_text())["requirements"]
    return [Requirement(**{**r, "stakeholders": tuple(r.get("stakeholders", ())),
                           "traces": tuple(r.get("traces", ()))}) for r in raw]


def validate(reqs: list[Requirement], audit=None) -> list[Issue]:
    """Check a requirements set; with an AuditReport, also check traceability against its findings."""
    issues: list[Issue] = []
    err = lambda rid, msg: issues.append(Issue("error", rid, msg))      # noqa: E731
    warn = lambda rid, msg: issues.append(Issue("warning", rid, msg))   # noqa: E731

    seen = set()
    for r in reqs:
        if r.id in seen:
            err(r.id, "duplicate id")
        seen.add(r.id)
        if r.component not in COMPONENTS:
            err(r.id, f"unknown component '{r.component}'")
        if r.priority not in PRIORITIES:
            err(r.id, f"priority must be one of {PRIORITIES}")
        if unknown := set(r.stakeholders) - set(STAKEHOLDERS):
            err(r.id, f"unknown stakeholders {sorted(unknown)}")
        a = r.acceptance
        if not (a.get("probe") or a.get("manual") or {"metric", "op", "target"} <= a.keys()):
            err(r.id, "not testable: needs acceptance.probe, acceptance.manual or metric/op/target")
        if "op" in a and a["op"] not in OPS:
            err(r.id, f"unknown operator '{a['op']}'")
        if not r.traces:
            err(r.id, "not traced to any audit or stakeholder finding")
        if names := sorted(set(PRODUCTS.findall(r.statement))):
            warn(r.id, f"names products {names}: state the outcome, or confirm it is a fixed constraint")

    for c in COMPONENTS:
        if not any(r.component == c for r in reqs):
            err("*", f"component '{c}' has no requirement")

    if audit is not None:
        known = {f.id for f in audit.findings} | {s["id"] for s in audit.stakeholder_findings}
        for r in reqs:
            for t in r.traces:
                if t not in known:
                    err(r.id, f"traces unknown finding '{t}'")
        traced = {t for r in reqs for t in r.traces}
        for f in audit.findings:
            if f.severity == "high" and f.id not in traced:
                err("*", f"high-severity finding {f.id} is not addressed by any requirement")
        for s in audit.stakeholder_findings:
            if s["id"] not in traced:
                err("*", f"stakeholder finding {s['id']} is not addressed by any requirement")
    return issues


def prioritized(reqs: list[Requirement]) -> list[Requirement]:
    """Critical first, then nice-to-have, then future-facing; component order inside each bucket."""
    return sorted(reqs, key=lambda r: (PRIORITIES.index(r.priority), COMPONENTS.index(r.component), r.id))


def evaluate_metric(req: Requirement, measured: float) -> bool:
    a = req.acceptance
    return OPS[a["op"]](measured, a["target"])
