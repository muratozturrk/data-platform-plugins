"""Architecture guide generator: decisions + requirements + audit -> one reviewable document.

Sections follow a reviewable architecture guide: data-flow diagram,
dataset map with regulatory classification, requirement traceability, staged migration plan with
fallback, and role-specific road-show briefings.
"""
from dataclasses import dataclass
from pathlib import Path

import yaml

from requirements_validation.requirements import COMPONENTS, STAKEHOLDERS, Requirement, prioritized

DEFAULT_FILE = Path(__file__).with_name("decisions.yaml")
HOT_READ_SHARE = 0.25            # lab assumption: same threshold as audit finding F-HOT-DATASETS
FALLBACK = {                     # verified in test_pilot_migration.py
    "add_files": "DROP TABLE without PURGE; legacy files stay readable by the old tools",
    "rewrite": "Drop the Iceberg copy; the source stays the system of record until cut-over",
    "recreate_view": "Keep the warehouse view until the new view is signed off",
}


@dataclass(frozen=True)
class Decision:
    id: str
    component: str
    source: str
    choice: str
    rationale: str
    satisfies: tuple[str, ...] = ()


@dataclass(frozen=True)
class Wave:
    dataset: str
    wave: int
    strategy: str            # add_files | rewrite | recreate_view
    complexity: int          # 1 (metadata only) .. 4
    sensitive: bool
    read_share: float
    reason: str


def load_decisions(path: Path = DEFAULT_FILE) -> list[Decision]:
    return [Decision(**{**d, "satisfies": tuple(d.get("satisfies", ()))})
            for d in yaml.safe_load(Path(path).read_text())["decisions"]]


def uncovered(reqs: list[Requirement], decisions: list[Decision]) -> list[str]:
    covered = {r for d in decisions for r in d.satisfies}
    return [r.id for r in reqs if r.id not in covered]


def migration_waves(audit) -> list[Wave]:
    """Order datasets by access frequency, sensitivity and transformation complexity.

    Rules (lab assumptions): sensitive data waits for RBAC and residency controls (wave 3); hot datasets
    and metadata-only moves go first (wave 1, as the road show front-loaded the most-used dashboards).
    """
    total_reads = sum(d.reads for d in audit.datasets) or 1
    waves = []
    for d in audit.datasets:
        if d.kind == "view":
            strategy, complexity = "recreate_view", 2
        elif d.main_format == "parquet":
            strategy, complexity = "add_files", 1
        else:
            strategy, complexity = "rewrite", 2
        complexity += int(d.semi_structured) + int(d.has_primary_key is False)
        share = round(d.reads / total_reads, 3)
        sensitive = bool(d.pii_columns)
        if sensitive:
            wave, reason = 3, f"personal data in {', '.join(d.pii_columns)}: needs access controls first"
        elif share >= HOT_READ_SHARE:
            wave, reason = 1, f"{share:.0%} of warehouse reads"
        elif strategy == "add_files":
            wave, reason = 1, "metadata-only move, no data rewrite"
        else:
            wave, reason = 2, f"complexity {complexity}"
        waves.append(Wave(d.id, wave, strategy, complexity, sensitive, share, reason))
    return sorted(waves, key=lambda w: (w.wave, -w.read_share, w.complexity, w.dataset))


def mermaid(decisions: list[Decision], audit) -> str:
    """Data-flow diagram: legacy sources -> ingestion -> storage + catalog -> federation -> consumption."""
    by = {c: [d for d in decisions if d.component == c] for c in COMPONENTS}
    lines = ["flowchart TD"]
    systems = sorted({d.system for d in audit.datasets})
    for s in systems:
        lines.append(f'    SRC_{s}["legacy {s}"]')
    for c in COMPONENTS:
        lines.append(f'    subgraph {c.upper()}["{c}"]')
        lines += [f'        {d.id.replace("-", "_")}["{d.choice.split(" - ")[0].split(";")[0]}"]' for d in by[c]]
        lines.append("    end")
    edges = [(f"SRC_{s}", "INGESTION") for s in systems] + [
        ("INGESTION", "STORAGE"), ("INGESTION", "CATALOG"), ("FEDERATION", "CATALOG"),
        ("FEDERATION", "STORAGE"), ("CONSUMPTION", "FEDERATION")]
    lines += [f"    {a} --> {b}" for a, b in edges]
    return "\n".join(lines)


def briefing(stakeholder: str, reqs: list[Requirement], decisions: list[Decision]) -> list[tuple[str, str]]:
    """(requirement id, decision ids) pairs that one stakeholder group must review in its road-show session."""
    mine = [r for r in prioritized(reqs) if stakeholder in r.stakeholders]
    return [(r.id, ", ".join(d.id for d in decisions if r.id in d.satisfies) or "OPEN") for r in mine]


def render_guide(reqs, decisions, audit, acceptance=None) -> str:
    status = {a.requirement: a.status for a in (acceptance or [])}
    waves = migration_waves(audit)
    out = ["# Architecture guide (proposal, open for review)", "", "## Data flow", "", "```mermaid",
           mermaid(decisions, audit), "```", "", "## Requirement traceability", "",
           "| Requirement | Priority | Component | Decision | Acceptance |", "|---|---|---|---|---|"]
    for r in prioritized(reqs):
        ds = ", ".join(d.id for d in decisions if r.id in d.satisfies) or "**OPEN**"
        out.append(f"| {r.id} | {r.priority} | {r.component} | {ds} | {status.get(r.id, 'not run')} |")
    gaps = [f"- {r}: no decision addresses it" for r in uncovered(reqs, decisions)]
    gaps += [f"- {k}: acceptance check failed on the evaluated platform" for k, v in status.items() if v == "fail"]
    out += ["", "## Open items", ""] + (gaps or ["- none"])
    out += ["", "## Dataset map", "", "| Dataset | Format | Classification |", "|---|---|---|"]
    out += [f"| {d.id} | {d.main_format} | {'regulated (PII)' if d.pii_columns else 'internal'} |"
            for d in audit.datasets]
    out += ["", "## Staged migration plan", "", "| Wave | Dataset | Strategy | Reason | Fallback |",
            "|---|---|---|---|---|"]
    out += [f"| {w.wave} | {w.dataset} | {w.strategy} | {w.reason} | {FALLBACK[w.strategy]} |" for w in waves]
    out += ["", "## Road-show briefings", ""]
    for s in STAKEHOLDERS:
        out.append(f"- **{s}**: " + "; ".join(f"{rid} -> {ds}" for rid, ds in briefing(s, reqs, decisions)))
    return "\n".join(out) + "\n"
