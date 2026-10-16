#!/usr/bin/env python3
"""sparkopt CLI: run the sparkopt rule engine without a SparkSession.

Input is a DataFrame profile (JSON) and/or the text of a Spark physical plan
(``df.explain()`` output). Output is the matched optimization cases, the
PersistenceAdvisor decision and the generated PySpark code.

    python sparkopt_cli.py --profile profile.json
    python sparkopt_cli.py --profile profile.json --plan plan.txt --advice
    python sparkopt_cli.py --plan plan.txt --case multi_stage_agg --format json
    python sparkopt_cli.py --profile profile.json --case broadcast_join --case skew_join
    python sparkopt_cli.py --list-cases

--case NAME (repeatable): a built-in name restricts evaluation to those built-ins;
a complex name (ml_pipeline, graph_algorithm, multi_stage_agg, incremental_etl,
cached_broadcast, staged_materialization) adds that opt-in case.

Profile JSON (every field optional; values given here win over plan-derived ones):
    {
      "row_count": 50000000, "col_count": 12,
      "estimated_bytes": 8589934592,          # or "estimated_mb": 8192
      "partition": {"count": 200, "skew_ratio": 5.5, "avg_rows": 250000},
      "lineage": {"depth": 12, "estimated_reuse_count": 3,
                  "has_iterative_loop": false, "has_streaming_source": false,
                  "wide_transform_count": 3},
      "memory": {"executor_memory_mb": 8192, "executor_count": 20},
      "columns": {"user_id": {"dtype": "long", "null_ratio": 0.0, "distinct_count": 1000000}},
      "join_cols": ["user_id"], "agg_cols": ["category"],
      "right_table_bytes": 52428800,           # size of the other side of the join
      "df_name": "df", "spark_parallelism": 200
    }

Requires only the Python standard library (pyspark is NOT needed).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sparkopt.cases import ALL_BUILTIN_CASES, BroadcastJoinCase  # noqa: E402
from sparkopt.cases.complex_cases import ALL_COMPLEX_CASES  # noqa: E402
from sparkopt.codegen.context import CodegenContext, CodeRenderer  # noqa: E402
from sparkopt.core.analyzer import (  # noqa: E402
    ColumnProfile, DataProfile, LineageProfile, MemoryProfile, PartitionProfile,
)
from sparkopt.core.lineage_inspector import LineageInspector, PlanFeatures  # noqa: E402
from sparkopt.core.persistence_advisor import PersistenceAdvisor  # noqa: E402

# Complex cases are opt-in (the DSL adds them via .ml_pipeline() etc.).
OPT_IN_CASES = {c.name: c for c in ALL_COMPLEX_CASES}


SKILLS_DIR = Path(__file__).resolve().parent.parent / "skills"


def reviewed_example(case_name: str) -> str | None:
    """Corrected, reviewed PySpark for a case. The generated code is only a sketch."""
    hits = sorted(SKILLS_DIR.glob(f"*/examples/{case_name}.py"))
    # Relative to the plugin root (${CLAUDE_PLUGIN_ROOT}).
    return hits[0].relative_to(SKILLS_DIR.parent).as_posix() if hits else None


def parse_plan(text: str) -> PlanFeatures:
    # _parse_plan is pure string parsing; no DataFrame is needed.
    return LineageInspector(None)._parse_plan(text)


def build_profile(raw: dict, plan: PlanFeatures | None) -> DataProfile:
    part = PartitionProfile(**raw.get("partition", {}))
    lin_raw = dict(raw.get("lineage", {}))
    # Same precedence as DataFrameAnalyzer.analyze: explicit values win over the plan.
    if plan is not None:
        lin_raw.setdefault("depth", plan.depth)
        lin_raw.setdefault("has_streaming_source", plan.has_streaming)
        lin_raw.setdefault("wide_transform_count", plan.exchange_count)
        if plan.estimated_reuse > 1:
            lin_raw.setdefault("estimated_reuse_count", plan.estimated_reuse)
    lineage = LineageProfile(**lin_raw)

    if "estimated_bytes" in raw:
        est_bytes = int(raw["estimated_bytes"])
    else:
        est_bytes = int(float(raw.get("estimated_mb", 0)) * 1024 ** 2)
    mem_raw = dict(raw.get("memory", {}))
    mem_raw.setdefault("df_size_mb", est_bytes / 1024 ** 2)
    columns = {
        # collect_profile output repeats "name" inside each column entry
        name: ColumnProfile(name=name, dtype=c.get("dtype", "string"),
                            **{k: v for k, v in c.items() if k not in ("dtype", "name")})
        for name, c in raw.get("columns", {}).items()
    }
    return DataProfile(
        row_count=int(raw.get("row_count", 0)),
        col_count=int(raw.get("col_count", 0)),
        estimated_bytes=est_bytes,
        partition=part,
        columns=columns,
        lineage=lineage,
        memory=MemoryProfile(**mem_raw),
    )


BUILTIN_NAMES = {c.name for c in ALL_BUILTIN_CASES}


def select_cases(raw: dict, opt_in: list[str], exclude: set[str]):
    # --case <builtin> narrows the built-ins to the given ones (focus);
    # --case <complex> adds an opt-in complex case. Both can be mixed.
    focus = {n for n in opt_in if n in BUILTIN_NAMES}
    opt_in = [n for n in opt_in if n not in BUILTIN_NAMES]
    cases = [c for c in ALL_BUILTIN_CASES if not focus or c.name in focus]
    right_bytes = raw.get("right_table_bytes")
    if right_bytes is not None:
        cases = [BroadcastJoinCase(int(right_bytes)) if c.name == "broadcast_join" else c
                 for c in cases]
    elif not raw.get("join_cols"):
        # Without a join, BroadcastJoinCase would judge the main table's own size.
        exclude = exclude | {"broadcast_join"}
    if not raw.get("partition", {}).get("count"):
        # Unknown partition count defaults to 0, which would always look "undersized".
        exclude = exclude | {"repartition"}
    for name in opt_in:
        if name not in OPT_IN_CASES:
            raise SystemExit(f"unknown --case {name!r}; see --list-cases")
        cases.append(OPT_IN_CASES[name])
    return [c for c in cases if c.name not in exclude]


def run(raw: dict, plan_text: str | None, opt_in: list[str], exclude: set[str],
        with_advice: bool) -> dict:
    plan = parse_plan(plan_text) if plan_text else None
    profile = build_profile(raw, plan)
    matched = sorted((c for c in select_cases(raw, opt_in, exclude) if c.matches(profile)),
                     key=lambda c: c.priority, reverse=True)

    ctx = CodegenContext(
        profile=profile,
        join_cols=list(raw.get("join_cols", [])),
        agg_cols=list(raw.get("agg_cols", [])),
        df_name=raw.get("df_name", "df"),
        spark_parallelism=int(raw.get("spark_parallelism", 200)),
    )
    for case in matched:
        case.apply(ctx)
        ctx.mark_applied(case.name)

    result = {
        "profile": {
            "row_count": profile.row_count,
            "estimated_mb": round(profile.estimated_mb, 1),
            "partitions": profile.partition.count,
            "skew_ratio": profile.partition.skew_ratio,
            "lineage_depth": profile.lineage.depth,
            "reuse_count": profile.lineage.estimated_reuse_count,
            "iterative": profile.lineage.has_iterative_loop,
            "streaming": profile.lineage.has_streaming_source,
            "usable_memory_mb": round(profile.memory.usable_memory_mb),
            "recommended_storage_level": profile.memory.recommended_storage_level,
        },
        "matched_cases": [
            {"name": c.name, "priority": c.priority.name, "explain": c.explain(),
             "reviewed_example": reviewed_example(c.name)}
            for c in matched
        ],
        "code": CodeRenderer(ctx).render(),
    }
    if plan is not None:
        result["plan"] = {
            "depth": plan.depth, "depth_label": plan.recommended_depth_label,
            "exchange_count": plan.exchange_count, "join_types": plan.join_types,
            "has_in_memory_relation": plan.has_in_memory_relation,
            "has_streaming": plan.has_streaming, "has_cross_join": plan.has_cross_join,
            "window_count": plan.window_count, "agg_count": plan.agg_count,
            "estimated_reuse": plan.estimated_reuse, "is_complex": plan.is_complex,
            "lineage_break_needed": plan.lineage_break_needed,
        }
    if with_advice:
        d = PersistenceAdvisor(profile).decide()
        result["advice"] = {
            "strategy": d.strategy.name, "storage_level": d.storage_level,
            "rationale": d.rationale, "warnings": d.warnings, "code": d.code_snippet,
        }
    return result


def render_text(r: dict) -> str:
    out = ["== sparkopt report =="]
    out += [f"  {k:<26}: {v}" for k, v in r["profile"].items()]
    if "plan" in r:
        out.append("== plan ==")
        out += [f"  {k:<26}: {v}" for k, v in r["plan"].items()]
    out.append("== matched cases (priority order) ==")
    for c in r["matched_cases"]:
        out.append(f"  [{c['priority']:<8}] {c['name']}: {c['explain']}")
        if c["reviewed_example"]:
            out.append(f"             reviewed example: {c['reviewed_example']}")
    if "advice" in r:
        a = r["advice"]
        out += ["== persistence advice ==", f"  strategy     : {a['strategy']}",
                f"  storage level: {a['storage_level'] or '-'}", f"  rationale    : {a['rationale']}"]
        out += [f"  warning      : {w}" for w in a["warnings"]]
        out += ["-- advice code --", a["code"]]
    out += ["== generated code (sketch: adapt names; prefer the reviewed examples above) ==",
            r["code"]]
    return "\n".join(out)


def list_cases() -> str:
    rows = [(c.name, c.priority.name, "builtin") for c in ALL_BUILTIN_CASES]
    rows += [(c.name, c.priority.name, "opt-in (--case)") for c in ALL_COMPLEX_CASES]
    return "\n".join(f"{n:<24} {p:<9} {k}" for n, p, k in rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", help="DataFrame profile JSON file ('-' for stdin)")
    ap.add_argument("--plan", help="text file with df.explain() / queryExecution output")
    ap.add_argument("--case", action="append", default=[],
                    help="repeatable; a built-in name restricts evaluation to the given built-ins, "
                         "a complex name (ml_pipeline, ...) adds that opt-in case")
    ap.add_argument("--exclude", action="append", default=[], help="case name to skip")
    ap.add_argument("--advice", action="store_true", help="add the PersistenceAdvisor decision")
    ap.add_argument("--format", choices=["text", "json"], default="text")
    ap.add_argument("--list-cases", action="store_true")
    a = ap.parse_args(argv)

    if a.list_cases:
        print(list_cases())
        return 0
    if not a.profile and not a.plan:
        ap.error("give --profile and/or --plan")
    raw = {}
    if a.profile:
        text = sys.stdin.read() if a.profile == "-" else Path(a.profile).read_text(encoding="utf-8")
        raw = json.loads(text)
    plan_text = Path(a.plan).read_text(encoding="utf-8") if a.plan else None

    result = run(raw, plan_text, a.case, set(a.exclude), a.advice)
    if a.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(render_text(result))
    return 0


if __name__ == "__main__":
    # Generated code and explanations contain Turkish characters / box drawing.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
