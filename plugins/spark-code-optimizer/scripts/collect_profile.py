"""Collect a sparkopt profile from a live DataFrame (run inside your Spark job or notebook).

Writes two files that sparkopt_cli.py consumes on any machine, Spark not required:
  <out>.profile.json  - DataProfile (row count, size, partitions, lineage, memory)
  <out>.plan.txt      - physical plan text (queryExecution / explain output)

    import sys; sys.path.insert(0, "<plugin>/scripts")
    from collect_profile import collect
    collect(df, "orders", join_cols=["user_id"], agg_cols=["category"],
            right_df=users_df, reuse_count=3,
            executor_memory_mb=8192, executor_count=20)

Then:  python sparkopt_cli.py --profile orders.profile.json --plan orders.plan.txt --advice

Cost: df.count() plus a 5% sample scan per focus column. full_partition_scan=True also
counts rows per partition (real skew ratio) but scans the whole DataFrame.
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sparkopt.core.analyzer import DataFrameAnalyzer  # noqa: E402
from sparkopt.core.lineage_inspector import LineageInspector  # noqa: E402


def collect(df, out_prefix: str, *, join_cols=None, agg_cols=None, right_df=None,
            reuse_count: int = 1, iterative: bool = False, streaming: bool = False,
            full_partition_scan: bool = False, sample_fraction: float = 0.05,
            executor_memory_mb: int | None = None, executor_count: int | None = None) -> dict:
    conf = df.sparkSession.sparkContext.getConf()
    if executor_memory_mb is None:
        executor_memory_mb = _parse_mem_mb(conf.get("spark.executor.memory", "4g"))
    if executor_count is None:
        executor_count = int(conf.get("spark.executor.instances", "10"))

    profile = DataFrameAnalyzer(df, sample_fraction).analyze(
        join_cols=join_cols, agg_cols=agg_cols, full_partition_scan=full_partition_scan,
        reuse_count=reuse_count, iterative=iterative, streaming=streaming,
        executor_memory_mb=executor_memory_mb, executor_count=executor_count,
    )
    raw = asdict(profile)
    raw.pop("large_table_threshold_bytes", None)
    raw["memory"].pop("total_cluster_mb", None)
    raw["join_cols"] = list(join_cols or [])
    raw["agg_cols"] = list(agg_cols or [])
    raw["spark_parallelism"] = df.sparkSession.sparkContext.defaultParallelism
    if right_df is not None:
        raw["right_table_bytes"] = DataFrameAnalyzer(right_df, sample_fraction).analyze(
            auto_inspect_plan=False).estimated_bytes

    Path(f"{out_prefix}.profile.json").write_text(json.dumps(raw, indent=2), encoding="utf-8")
    Path(f"{out_prefix}.plan.txt").write_text(_explain_text(df), encoding="utf-8")
    return raw


def _explain_text(df) -> str:
    """df.explain() output (physical plan). queryExecution().toString() is the
    fallback; for streaming DataFrames it shows an AnalysisException instead of a plan."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            df.explain()
        if buf.getvalue().strip():
            return buf.getvalue()
    except Exception:
        pass
    return LineageInspector(df)._get_plan_string()


def _parse_mem_mb(value: str) -> int:
    value = value.strip().lower()
    units = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}
    if value and value[-1] in units:
        return int(float(value[:-1]) * units[value[-1]])
    if value.endswith("b") and value[-2:-1] in units:
        return int(float(value[:-2]) * units[value[-2]])
    return int(int(value) / (1024 * 1024))  # plain bytes


if __name__ == "__main__":
    assert _parse_mem_mb("8g") == 8192 and _parse_mem_mb("512m") == 512
    assert _parse_mem_mb("2gb") == 2048 and _parse_mem_mb(str(1024 ** 3)) == 1024
    print("collect_profile: self-check ok (import collect() inside a Spark session to use it)")
