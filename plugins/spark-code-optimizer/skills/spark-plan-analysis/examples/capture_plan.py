"""Capture a DataFrame's query plan to a text file for `sparkopt_cli.py --plan`.

Three capture modes:
  physical  - df.explain() text ("== Physical Plan ==" only). Use this for --plan; it is also what
              collect_profile.py writes.
  full      - df._jdf.queryExecution().toString(): parsed + analyzed + optimized + physical plans.
              The inspector counts only the "== Physical Plan ==" section (streaming is detected
              anywhere), so it matches "physical"; for a streaming DataFrame the physical section
              is an AnalysisException.
  formatted - df.explain(mode="formatted"): operator tree + per-operator details. For humans only:
              the detail blocks sit inside the physical section (counts double) and scans print
              as "Scan parquet", so sparkopt's FileScan-based reuse estimate sees nothing.

Usage inside a job / notebook:
    from capture_plan import save_plan
    save_plan(df, "orders.plan.txt")                 # physical
    save_plan(df, "orders.full.txt", mode="full")
Then (no Spark needed):
    python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --plan orders.plan.txt --format json

Capture AFTER an action (df.collect()/write) to get the AQE final plan (isFinalPlan=true); note that
the final plan text also contains the initial plan, so Exchange counts double.

Run the demo: python capture_plan.py [out_dir]   (PySpark 3.3+)
"""
from __future__ import annotations

import io
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

MODES = ("physical", "full", "formatted")


def plan_text(df, mode: str = "physical") -> str:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if mode == "full":
        return df._jdf.queryExecution().toString()  # internal API, stable across 3.x
    buf = io.StringIO()
    with redirect_stdout(buf):  # PySpark's explain() prints from Python
        df.explain(mode="formatted" if mode == "formatted" else "simple")
    return buf.getvalue()


def save_plan(df, path: str | Path, mode: str = "physical") -> Path:
    path = Path(path)
    path.write_text(plan_text(df, mode), encoding="utf-8")
    return path


if __name__ == "__main__":
    from pyspark.sql import SparkSession, functions as F

    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp())
    spark = SparkSession.builder.master("local[2]").appName("capture_plan").getOrCreate()
    orders = spark.range(100_000).select(
        F.col("id").alias("order_id"), (F.col("id") % 1000).alias("user_id"), (F.rand(7) * 50).alias("amount"))
    users = spark.range(1000).select(F.col("id").alias("user_id"), (F.col("id") % 10).alias("segment"))
    df = (orders.join(users, "user_id")
          .groupBy("segment").agg(F.sum("amount").alias("revenue"))
          .orderBy(F.desc("revenue")))

    for mode in MODES:
        p = save_plan(df, out_dir / f"demo.{mode}.txt", mode)
        text = p.read_text(encoding="utf-8")
        assert "Physical Plan" in text, mode
        print(f"{mode:<9} {len(text):>6} chars -> {p}")
    print(f'python "$CLAUDE_PLUGIN_ROOT/scripts/sparkopt_cli.py" --plan {out_dir / "demo.physical.txt"}')
    spark.stop()
