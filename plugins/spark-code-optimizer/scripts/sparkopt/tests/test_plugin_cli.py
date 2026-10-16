"""Tests for the plugin additions: sparkopt_cli.py and the PySpark-compatibility fixes."""
from __future__ import annotations

import json
import os
import re
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPTS))

import sparkopt_cli  # noqa: E402
from sparkopt.cases import ALL_BUILTIN_CASES  # noqa: E402
from sparkopt.cases.complex_cases import ALL_COMPLEX_CASES  # noqa: E402
from sparkopt.codegen.context import CodegenContext, CodeRenderer  # noqa: E402

PLAN = """== Physical Plan ==
AdaptiveSparkPlan isAdaptive=true
+- HashAggregate(keys=[category#1], functions=[sum(amount#2)])
   +- Exchange hashpartitioning(category#1, 200)
      +- HashAggregate(keys=[category#1], functions=[partial_sum(amount#2)])
         +- SortMergeJoin [user_id#0], [user_id#5], Inner
            :- Sort [user_id#0 ASC NULLS FIRST]
            :  +- Exchange hashpartitioning(user_id#0, 200)
            :     +- FileScan parquet [user_id#0,category#1,amount#2] Batched: true
            +- Sort [user_id#5 ASC NULLS FIRST]
               +- Exchange hashpartitioning(user_id#5, 200)
                  +- FileScan parquet [user_id#0,category#1,amount#2] Batched: true
"""

BIG = {
    "row_count": 50_000_000, "estimated_mb": 8192,
    "partition": {"count": 200, "skew_ratio": 5.5, "avg_rows": 250_000},
    "lineage": {"depth": 12, "estimated_reuse_count": 3},
    "memory": {"executor_memory_mb": 8192, "executor_count": 20},
    "join_cols": ["user_id"], "agg_cols": ["category"],
}


class CliTest(unittest.TestCase):
    def test_big_skewed_profile(self):
        r = sparkopt_cli.run(dict(BIG, right_table_bytes=50 * 1024 ** 2), None, [], set(), True)
        names = [c["name"] for c in r["matched_cases"]]
        for expected in ("adaptive_qe", "skew_join", "broadcast_join", "partial_agg", "fanout_cache"):
            self.assertIn(expected, names)
        prios = [c["priority"] for c in r["matched_cases"]]
        self.assertEqual(prios[0], "CRITICAL")
        self.assertEqual(r["advice"]["strategy"], "LOCAL_CHECKPOINT")  # depth 12 + reuse 3

    def test_no_join_means_no_broadcast(self):
        r = sparkopt_cli.run({"row_count": 1000, "estimated_mb": 1}, None, [], set(), False)
        self.assertNotIn("broadcast_join", [c["name"] for c in r["matched_cases"]])

    def test_plan_fills_lineage_but_profile_wins(self):
        r = sparkopt_cli.run({"lineage": {"depth": 3}}, PLAN, [], set(), False)
        self.assertEqual(r["profile"]["lineage_depth"], 3)
        self.assertEqual(r["plan"]["exchange_count"], 3)
        self.assertIn("sort_merge", r["plan"]["join_types"])
        self.assertEqual(r["profile"]["reuse_count"], 2)  # duplicate FileScan → reuse

    def test_opt_in_and_exclude(self):
        r = sparkopt_cli.run(BIG, None, ["multi_stage_agg"], {"adaptive_qe"}, False)
        names = [c["name"] for c in r["matched_cases"]]
        self.assertIn("multi_stage_agg", names)
        self.assertNotIn("adaptive_qe", names)
        with self.assertRaises(SystemExit):
            sparkopt_cli.run(BIG, None, ["nope"], set(), False)

    def test_builtin_case_focus(self):
        r = sparkopt_cli.run(BIG, None, ["skew_join", "partial_agg", "ml_pipeline"], set(), False)
        names = {c["name"] for c in r["matched_cases"]}
        self.assertEqual(names - {"ml_pipeline"}, {"skew_join", "partial_agg"})

    def test_main_json_output(self):
        import io
        from contextlib import redirect_stdout
        tmp = Path(os.environ.get("TMPDIR", SCRIPTS)) / "_sparkopt_test_profile.json"
        tmp.write_text(json.dumps(BIG), encoding="utf-8")
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertEqual(sparkopt_cli.main(["--profile", str(tmp), "--format", "json"]), 0)
            self.assertIn("matched_cases", json.loads(buf.getvalue()))
        finally:
            tmp.unlink()


class LineageInspectorOperatorNamesTest(unittest.TestCase):
    """Real Spark 3.5 operator names that the original regexes missed."""

    def test_spark35_operator_names(self):
        plan = """== Physical Plan ==
*(3) ObjectHashAggregate(keys=[k#1], functions=[collect_list(v#2)])
+- CartesianProduct
   :- InMemoryTableScan [k#1, v#2]
   +- BroadcastNestedLoopJoin BuildRight, Inner
"""
        f = sparkopt_cli.parse_plan(plan)
        self.assertTrue(f.has_cross_join)
        self.assertTrue(f.has_broadcast_join)
        self.assertTrue(f.has_in_memory_relation)
        self.assertEqual(f.agg_count, 1)

    def test_streaming_v2_and_physical_section_only(self):
        plan = """== Parsed Logical Plan ==
StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RateStreamProvider
== Analyzed Logical Plan ==
Aggregate [k#1], [k#1, count(1)]
== Physical Plan ==
*(2) HashAggregate(keys=[k#1], functions=[count(1)])
+- Exchange hashpartitioning(k#1, 200)
   +- *(1) HashAggregate(keys=[k#1], functions=[partial_count(1)])
"""
        f = sparkopt_cli.parse_plan(plan)
        self.assertTrue(f.has_streaming)          # found in the logical section
        self.assertEqual(f.exchange_count, 1)     # physical section only
        self.assertEqual(f.agg_count, 2)          # not the logical Aggregate

    def test_accepts_collect_profile_column_format(self):
        # dataclasses.asdict(ColumnProfile) repeats "name" inside the entry
        raw = {"columns": {"user_id": {"name": "user_id", "dtype": "long", "null_ratio": 0.1,
                                       "distinct_count": 5, "min_val": None, "max_val": None,
                                       "mean_val": None, "std_val": None}}}
        self.assertEqual(sparkopt_cli.build_profile(raw, None).col("user_id").null_ratio, 0.1)

    def test_plan_only_does_not_force_repartition(self):
        r = sparkopt_cli.run({}, PLAN, [], set(), False)
        self.assertNotIn("repartition", [c["name"] for c in r["matched_cases"]])


class PySparkCompatibilityTest(unittest.TestCase):
    """Generated code must only use StorageLevel constants that exist in PySpark."""

    PYSPARK_LEVELS = {
        "NONE", "DISK_ONLY", "DISK_ONLY_2", "DISK_ONLY_3", "MEMORY_ONLY", "MEMORY_ONLY_2",
        "MEMORY_AND_DISK", "MEMORY_AND_DISK_2", "MEMORY_AND_DISK_DESER", "OFF_HEAP",
    }

    def _all_code(self, raw: dict) -> str:
        profile = sparkopt_cli.build_profile(raw, None)
        ctx = CodegenContext(profile=profile, join_cols=["k"], agg_cols=["g"])
        for case in ALL_BUILTIN_CASES + ALL_COMPLEX_CASES:
            case.apply(ctx)  # apply every case regardless of matches()
        return CodeRenderer(ctx).render()

    def test_storage_levels_exist_in_pyspark(self):
        for size_mb in (10, 30_000, 10_000_000):  # MEMORY_ONLY / MEMORY_AND_DISK / DISK_ONLY
            code = self._all_code({"estimated_mb": size_mb, "lineage": {"has_streaming_source": True}})
            used = set(re.findall(r"StorageLevel\.([A-Z_0-9]+)", code))
            self.assertTrue(used, "expected StorageLevel usage")
            self.assertLessEqual(used, self.PYSPARK_LEVELS, f"invalid in PySpark: {used - self.PYSPARK_LEVELS}")

    def test_all_generated_code_is_valid_python(self):
        import ast
        from sparkopt.core.persistence_advisor import PersistenceAdvisor
        profiles = (
            {"estimated_mb": 10, "lineage": {"has_streaming_source": True, "estimated_reuse_count": 3}},
            {"estimated_mb": 30_000, "lineage": {"depth": 25}},
            {"estimated_mb": 500, "lineage": {"has_iterative_loop": True}},
            {"estimated_mb": 500, "lineage": {"depth": 15, "estimated_reuse_count": 2}},
        )
        for raw in profiles:
            profile = sparkopt_cli.build_profile(raw, None)
            for case in ALL_BUILTIN_CASES + ALL_COMPLEX_CASES:
                ctx = CodegenContext(profile=profile, join_cols=["user_id"], agg_cols=["category"])
                case.apply(ctx)
                with self.subTest(case=case.name, profile=raw):
                    ast.parse(CodeRenderer(ctx).render(include_header=False))
            ast.parse(PersistenceAdvisor(profile).decide().code_snippet)

    def test_no_runtime_serializer_conf(self):
        code = self._all_code({"estimated_mb": 10})
        self.assertNotRegex(code, r'spark\.conf\.set\(\s*["\']spark\.serializer')

    def test_dsl_iterative_and_streaming_set_profile_flags(self):
        from sparkopt.dsl import SparkOptimizer
        opt = SparkOptimizer.__new__(SparkOptimizer)
        opt._custom_cases, opt._iterative, opt._streaming = [], False, False
        opt.iterative(checkpoint_interval=3).streaming()
        self.assertTrue(opt._iterative)
        self.assertTrue(opt._streaming)
        self.assertEqual(opt._checkpoint_interval, 3)


if __name__ == "__main__":
    unittest.main()
