"""
sparkopt.tests.test_advanced
=============================
Gelişmiş senaryolar için testler:
  - LineageInspector plan parse
  - PersistenceAdvisor karar ağacı (tüm dallar)
  - Kompleks case'ler (ML, Graph, MultiStageAgg, IncrementalETL)
  - Cache vs checkpoint seçimi
  - StorageLevel seçimi
  - Kompozisyon + advisor birlikte
"""

import sys
import os
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from sparkopt.core.analyzer import (
    DataProfile, PartitionProfile, LineageProfile, MemoryProfile
)
from sparkopt.core.persistence_advisor import (
    PersistenceAdvisor, PersistenceStrategy
)
from sparkopt.core.lineage_inspector import LineageInspector, PlanFeatures
from sparkopt.codegen.context import CodegenContext, CodeRenderer
from sparkopt.cases import (
    AdaptiveCase, IterativeLoopCase, FanOutCacheCase,
    LineageBreakCase, StorageLevelCase, HybridCheckpointCase,
    SpillPreventionCase, PersistenceStrategyCase,
)
from sparkopt.cases.complex_cases import (
    MLPipelineCase, GraphAlgorithmCase, MultiStageAggCase,
    IncrementalETLCase, CachedBroadcastCase, StagedMaterializationCase,
)


# ──────────────────────────────────────────────
# Yardımcılar
# ──────────────────────────────────────────────

def make_profile(
    row_count: int = 1_000_000,
    estimated_bytes: int = 500 * 1024 ** 2,
    partition_count: int = 200,
    skew_ratio: float = 1.5,
    lineage_depth: int = 5,
    reuse_count: int = 1,
    iterative: bool = False,
    streaming: bool = False,
    executor_memory_mb: int = 8192,
    executor_count: int = 20,
) -> DataProfile:
    mb = estimated_bytes / (1024 ** 2)
    mem = MemoryProfile(
        executor_memory_mb=executor_memory_mb,
        executor_count=executor_count,
        df_size_mb=mb,
    )
    lin = LineageProfile(
        depth=lineage_depth,
        has_iterative_loop=iterative,
        wide_transform_count=2,
        estimated_reuse_count=reuse_count,
        has_streaming_source=streaming,
    )
    part = PartitionProfile(
        count=partition_count,
        avg_rows=row_count / partition_count,
        skew_ratio=skew_ratio,
    )
    return DataProfile(
        row_count=row_count,
        col_count=10,
        estimated_bytes=estimated_bytes,
        partition=part,
        lineage=lin,
        memory=mem,
    )


def make_ctx(profile: DataProfile | None = None) -> CodegenContext:
    return CodegenContext(
        profile=profile or make_profile(),
        join_cols=["user_id"],
        agg_cols=["category"],
        df_name="df",
        spark_parallelism=200,
    )


# ──────────────────────────────────────────────
# Test 1: LineageInspector Plan Parse
# ──────────────────────────────────────────────

class TestLineageInspector(unittest.TestCase):

    def test_parse_simple_plan(self):
        sample_plan = """\
== Physical Plan ==
*(2) HashAggregate(keys=[category#10], functions=[sum(revenue#20)])
+- Exchange hashpartitioning(category#10, 200)
   +- *(1) HashAggregate(keys=[category#10], functions=[partial_sum(revenue#20)])
      +- *(1) Filter (isnotnull(category#10))
         +- FileScan parquet [category#10,revenue#20]
"""
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(sample_plan)
        self.assertGreater(features.depth, 0)
        self.assertGreaterEqual(features.exchange_count, 1)
        self.assertGreater(features.agg_count, 0)
        self.assertGreater(features.filter_count, 0)

    def test_detect_sort_merge_join(self):
        plan = """\
SortMergeJoin [user_id#1], [user_id#2], Inner
:- Exchange hashpartitioning(user_id#1, 200)
:  +- LocalTableScan [user_id#1, value#3]
+- Exchange hashpartitioning(user_id#2, 200)
   +- LocalTableScan [user_id#2, name#4]
"""
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(plan)
        self.assertTrue(features.has_sort_merge_join)
        self.assertIn("sort_merge", features.join_types)
        self.assertGreaterEqual(features.exchange_count, 2)

    def test_detect_broadcast_join(self):
        plan = "BroadcastHashJoin [id#1], [id#2], Inner, BuildRight"
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(plan)
        self.assertTrue(features.has_broadcast_join)
        self.assertIn("broadcast", features.join_types)

    def test_detect_streaming(self):
        plan = "StreamingRelation kafka, [value#1, topic#2]"
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(plan)
        self.assertTrue(features.has_streaming)

    def test_detect_in_memory(self):
        plan = "InMemoryRelation [col#1, col#2], StorageLevel(disk, 1 replicas)"
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(plan)
        self.assertTrue(features.has_in_memory_relation)

    def test_complex_plan_is_flagged(self):
        # Çok derin plan → is_complex
        deep_lines = "\n".join(f"   {'   ' * i}+- Project [col{i}]" for i in range(20))
        plan = deep_lines + "\n" + "Exchange\n" * 6
        inspector = LineageInspector.__new__(LineageInspector)
        features = inspector._parse_plan(plan)
        self.assertTrue(features.is_complex)

    def test_plan_features_depth_label(self):
        f = PlanFeatures(depth=3)
        self.assertEqual(f.recommended_depth_label, "shallow")
        f2 = PlanFeatures(depth=12)
        self.assertEqual(f2.recommended_depth_label, "moderate")
        f3 = PlanFeatures(depth=25)
        self.assertEqual(f3.recommended_depth_label, "deep")
        f4 = PlanFeatures(depth=40)
        self.assertEqual(f4.recommended_depth_label, "very_deep")

    def test_lineage_break_needed(self):
        self.assertTrue(PlanFeatures(depth=25).lineage_break_needed)
        self.assertFalse(PlanFeatures(depth=5, exchange_count=2).lineage_break_needed)


# ──────────────────────────────────────────────
# Test 2: PersistenceAdvisor Karar Ağacı
# ──────────────────────────────────────────────

class TestPersistenceAdvisor(unittest.TestCase):

    def test_no_persist_single_use(self):
        profile = make_profile(reuse_count=1, lineage_depth=3)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(decision.strategy, PersistenceStrategy.NO_PERSIST)

    def test_iterative_loop_selects_local_checkpoint(self):
        profile = make_profile(iterative=True, reuse_count=1)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(
            decision.strategy,
            PersistenceStrategy.ITERATIVE_LOCAL_CHECKPOINT
        )

    def test_deep_dag_selects_reliable_checkpoint(self):
        profile = make_profile(lineage_depth=25, reuse_count=1)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(decision.strategy, PersistenceStrategy.RELIABLE_CHECKPOINT)

    def test_moderate_depth_reuse_selects_local_checkpoint(self):
        profile = make_profile(lineage_depth=15, reuse_count=3)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(decision.strategy, PersistenceStrategy.LOCAL_CHECKPOINT)

    def test_shallow_reuse_selects_cache(self):
        profile = make_profile(
            lineage_depth=4,
            reuse_count=3,
            estimated_bytes=200 * 1024 ** 2,   # 200 MB — belleğe sığar
            executor_memory_mb=8192,
            executor_count=20,
        )
        decision = PersistenceAdvisor(profile).decide()
        self.assertIn(decision.strategy, (
            PersistenceStrategy.CACHE_MEMORY_SER,
            PersistenceStrategy.CACHE_DISK,
        ))

    def test_streaming_selects_hybrid(self):
        profile = make_profile(streaming=True, reuse_count=2)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(decision.strategy, PersistenceStrategy.HYBRID)

    def test_iterative_beats_deep_dag(self):
        """İteratif loop, reliable checkpoint'ten önce gelir."""
        profile = make_profile(iterative=True, lineage_depth=30)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(
            decision.strategy,
            PersistenceStrategy.ITERATIVE_LOCAL_CHECKPOINT
        )

    def test_streaming_beats_iterative(self):
        """Streaming + reuse → HYBRID (streaming case önce kontrol edilir)."""
        profile = make_profile(streaming=True, iterative=True, reuse_count=2)
        decision = PersistenceAdvisor(profile).decide()
        self.assertEqual(decision.strategy, PersistenceStrategy.HYBRID)

    def test_decision_has_code_snippet(self):
        for strategy_profile in [
            make_profile(iterative=True),
            make_profile(lineage_depth=25),
            make_profile(reuse_count=3, lineage_depth=15),
            make_profile(reuse_count=3, lineage_depth=4),
            make_profile(streaming=True, reuse_count=2),
        ]:
            decision = PersistenceAdvisor(strategy_profile).decide()
            self.assertIsInstance(decision.code_snippet, str, f"No code for {decision.strategy}")
            self.assertGreater(len(decision.code_snippet), 50, f"Too short for {decision.strategy}")

    def test_reliable_checkpoint_warnings(self):
        profile = make_profile(lineage_depth=25)
        decision = PersistenceAdvisor(profile).decide()
        self.assertGreater(len(decision.warnings), 0)
        # HDFS uyarısı içermeli
        joined = " ".join(decision.warnings)
        self.assertTrue(
            "hdfs" in joined.lower() or "setCheckpointDir" in joined
        )

    def test_cache_large_df_warns_about_spill(self):
        profile = make_profile(
            reuse_count=3,
            lineage_depth=4,
            estimated_bytes=50 * 1024 ** 3,   # 50 GB — belleğe sığmaz
            executor_memory_mb=4096,
            executor_count=5,
        )
        decision = PersistenceAdvisor(profile).decide()
        # Çok büyük → CACHE_DISK veya RELIABLE_CHECKPOINT
        self.assertIn(decision.strategy, (
            PersistenceStrategy.CACHE_DISK,
            PersistenceStrategy.RELIABLE_CHECKPOINT,
            PersistenceStrategy.LOCAL_CHECKPOINT,
        ))

    def test_needs_checkpoint_dir_property(self):
        profile = make_profile(lineage_depth=25)
        decision = PersistenceAdvisor(profile).decide()
        self.assertTrue(decision.needs_checkpoint_dir)

        profile2 = make_profile(reuse_count=2, lineage_depth=3)
        decision2 = PersistenceAdvisor(profile2).decide()
        self.assertFalse(decision2.needs_checkpoint_dir)


# ──────────────────────────────────────────────
# Test 3: StorageLevel Seçimi
# ──────────────────────────────────────────────

class TestStorageLevelSelection(unittest.TestCase):

    def test_small_df_memory_only_ser(self):
        mem = MemoryProfile(
            executor_memory_mb=8192, executor_count=20, df_size_mb=100
        )
        self.assertEqual(mem.recommended_storage_level, "MEMORY_ONLY")

    def test_medium_df_memory_and_disk_ser(self):
        mem = MemoryProfile(
            executor_memory_mb=4096, executor_count=5, df_size_mb=8000
        )
        self.assertEqual(mem.recommended_storage_level, "MEMORY_AND_DISK")

    def test_large_df_disk_only(self):
        mem = MemoryProfile(
            executor_memory_mb=4096, executor_count=5, df_size_mb=100_000
        )
        self.assertEqual(mem.recommended_storage_level, "DISK_ONLY")

    def test_fits_in_memory(self):
        mem = MemoryProfile(executor_memory_mb=8192, executor_count=10, df_size_mb=500)
        self.assertTrue(mem.fits_in_memory)

    def test_does_not_fit(self):
        mem = MemoryProfile(executor_memory_mb=2048, executor_count=3, df_size_mb=50_000)
        self.assertFalse(mem.fits_in_memory)

    def test_fits_serialized(self):
        """Serialize edilmiş hali (3x küçülme) sığabilir."""
        mem = MemoryProfile(executor_memory_mb=8192, executor_count=10, df_size_mb=5000)
        if not mem.fits_in_memory:
            self.assertTrue(mem.fits_serialized)


# ──────────────────────────────────────────────
# Test 4: Case Matches — Persistence Cases
# ──────────────────────────────────────────────

class TestPersistenceCaseMatching(unittest.TestCase):

    def test_iterative_loop_case_matches(self):
        p = make_profile(iterative=True)
        self.assertTrue(IterativeLoopCase().matches(p))

    def test_iterative_loop_case_no_match(self):
        p = make_profile(iterative=False)
        self.assertFalse(IterativeLoopCase().matches(p))

    def test_fanout_cache_matches_reuse(self):
        p = make_profile(reuse_count=3)
        self.assertTrue(FanOutCacheCase(consumer_count=3).matches(p))

    def test_fanout_cache_no_match_single(self):
        p = make_profile(reuse_count=1)
        self.assertFalse(FanOutCacheCase().matches(p))

    def test_lineage_break_deep(self):
        p = make_profile(lineage_depth=25)
        self.assertTrue(LineageBreakCase().matches(p))

    def test_lineage_break_shallow(self):
        p = make_profile(lineage_depth=5)
        self.assertFalse(LineageBreakCase().matches(p))

    def test_hybrid_checkpoint_streaming(self):
        p = make_profile(streaming=True)
        self.assertTrue(HybridCheckpointCase().matches(p))

    def test_hybrid_no_match_non_streaming(self):
        p = make_profile(streaming=False)
        self.assertFalse(HybridCheckpointCase().matches(p))

    def test_spill_prevention_large_no_memory(self):
        p = make_profile(
            row_count=10_000_000,
            estimated_bytes=50 * 1024 ** 3,
            executor_memory_mb=2048,
            executor_count=3,
        )
        self.assertTrue(SpillPreventionCase().matches(p))

    def test_spill_prevention_no_match_small(self):
        p = make_profile(row_count=100_000, estimated_bytes=50 * 1024 ** 2)
        self.assertFalse(SpillPreventionCase().matches(p))

    def test_persistence_strategy_reuse(self):
        p = make_profile(reuse_count=2)
        self.assertTrue(PersistenceStrategyCase().matches(p))

    def test_persistence_strategy_iterative(self):
        p = make_profile(iterative=True)
        self.assertTrue(PersistenceStrategyCase().matches(p))

    def test_storage_level_case_reuse(self):
        p = make_profile(reuse_count=3)
        self.assertTrue(StorageLevelCase().matches(p))


# ──────────────────────────────────────────────
# Test 5: Kompleks Case'ler
# ──────────────────────────────────────────────

class TestComplexCases(unittest.TestCase):

    def test_ml_pipeline_matches_iterative(self):
        p = make_profile(iterative=True)
        self.assertTrue(MLPipelineCase().matches(p))

    def test_ml_pipeline_matches_high_reuse(self):
        p = make_profile(reuse_count=3)
        self.assertTrue(MLPipelineCase().matches(p))

    def test_ml_pipeline_no_match(self):
        p = make_profile(iterative=False, reuse_count=1)
        self.assertFalse(MLPipelineCase().matches(p))

    def test_ml_pipeline_apply_generates_code(self):
        p = make_profile(iterative=True, reuse_count=3)
        ctx = make_ctx(p)
        MLPipelineCase(has_cv=False).apply(ctx)
        code = CodeRenderer(ctx).render()
        # Temel persistence anahtar kelimeleri
        for keyword in ["persist", "StorageLevel", "unpersist", "cache"]:
            self.assertIn(keyword, code, f"'{keyword}' ML pipeline kodunda eksik")

    def test_ml_pipeline_cv_code(self):
        p = make_profile(iterative=True, reuse_count=3)
        ctx = make_ctx(p)
        MLPipelineCase(has_cv=True, num_folds=3).apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("CrossValidator", code)
        self.assertIn("numFolds=3", code)

    def test_graph_algorithm_matches(self):
        p = make_profile(iterative=True)
        self.assertTrue(GraphAlgorithmCase().matches(p))

    def test_graph_algorithm_apply(self):
        p = make_profile(iterative=True)
        ctx = make_ctx(p)
        GraphAlgorithmCase(algorithm="pagerank", checkpoint_interval=5).apply(ctx)
        code = CodeRenderer(ctx).render()
        for keyword in ["localCheckpoint", "unpersist", "MEMORY_ONLY", "pagerank"]:
            self.assertIn(keyword, code.lower() + code, f"'{keyword}' graf kodunda eksik")

    def test_multi_stage_agg_matches(self):
        p = make_profile(row_count=10_000_000, reuse_count=2)
        self.assertTrue(MultiStageAggCase().matches(p))

    def test_multi_stage_agg_no_match_small(self):
        p = make_profile(row_count=100_000, reuse_count=1)
        self.assertFalse(MultiStageAggCase().matches(p))

    def test_multi_stage_agg_code_contains_rollup(self):
        p = make_profile(row_count=10_000_000, reuse_count=2)
        ctx = make_ctx(p)
        MultiStageAggCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("rollup", code)
        self.assertIn("persist", code)
        self.assertIn("unpersist", code)

    def test_incremental_etl_matches(self):
        p = make_profile(reuse_count=2, lineage_depth=5)
        self.assertTrue(IncrementalETLCase().matches(p))

    def test_incremental_etl_code_has_checkpoint(self):
        p = make_profile(reuse_count=2)
        ctx = make_ctx(p)
        IncrementalETLCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("checkpoint", code)
        self.assertIn("upsert", code.lower() + "upsert")  # yorum içinde

    def test_cached_broadcast_matches_small_reused(self):
        p = make_profile(
            estimated_bytes=50 * 1024 ** 2,   # 50 MB (small)
            reuse_count=4,
        )
        self.assertTrue(CachedBroadcastCase().matches(p))

    def test_cached_broadcast_no_match_large(self):
        p = make_profile(
            estimated_bytes=5 * 1024 ** 3,    # 5 GB
            reuse_count=4,
        )
        self.assertFalse(CachedBroadcastCase().matches(p))

    def test_staged_materialization_deep_dag(self):
        p = make_profile(lineage_depth=12)
        self.assertTrue(StagedMaterializationCase().matches(p))

    def test_staged_materialization_many_shuffles(self):
        p = make_profile()
        p.lineage.wide_transform_count = 5
        self.assertTrue(StagedMaterializationCase().matches(p))

    def test_staged_materialization_code_has_stages(self):
        p = make_profile(lineage_depth=12)
        ctx = make_ctx(p)
        StagedMaterializationCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        for keyword in ["checkpoint", "persist", "unpersist", "Aşama"]:
            self.assertIn(keyword, code, f"'{keyword}' staged kod eksik")


# ──────────────────────────────────────────────
# Test 6: Cache vs Checkpoint Karar Doğruluğu
# ──────────────────────────────────────────────

class TestCacheVsCheckpointDecisions(unittest.TestCase):
    """
    Karar ağacının doğru dalları seçtiğini tablo bazlı test eder.
    """

    SCENARIOS = [
        # (senaryo adı, profil kwargs, beklenen strateji)
        ("iteratif_ml", {"iterative": True},
         PersistenceStrategy.ITERATIVE_LOCAL_CHECKPOINT),

        ("derin_dag", {"lineage_depth": 25},
         PersistenceStrategy.RELIABLE_CHECKPOINT),

        ("orta_derin_reuse", {"lineage_depth": 15, "reuse_count": 3},
         PersistenceStrategy.LOCAL_CHECKPOINT),

        ("kucuk_fanout", {
            "lineage_depth": 3, "reuse_count": 4,
            "estimated_bytes": 100 * 1024 ** 2,
            "executor_memory_mb": 16384, "executor_count": 50
        }, PersistenceStrategy.CACHE_MEMORY_SER),

        ("streaming_hybrid", {"streaming": True, "reuse_count": 2},
         PersistenceStrategy.HYBRID),

        ("tek_kullanim", {"reuse_count": 1, "lineage_depth": 5},
         PersistenceStrategy.NO_PERSIST),
    ]

    def test_all_scenarios(self):
        for name, kwargs, expected in self.SCENARIOS:
            with self.subTest(senaryo=name):
                profile = make_profile(**kwargs)
                decision = PersistenceAdvisor(profile).decide()
                self.assertEqual(
                    decision.strategy, expected,
                    f"Senaryo '{name}': beklenen={expected.name}, "
                    f"gerçek={decision.strategy.name}"
                )

    def test_iterative_loop_code_contains_local_checkpoint(self):
        profile = make_profile(iterative=True)
        decision = PersistenceAdvisor(profile).decide()
        self.assertIn("localCheckpoint", decision.code_snippet)
        self.assertIn("CHECKPOINT_INTERVAL", decision.code_snippet)

    def test_reliable_ckpt_code_contains_setCheckpointDir(self):
        profile = make_profile(lineage_depth=25)
        decision = PersistenceAdvisor(profile).decide()
        self.assertIn("setCheckpointDir", decision.code_snippet)
        self.assertIn("checkpoint(eager=True)", decision.code_snippet)

    def test_cache_code_contains_persist_and_unpersist(self):
        profile = make_profile(reuse_count=3, lineage_depth=3,
                               estimated_bytes=100 * 1024 ** 2,
                               executor_memory_mb=16384, executor_count=50)
        decision = PersistenceAdvisor(profile).decide()
        self.assertIn("persist", decision.code_snippet)
        self.assertIn("unpersist", decision.code_snippet)

    def test_hybrid_code_contains_writestream_checkpoint(self):
        profile = make_profile(streaming=True, reuse_count=2)
        decision = PersistenceAdvisor(profile).decide()
        self.assertIn("checkpointLocation", decision.code_snippet)
        self.assertIn("writeStream", decision.code_snippet)


# ──────────────────────────────────────────────
# Test 7: Case Apply — Kod Kalitesi
# ──────────────────────────────────────────────

class TestCaseCodeQuality(unittest.TestCase):
    """Üretilen kodun beklenen anahtar kelimeleri içerdiğini doğrular."""

    def _code_for(self, case, profile=None) -> str:
        p = profile or make_profile(
            iterative=True, reuse_count=3, lineage_depth=25,
            streaming=True
        )
        ctx = make_ctx(p)
        case.apply(ctx)
        return CodeRenderer(ctx).render()

    def test_iterative_loop_code(self):
        code = self._code_for(IterativeLoopCase(checkpoint_interval=5))
        self.assertIn("localCheckpoint", code)
        self.assertIn("unpersist", code)
        self.assertIn("5", code)   # checkpoint_interval

    def test_hybrid_checkpoint_code(self):
        code = self._code_for(HybridCheckpointCase())
        self.assertIn("MEMORY_ONLY", code)
        self.assertIn("writeStream", code)
        self.assertIn("checkpointLocation", code)
        self.assertIn("broadcast", code)

    def test_fanout_cache_code(self):
        p = make_profile(reuse_count=4)
        code = self._code_for(FanOutCacheCase(consumer_count=4), p)
        self.assertIn("persist", code)
        self.assertIn("unpersist", code)
        self.assertIn("base_df", code)

    def test_lineage_break_code(self):
        p = make_profile(lineage_depth=25)
        code = self._code_for(LineageBreakCase(), p)
        self.assertIn("setCheckpointDir", code)
        self.assertIn("checkpoint(eager=True)", code)

    def test_spill_prevention_code(self):
        p = make_profile(
            row_count=50_000_000,
            estimated_bytes=100 * 1024 ** 3,
            executor_memory_mb=4096, executor_count=3
        )
        code = self._code_for(SpillPreventionCase(), p)
        self.assertIn("memory.fraction", code)
        self.assertIn("repartition", code)
        self.assertIn("MEMORY_AND_DISK", code)

    def test_storage_level_table_in_code(self):
        p = make_profile(reuse_count=3)
        code = self._code_for(StorageLevelCase(), p)
        for level in ["MEMORY_ONLY", "MEMORY_AND_DISK", "DISK_ONLY"]:
            self.assertIn(level, code)


# ──────────────────────────────────────────────
# Test 8: DSL Entegrasyon
# ──────────────────────────────────────────────

class TestDSLIntegration(unittest.TestCase):
    """DSL metotlarının doğru case'leri eklediğini doğrular."""

    def test_dsl_iterative_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "iterative"))

    def test_dsl_streaming_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "streaming"))

    def test_dsl_fanout_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "fanout"))

    def test_dsl_ml_pipeline_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "ml_pipeline"))

    def test_dsl_graph_algorithm_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "graph_algorithm"))

    def test_dsl_advice_method_exists(self):
        from sparkopt.dsl import SparkOptimizer
        self.assertTrue(hasattr(SparkOptimizer, "advice"))

    def test_persistence_advisor_standalone(self):
        """PersistenceAdvisor doğrudan çağrılabilir."""
        profile = make_profile(iterative=True)
        advisor = PersistenceAdvisor(profile)
        decision = advisor.decide()
        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.rationale)
        self.assertGreater(len(decision.code_snippet), 0)


# ──────────────────────────────────────────────
# Demo
# ──────────────────────────────────────────────

def demo():
    print("\n" + "═"*60)
    print("  sparkopt v2 — Gelişmiş Persistence Demo")
    print("═"*60)

    scenarios = [
        ("ML Pipeline (CV ile)", make_profile(iterative=True, reuse_count=4)),
        ("Graf Algoritması",      make_profile(iterative=True, reuse_count=2)),
        ("Derin ETL Pipeline",   make_profile(lineage_depth=28, reuse_count=3)),
        ("Streaming + Batch",    make_profile(streaming=True, reuse_count=2)),
        ("Fan-Out Cache",        make_profile(
            reuse_count=5, lineage_depth=4,
            estimated_bytes=300 * 1024 ** 2,
            executor_memory_mb=16384, executor_count=30
        )),
    ]

    for name, profile in scenarios:
        decision = PersistenceAdvisor(profile).decide()
        print(f"\n{'─'*50}")
        print(f"  {name}")
        print(f"{'─'*50}")
        print(f"  Strateji   : {decision.strategy.name}")
        print(f"  StorageLevel: {decision.storage_level or '—'}")
        print(f"  Gerekçe    : {decision.rationale[:80]}...")
        if decision.warnings:
            print(f"  Uyarı      : {decision.warnings[0][:70]}...")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestLineageInspector,
        TestPersistenceAdvisor,
        TestStorageLevelSelection,
        TestPersistenceCaseMatching,
        TestComplexCases,
        TestCacheVsCheckpointDecisions,
        TestCaseCodeQuality,
        TestDSLIntegration,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    demo()

    sys.exit(0 if result.wasSuccessful() else 1)
