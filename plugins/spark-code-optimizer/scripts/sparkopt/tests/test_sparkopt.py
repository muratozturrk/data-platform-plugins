"""
sparkopt testleri — persistence + complex scenario coverage
"""
from __future__ import annotations
import sys, os, unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from sparkopt.core.analyzer import (
    DataProfile, PartitionProfile, ColumnProfile,
    LineageProfile, MemoryProfile,
)
from sparkopt.core.base import (
    OptimizationCase, Priority, CaseRegistry,
    AndCase, OrCase, NotCase,
)
from sparkopt.codegen.context import CodegenContext, CodeRenderer
from sparkopt.cases import (
    # Persistence
    PersistenceStrategyCase, IterativeLoopCase, FanOutCacheCase,
    LineageBreakCase, StorageLevelCase, HybridCheckpointCase,
    SpillPreventionCase,
    # Join
    BroadcastJoinCase, SkewJoinCase,
    # Aggregation
    PartialAggCase, ApproxDistinctCase,
    # Partition
    AdaptiveCase, RepartitionCase, CoalesceCase,
)


# ── Yardımcılar ───────────────────────────────

def make_profile(
    row_count: int = 1_000_000,
    estimated_bytes: int = 500 * 1024 ** 2,
    partition_count: int = 200,
    skew_ratio: float = 1.5,
    lineage_depth: int = 5,
    reuse_count: int = 1,
    iterative: bool = False,
    streaming: bool = False,
    executor_memory_mb: int = 4096,
    executor_count: int = 10,
) -> DataProfile:
    avg = row_count / partition_count
    return DataProfile(
        row_count=row_count,
        col_count=10,
        estimated_bytes=estimated_bytes,
        partition=PartitionProfile(
            count=partition_count,
            min_rows=int(avg * 0.5),
            max_rows=int(avg * skew_ratio),
            avg_rows=avg,
            skew_ratio=skew_ratio,
            empty_count=0,
        ),
        lineage=LineageProfile(
            depth=lineage_depth,
            has_iterative_loop=iterative,
            wide_transform_count=2,
            estimated_reuse_count=reuse_count,
            has_streaming_source=streaming,
        ),
        memory=MemoryProfile(
            executor_memory_mb=executor_memory_mb,
            executor_count=executor_count,
            df_size_mb=estimated_bytes / (1024 ** 2),
        ),
    )


def make_ctx(profile: DataProfile | None = None) -> CodegenContext:
    return CodegenContext(
        profile=profile or make_profile(),
        join_cols=["user_id"],
        agg_cols=["category"],
        df_name="df",
        spark_parallelism=200,
    )


# ════════════════════════════════════════════════
# 1. Persistence Karar Ağacı
# ════════════════════════════════════════════════

class TestPersistenceDecisionTree(unittest.TestCase):

    def test_iterative_triggers_local_checkpoint(self):
        profile = make_profile(iterative=True, reuse_count=1)
        case = PersistenceStrategyCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("local_checkpoint", ctx.hints)
        self.assertIn("localCheckpoint", ctx.hints["local_checkpoint"])

    def test_deep_dag_triggers_reliable_checkpoint(self):
        profile = make_profile(lineage_depth=25, reuse_count=1, iterative=False)
        case = PersistenceStrategyCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("reliable_checkpoint", ctx.hints)
        self.assertIn("checkpoint(eager=True)", ctx.hints["reliable_checkpoint"])

    def test_fanout_triggers_cache(self):
        profile = make_profile(reuse_count=4, lineage_depth=5)
        case = PersistenceStrategyCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("cache_persist", ctx.hints)
        self.assertIn("persist(StorageLevel.", ctx.hints["cache_persist"])

    def test_single_use_no_persist(self):
        profile = make_profile(reuse_count=1, lineage_depth=3, iterative=False)
        case = PersistenceStrategyCase()
        self.assertFalse(case.matches(profile))

    def test_iterative_loop_case_matches(self):
        profile = make_profile(iterative=True)
        case = IterativeLoopCase(checkpoint_interval=5)
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("iterative_loop_checkpoint", ctx.hints)
        self.assertIn("localCheckpoint", ctx.hints["iterative_loop_checkpoint"])
        self.assertIn("% ckpt_interval", ctx.hints["iterative_loop_checkpoint"])

    def test_iterative_loop_no_match_without_flag(self):
        profile = make_profile(iterative=False)
        self.assertFalse(IterativeLoopCase().matches(profile))

    def test_fanout_cache_case_matches(self):
        profile = make_profile(reuse_count=3)
        case = FanOutCacheCase(consumer_count=3)
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("fanout_cache", ctx.hints)
        self.assertIn("unpersist", ctx.hints["fanout_cache"])

    def test_lineage_break_triggers_above_threshold(self):
        deep = make_profile(lineage_depth=25)
        shallow = make_profile(lineage_depth=10)
        case = LineageBreakCase()
        self.assertTrue(case.matches(deep))
        self.assertFalse(case.matches(shallow))

    def test_lineage_break_code_contains_checkpoint_dir(self):
        profile = make_profile(lineage_depth=25)
        ctx = make_ctx(profile)
        LineageBreakCase().apply(ctx)
        self.assertIn("lineage_break", ctx.hints)
        self.assertIn("setCheckpointDir", ctx.hints["lineage_break"])

    def test_hybrid_checkpoint_for_streaming(self):
        profile = make_profile(streaming=True)
        case = HybridCheckpointCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("hybrid_checkpoint", ctx.hints)
        code = ctx.hints["hybrid_checkpoint"]
        self.assertIn("readStream", code)
        self.assertIn("checkpointLocation", code)
        self.assertIn("broadcast", code)

    def test_no_hybrid_checkpoint_without_streaming(self):
        profile = make_profile(streaming=False)
        self.assertFalse(HybridCheckpointCase().matches(profile))


# ════════════════════════════════════════════════
# 2. StorageLevel Seçimi
# ════════════════════════════════════════════════

class TestStorageLevelSelection(unittest.TestCase):

    def test_small_df_fits_memory(self):
        profile = make_profile(
            estimated_bytes=100 * 1024 ** 2,  # 100 MB
            executor_memory_mb=8192,
            executor_count=20,
        )
        self.assertTrue(profile.memory.fits_in_memory)

    def test_large_df_does_not_fit(self):
        profile = make_profile(
            estimated_bytes=50 * 1024 ** 3,   # 50 GB
            executor_memory_mb=4096,
            executor_count=5,
        )
        self.assertFalse(profile.memory.fits_in_memory)

    def test_memory_only_ser_for_fitting_df(self):
        profile = make_profile(
            estimated_bytes=200 * 1024 ** 2,
            executor_memory_mb=8192,
            executor_count=20,
        )
        self.assertEqual(profile.memory.recommended_storage_level, "MEMORY_ONLY")

    def test_disk_only_for_huge_df(self):
        profile = make_profile(
            estimated_bytes=200 * 1024 ** 3,  # 200 GB
            executor_memory_mb=4096,
            executor_count=5,
        )
        self.assertEqual(profile.memory.recommended_storage_level, "DISK_ONLY")

    def test_storage_level_case_adds_hint(self):
        profile = make_profile(reuse_count=3, estimated_bytes=300 * 1024 ** 2)
        case = StorageLevelCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("storage_level_selection", ctx.hints)
        self.assertIn("StorageLevel", ctx.hints["storage_level_selection"])

    def test_spill_prevention_triggers_when_df_too_large(self):
        profile = make_profile(
            row_count=10_000_000,
            estimated_bytes=20 * 1024 ** 3,  # 20 GB
            executor_memory_mb=4096,
            executor_count=5,
        )
        case = SpillPreventionCase()
        self.assertTrue(case.matches(profile))
        ctx = make_ctx(profile)
        case.apply(ctx)
        self.assertIn("spill_prevention", ctx.hints)
        self.assertIn("repartition", ctx.hints["spill_prevention"])
        self.assertIn("spark.memory.fraction", ctx.configs)

    def test_spill_prevention_no_trigger_small_df(self):
        profile = make_profile(
            row_count=100_000,
            estimated_bytes=10 * 1024 ** 2,
            executor_memory_mb=8192,
            executor_count=20,
        )
        self.assertFalse(SpillPreventionCase().matches(profile))


# ════════════════════════════════════════════════
# 3. Lineage Profil
# ════════════════════════════════════════════════

class TestLineageProfile(unittest.TestCase):

    def test_needs_checkpoint_deep_dag(self):
        lin = LineageProfile(depth=25)
        self.assertTrue(lin.needs_checkpoint)

    def test_needs_checkpoint_iterative(self):
        lin = LineageProfile(depth=5, has_iterative_loop=True)
        self.assertTrue(lin.needs_checkpoint)

    def test_no_checkpoint_shallow_dag(self):
        lin = LineageProfile(depth=8)
        self.assertFalse(lin.needs_checkpoint)

    def test_local_checkpoint_for_iterative_non_streaming(self):
        lin = LineageProfile(has_iterative_loop=True, has_streaming_source=False)
        self.assertTrue(lin.needs_local_checkpoint)

    def test_no_local_checkpoint_for_streaming(self):
        lin = LineageProfile(has_iterative_loop=True, has_streaming_source=True)
        self.assertFalse(lin.needs_local_checkpoint)

    def test_reliable_checkpoint_deep_non_iterative(self):
        lin = LineageProfile(depth=25, has_iterative_loop=False)
        self.assertTrue(lin.needs_reliable_checkpoint)


# ════════════════════════════════════════════════
# 4. Mevcut Case'ler (regresyon)
# ════════════════════════════════════════════════

class TestExistingCases(unittest.TestCase):

    def test_broadcast_small(self):
        self.assertTrue(BroadcastJoinCase().matches(make_profile(estimated_bytes=50*1024**2)))

    def test_broadcast_large(self):
        self.assertFalse(BroadcastJoinCase().matches(make_profile(estimated_bytes=5*1024**3)))

    def test_skew_join(self):
        self.assertTrue(SkewJoinCase().matches(make_profile(skew_ratio=5.0)))
        self.assertFalse(SkewJoinCase().matches(make_profile(skew_ratio=1.2)))

    def test_aqe_always(self):
        self.assertTrue(AdaptiveCase().matches(make_profile()))

    def test_repartition_few_partitions(self):
        self.assertTrue(RepartitionCase().matches(make_profile(partition_count=4)))

    def test_approx_distinct_big(self):
        self.assertTrue(ApproxDistinctCase().matches(make_profile(row_count=10_000_000)))
        self.assertFalse(ApproxDistinctCase().matches(make_profile(row_count=100_000)))


# ════════════════════════════════════════════════
# 5. Kompozisyon Operatörleri
# ════════════════════════════════════════════════

class TestComposition(unittest.TestCase):

    def test_and(self):
        a, b = AdaptiveCase(), PartialAggCase()
        composed = a & b
        self.assertIsInstance(composed, AndCase)
        self.assertTrue(composed.matches(make_profile(row_count=5_000_000)))

    def test_or(self):
        iterative_profile = make_profile(iterative=True)
        composed = IterativeLoopCase() | LineageBreakCase()
        self.assertIsInstance(composed, OrCase)
        self.assertTrue(composed.matches(iterative_profile))

    def test_not(self):
        composed = ~BroadcastJoinCase()
        self.assertIsInstance(composed, NotCase)
        large = make_profile(estimated_bytes=5*1024**3)
        self.assertTrue(composed.matches(large))

    def test_complex_chain(self):
        # (iterative OR deep_dag) AND aqe
        p_iterative = make_profile(iterative=True)
        composed = (IterativeLoopCase() | LineageBreakCase()) & AdaptiveCase()
        self.assertTrue(composed.matches(p_iterative))

    def test_persistence_or_spill(self):
        """Fan-out OR spill riski → her ikisini de yakala."""
        fanout = make_profile(reuse_count=4)
        composed = FanOutCacheCase() | SpillPreventionCase()
        self.assertTrue(composed.matches(fanout))


# ════════════════════════════════════════════════
# 6. CodegenContext ve Renderer
# ════════════════════════════════════════════════

class TestCodegen(unittest.TestCase):

    def test_header_contains_lineage_info(self):
        profile = make_profile(lineage_depth=15, reuse_count=3, iterative=True)
        ctx = make_ctx(profile)
        AdaptiveCase().apply(ctx)
        ctx.mark_applied("adaptive_qe")
        code = CodeRenderer(ctx).render()
        self.assertIn("DAG derinliği", code)
        self.assertIn("İteratif", code)
        self.assertIn("Executor RAM", code)

    def test_dedup_imports(self):
        ctx = make_ctx()
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_import("from pyspark.sql import functions as F")
        self.assertEqual(len(ctx.imports), 1)

    def test_aqe_configs_present(self):
        ctx = make_ctx()
        AdaptiveCase().apply(ctx)
        self.assertIn("spark.sql.adaptive.enabled", ctx.configs)

    def test_spill_prevention_config(self):
        profile = make_profile(
            row_count=10_000_000,
            estimated_bytes=20 * 1024 ** 3,
            executor_memory_mb=4096, executor_count=5,
        )
        ctx = make_ctx(profile)
        SpillPreventionCase().apply(ctx)
        self.assertIn("spark.memory.fraction", ctx.configs)

    def test_full_render_sections(self):
        ctx = make_ctx()
        ctx.add_import("import pyspark")
        ctx.add_config("k", "v")
        ctx.add_transformation("t1", "df = df.filter(...)")
        ctx.add_hint("h1", "# hint")
        ctx.add_note("n1", "# not")
        ctx.add_warning("Dikkat!")
        code = CodeRenderer(ctx).render()
        for kw in ["import", "konfig", "dönüşüm", "öneri", "notlar", "UYARI"]:
            self.assertIn(kw, code)


# ════════════════════════════════════════════════
# 7. Senaryo Entegrasyon Testleri
# ════════════════════════════════════════════════

class TestScenarios(unittest.TestCase):

    def test_scenario_iterative_ml(self):
        """İteratif ML: localCheckpoint + iterativeLoopCase."""
        profile = make_profile(
            row_count=5_000_000,
            estimated_bytes=2 * 1024 ** 3,
            iterative=True,
            reuse_count=1,
            executor_memory_mb=16384,
            executor_count=20,
        )
        ctx = make_ctx(profile)
        PersistenceStrategyCase().apply(ctx)
        IterativeLoopCase(checkpoint_interval=5).apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("localCheckpoint", code)
        self.assertIn("% 5", code)

    def test_scenario_deep_pipeline(self):
        """Derin pipeline: reliable checkpoint."""
        profile = make_profile(
            lineage_depth=30,
            reuse_count=1,
            iterative=False,
        )
        ctx = make_ctx(profile)
        PersistenceStrategyCase().apply(ctx)
        LineageBreakCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("setCheckpointDir", code)

    def test_scenario_fanout(self):
        """Fan-out: 4 consumer, cache + StorageLevel."""
        profile = make_profile(
            row_count=10_000_000,
            estimated_bytes=500 * 1024 ** 2,
            reuse_count=4,
            executor_memory_mb=8192,
            executor_count=20,
        )
        ctx = make_ctx(profile)
        FanOutCacheCase(consumer_count=4).apply(ctx)
        StorageLevelCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("persist(StorageLevel", code)
        self.assertIn("unpersist", code)

    def test_scenario_streaming_batch_hybrid(self):
        """Streaming + batch join."""
        profile = make_profile(streaming=True, reuse_count=2)
        ctx = make_ctx(profile)
        HybridCheckpointCase().apply(ctx)
        StorageLevelCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("readStream", code)
        self.assertIn("checkpointLocation", code)
        self.assertIn("persist(StorageLevel", code)

    def test_scenario_spill_risk(self):
        """Büyük DF, küçük cluster → spill önleme."""
        profile = make_profile(
            row_count=50_000_000,
            estimated_bytes=40 * 1024 ** 3,
            executor_memory_mb=4096,
            executor_count=5,
        )
        ctx = make_ctx(profile)
        SpillPreventionCase().apply(ctx)
        code = CodeRenderer(ctx).render()
        self.assertIn("repartition", code)
        self.assertIn("MEMORY_AND_DISK", code)

    def test_scenario_composed_case(self):
        """Özel case + built-in compose."""
        class CustomCase(OptimizationCase):
            name = "custom_opt"
            priority = Priority.MEDIUM
            def matches(self, p): return p.row_count > 1000
            def apply(self, ctx): ctx.add_hint("custom", "# özel ipucu")
            def explain(self): return "Özel"

        profile = make_profile(row_count=5_000_000, iterative=True)
        composed = CustomCase() & IterativeLoopCase()
        self.assertTrue(composed.matches(profile))
        ctx = make_ctx(profile)
        composed.apply(ctx)
        self.assertIn("custom", ctx.hints)
        self.assertIn("iterative_loop_checkpoint", ctx.hints)


# ── Demo ──────────────────────────────────────

def demo():
    print("\n" + "="*60)
    print("  sparkopt v2 — Persistence Demo")
    print("="*60)

    scenarios = [
        ("İteratif ML", make_profile(iterative=True, reuse_count=1, lineage_depth=8,
                                      executor_memory_mb=16384, executor_count=50)),
        ("Fan-out (4 consumer)", make_profile(reuse_count=4, estimated_bytes=400*1024**2,
                                               executor_memory_mb=8192, executor_count=20)),
        ("Derin DAG (30 adım)", make_profile(lineage_depth=30, reuse_count=1)),
        ("Streaming+Batch", make_profile(streaming=True, reuse_count=2)),
        ("Spill Riski",  make_profile(row_count=50_000_000, estimated_bytes=40*1024**3,
                                       executor_memory_mb=4096, executor_count=5)),
    ]

    persistence_cases = [
        PersistenceStrategyCase(), IterativeLoopCase(),
        FanOutCacheCase(), LineageBreakCase(),
        HybridCheckpointCase(), SpillPreventionCase(),
    ]

    for scenario_name, profile in scenarios:
        matched = [c for c in persistence_cases if c.matches(profile)]
        print(f"\n  [{scenario_name}]")
        print(f"    DAG={profile.lineage.depth}, reuse={profile.lineage.estimated_reuse_count}x, "
              f"iterative={profile.lineage.has_iterative_loop}, "
              f"streaming={profile.lineage.has_streaming_source}")
        print(f"    RAM: {profile.memory.executor_memory_mb}MB×{profile.memory.executor_count}, "
              f"DF:{profile.estimated_mb:.0f}MB, "
              f"level={profile.memory.recommended_storage_level}")
        for c in matched:
            print(f"    → [{c.name}] {c.explain()[:70]}...")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestPersistenceDecisionTree,
        TestStorageLevelSelection,
        TestLineageProfile,
        TestExistingCases,
        TestComposition,
        TestCodegen,
        TestScenarios,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    demo()
    sys.exit(0 if result.wasSuccessful() else 1)
