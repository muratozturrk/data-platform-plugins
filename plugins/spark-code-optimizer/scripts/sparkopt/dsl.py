"""
sparkopt.dsl
============
Ana fluent DSL.

Kullanım:
    from sparkopt import SparkOptimizer

    # Senaryo 1: Büyük tablo, fan-out
    code, report = (
        SparkOptimizer(df, spark)
        .reused(times=4)
        .with_executor(memory_mb=8192, count=20)
        .group_by("category")
        .build()
    )

    # Senaryo 2: İteratif ML
    code, report = (
        SparkOptimizer(features_df, spark)
        .iterative(checkpoint_interval=5)
        .with_executor(memory_mb=16384, count=50)
        .build()
    )

    # Senaryo 3: Derin pipeline
    code, report = (
        SparkOptimizer(df, spark)
        .lineage_depth(25)
        .with_join(right_df, on=["user_id"])
        .build()
    )
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from sparkopt.core.analyzer import DataFrameAnalyzer, DataProfile
from sparkopt.core.base import OptimizationCase, CaseRegistry
from sparkopt.cases import ALL_BUILTIN_CASES
from sparkopt.codegen.context import CodegenContext, CodeRenderer

_registry = CaseRegistry.instance()
if not _registry.all_cases():
    _registry.register_all(ALL_BUILTIN_CASES)


class OptimizationReport:
    def __init__(
        self,
        profile: DataProfile,
        matched_cases: list[OptimizationCase],
        generated_code: str,
    ):
        self.profile = profile
        self.matched_cases = matched_cases
        self.generated_code = generated_code

    def summary(self) -> str:
        p = self.profile
        lin = p.lineage
        mem = p.memory
        lines = [
            "╔══════════════════════════════════════════════════╗",
            "║         sparkopt — Optimizasyon Raporu           ║",
            "╠══════════════════════════════════════════════════╣",
            f"║  Satır sayısı  : {p.row_count:>12,}                  ║",
            f"║  Boyut         : {p.estimated_mb:>10.1f} MB                ║",
            f"║  Partition     : {p.partition.count:>12} (skew={p.partition.skew_ratio:.1f}x)   ║",
            "╠══════════════════════════════════════════════════╣",
            f"║  DAG derinliği : {lin.depth:>12} adım               ║",
            f"║  Reuse sayısı  : {lin.estimated_reuse_count:>12}x                  ║",
            f"║  İteratif      : {'Evet':>12}                  ║" if lin.has_iterative_loop else
            f"║  İteratif      : {'Hayır':>12}                  ║",
            f"║  Executor RAM  : {mem.executor_memory_mb:>8} MB × {mem.executor_count:<5}           ║",
            f"║  Önerilen lvl  : {mem.recommended_storage_level:>20}          ║",
            "╠══════════════════════════════════════════════════╣",
            "║  Uygulanan optimizasyonlar:                      ║",
        ]
        for c in self.matched_cases:
            tag = f"[{c.name}]"
            lines.append(f"║  ✓ {tag:<22} prio={int(c.priority):<4}          ║")
        lines.append("╠══════════════════════════════════════════════════╣")
        lines.append("║  Açıklamalar:                                    ║")
        for c in self.matched_cases:
            for i, line in enumerate(textwrap.wrap(c.explain(), width=46)):
                prefix = "  • " if i == 0 else "    "
                lines.append(f"║{prefix}{line:<46}║")
        lines.append("╚══════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self.summary()


class SparkOptimizer:
    """
    Fluent builder API.
    Her metot self döndürür (chain), build() ile sonlanır.
    """

    def __init__(
        self,
        df: DataFrame,
        spark: Optional[SparkSession] = None,
        sample_fraction: float = 0.05,
    ):
        self._df = df
        self._spark = spark or df.sparkSession
        self._sample_fraction = sample_fraction

        self._join_cols: list[str] = []
        self._agg_cols: list[str] = []
        self._right_df: Optional[DataFrame] = None
        self._df_name: str = "df"
        self._custom_cases: list[OptimizationCase] = []
        self._exclude_cases: set[str] = set()
        self._force_full_scan: bool = False

        # Lineage parametreleri
        self._lineage_depth: int = 0
        self._reuse_count: int = 1
        self._iterative: bool = False
        self._checkpoint_interval: int = 5
        self._streaming: bool = False

        # Cluster parametreleri
        self._executor_memory_mb: int = 4096
        self._executor_count: int = 10

    # ── Temel yapılandırma ──────────────────────

    def named(self, name: str) -> "SparkOptimizer":
        self._df_name = name
        return self

    def with_join(
        self, right_df: DataFrame, on: list[str], how: str = "inner",
    ) -> "SparkOptimizer":
        self._right_df = right_df
        self._join_cols = on
        return self

    def group_by(self, *cols: str) -> "SparkOptimizer":
        self._agg_cols = list(cols)
        return self

    def full_scan(self) -> "SparkOptimizer":
        self._force_full_scan = True
        return self

    # ── Lineage / persistence parametreleri ────

    def reused(self, times: int) -> "SparkOptimizer":
        """Bu DF kaç farklı noktada tüketiliyor?"""
        self._reuse_count = times
        return self

    def lineage_depth(self, depth: int) -> "SparkOptimizer":
        """DAG derinliğini manuel belirt (0 → otomatik tahmin)."""
        self._lineage_depth = depth
        return self

    def iterative(self, checkpoint_interval: int = 5) -> "SparkOptimizer":
        """İteratif ML/graph döngüsü — localCheckpoint stratejisini aktif et."""
        self._iterative = True
        self._checkpoint_interval = checkpoint_interval
        from sparkopt.cases import IterativeLoopCase
        self._custom_cases.append(IterativeLoopCase(checkpoint_interval))
        return self

    def streaming(self) -> "SparkOptimizer":
        """Streaming kaynak var — hybrid checkpoint stratejisini aktif et."""
        self._streaming = True
        return self

    # ── Cluster parametreleri ───────────────────

    def with_executor(
        self,
        memory_mb: int = 4096,
        count: int = 10,
    ) -> "SparkOptimizer":
        """Executor bellek ve sayısını bildir — StorageLevel seçimi için."""
        self._executor_memory_mb = memory_mb
        self._executor_count = count
        return self

    # ── Kısayol metotları ───────────────────────

    def when_skewed(self, *cols: str, salt_buckets: int = 10) -> "SparkOptimizer":
        from sparkopt.cases import SkewJoinCase
        self._custom_cases.append(SkewJoinCase(cols[0] if cols else None, salt_buckets))
        return self

    def use_approx_distinct(self) -> "SparkOptimizer":
        from sparkopt.cases import ApproxDistinctCase
        self._custom_cases.append(ApproxDistinctCase())
        return self

    def read_parquet(self, path: str = "") -> "SparkOptimizer":
        from sparkopt.cases import ParquetPushdownCase
        self._custom_cases.append(ParquetPushdownCase())
        return self

    def add_case(self, case: OptimizationCase) -> "SparkOptimizer":
        self._custom_cases.append(case)
        return self

    def exclude(self, *case_names: str) -> "SparkOptimizer":
        self._exclude_cases.update(case_names)
        return self

    # ── build ───────────────────────────────────

    def build(self) -> tuple[str, OptimizationReport]:
        profile = DataFrameAnalyzer(
            self._df, self._sample_fraction
        ).analyze(
            join_cols=self._join_cols or None,
            agg_cols=self._agg_cols or None,
            full_partition_scan=self._force_full_scan,
            lineage_depth=self._lineage_depth,
            reuse_count=self._reuse_count,
            iterative=self._iterative,
            streaming=self._streaming,
            executor_memory_mb=self._executor_memory_mb,
            executor_count=self._executor_count,
        )

        right_profile = None
        if self._right_df is not None:
            right_profile = DataFrameAnalyzer(
                self._right_df, self._sample_fraction
            ).analyze()

        parallelism = self._spark.sparkContext.defaultParallelism
        ctx = CodegenContext(
            profile=profile,
            join_cols=self._join_cols,
            agg_cols=self._agg_cols,
            df_name=self._df_name,
            spark_parallelism=parallelism,
        )

        all_cases = list(_registry.all_cases())
        if right_profile is not None:
            from sparkopt.cases import BroadcastJoinCase
            all_cases = [
                BroadcastJoinCase(right_profile.estimated_bytes)
                if isinstance(c, BroadcastJoinCase) else c
                for c in all_cases
            ]
        # Kullanıcının eklediği case, aynı isimli built-in case'in yerine geçer
        # (örn. .iterative(checkpoint_interval=3) → tek iterative_loop).
        custom_names = {c.name for c in self._custom_cases}
        all_cases = [c for c in all_cases if c.name not in custom_names]
        all_cases.extend(self._custom_cases)

        matched = [
            c for c in all_cases
            if c.matches(profile) and c.name not in self._exclude_cases
        ]
        matched.sort(key=lambda c: c.priority, reverse=True)

        for case in matched:
            case.apply(ctx)
            ctx.mark_applied(case.name)

        if getattr(self, "_use_persistence_advisor", False):
            from sparkopt.core.persistence_advisor import PersistenceAdvisor
            decision = PersistenceAdvisor(profile).decide()
            ctx.add_hint("persistence_decision", decision.code_snippet)
            ctx.add_note("persistence_decision", f"# {decision.strategy.name}: {decision.rationale}")
            for w in decision.warnings:
                ctx.add_warning(w)

        code = CodeRenderer(ctx).render()
        report = OptimizationReport(profile=profile, matched_cases=matched, generated_code=code)
        return code, report

    def analyze_only(self) -> DataProfile:
        return DataFrameAnalyzer(self._df, self._sample_fraction).analyze(
            join_cols=self._join_cols or None,
            agg_cols=self._agg_cols or None,
            lineage_depth=self._lineage_depth,
            reuse_count=self._reuse_count,
            iterative=self._iterative,
            streaming=self._streaming,
            executor_memory_mb=self._executor_memory_mb,
            executor_count=self._executor_count,
        )

    def explain(self) -> str:
        profile = self.analyze_only()
        matched = _registry.find_matches(profile)
        lines = [f"Tespit edilen {len(matched)} optimizasyon:\n"]
        for c in matched:
            lines.append(f"  [{c.priority.name:8}] {c.name}")
            lines.append(f"           → {c.explain()}\n")
        return "\n".join(lines)


# ── Yeni DSL metotları (monkey-patch yerine mixin ile) ──────────────────────

def _iterative(self, checkpoint_interval: int = 5) -> "SparkOptimizer":
    """İteratif ML/graph döngüsü olduğunu belirtir."""
    from sparkopt.cases import IterativeLoopCase
    self._custom_cases.append(IterativeLoopCase(checkpoint_interval))
    self._iterative = True
    self._checkpoint_interval = checkpoint_interval
    return self

def _streaming(self) -> "SparkOptimizer":
    """Streaming kaynak içerdiğini belirtir."""
    from sparkopt.cases import HybridCheckpointCase
    self._custom_cases.append(HybridCheckpointCase())
    self._streaming = True
    return self

def _fanout(self, consumer_count: int = 3) -> "SparkOptimizer":
    """Aynı DF'nin N farklı noktada tüketileceğini belirtir."""
    from sparkopt.cases import FanOutCacheCase
    self._custom_cases.append(FanOutCacheCase(consumer_count))
    return self

def _ml_pipeline(self, has_cv: bool = False, num_folds: int = 5) -> "SparkOptimizer":
    """ML pipeline senaryosu — feature fan-out + iteratif eğitim."""
    from sparkopt.cases.complex_cases import MLPipelineCase
    self._custom_cases.append(MLPipelineCase(has_cv=has_cv, num_folds=num_folds))
    self._iterative = True
    return self

def _graph_algorithm(
    self, algorithm: str = "pagerank", checkpoint_interval: int = 5
) -> "SparkOptimizer":
    """İteratif graf algoritması — vertex/edge persist + localCheckpoint."""
    from sparkopt.cases.complex_cases import GraphAlgorithmCase
    self._custom_cases.append(
        GraphAlgorithmCase(algorithm=algorithm, checkpoint_interval=checkpoint_interval)
    )
    self._iterative = True
    return self

def _incremental_etl(self) -> "SparkOptimizer":
    """Upsert/merge tabanlı incremental pipeline."""
    from sparkopt.cases.complex_cases import IncrementalETLCase
    self._custom_cases.append(IncrementalETLCase())
    return self

def _with_persistence_advisor(self) -> "SparkOptimizer":
    """
    build() öncesinde PersistenceAdvisor'ı çalıştırır —
    çakışan case önerilerini tek bir kararla çözer.
    """
    self._use_persistence_advisor = True
    return self

def _advice(self) -> str:
    """
    build() yapmadan PersistenceAdvisor kararını göster.
    Sadece profil analizi + advisor çalışır.
    """
    profile = self.analyze_only()
    from sparkopt.core.persistence_advisor import PersistenceAdvisor
    decision = PersistenceAdvisor(profile).decide()
    lines = [
        "═══ PersistenceAdvisor Kararı ═══",
        f"Strateji  : {decision.strategy.name}",
        f"StorageLevel: {decision.storage_level or '—'}",
        f"Gerekçe   : {decision.rationale}",
    ]
    if decision.warnings:
        lines.append("Uyarılar  :")
        for w in decision.warnings:
            lines.append(f"  ⚠  {w}")
    lines.append("\n── Kod ──")
    lines.append(decision.code_snippet)
    return "\n".join(lines)


# Metotları SparkOptimizer'a bağla
SparkOptimizer.iterative        = _iterative
SparkOptimizer.streaming        = _streaming
SparkOptimizer.fanout           = _fanout
SparkOptimizer.ml_pipeline      = _ml_pipeline
SparkOptimizer.graph_algorithm  = _graph_algorithm
SparkOptimizer.incremental_etl  = _incremental_etl
SparkOptimizer.with_persistence_advisor = _with_persistence_advisor
SparkOptimizer.advice           = _advice
