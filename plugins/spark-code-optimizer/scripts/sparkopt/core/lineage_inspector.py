"""
sparkopt.core.lineage_inspector
=================================
Spark'ın fiziksel planını parse ederek DAG özelliklerini otomatik çıkarır.
Kullanıcının `lineage_depth` veya `iterative=True` söylemesine gerek kalmaz.

Ne tespit eder:
  - Plan derinliği (Exchange, Project, Filter sayısı)
  - Shuffle adım sayısı (Exchange → wide transformation)
  - Reuse: aynı plan alt-ağacının birden fazla yerde geçmesi
  - Iteratif işaret: plan string'inde döngü patikası
  - Streaming: StreamingRelation/MicroBatch planı
  - Cached: plan'da InMemoryRelation var mı (zaten cache'li)
  - Join türleri: BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class PlanFeatures:
    """Spark plan'dan çıkarılan özellikler."""
    depth: int = 0
    exchange_count: int = 0          # shuffle sayısı (wide transformations)
    join_types: list[str] = field(default_factory=list)
    has_broadcast_join: bool = False
    has_sort_merge_join: bool = False
    has_shuffled_hash_join: bool = False
    has_in_memory_relation: bool = False    # zaten cache'li
    has_streaming: bool = False
    estimated_reuse: int = 1
    plan_size_chars: int = 0         # plan string boyutu → karmaşıklık göstergesi
    has_cross_join: bool = False
    subquery_count: int = 0
    window_count: int = 0
    agg_count: int = 0
    filter_count: int = 0
    # Iterasyon sinyalleri (genellikle dışarıdan set edilir)
    is_iterative: bool = False

    @property
    def is_complex(self) -> bool:
        """Plan karmaşık mı? Checkpoint değerlendirmesi için."""
        return (
            self.depth > 15
            or self.exchange_count > 3
            or self.plan_size_chars > 5000
            or self.subquery_count > 2
        )

    @property
    def lineage_break_needed(self) -> bool:
        return self.depth > 20 or self.exchange_count > 5

    @property
    def recommended_depth_label(self) -> str:
        if self.depth <= 5: return "shallow"
        if self.depth <= 15: return "moderate"
        if self.depth <= 30: return "deep"
        return "very_deep"


class LineageInspector:
    """
    DataFrame.explain() çıktısını veya queryExecution planını analiz eder.

    Kullanım:
        inspector = LineageInspector(df)
        features = inspector.inspect()
        # features.depth, features.exchange_count, vb.
    """

    # Plan satırlarında aranacak pattern'lar
    _EXCHANGE_RE  = re.compile(r'\bExchange\b', re.I)
    _SMJ_RE       = re.compile(r'\bSortMergeJoin\b', re.I)
    # Operatör adları Spark 3.5 explain() çıktısına göre (StreamingRelationV2,
    # CartesianProduct, InMemoryTableScan, ObjectHashAggregate tam adlarıyla geçer).
    _BHJ_RE       = re.compile(r'\bBroadcastHashJoin\b|\bBroadcastNestedLoopJoin\b', re.I)
    _SHJ_RE       = re.compile(r'\bShuffledHashJoin\b', re.I)
    _IMR_RE       = re.compile(r'\bInMemoryRelation\b|\bInMemoryTableScan\b', re.I)
    _STREAM_RE    = re.compile(r'\bStreamingRelation(?:V2)?\b|\bMicroBatch\w*\b|\bStreamExec\b|\bStreamingDataSourceV2\w*\b', re.I)
    _CROSS_RE     = re.compile(r'\bBroadcastNestedLoopJoin\b|\bCartesianProduct\b', re.I)
    _SUBQ_RE      = re.compile(r'\bSubquery\b|\bScalarSubquery\b', re.I)
    _WINDOW_RE    = re.compile(r'\bWindow\b', re.I)
    _AGG_RE       = re.compile(r'\b(?:Hash|Sort|ObjectHash)?Aggregate\b', re.I)
    _PHYSICAL_RE  = re.compile(r'^== Physical Plan ==\s*$(.*?)(?=^== |\Z)', re.M | re.S)
    _FILTER_RE    = re.compile(r'\bFilter\b', re.I)
    _INDENT_RE    = re.compile(r'^(\s*)[+\-\:!\|]')   # plan indent → depth

    def __init__(self, df: "DataFrame"):
        self._df = df

    def inspect(self) -> PlanFeatures:
        """Ana giriş noktası — plan'ı analiz eder."""
        try:
            plan_str = self._get_plan_string()
        except Exception:
            # SparkSession kapalı veya test ortamı
            return PlanFeatures()

        return self._parse_plan(plan_str)

    def _get_plan_string(self) -> str:
        """
        Önce optimized plan'ı dene, yoksa explain(extended=True) kullan.
        Extended plan daha fazla detay içerir ama daha uzundur.
        """
        try:
            # Spark internal API — daha temiz
            qe = self._df._jdf.queryExecution()
            return qe.toString()
        except Exception:
            pass
        try:
            import io
            from contextlib import redirect_stdout
            buf = io.StringIO()
            with redirect_stdout(buf):
                self._df.explain(extended=False)
            return buf.getvalue()
        except Exception:
            return ""

    def _parse_plan(self, plan: str) -> PlanFeatures:
        if not plan:
            return PlanFeatures()

        # explain(extended)/queryExecution çıktısı parsed/analyzed/optimized/physical
        # planların hepsini içerir → aynı operatör birden çok kez sayılırdı.
        # Physical bölüm varsa yalnızca onu say; streaming ise logical planda da görünür.
        full_plan = plan
        m = self._PHYSICAL_RE.search(plan)
        if m and m.group(1).strip():
            plan = m.group(1)

        lines = plan.splitlines()
        features = PlanFeatures(plan_size_chars=len(plan))

        # Derinlik: maksimum girinti seviyesi
        max_depth = 0
        for line in lines:
            m = self._INDENT_RE.match(line)
            if m:
                indent = len(m.group(1))
                depth = indent // 3  # Spark genellikle 3 boşluk kullanır
                max_depth = max(max_depth, depth)
        features.depth = max_depth

        # Sayım
        features.exchange_count       = len(self._EXCHANGE_RE.findall(plan))
        features.has_sort_merge_join  = bool(self._SMJ_RE.search(plan))
        features.has_broadcast_join   = bool(self._BHJ_RE.search(plan))
        features.has_shuffled_hash_join = bool(self._SHJ_RE.search(plan))
        features.has_in_memory_relation = bool(self._IMR_RE.search(plan))
        features.has_streaming         = bool(self._STREAM_RE.search(full_plan))
        features.has_cross_join        = bool(self._CROSS_RE.search(plan))
        features.subquery_count        = len(self._SUBQ_RE.findall(plan))
        features.window_count          = len(self._WINDOW_RE.findall(plan))
        features.agg_count             = len(self._AGG_RE.findall(plan))
        features.filter_count          = len(self._FILTER_RE.findall(plan))

        # Join türleri listesi
        if features.has_broadcast_join: features.join_types.append("broadcast")
        if features.has_sort_merge_join: features.join_types.append("sort_merge")
        if features.has_shuffled_hash_join: features.join_types.append("shuffled_hash")

        # Reuse tahmini: aynı tablo referansının birden fazla geçmesi
        # (kaba: plan'da aynı dosya yolunun tekrar sayısı)
        path_re = re.compile(r'FileScan\s+\w+\s+\[([^\]]+)\]')
        paths = path_re.findall(plan)
        if paths:
            from collections import Counter
            counts = Counter(paths)
            features.estimated_reuse = max(counts.values())
        else:
            features.estimated_reuse = 1

        return features

    def plan_summary(self) -> str:
        """İnsan okunabilir plan özeti."""
        f = self.inspect()
        lines = [
            f"Plan Derinliği  : {f.depth} ({f.recommended_depth_label})",
            f"Shuffle (Exchange): {f.exchange_count}",
            f"Join türleri    : {', '.join(f.join_types) or 'yok'}",
            f"Zaten cache'li  : {f.has_in_memory_relation}",
            f"Streaming plan  : {f.has_streaming}",
            f"Window sayısı   : {f.window_count}",
            f"Aggregation     : {f.agg_count}",
            f"Plan karmaşıklığı: {'YÜKSEK ⚠' if f.is_complex else 'normal'}",
        ]
        return "\n".join(lines)
