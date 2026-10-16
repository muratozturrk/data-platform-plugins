"""
sparkopt.core.analyzer
======================
DataFrame'i otomatik analiz eder:
  - Boyut (satır/sütun sayısı, tahmini byte)
  - Partition istatistikleri (skew oranı, boş partition)
  - Join sütunu kardinalitesi
  - Sütun null oranları
  - DAG lineage derinliği (checkpoint kararı için)
  - Executor bellek baskısı tahmini

Sonuç: DataProfile nesnesi — tüm OptimizationCase'ler bunu tüketir.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

# pyspark yalnızca DataFrameAnalyzer için gerekir; DataProfile/case'ler/CLI
# Spark kurulu olmadan da çalışır.
if TYPE_CHECKING:
    from pyspark.sql import DataFrame


# ──────────────────────────────────────────────
# Veri Yapıları
# ──────────────────────────────────────────────

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_ratio: float = 0.0
    distinct_count: int = 0
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    mean_val: Optional[float] = None
    std_val: Optional[float] = None


@dataclass
class PartitionProfile:
    count: int = 0
    min_rows: int = 0
    max_rows: int = 0
    avg_rows: float = 0.0
    skew_ratio: float = 0.0       # max / avg — 1.0 = mükemmel denge
    empty_count: int = 0

    @property
    def is_skewed(self) -> bool:
        return self.skew_ratio > 3.0

    @property
    def is_undersized(self) -> bool:
        return self.count < 8

    @property
    def is_oversized(self) -> bool:
        return self.count > 2000 and self.avg_rows < 10_000


@dataclass
class LineageProfile:
    """
    DAG derinliği ve yapısı — checkpoint kararının temelidir.

    Spark, her transformation'ı RDD lineage olarak zincirler.
    Uzun veya geniş DAG'lar:
      - Task failure'da tüm zinciri yeniden hesaplar (maliyet)
      - Driver'da OOM yapabilir (lineage metadata birikimi)
      - GC pause'larını artırır

    Karar eşikleri (ampirik):
      depth > 20        → checkpoint zorunlu
      depth 10-20       → localCheckpoint veya cache (fault-tolerance gerek mi?)
      depth < 10        → cache yeterli
      iterative = True  → her iterasyon localCheckpoint'le kırılmalı
    """
    depth: int = 0                    # tahmini RDD/plan derinliği
    has_iterative_loop: bool = False  # ML/graph iterasyonu var mı?
    wide_transform_count: int = 0     # shuffle gerektiren adım sayısı
    estimated_reuse_count: int = 1    # kaç farklı noktada tüketiliyor?
    has_streaming_source: bool = False

    @property
    def needs_checkpoint(self) -> bool:
        """Lineage kırılmalı mı?"""
        return self.depth > 20 or self.has_iterative_loop

    @property
    def needs_local_checkpoint(self) -> bool:
        """
        localCheckpoint: driver'a yazılmaz, executor local disk'e.
        Daha hızlı ama fault-tolerant değil.
        Uygun: iteratif ML, kısa ömürlü ara sonuçlar.
        """
        return self.has_iterative_loop and not self.has_streaming_source

    @property
    def needs_reliable_checkpoint(self) -> bool:
        """
        Reliable checkpoint: HDFS/S3'e yazar, DAG tamamen kırılır.
        Uygun: uzun job, streaming, kritik pipeline.
        """
        return self.depth > 20 and not self.has_iterative_loop


@dataclass
class MemoryProfile:
    """
    Executor bellek durumu tahmini — StorageLevel seçimi için.

    StorageLevel karar ağacı (PySpark sabitleri; *_SER PySpark'ta yoktur):
      fits_in_memory=True                      → MEMORY_ONLY
      fits_serialized=True (sıkıştırılmış hali) → MEMORY_AND_DISK
      hiçbiri                                  → DISK_ONLY
      replication > 1                          → _2 varyantlar (fault-tolerance)
    """
    executor_memory_mb: int = 4096       # spark.executor.memory
    executor_count: int = 10
    total_cluster_mb: int = 0            # hesaplanır
    df_size_mb: float = 0.0
    serialization_overhead: float = 0.3  # Java object overhead ~%30

    def __post_init__(self):
        if self.total_cluster_mb == 0:
            self.total_cluster_mb = self.executor_memory_mb * self.executor_count

    @property
    def usable_memory_mb(self) -> float:
        """spark.memory.fraction (0.6) × spark.memory.storageFraction (0.5)"""
        return self.total_cluster_mb * 0.6 * 0.5

    @property
    def fits_in_memory(self) -> bool:
        return self.df_size_mb < self.usable_memory_mb * 0.8  # %80 eşik

    @property
    def fits_serialized(self) -> bool:
        """Sütunsal/sıkıştırılmış cache hali sığar mı? (~3x küçülme, sparkopt heuristiği)"""
        return (self.df_size_mb / 3.0) < self.usable_memory_mb * 0.8

    @property
    def recommended_storage_level(self) -> str:
        """Otomatik StorageLevel seçimi."""
        if self.fits_in_memory:
            return "MEMORY_ONLY"
        elif self.fits_serialized:
            return "MEMORY_AND_DISK"
        else:
            return "DISK_ONLY"


@dataclass
class DataProfile:
    row_count: int = 0
    col_count: int = 0
    estimated_bytes: int = 0
    partition: PartitionProfile = field(default_factory=PartitionProfile)
    columns: dict[str, ColumnProfile] = field(default_factory=dict)
    lineage: LineageProfile = field(default_factory=LineageProfile)
    memory: MemoryProfile = field(default_factory=MemoryProfile)

    large_table_threshold_bytes: int = 10 * 1024 ** 3   # 10 GB

    @property
    def is_large(self) -> bool:
        return self.estimated_bytes >= self.large_table_threshold_bytes

    @property
    def is_small(self) -> bool:
        return self.estimated_bytes <= 200 * 1024 ** 2

    @property
    def estimated_mb(self) -> float:
        return self.estimated_bytes / (1024 ** 2)

    def col(self, name: str) -> Optional[ColumnProfile]:
        return self.columns.get(name)


# ──────────────────────────────────────────────
# Analyzer
# ──────────────────────────────────────────────

class DataFrameAnalyzer:
    """
    DataFrame'i analiz ederek DataProfile döndürür.

    Kullanım:
        profile = DataFrameAnalyzer(df).analyze(
            join_cols=["user_id"],
            lineage_depth=15,
            reuse_count=3,
            executor_memory_mb=8192,
            executor_count=20,
        )
    """

    _DTYPE_BYTES: dict[str, int] = {
        "byte": 1, "short": 2, "integer": 4, "int": 4,
        "long": 8, "bigint": 8, "float": 4, "double": 8,
        "boolean": 1, "date": 4, "timestamp": 8,
        "string": 32, "binary": 64,
    }

    def __init__(self, df: DataFrame, sample_fraction: float = 0.05):
        self._df = df
        self._sample_fraction = min(max(sample_fraction, 0.001), 1.0)

    def _estimate_bytes_fast(self, row_count: int) -> int:
        bytes_per_row = sum(
            self._DTYPE_BYTES.get(f.dataType.typeName(), 32)
            for f in self._df.schema.fields
        )
        return row_count * bytes_per_row

    def _profile_partitions(self) -> PartitionProfile:
        rdd = self._df.rdd
        part_sizes = rdd.mapPartitions(
            lambda it: [sum(1 for _ in it)]
        ).collect()
        if not part_sizes:
            return PartitionProfile()
        total = sum(part_sizes)
        count = len(part_sizes)
        avg = total / count if count else 0
        mx = max(part_sizes)
        mn = min(part_sizes)
        empty = sum(1 for s in part_sizes if s == 0)
        return PartitionProfile(
            count=count, min_rows=mn, max_rows=mx,
            avg_rows=round(avg, 1),
            skew_ratio=round(mx / avg if avg > 0 else 1.0, 2),
            empty_count=empty,
        )

    def _estimate_partition_profile(self, row_count: int) -> PartitionProfile:
        num_partitions = self._df.rdd.getNumPartitions()
        avg = row_count / num_partitions if num_partitions else 0
        return PartitionProfile(
            count=num_partitions,
            min_rows=0,
            max_rows=int(avg * 2),
            avg_rows=round(avg, 1),
            skew_ratio=1.0,
            empty_count=0,
        )

    def _profile_columns(
        self, focus_cols: Optional[list[str]] = None,
    ) -> dict[str, ColumnProfile]:
        from pyspark.sql import functions as F

        cols = focus_cols or [f.name for f in self._df.schema.fields]
        sample = self._df.sample(self._sample_fraction, seed=42)
        total_sample = sample.count()
        profiles: dict[str, ColumnProfile] = {}

        for f in self._df.schema.fields:
            if f.name not in cols:
                continue
            dtype_name = f.dataType.typeName()
            null_count = sample.filter(F.col(f.name).isNull()).count()
            null_ratio = null_count / total_sample if total_sample else 0.0
            distinct = sample.agg(
                F.approx_count_distinct(f.name).alias("d")
            ).collect()[0]["d"]
            cp = ColumnProfile(
                name=f.name, dtype=dtype_name,
                null_ratio=round(null_ratio, 4),
                distinct_count=distinct,
            )
            if dtype_name in ("integer", "int", "long", "bigint",
                              "float", "double", "short", "byte"):
                stats = sample.agg(
                    F.min(f.name).alias("mn"), F.max(f.name).alias("mx"),
                    F.mean(f.name).alias("me"), F.stddev(f.name).alias("sd"),
                ).collect()[0]
                cp.min_val = stats["mn"]
                cp.max_val = stats["mx"]
                cp.mean_val = round(float(stats["me"] or 0), 4)
                cp.std_val  = round(float(stats["sd"] or 0), 4)
            profiles[f.name] = cp
        return profiles

    def analyze(
        self,
        join_cols: Optional[list[str]] = None,
        agg_cols: Optional[list[str]] = None,
        full_partition_scan: bool = False,
        lineage_depth: int = 0,
        reuse_count: int = 1,
        iterative: bool = False,
        streaming: bool = False,
        auto_inspect_plan: bool = True,
        executor_memory_mb: int = 4096,
        executor_count: int = 10,
    ) -> DataProfile:
        focus = list({*(join_cols or []), *(agg_cols or [])}) or None
        row_count = self._df.count()
        col_count = len(self._df.schema.fields)
        est_bytes = self._estimate_bytes_fast(row_count)
        est_mb = est_bytes / (1024 ** 2)

        part_profile = (
            self._profile_partitions()
            if full_partition_scan
            else self._estimate_partition_profile(row_count)
        )

        col_profiles = self._profile_columns(focus_cols=focus)

        # Plan'ı otomatik inspect et — LineageInspector
        plan_features = None
        if auto_inspect_plan:
            try:
                from sparkopt.core.lineage_inspector import LineageInspector
                plan_features = LineageInspector(self._df).inspect()
                # Plan'dan gelen değerler kullanıcı override'ını geçersiz kılmaz
                if lineage_depth == 0:
                    lineage_depth = plan_features.depth
                if not streaming:
                    streaming = plan_features.has_streaming
                if reuse_count == 1 and plan_features.estimated_reuse > 1:
                    reuse_count = plan_features.estimated_reuse
            except Exception:
                pass  # test ortamı veya plan erişimi yoksa fallback

        # Hâlâ 0 ise heuristik tahmin
        if lineage_depth == 0:
            lineage_depth = (
                part_profile.count // 50
                + len(join_cols or []) * 5
                + len(agg_cols or []) * 3
            )
            lineage_depth = max(lineage_depth, 3)

        lineage = LineageProfile(
            depth=lineage_depth,
            has_iterative_loop=iterative,
            wide_transform_count=(
                (plan_features.exchange_count if plan_features else 0)
                or len(join_cols or []) + len(agg_cols or [])
            ),
            estimated_reuse_count=reuse_count,
            has_streaming_source=streaming,
        )

        memory = MemoryProfile(
            executor_memory_mb=executor_memory_mb,
            executor_count=executor_count,
            df_size_mb=est_mb,
        )

        return DataProfile(
            row_count=row_count,
            col_count=col_count,
            estimated_bytes=est_bytes,
            partition=part_profile,
            columns=col_profiles,
            lineage=lineage,
            memory=memory,
        )
