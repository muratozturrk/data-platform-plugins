"""
sparkopt.core.persistence_advisor
===================================
Birden fazla case çakıştığında hangi persistence stratejisinin kazanacağını
belirleyen merkezi karar makinesi.

Problem:
  FanOutCacheCase      → MEMORY_ONLY önerir
  SpillPreventionCase  → MEMORY_AND_DISK önerir
  LineageBreakCase     → checkpoint önerir
  IterativeLoopCase    → localCheckpoint önerir

Bunlar çakışabilir. Advisor, tüm sinyalleri değerlendirip
TEK bir strateji kararı verir ve çakışan önerileri bastırır.

Karar hiyerarşisi (öncelik sırası):
  1. ITERATIVE_LOCAL_CHECKPOINT  — döngü var, fault-tolerance gerekmez
  2. RELIABLE_CHECKPOINT         — derin DAG, streaming, kritik job
  3. LOCAL_CHECKPOINT            — orta derin, kısa ömürlü ara sonuç
  4. CACHE_DISK                  — belleğe sığmıyor, reuse var
  5. CACHE_MEMORY_SER            — belleğe sığıyor, reuse var
  6. CACHE_MEMORY_ONLY           — küçük, hız kritik
  7. NO_PERSIST                  — tek kullanım
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sparkopt.core.analyzer import DataProfile


class PersistenceStrategy(Enum):
    NO_PERSIST                  = auto()
    CACHE_MEMORY_ONLY           = auto()
    CACHE_MEMORY_SER            = auto()
    CACHE_DISK                  = auto()
    CACHE_OFF_HEAP              = auto()
    LOCAL_CHECKPOINT            = auto()
    ITERATIVE_LOCAL_CHECKPOINT  = auto()
    RELIABLE_CHECKPOINT         = auto()
    HYBRID                      = auto()    # streaming+batch karma


@dataclass
class PersistenceDecision:
    strategy: PersistenceStrategy
    storage_level: str                  # StorageLevel enum adı
    rationale: str                      # neden bu karar?
    code_snippet: str                   # hazır kod
    warnings: list[str]

    @property
    def needs_checkpoint_dir(self) -> bool:
        return self.strategy in (
            PersistenceStrategy.RELIABLE_CHECKPOINT,
        )

    @property
    def is_checkpoint(self) -> bool:
        return self.strategy in (
            PersistenceStrategy.LOCAL_CHECKPOINT,
            PersistenceStrategy.ITERATIVE_LOCAL_CHECKPOINT,
            PersistenceStrategy.RELIABLE_CHECKPOINT,
        )


class PersistenceAdvisor:
    """
    DataProfile'dan tek bir PersistenceDecision üretir.

    Tüm case'lerin çakışan önerilerini bu sınıf çözer —
    case'ler sadece profile'a bakar, advisor bütünü görür.

    Kullanım:
        advisor = PersistenceAdvisor(profile)
        decision = advisor.decide()
        print(decision.rationale)
        print(decision.code_snippet)
    """

    # Eşikler
    DEPTH_LOCAL_CKPT    = 10   # localCheckpoint düşünülmeli
    DEPTH_RELIABLE_CKPT = 20   # reliable checkpoint zorunlu
    MIN_REUSE           = 2    # persist değeri var

    def __init__(self, profile: "DataProfile"):
        self._p = profile

    def decide(self) -> PersistenceDecision:
        lin = self._p.lineage
        mem = self._p.memory
        size_mb = self._p.estimated_mb

        # ── Karar ağacı (öncelik sırası) ────────────────

        # 1. Streaming + batch karma → HYBRID
        if lin.has_streaming_source and lin.estimated_reuse_count >= 2:
            return self._hybrid_decision(size_mb, mem)

        # 2. İteratif döngü → ITERATIVE_LOCAL_CHECKPOINT
        if lin.has_iterative_loop:
            return self._iterative_local_ckpt_decision(size_mb)

        # 3. Çok derin DAG → RELIABLE_CHECKPOINT
        if lin.depth > self.DEPTH_RELIABLE_CKPT:
            return self._reliable_ckpt_decision(lin.depth, size_mb)

        # 4. Orta derin DAG + reuse → LOCAL_CHECKPOINT (hız öncelikli)
        if lin.depth > self.DEPTH_LOCAL_CKPT and lin.estimated_reuse_count >= self.MIN_REUSE:
            # Fault-tolerance gerekmiyorsa local daha hızlı
            return self._local_ckpt_decision(lin.depth, size_mb)

        # 5. Reuse var ama DAG düz → CACHE
        if lin.estimated_reuse_count >= self.MIN_REUSE:
            return self._cache_decision(size_mb, mem)

        # 6. Tek kullanım
        return PersistenceDecision(
            strategy=PersistenceStrategy.NO_PERSIST,
            storage_level="",
            rationale=(
                f"Reuse sayısı={lin.estimated_reuse_count}, "
                f"DAG derinliği={lin.depth} — persist maliyeti karşılamaz."
            ),
            code_snippet="# persist gerekmez — tek kullanım",
            warnings=[],
        )

    # ── Özel karar oluşturucular ─────────────────────

    def _iterative_local_ckpt_decision(self, size_mb: float) -> PersistenceDecision:
        return PersistenceDecision(
            strategy=PersistenceStrategy.ITERATIVE_LOCAL_CHECKPOINT,
            storage_level="",
            rationale=(
                "İteratif döngü tespit edildi. Her N adımda localCheckpoint ile "
                "lineage kırılır — driver OOM ve GC storm önlenir. "
                "Executor local disk kullandığı için HDFS round-trip yok, ~3x hızlı."
            ),
            code_snippet=self._iterative_code(),
            warnings=[
                "localCheckpoint executor ölürse veri kaybeder — kritik job için "
                "reliable checkpoint'e geçin veya checkpoint_interval'ı küçültün.",
                "localCheckpoint için setCheckpointDir() gerekmez; yalnızca reliable "
                "checkpoint() HDFS/S3 checkpoint dizini ister.",
            ],
        )

    def _reliable_ckpt_decision(self, depth: int, size_mb: float) -> PersistenceDecision:
        return PersistenceDecision(
            strategy=PersistenceStrategy.RELIABLE_CHECKPOINT,
            storage_level="",
            rationale=(
                f"DAG derinliği {depth} adım (eşik: {self.DEPTH_RELIABLE_CKPT}). "
                "Reliable checkpoint: downstream adımlar HDFS/S3'teki veriyi taze "
                "DataFrame gibi görür. Lineage tamamen sıfırlanır."
            ),
            code_snippet=self._reliable_ckpt_code(depth, size_mb),
            warnings=[
                f"Checkpoint I/O maliyeti: ~{size_mb:.0f} MB yazma + okuma.",
                "setCheckpointDir() HDFS/S3 URI olmalı — local path cluster'da çalışmaz.",
                "Job bitince checkpoint dizinini temizleyin (otomatik silinmez).",
            ],
        )

    def _local_ckpt_decision(self, depth: int, size_mb: float) -> PersistenceDecision:
        return PersistenceDecision(
            strategy=PersistenceStrategy.LOCAL_CHECKPOINT,
            storage_level="",
            rationale=(
                f"DAG derinliği {depth}, reuse={self._p.lineage.estimated_reuse_count}. "
                "localCheckpoint: executor local disk'e yazar, HDFS gerekmez. "
                "Cache'den farkı: DAG'ı kırar, executor failure'da yeniden hesaplanmaz "
                "(ama veri kaybı riski var)."
            ),
            code_snippet=self._local_ckpt_code(size_mb),
            warnings=[
                "localCheckpoint fault-tolerant değil — kritik pipeline için "
                "reliable checkpoint tercih edin.",
            ],
        )

    def _cache_decision(self, size_mb: float, mem) -> PersistenceDecision:
        level = mem.recommended_storage_level
        strategy_map = {
            "MEMORY_ONLY":      PersistenceStrategy.CACHE_MEMORY_SER,
            "MEMORY_AND_DISK":  PersistenceStrategy.CACHE_DISK,
            "DISK_ONLY":            PersistenceStrategy.CACHE_DISK,
        }
        strategy = strategy_map.get(level, PersistenceStrategy.CACHE_MEMORY_SER)
        warnings = []
        if size_mb > mem.usable_memory_mb:
            warnings.append(
                f"DF ({size_mb:.0f} MB) kullanılabilir belleği ({mem.usable_memory_mb:.0f} MB) "
                f"aşıyor — {level} seçildi, disk spill yaşanabilir."
            )
        return PersistenceDecision(
            strategy=strategy,
            storage_level=level,
            rationale=(
                f"Reuse={self._p.lineage.estimated_reuse_count}, boyut={size_mb:.0f} MB, "
                f"bellek={mem.usable_memory_mb:.0f} MB → {level}."
            ),
            code_snippet=self._cache_code(level, size_mb, mem.usable_memory_mb),
            warnings=warnings,
        )

    def _hybrid_decision(self, size_mb: float, mem) -> PersistenceDecision:
        return PersistenceDecision(
            strategy=PersistenceStrategy.HYBRID,
            storage_level="MEMORY_ONLY",
            rationale=(
                "Streaming + batch karma senaryo. "
                "Statik lookup: MEMORY_ONLY (broadcast + cache). "
                "Stream state: writeStream checkpoint (exactly-once). "
                "Ara join sonucu: localCheckpoint (kısa ömürlü)."
            ),
            code_snippet=self._hybrid_code(size_mb),
            warnings=[
                "writeStream checkpoint dizini değiştirilmemeli — offset kaybı olur.",
                "Statik tablo güncellendiyse unpersist() + re-persist() gerekir.",
                "Streaming watermark'ı ayarlayın — geç veri yönetimi için kritik.",
            ],
        )

    # ── Kod parçacıkları ─────────────────────────────

    def _iterative_code(self) -> str:
        return '''\
# İTERATİF DÖNGÜ — localCheckpoint ile lineage kontrolü
# ─────────────────────────────────────────────────────
# Neden localCheckpoint > cache (iteratif için):
#   cache  → DAG'ı KIRMAZ, lineage büyümeye devam eder
#   localCheckpoint → DAG'ı KIRAR, her N iterasyonda temiz başlangıç

# localCheckpoint executor yerel diskine yazar; setCheckpointDir() GEREKMEZ.

CHECKPOINT_INTERVAL = 5   # her 5 iterasyonda bir kır
MAX_ITER = 100

# Başlangıç noktasını sabitle
current_df = initial_df.localCheckpoint(eager=True)

for i in range(MAX_ITER):
    # ── iterasyon mantığı (örnek: gradient descent adımı) ──
    current_df = compute_gradient(current_df, learning_rate=0.01)
    current_df = update_weights(current_df)

    if (i + 1) % CHECKPOINT_INTERVAL == 0:
        prev_df = current_df
        # localCheckpoint: executor yerel disk → HDFS round-trip yok
        current_df = current_df.localCheckpoint(eager=True)
        prev_df.unpersist()   # eski materializasyonu serbest bırak
        # Opsiyonel: ilerleme logu
        # current_df.agg(F.sum("loss")).show()

# Sonuç
final_model = current_df
# NOT: localCheckpoint executor ölürse veri kaybeder.
# Kritik job için reliable checkpoint'e geçin (setCheckpointDir("hdfs://..."))'''

    def _reliable_ckpt_code(self, depth: int, size_mb: float) -> str:
        return f'''\
# RELIABLE CHECKPOINT — DAG derinliği: {depth} adım
# ─────────────────────────────────────────────────
# DAG bu kadar derinken:
#   - Executor failure → tüm {depth} adım yeniden hesaplanır
#   - Driver lineage metadata'sı → heap basıncı
#   - Plan serialize/deserialize → her action yavaşlar
# Checkpoint bu bağımlılık zincirini keser.

import os
CKPT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "hdfs:///checkpoints/sparkopt/")
spark.sparkContext.setCheckpointDir(CKPT_DIR)

# Ağır transformation zinciri sonrası checkpoint
intermediate = (
    df
    .join(dim_table, on="key", how="left")     # +5 depth
    .join(fact_table, on="fact_key", how="inner")  # +5 depth
    .groupBy("category").agg(...)              # +3 depth
    .withColumn("enriched", udf_transform(...))# +2 depth
)
# Burada checkpoint: ~{size_mb:.0f} MB yazılacak (HDFS/S3)
intermediate = intermediate.checkpoint(eager=True)

# Bu noktadan sonra plan tamamen temiz (depth=0)
final = intermediate.filter(...).select(...)

# İş bitince temizle (Spark otomatik silmez):
# sc = spark.sparkContext
# hadoop = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
# hadoop.delete(sc._jvm.org.apache.hadoop.fs.Path(CKPT_DIR), True)'''

    def _local_ckpt_code(self, size_mb: float) -> str:
        return f'''\
# LOCAL CHECKPOINT — executor local disk'e yazar
# ─────────────────────────────────────────────
# Reliable checkpoint'ten ~3x hızlı (HDFS round-trip yok)
# Dikkat: executor ölürse veri kaybolur
# Kullanım: kısa ömürlü ara sonuçlar, yeniden hesaplaması ucuz değil

# setCheckpointDir() gerekmez — localCheckpoint executor yerel diskini kullanır.

# Ağır transformation sonrası DAG'ı kır
intermediate_df = (
    df
    .join(other_df, on="key")
    .withColumn(...)
    # ... derin zincir ...
)
# eager=True → hemen materialize et, downstream temiz plan alsın
intermediate_df = intermediate_df.localCheckpoint(eager=True)

# Devam eden hesaplamalar kısa DAG ile çalışır
# Tahmini boyut: ~{size_mb:.0f} MB'''

    def _cache_code(self, level: str, size_mb: float, usable_mb: float) -> str:
        return f'''\
# CACHE — StorageLevel.{level}
# DF: {size_mb:.0f} MB  |  Kullanılabilir bellek: {usable_mb:.0f} MB
# ─────────────────────────────────────────────────────────────

from pyspark import StorageLevel

# Persist et (lazy — ilk action'da materialize edilir)
df.persist(StorageLevel.{level})

# İlk action tetikle → eager materializasyon (opsiyonel ama önerilir):
# df.count()   # böylece sonraki action'lar kesinlikle cache'den okur

# ── Birden fazla tüketim noktası ──
result_a = df.filter(F.col("region") == "EU").groupBy("cat").agg(...)
result_b = df.join(other_df, on="id", how="left")
result_c = df.select("feature_1", "feature_2").toPandas()   # ML için

# ── İşler bittikten sonra belleği serbest bırak ──
df.unpersist()
# NOT: unpersist() çağrılmazsa GC'ye bırakılır ama uzun job'larda
# executor OOM riskini artırır — her zaman açıkça çağırın.

# PySpark StorageLevel tablosu (referans — *_SER sabitleri PySpark'ta yoktur):
# MEMORY_ONLY           : yalnızca bellek, sığmayan partition yeniden hesaplanır
# MEMORY_AND_DISK       : bellek sonra disk  ← production default
# MEMORY_AND_DISK_DESER : df.cache() varsayılanı (Spark 3.x)
# DISK_ONLY             : sadece disk, çok büyük DF için
# *_2 varyantlar        : 2 replika, streaming fault-tolerance için'''

    def _hybrid_code(self, size_mb: float) -> str:
        return '''\
# HYBRID PERSISTENCE — Streaming × Batch
# ─────────────────────────────────────────────────────────────
# Her bileşen farklı persistence stratejisi kullanır:
#
#  Bileşen              | Strateji              | Neden
#  ─────────────────────┼───────────────────────┼──────────────────────
#  Statik lookup tablosu| cache MEMORY_ONLY | nadiren değişir
#  Streaming state      | writeStream ckpt      | exactly-once garantisi
#  Stream×Batch join    | localCheckpoint       | kısa ömürlü ara sonuç
#  Sonuç tablosu        | Delta/Parquet sink    | kalıcı

from pyspark import StorageLevel
from pyspark.sql import functions as F

# 1. Statik lookup: bir kere yükle, executor'lara broadcast + cache
lookup_df = (
    spark.read.parquet("s3://bucket/lookup/")
    .persist(StorageLevel.MEMORY_ONLY)
)
lookup_df.count()   # eager materializasyon

# 2. Streaming kaynak
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select("e.*")
)

# 3. Stream × Statik join (lookup cache'den → her micro-batch ucuz)
enriched = stream_df.join(F.broadcast(lookup_df), on="product_id", how="left")

# 4. writeStream: exactly-once için checkpoint ZORUNLU
#    Checkpoint dizinini değiştirmeyin — offset tutarlılığı bozulur
query = (
    enriched
    .withWatermark("event_time", "5 minutes")   # geç veri yönetimi
    .writeStream
    .format("delta")
    .option("checkpointLocation", "hdfs:///checkpoints/hybrid_job/")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .start("s3://bucket/output/")
)

query.awaitTermination()

# 5. Temizlik (job durduğunda)
# lookup_df.unpersist()'''
