"""
sparkopt.cases
==============
Optimizasyon case'leri — 5 kategori:

PERSISTENCE CASES  ← yeni, en kritik
  PersistenceStrategyCase  — cache vs localCheckpoint vs checkpoint karar ağacı
  IterativeLoopCase        — ML/graph döngüsü → her iterasyon localCheckpoint
  FanOutCacheCase          — 1 DF → N consumer → cache
  LineageBreakCase         — derin DAG → reliable checkpoint
  StorageLevelCase         — boyut + bellek → optimal StorageLevel

JOIN CASES
  BroadcastJoinCase
  SkewJoinCase
  BucketJoinCase
  SortMergeJoinCase

AGGREGATION CASES
  PartialAggCase
  ApproxDistinctCase
  WindowOptCase
  RepartitionBeforeAggCase

I/O CASES
  ParquetPushdownCase
  CoalesceOnWriteCase
  DeltaZOrderCase

PARTITION CASES
  RepartitionCase
  CoalesceCase
  AdaptiveCase
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sparkopt.core.base import OptimizationCase, Priority

if TYPE_CHECKING:
    from sparkopt.core.analyzer import DataProfile
    from sparkopt.codegen.context import CodegenContext


# ════════════════════════════════════════════════
# PERSISTENCE CASES  (en kritik kategori)
# ════════════════════════════════════════════════

class PersistenceStrategyCase(OptimizationCase):
    """
    Cache / LocalCheckpoint / Checkpoint arasında otomatik karar verir.

    Karar ağacı:
    ┌─ iteratif döngü var mı?
    │   ├─ Evet + fault-tolerance gerekmez → localCheckpoint()
    │   └─ Evet + streaming/kritik         → checkpoint() (HDFS/S3)
    ├─ DAG derinliği > 20?
    │   └─ Evet → checkpoint() (lineage kırılmalı)
    ├─ Aynı DF 2+ noktada tüketiliyor?
    │   ├─ Belleğe sığıyor → MEMORY_ONLY
    │   ├─ Yarı sığıyor   → MEMORY_AND_DISK
    │   └─ Sığmıyor       → DISK_ONLY
    └─ Tek kullanım → persist etme
    """
    name = "persistence_strategy"
    priority = Priority.CRITICAL

    def matches(self, profile: "DataProfile") -> bool:
        lin = profile.lineage
        return (
            lin.estimated_reuse_count >= 2
            or lin.has_iterative_loop
            or lin.depth > 10
        )

    def apply(self, ctx: "CodegenContext") -> None:
        lin = ctx.profile.lineage
        mem = ctx.profile.memory

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")

        if lin.needs_local_checkpoint:
            self._apply_local_checkpoint(ctx)
        elif lin.needs_reliable_checkpoint:
            self._apply_reliable_checkpoint(ctx)
        elif lin.depth > 10 and lin.estimated_reuse_count >= 2:
            # PersistenceAdvisor ile aynı eşik: orta derin DAG + reuse → localCheckpoint
            self._apply_local_checkpoint(ctx)
        elif lin.estimated_reuse_count >= 2:
            self._apply_cache(ctx, mem.recommended_storage_level)
        else:
            ctx.add_note("no_persist", "# Tek kullanım — persist gerekmez.")

    def _apply_local_checkpoint(self, ctx: "CodegenContext") -> None:
        ctx.add_hint("local_checkpoint", f"""
# LOCAL CHECKPOINT — executor local disk'e yazar
# Neden: iteratif döngü, her turda aynı base DF tüketiliyor
# Avantaj: reliable checkpoint'ten ~3x hızlı (HDFS round-trip yok)
# Dikkat: executor ölürse veri kaybolur → production streaming için uygun değil

# İterasyon başlangıcında bir kez:
df = df.localCheckpoint(eager=True)   # dönen DF kullanılmalı; eager=True → hemen materialize et

# ML döngüsü örneği:
model_df = features_df.localCheckpoint(eager=True)
for iteration in range(max_iter):
    model_df = update_step(model_df)
    # Her N iterasyonda lineage'ı kır (GC ve driver OOM önleme):
    if iteration % 5 == 0 and iteration > 0:
        model_df = model_df.localCheckpoint(eager=True)
""".strip())

    def _apply_reliable_checkpoint(self, ctx: "CodegenContext") -> None:
        ctx.add_hint("reliable_checkpoint", f"""
# RELIABLE CHECKPOINT — HDFS/S3'e yazar, DAG tamamen kırılır
# Neden: DAG derinliği ~{ctx.profile.lineage.depth} adım
#   → task failure'da tüm zincir yeniden hesaplanıyor
#   → driver'da lineage metadata birikimi → OOM riski
# Avantaj: executor ölse bile veri korunur, lineage sıfırlanır

spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/sparkopt/")
# veya: spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")

df_checkpointed = df.checkpoint(eager=True)
# eager=True: action hemen tetiklenir, downstream'e temiz plan gider

# Kullanım sonrası checkpoint dizini temizle:
# spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \\
#     .get(sc._jsc.hadoopConfiguration()) \\
#     .delete(sc._jvm.org.apache.hadoop.fs.Path("hdfs:///checkpoints/sparkopt/"), True)
""".strip())

    def _apply_cache(self, ctx: "CodegenContext", storage_level: str) -> None:
        reuse = ctx.profile.lineage.estimated_reuse_count
        size_mb = ctx.profile.estimated_mb
        ctx.add_hint("cache_persist", f"""
# CACHE — {reuse}x reuse tespit edildi, boyut: {size_mb:.0f} MB
# Seçilen StorageLevel: {storage_level}
# Karar mantığı:
#   Bellek kapasitesi: ~{ctx.profile.memory.usable_memory_mb:.0f} MB
#   DF boyutu:         {size_mb:.0f} MB
#   {'✓ Belleğe sığıyor' if ctx.profile.memory.fits_in_memory else '✗ Sığmıyor → disk spill'}

df.persist(StorageLevel.{storage_level})

# ── Kullanım noktaları (unpersist unutma!) ──
# result_1 = df.filter(...)   # 1. kullanım — cache'den okur
# result_2 = df.groupBy(...)  # 2. kullanım — cache'den okur
# df.unpersist()              # ← belleği serbest bırak
""".strip())

    def explain(self) -> str:
        return (
            "Reuse sayısı, DAG derinliği ve bellek kapasitesine göre "
            "cache / localCheckpoint / checkpoint arasında otomatik seçim yapar."
        )


class IterativeLoopCase(OptimizationCase):
    """
    ML veya graph iterasyonlarında lineage explosion'ı önler.

    Problem: Her iterasyon yeni RDD transformation ekler.
    100 iterasyon = 100 derinlikli DAG → driver OOM, GC durması.

    Çözüm: Her N iterasyonda localCheckpoint ile lineage'ı sıfırla.
    """
    name = "iterative_loop"
    priority = Priority.CRITICAL

    def __init__(self, checkpoint_interval: int = 5):
        self.checkpoint_interval = checkpoint_interval

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.has_iterative_loop

    def apply(self, ctx: "CodegenContext") -> None:
        n = self.checkpoint_interval
        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_hint("iterative_loop_checkpoint", f"""
# İTERATİF DÖNGÜ — her {n} iterasyonda lineage kır
#
# Neden kritik:
#   Spark, her map/filter/join'i önceki RDD'ye referans olarak saklar.
#   100 iterasyon = driver'da 100 katmanlı lineage graph → OOM + yavaş GC
#
# Pattern: MLlib'in GradientBoostedTrees ve PageRank'i de bunu kullanır.

from pyspark import StorageLevel

def train_loop(df, max_iter=100, ckpt_interval={n}):
    # localCheckpoint için setCheckpointDir() gerekmez
    current = df.localCheckpoint(eager=True)  # başlangıç noktasını sabitle

    for i in range(max_iter):
        # ── iterasyon mantığı ──
        current = iteration_step(current, i)

        # Her {n} adımda lineage kır
        if (i + 1) % ckpt_interval == 0:
            # localCheckpoint: HDFS gerekmez, executor disk'i kullanır.
            # Eski checkpoint blokları referans bırakılınca (GC) temizlenir;
            # persist edilmemiş DF'de unpersist() etkisizdir.
            current = current.localCheckpoint(eager=True)
            print(f"Iterasyon {{i+1}}: checkpoint alındı")

    return current

# Kıyaslama:
#   checkpoint() yok    → 100 iter sonrası GC pause: ~30s, driver heap: kriz
#   localCheckpoint({n}n) → GC pause: <1s,  driver heap: stabil
""".strip())

    def explain(self) -> str:
        return (
            f"İteratif döngü tespit edildi — her {self.checkpoint_interval} adımda "
            f"localCheckpoint ile lineage kırılır, driver OOM ve GC durması önlenir."
        )


class FanOutCacheCase(OptimizationCase):
    """
    Bir DataFrame'in N farklı yolda tüketildiği fan-out senaryosu.

    Örnek:
        enriched_df → [report_A, report_B, report_C, ml_features]

    enriched_df her branch'te sıfırdan hesaplanır → N × maliyet.
    Cache ile 1 × maliyet.
    """
    name = "fanout_cache"
    priority = Priority.HIGH

    def __init__(self, consumer_count: int = 2):
        self._consumers = consumer_count

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.estimated_reuse_count >= 2

    def apply(self, ctx: "CodegenContext") -> None:
        n = max(self._consumers, ctx.profile.lineage.estimated_reuse_count)
        mem = ctx.profile.memory
        size_mb = ctx.profile.estimated_mb
        level = mem.recommended_storage_level

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_hint("fanout_cache", f"""
# FAN-OUT CACHE — {n} consumer, 1 materializasyon
# DF boyutu: {size_mb:.0f} MB  |  Kullanılabilir bellek: {mem.usable_memory_mb:.0f} MB
# StorageLevel: {level}

# ── Hazırlık ──
base_df = (
    raw_df
    .join(dim_df, on="key", how="left")
    .filter(F.col("is_valid") == True)
    .withColumn("enriched_col", F.col("a") * F.col("b"))
)
base_df.persist(StorageLevel.{level})

# ── Paralel tüketim (her biri base_df'i cache'den okur) ──
report_A   = base_df.groupBy("region").agg(F.sum("revenue"))
report_B   = base_df.filter(F.col("category") == "electronics").count()
report_C   = base_df.withColumn("rank", F.rank().over(window_spec))
ml_input   = base_df.select("feature_1", "feature_2", "label")

# ── Tüm işler bittikten sonra ──
base_df.unpersist()
""".strip())

    def explain(self) -> str:
        return (
            "DataFrame birden fazla consumer tarafından tüketiliyor (reuse>=2) — "
            "cache ile N×hesaplama yerine 1×materializasyon."
        )


class LineageBreakCase(OptimizationCase):
    """
    DAG derinliği > 20 → reliable checkpoint ile lineage tamamen kır.

    Cache DAG'ı kırmaz! Executor failure'da tüm lineage yeniden hesaplanır.
    Checkpoint ise Spark plan'ını sıfırlar — downstream adımlar
    checkpointed veriyi taze DataFrame gibi görür.
    """
    name = "lineage_break"
    priority = Priority.HIGH

    DEPTH_THRESHOLD = 20

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.depth > self.DEPTH_THRESHOLD

    def apply(self, ctx: "CodegenContext") -> None:
        depth = ctx.profile.lineage.depth
        ctx.add_hint("lineage_break", f"""
# LİNEAGE KIRMA — DAG derinliği: ~{depth} adım (eşik: {self.DEPTH_THRESHOLD})
#
# Cache DAG'ı kırmaz!
#   - Executor ölürse Spark, tüm {depth} adımı yeniden hesaplar
#   - Driver'daki plan objesi büyür → serialize/deserialize yavaşlar
#
# Checkpoint DAG'ı sıfırlar:
#   - Downstream adımlar sadece HDFS/S3'teki veriyi görür
#   - Lineage depth → 0 (temiz başlangıç)

import os
CKPT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "hdfs:///checkpoints/job/")
spark.sparkContext.setCheckpointDir(CKPT_DIR)

# Pipeline'da kritik noktaya checkpoint ekle:
after_expensive_join = (
    left_df
    .join(right_df, on="key", how="inner")
    .join(third_df, on="other_key", how="left")
    # ... daha fazla join/transform ...
)
# Burada checkpoint: downstream adımlar bu veriyi HDFS'ten okur
after_expensive_join = after_expensive_join.checkpoint(eager=True)

# Devam eden transformasyonlar artık kısa DAG'a sahip:
final = after_expensive_join.groupBy("category").agg(F.sum("value"))
""".strip())

    def explain(self) -> str:
        return (
            f"DAG derinliği ~{self.DEPTH_THRESHOLD}+ adım — "
            f"reliable checkpoint ile lineage sıfırlanır, "
            f"executor failure maliyeti dramatik düşer."
        )


class StorageLevelCase(OptimizationCase):
    """
    DataFrame boyutu + cluster belleği → optimal StorageLevel seçer.

    Tüm seçenekler ve ne zaman kullanılır:

    PySpark'ta geçerli seçenekler (Scala'daki *_SER sabitleri PySpark'ta YOK):

    MEMORY_ONLY           : belleğe sığan DF, hız kritik
    MEMORY_AND_DISK       : orta-büyük DF, spill kabul edilebilir (production default)
    MEMORY_AND_DISK_DESER : df.cache() varsayılanı (Spark 3.x)
    DISK_ONLY             : çok büyük DF, bellekte tutmak anlamsız
    OFF_HEAP              : off-heap bellek (spark.memory.offHeap.enabled gerekir)
    _2 / _3 varyantlar    : replica (fault-tolerance, streaming için)

    DataFrame cache'i zaten sütunsal ve sıkıştırılmış (Tungsten) tutulur;
    "serialized vs deserialized" seçimi DataFrame'ler için anlamlı değildir.
    """
    name = "storage_level"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.estimated_reuse_count >= 2

    def apply(self, ctx: "CodegenContext") -> None:
        mem = ctx.profile.memory
        size_mb = ctx.profile.estimated_mb
        level = mem.recommended_storage_level
        streaming = ctx.profile.lineage.has_streaming_source

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_hint("storage_level_selection", f"""
# STORAGELEVEL SEÇİMİ
# DF boyutu   : {size_mb:.0f} MB
# Cluster RAM : {mem.usable_memory_mb:.0f} MB (kullanılabilir)
# Önerilen    : StorageLevel.{level}
#
# Tam karar tablosu:
# ┌───────────────────────────┬──────────────────────────────────────────┐
# │ StorageLevel              │ Ne zaman?                                │
# ├───────────────────────────┼──────────────────────────────────────────┤
# │ MEMORY_ONLY               │ < {mem.usable_memory_mb*0.8:.0f} MB, hız kritik                  │
# │ MEMORY_AND_DISK           │ {mem.usable_memory_mb*0.8:.0f}-{mem.usable_memory_mb*2.4:.0f} MB, spill kabul (production)    │
# │ DISK_ONLY                 │ > {mem.usable_memory_mb*2.4:.0f} MB, bellek baskısı var          │
# │ MEMORY_AND_DISK_2         │ Streaming, node failure kritik           │
# │ OFF_HEAP                  │ spark.memory.offHeap.enabled=true gerekir│
# └───────────────────────────┴──────────────────────────────────────────┘
# Not: PySpark'ta *_SER sabitleri yoktur (AttributeError verir).

from pyspark import StorageLevel

df.persist(StorageLevel.{level})

{'# Streaming için 2 replica önerisi:' if streaming else ''}
{'# df.persist(StorageLevel.MEMORY_AND_DISK_2)' if streaming else ''}
""".strip())

    def explain(self) -> str:
        return (
            "DF boyutu ve cluster belleğine göre MEMORY_ONLY / "
            "MEMORY_AND_DISK / DISK_ONLY arasında otomatik seçim."
        )


class HybridCheckpointCase(OptimizationCase):
    """
    Streaming + batch join senaryosu — ikisi farklı checkpoint stratejisi gerektirir.

    Static lookup tablo: cache (nadiren değişir)
    Stream verisi: writeStream checkpoint (exactly-once)
    Join sonucu: localCheckpoint (ara madde, kısa ömürlü)
    """
    name = "hybrid_checkpoint"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.has_streaming_source

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_hint("hybrid_checkpoint", """
# HYBRİD CHECKPOINT — Streaming × Batch Join
#
# Problem: Stream + statik tablo join'inde farklı persistence gerekir:
#   - Statik tablo: değişmez → cache (broadcast + cache)
#   - Stream state: exactly-once garantisi → writeStream checkpoint
#   - Ara join sonucu: kısa ömürlü → localCheckpoint

# 1. Statik referans tablosu (bir kere yükle, broadcast + cache)
from pyspark.sql import functions as F
from pyspark import StorageLevel

static_lookup = (
    spark.read.parquet("s3://bucket/lookup_table/")
    .persist(StorageLevel.MEMORY_ONLY)  # belleğe al, tekrar okuma
)
static_lookup.count()  # eager materializasyon

# 2. Streaming okuma
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "events")
    .load()
    .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
    .select("d.*")
)

# 3. Stream × Statik join
enriched_stream = stream_df.join(
    F.broadcast(static_lookup), on="key", how="left"
)

# 4. writeStream checkpoint — exactly-once garantisi
query = (
    enriched_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "hdfs:///checkpoints/streaming_job/")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start("s3://bucket/output/")
)

# 5. Temizlik
# static_lookup.unpersist()  ← job bitince
""".strip())

    def explain(self) -> str:
        return (
            "Streaming + batch join: statik tablo cache, "
            "stream state için writeStream checkpoint, ara sonuçlar localCheckpoint."
        )


class SpillPreventionCase(OptimizationCase):
    """
    Aggregation veya sort sırasında disk spill'i önle.

    Spill = bellek dolunca ara veriyi diske yaz → dramatik yavaşlama.
    Önleme:
      - executor belleğini artır (config)
      - spark.memory.fraction ayarla
      - aggregation öncesi repartition (her partition'a daha az veri)
      - persist ile tekrar hesaplamayı kesmek (spill'e yol açan re-compute)
    """
    name = "spill_prevention"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        mem = profile.memory
        # DF büyük VE yeterince bellek yoksa spill riski var
        return not mem.fits_in_memory and profile.row_count > 5_000_000

    def apply(self, ctx: "CodegenContext") -> None:
        mem = ctx.profile.memory
        size_mb = ctx.profile.estimated_mb
        target_partitions = max(
            200,
            int(size_mb / 128)  # partition başına ~128 MB hedef
        )

        ctx.add_config("spark.sql.adaptive.enabled", "true")
        ctx.add_config("spark.memory.fraction", "0.75")
        ctx.add_config("spark.memory.storageFraction", "0.4")
        ctx.add_hint("spill_prevention", f"""
# SPİLL ÖNLEMESİ
# DF: {size_mb:.0f} MB  |  Kullanılabilir bellek: {mem.usable_memory_mb:.0f} MB
# → Belleğe sığmıyor, aggregation/sort sırasında spill riski var

# 1. Bellek konfigürasyonu
#    spark.memory.* launch-time ayardır, spark.conf.set ile değişmez:
#    spark-submit --conf spark.memory.fraction=0.75 --conf spark.memory.storageFraction=0.4
spark.conf.set("spark.sql.adaptive.enabled", "true")   # AQE otomatik ayarlar

# 2. Aggregation öncesi repartition — partition başına ~128 MB
#    Her executor daha az veri işler → spill azalır
df = df.repartition({target_partitions})

# 3. Aggregation sırasında cache'den faydalanma
#    (aynı DF birden fazla agg yapılıyorsa)
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # bellek dolunca disk'e

result_1 = df.groupBy("cat").agg(F.sum("val"))
result_2 = df.groupBy("region").agg(F.count("*"))

df.unpersist()

# 4. Spill'i izle (Spark UI → Stages → Spill (Memory) ve Spill (Disk)):
#    Spill varsa partition sayısını artır veya executor belleğini büyüt.
""".strip())

    def explain(self) -> str:
        return (
            "DF belleğe sığmıyor — aggregation/sort spill riski var; "
            "repartition + MEMORY_AND_DISK + bellek ayarları ile önlenir."
        )


# ════════════════════════════════════════════════
# JOIN CASES
# ════════════════════════════════════════════════

class BroadcastJoinCase(OptimizationCase):
    name = "broadcast_join"
    priority = Priority.HIGH

    def __init__(self, right_table_bytes: int | None = None):
        self._right_bytes = right_table_bytes

    def matches(self, profile: "DataProfile") -> bool:
        b = self._right_bytes or profile.estimated_bytes
        return b <= 200 * 1024 ** 2

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("broadcast_join", """
# BROADCAST JOIN — küçük tablo her executor'a kopyalanır, shuffle yok
result = large_df.join(F.broadcast(small_df), on=join_keys, how="inner")
# spark.sql.autoBroadcastJoinThreshold değerini de kontrol edin (default: 10MB)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))
""".strip())

    def explain(self) -> str:
        if self._right_bytes is None:
            return "Küçük tablo (≤200 MB, sparkopt heuristiği) — broadcast join ile shuffle kalkar."
        mb = self._right_bytes / (1024 ** 2)
        return f"Sağ tablo ~{mb:.0f} MB — broadcast join ile shuffle maliyeti sıfırlanır."


class SkewJoinCase(OptimizationCase):
    name = "skew_join"
    priority = Priority.HIGH

    def __init__(self, skew_col: str | None = None, salt_buckets: int = 10):
        self._skew_col = skew_col
        self._salt_buckets = salt_buckets

    def matches(self, profile: "DataProfile") -> bool:
        return profile.partition.is_skewed

    def apply(self, ctx: "CodegenContext") -> None:
        col = (self._skew_col or (ctx.join_cols[0] if ctx.join_cols else "join_key"))
        n = self._salt_buckets
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_transformation("skew_salt", f"""
# SKEW JOIN — salting ile yük {n} bucket'a dağıtılır
# Skew oranı: {ctx.profile.partition.skew_ratio:.1f}x
SALT_BUCKETS = {n}

left_salted = left_df \\
    .withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")) \\
    .withColumn("{col}_s", F.concat(F.col("{col}"), F.lit("_"), F.col("_salt")))

salt_range    = spark.range(SALT_BUCKETS).withColumnRenamed("id", "_salt")
right_exploded = right_df.crossJoin(salt_range) \\
    .withColumn("{col}_s", F.concat(F.col("{col}"), F.lit("_"), F.col("_salt"))) \\
    .drop("{col}", "_salt")   # sol taraftaki kopyalarla çakışmasın (ambiguous column)

result = left_salted.join(right_exploded, on="{col}_s", how="inner") \\
                    .drop("_salt", "{col}_s")
""".strip())

    def explain(self) -> str:
        return (
            "Partition skew tespit edildi (skew_ratio > 3) — "
            f"salting ile yük {self._salt_buckets} bucket'a dağıtılır "
            "(önce AQE skewJoin'i deneyin)."
        )


class BucketJoinCase(OptimizationCase):
    name = "bucket_join"
    priority = Priority.MEDIUM

    def __init__(self, num_buckets: int = 200, bucket_col: str = "id"):
        self.num_buckets = num_buckets
        self.bucket_col = bucket_col

    def matches(self, profile: "DataProfile") -> bool:
        return profile.row_count > 10_000_000 and not profile.is_small

    def apply(self, ctx: "CodegenContext") -> None:
        n, col = self.num_buckets, self.bucket_col
        ctx.add_note("bucket_join", f"""
# BUCKET JOIN — her iki tablo aynı bucket'a yazılırsa shuffle-free join
df.write.bucketBy({n}, "{col}").sortBy("{col}").saveAsTable("db.bucketed")
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
result = df1.join(df2, on="{col}", how="inner")
""".strip())

    def explain(self) -> str:
        return f"{self.num_buckets} bucket: tekrarlı join'lerde shuffle ortadan kalkar."


class SortMergeJoinCase(OptimizationCase):
    name = "sort_merge_join"
    priority = Priority.LOW

    def matches(self, profile: "DataProfile") -> bool:
        return profile.is_large

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_hint("sort_merge_join", """
# SORT-MERGE JOIN — büyük × büyük tablo
left_df  = left_df.repartition(200, "join_key").sortWithinPartitions("join_key")
right_df = right_df.repartition(200, "join_key").sortWithinPartitions("join_key")
result   = left_df.join(right_df, on="join_key", how="inner")
""".strip())

    def explain(self) -> str:
        return "Büyük × büyük: repartition + sortWithinPartitions ile sort-merge join."


# ════════════════════════════════════════════════
# AGGREGATION CASES
# ════════════════════════════════════════════════

class PartialAggCase(OptimizationCase):
    name = "partial_agg"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return profile.row_count > 1_000_000

    def apply(self, ctx: "CodegenContext") -> None:
        # Partial (map-side) aggregation Spark'ta varsayılan olarak açıktır
        # (HashAggregate partial_* → Exchange → HashAggregate final).
        ctx.add_hint("partial_agg", """
# PARTIAL AGGREGATION — map tarafında ön-toplama, shuffle verisi küçülür
# groupBy'dan ÖNCE repartition YAPMAYIN: ham satırları shuffle eder ve
# map-side partial aggregation'ın kazancını yok eder.
from pyspark.sql import functions as F
result = (
    df.groupBy("group_col")
    .agg(F.sum("value").alias("total"), F.count("*").alias("cnt"))
)
""".strip())

    def explain(self) -> str:
        return "Partial aggregation ile shuffle öncesi veri ~5-10x küçültülür."


class ApproxDistinctCase(OptimizationCase):
    name = "approx_distinct"
    priority = Priority.MEDIUM

    def matches(self, profile: "DataProfile") -> bool:
        return profile.row_count > 5_000_000

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("approx_distinct", """
# approx_count_distinct (HyperLogLog, ~%2 hata) — countDistinct'ten 100x hızlı
result = df.groupBy("group_col").agg(
    F.approx_count_distinct("user_id", rsd=0.02).alias("approx_unique_users")
)
""".strip())

    def explain(self) -> str:
        return "countDistinct tam shuffle; approx_count_distinct ~%2 hata ile 100x hızlı."


class WindowOptCase(OptimizationCase):
    name = "window_opt"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return profile.row_count > 500_000

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_import("from pyspark.sql.window import Window")
        ctx.add_hint("window_opt", """
# WINDOW OPT — sınırlı frame ile OOM önleme
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# unboundedPreceding tüm partition'ı belleğe alır — OOM riski
# Bunun yerine sınırlı frame:
win = (Window.partitionBy("user_id")
             .orderBy(F.unix_timestamp("ts"))
             .rangeBetween(-7 * 86400, 0))  # son 7 gün

df = df.withColumn("rolling_avg", F.avg("value").over(win))
""".strip())

    def explain(self) -> str:
        return "unboundedPreceding partition'ı tümüyle belleğe alır; sınırlı frame OOM riskini kaldırır."


class RepartitionBeforeAggCase(OptimizationCase):
    name = "repartition_before_agg"
    priority = Priority.MEDIUM

    def matches(self, profile: "DataProfile") -> bool:
        return profile.partition.is_skewed or profile.row_count > 10_000_000

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_hint("repartition_agg", """
# AGG ÖNCESİ REPARTİTİON — aynı key'ler aynı partition'da, spill azalır
result = (
    df.repartition(spark.sparkContext.defaultParallelism * 2, *group_keys)
    .groupBy(*group_keys).agg(...)
)
""".strip())

    def explain(self) -> str:
        return "Aggregation key'e göre repartition: partition başına veri dengelenir, spill önlenir."


# ════════════════════════════════════════════════
# I/O CASES
# ════════════════════════════════════════════════

class ParquetPushdownCase(OptimizationCase):
    name = "parquet_pushdown"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return True

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_config("spark.sql.parquet.filterPushdown", "true")
        ctx.add_config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        ctx.add_hint("parquet_pushdown", """
# PARQUET PUSHDOWN — column pruning + predicate pushdown
from pyspark.sql import functions as F
df = (
    spark.read.parquet("s3://bucket/table/")
    .select("col_a", "col_b", "col_c")           # column pruning
    .filter(F.col("event_date") >= "2024-01-01") # predicate pushdown
)
""".strip())

    def explain(self) -> str:
        return "Parquet pushdown: gereksiz sütun ve dosyalar okuma aşamasında elenir."


class CoalesceOnWriteCase(OptimizationCase):
    name = "coalesce_write"
    priority = Priority.MEDIUM

    def matches(self, profile: "DataProfile") -> bool:
        return profile.partition.is_oversized

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_hint("coalesce_write", """
# COALESCE WRITE — çok küçük dosya → birleştir (shuffle yok)
df.coalesce(100).write.parquet("output/path/")
""".strip())

    def explain(self) -> str:
        return "Çok fazla küçük dosya okuma performansını düşürür; coalesce ile optimizasyon."


class DeltaZOrderCase(OptimizationCase):
    name = "delta_zorder"
    priority = Priority.LOW

    def __init__(self, zorder_cols: list[str] | None = None):
        self._cols = zorder_cols or ["event_date", "user_id"]

    def matches(self, profile: "DataProfile") -> bool:
        return profile.is_large

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_note("delta_zorder", f"""
# DELTA Z-ORDER — sık filtrelenen sütunlara göre cluster
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/path/to/delta_table")
dt.optimize().executeZOrderBy(*{self._cols!r})
""".strip())

    def explain(self) -> str:
        return f"Z-Order {self._cols}: sık sorgularda %80+ data skipping."


# ════════════════════════════════════════════════
# PARTITION CASES
# ════════════════════════════════════════════════

class RepartitionCase(OptimizationCase):
    name = "repartition"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return profile.partition.is_undersized

    def apply(self, ctx: "CodegenContext") -> None:
        target = max(
            ctx.profile.partition.count * 4,
            ctx.spark_parallelism * 2
        )
        ctx.add_transformation("repartition", f"""
# Az partition → parallelism düşük, executor'lar boş
df = df.repartition({target})
# Key bazlı: df = df.repartition({target}, "group_key")
""".strip())

    def explain(self) -> str:
        return "Düşük partition sayısı executor'ları boş bırakır; repartition ile CPU kullanımı maksimize edilir."


class CoalesceCase(OptimizationCase):
    name = "coalesce"
    priority = Priority.MEDIUM

    def matches(self, profile: "DataProfile") -> bool:
        return profile.partition.is_oversized

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_transformation("coalesce", """
# Çok fazla küçük partition → task overhead
df = df.coalesce(200)  # shuffle gerektirmez
""".strip())

    def explain(self) -> str:
        return "Aşırı partition sayısı task scheduling overhead'i artırır; coalesce ile ucuz birleştirme."


class AdaptiveCase(OptimizationCase):
    name = "adaptive_qe"
    priority = Priority.CRITICAL

    def matches(self, profile: "DataProfile") -> bool:
        return True

    def apply(self, ctx: "CodegenContext") -> None:
        ctx.add_config("spark.sql.adaptive.enabled", "true")
        ctx.add_config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        ctx.add_config("spark.sql.adaptive.skewJoin.enabled", "true")
        ctx.add_config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
        ctx.add_hint("aqe", """
# AQE — çalışma zamanı plan optimizasyonu (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
""".strip())

    def explain(self) -> str:
        return "AQE: shuffle istatistikleriyle çalışma zamanı plan optimizasyonu — sıfır kod değişikliği."


# ── Tüm built-in case'ler ─────────────────────

ALL_BUILTIN_CASES: list[OptimizationCase] = [
    # Persistence (CRITICAL önce)
    AdaptiveCase(),
    PersistenceStrategyCase(),
    IterativeLoopCase(),
    FanOutCacheCase(),
    LineageBreakCase(),
    StorageLevelCase(),
    HybridCheckpointCase(),
    SpillPreventionCase(),
    # Join
    BroadcastJoinCase(),
    SkewJoinCase(),
    BucketJoinCase(),
    SortMergeJoinCase(),
    # Aggregation
    PartialAggCase(),
    ApproxDistinctCase(),
    WindowOptCase(),
    RepartitionBeforeAggCase(),
    # I/O
    ParquetPushdownCase(),
    CoalesceOnWriteCase(),
    DeltaZOrderCase(),
    # Partition
    RepartitionCase(),
    CoalesceCase(),
]
