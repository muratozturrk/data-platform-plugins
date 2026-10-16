"""
sparkopt.cases.complex_cases
=============================
İleri seviye, bileşik senaryolar için case'ler.

Her case birden fazla optimizasyon boyutunu birleştirir:
  MLPipelineCase          — özellik mühendisliği + model eğitimi akışı
  GraphAlgorithmCase      — PageRank/SSSP gibi iteratif graph algoritmaları
  MultiStageAggCase       — cube/rollup + window + pivoting birlikte
  StreamBatchJoinCase     — Structured Streaming + statik tablo enrichment
  IncrementalETLCase      — Upsert/merge tabanlı incremental pipeline
  CachedBroadcastCase     — broadcast + cache birlikte (değişen ama sık ref lookup)
  StagedMaterializationCase— çok adımlı pipeline'da nereye checkpoint/cache koyulacak
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sparkopt.core.base import OptimizationCase, Priority

if TYPE_CHECKING:
    from sparkopt.core.analyzer import DataProfile
    from sparkopt.codegen.context import CodegenContext


class MLPipelineCase(OptimizationCase):
    """
    Özellik mühendisliği → model eğitimi → değerlendirme akışı.

    Kararlar:
      - Feature DataFrame'i hem eğitim hem değerlendirme için kullanılır
        → cache (FanOut)
      - Eğitim döngüsü → her N iterasyonda localCheckpoint
      - Train/test split sonrası iki branch → persist before split
      - Model prediction (inference): ayrı bir branch, cache gerekmez
    """
    name = "ml_pipeline"
    priority = Priority.HIGH

    def __init__(
        self,
        has_cv: bool = False,       # Cross-validation var mı?
        num_folds: int = 5,
        feature_cols: list[str] | None = None,
    ):
        self.has_cv = has_cv
        self.num_folds = num_folds
        self.feature_cols = feature_cols or ["feature_1", "feature_2"]

    def matches(self, profile: "DataProfile") -> bool:
        lin = profile.lineage
        # ML pipeline sinyali: iteratif VEYA yüksek reuse
        return lin.has_iterative_loop or lin.estimated_reuse_count >= 3

    def apply(self, ctx: "CodegenContext") -> None:
        mem = ctx.profile.memory
        size_mb = ctx.profile.estimated_mb
        level = mem.recommended_storage_level
        folds = self.num_folds if self.has_cv else 1

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.ml import Pipeline")
        ctx.add_import("from pyspark.ml.feature import VectorAssembler, StandardScaler")
        ctx.add_import("from pyspark.ml.classification import RandomForestClassifier")
        ctx.add_import("from pyspark.ml.evaluation import BinaryClassificationEvaluator")
        ctx.add_hint("ml_pipeline", f"""\
# ML PİPELİNE — Optimal Persistence Stratejisi
# DF: {size_mb:.0f} MB  |  CV folds: {folds}  |  StorageLevel: {level}
#
# Persistence kararları:
#   1. raw_df              → YOK (bir kez kullanılır)
#   2. features_df         → cache (train+test ikisi de kullanır)
#   3. train_df, test_df   → cache (eğitim + değerlendirme)
#   4. Eğitim döngüsü      → her 5 iterasyonda localCheckpoint
#   5. Prediction sonucu   → YOK (son adım, tek kullanım)

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ── 1. Özellik mühendisliği (ağır) ──
features_df = (
    raw_df
    .join(lookup_df, on="id", how="left")         # enrichment
    .withColumn("log_amount", F.log1p("amount"))   # dönüşüm
    .na.fill(0, subset={self.feature_cols!r})       # null doldur
)
# Hem train hem test için kullanılacak → cache ZORUNLU
features_df.persist(StorageLevel.{level})
features_df.count()   # eager materializasyon

# ── 2. Train/Test split ──
train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
# Split sonrası iki branch → her ikisini de cache'le
train_df.persist(StorageLevel.{level})
test_df.persist(StorageLevel.MEMORY_ONLY)   # test genelde daha küçük

# ── 3. Pipeline tanımı ──
assembler = VectorAssembler(inputCols={self.feature_cols!r}, outputCol="raw_features")
scaler    = StandardScaler(inputCol="raw_features", outputCol="features")
clf       = RandomForestClassifier(featuresCol="features", labelCol="label",
                                    numTrees=100, maxDepth=10)
pipeline  = Pipeline(stages=[assembler, scaler, clf])

{"# ── 4. Cross-validation (her fold features_df'i cache'den okur) ──" if self.has_cv else "# ── 4. Doğrudan eğitim ──"}
{"grid = ParamGridBuilder().addGrid(clf.maxDepth, [5, 10, 15]).build()" if self.has_cv else "model = pipeline.fit(train_df)"}
{"cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid," if self.has_cv else ""}
{"                    evaluator=BinaryClassificationEvaluator()," if self.has_cv else ""}
{"                    numFolds=" + str(folds) + ", parallelism=4)" if self.has_cv else ""}
{"model = cv.fit(train_df)" if self.has_cv else ""}

# ── 5. Değerlendirme ──
predictions = model.transform(test_df)
auc = BinaryClassificationEvaluator().evaluate(predictions)
print(f"AUC: {{auc:.4f}}")

# ── 6. Temizlik (bellek baskısı varsa önce train) ──
train_df.unpersist()
test_df.unpersist()
features_df.unpersist()\
""")

    def explain(self) -> str:
        return (
            "ML pipeline: feature DF ikili fan-out (train+test) nedeniyle cache'lenir, "
            "eğitim döngüsü localCheckpoint ile lineage patlaması önlenir."
        )


class GraphAlgorithmCase(OptimizationCase):
    """
    PageRank, SSSP, Label Propagation gibi iteratif graf algoritmaları.

    Graf algoritmalarında özel sorun:
      - Her iterasyon tüm vertex ve edge DataFrame'ini tüketir
      - Vertex DF genellikle küçük → cache (MEMORY_ONLY)
      - Edge DF büyük → gerekirse MEMORY_AND_DISK
      - Her N iterasyonda MUTLAKA localCheckpoint (lineage explosion önleme)
      - Convergence check için agg → bu da bir action (double materializes)
    """
    name = "graph_algorithm"
    priority = Priority.CRITICAL

    def __init__(
        self,
        algorithm: str = "pagerank",    # pagerank | sssp | label_prop
        checkpoint_interval: int = 5,
        max_iterations: int = 50,
    ):
        self.algorithm = algorithm
        self.ckpt_interval = checkpoint_interval
        self.max_iter = max_iterations

    def matches(self, profile: "DataProfile") -> bool:
        return profile.lineage.has_iterative_loop

    def apply(self, ctx: "CodegenContext") -> None:
        size_mb = ctx.profile.estimated_mb
        mem = ctx.profile.memory

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("graph_algorithm", f"""\
# GRAF ALGORİTMASI — {self.algorithm.upper()} Optimizasyonu
# ─────────────────────────────────────────────────────────────
# Persistence stratejisi:
#   vertices_df → MEMORY_ONLY (küçük, her iterasyonda okunur)
#   edges_df    → {mem.recommended_storage_level} (büyük, her iterasyonda okunur)
#   Her {self.ckpt_interval} iterasyonda localCheckpoint (lineage patlaması önler)
#   Convergence check: action tetikler — cache'den oku, tekrar hesaplatma

from pyspark import StorageLevel

# Başlangıç: graf yapısını materielize et
vertices_df.persist(StorageLevel.MEMORY_ONLY)
edges_df.persist(StorageLevel.{mem.recommended_storage_level})
vertices_df.count()   # eager
edges_df.count()      # eager

# localCheckpoint için setCheckpointDir() gerekmez

# Başlangıç scores
scores = vertices_df.withColumn("score", F.lit(1.0 / vertices_df.count()))
scores = scores.localCheckpoint(eager=True)

prev_sum = float("inf")
DAMPING = 0.85

for iteration in range({self.max_iter}):
    # ── PageRank adımı ──
    contributions = (
        scores.join(edges_df, on="src_id", how="inner")
        .withColumn("contrib", F.col("score") / F.col("out_degree"))
        .groupBy("dst_id").agg(F.sum("contrib").alias("incoming"))
    )
    scores = (
        vertices_df
        .join(contributions, on="id", how="left")
        .withColumn("score",
            (1 - DAMPING) + DAMPING * F.coalesce(F.col("incoming"), F.lit(0.0)))
    )

    # Convergence kontrolü
    if iteration % {self.ckpt_interval} == 0:
        # Önce checkpoint → sonra convergence check (checkpointed'dan okur)
        scores = scores.localCheckpoint(eager=True)  # eski bloklar GC ile temizlenir

        current_sum = scores.agg(F.sum("score")).collect()[0][0]
        delta = abs(current_sum - prev_sum)
        print(f"Iter {{iteration}}: sum={{current_sum:.6f}}, delta={{delta:.6f}}")
        if delta < 1e-6:
            print("Converged!")
            break
        prev_sum = current_sum

# Temizlik
vertices_df.unpersist()
edges_df.unpersist()\
""")

    def explain(self) -> str:
        return (
            f"Graf algoritması ({self.algorithm}): vertex DF cache, edge DF persist, "
            f"her {self.ckpt_interval} iterasyonda localCheckpoint ile lineage explosion önlenir."
        )


class MultiStageAggCase(OptimizationCase):
    """
    Cube/Rollup + Window + Pivot gibi birden fazla agg aşaması.

    Problem: Her agg aşaması shuffle tetikler.
    Ham veri 3 farklı agg pipeline'a giriyorsa 3 kez hesaplanır.

    Çözüm:
      - Ham veri cache edilir (fan-out)
      - Her agg sonucu da cache edilir (sonraki agg'a input olabilir)
      - En pahalı agg sonucu checkpoint edilir
    """
    name = "multi_stage_agg"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        return (
            profile.row_count > 5_000_000
            and profile.lineage.estimated_reuse_count >= 2
        )

    def apply(self, ctx: "CodegenContext") -> None:
        mem = ctx.profile.memory
        level = mem.recommended_storage_level
        size_mb = ctx.profile.estimated_mb

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("multi_stage_agg", f"""\
# MULTI-STAGE AGG — Cube + Window + Pivot optimizasyonu
# DF: {size_mb:.0f} MB  |  StorageLevel: {level}
#
# Strateji: base DF cache → her agg dalı cache'den okur

from pyspark import StorageLevel
from pyspark.sql.window import Window

# ── Ham veri cache ──
base_df.persist(StorageLevel.{level})

# ── Agg 1: Rollup (hiyerarşik özet) ──
rollup_result = (
    base_df
    .rollup("year", "quarter", "month")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("order_count"),
        F.avg("aov").alias("avg_order_value"),
    )
    .fillna({{"year": "ALL", "quarter": "ALL", "month": "ALL"}})
)
# Rollup sonucu da birden fazla yerde kullanılıyorsa cache
rollup_result.persist(StorageLevel.MEMORY_ONLY)

# ── Agg 2: Window (kayan ortalama + kümülatif toplam) ──
win_30d = (Window.partitionBy("category")
                  .orderBy("event_date")
                  .rowsBetween(-29, 0))
win_all = Window.partitionBy("category").orderBy("event_date") \\
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)

windowed_df = base_df.select(
    "category", "event_date", "revenue",
    F.avg("revenue").over(win_30d).alias("rolling_30d_avg"),
    F.sum("revenue").over(win_all).alias("cumulative_revenue"),
    F.rank().over(
        Window.partitionBy("event_date").orderBy(F.desc("revenue"))
    ).alias("daily_rank"),
)

# ── Agg 3: Pivot (matris görünümü) ──
pivot_df = (
    base_df
    .groupBy("category")
    .pivot("region", ["EU", "US", "APAC"])
    .agg(F.sum("revenue"))
)


# ── Action: cache'ler bu noktada kullanılır (unpersist'ten ÖNCE yazın) ──
OUTPUT_BASE = "s3://bucket/reports"  # hedef dizin
rollup_result.write.mode("overwrite").parquet(OUTPUT_BASE + "/rollup")
windowed_df.write.mode("overwrite").parquet(OUTPUT_BASE + "/windowed")
pivot_df.write.mode("overwrite").parquet(OUTPUT_BASE + "/pivot")

# ── Temizlik (tüm action'lardan sonra) ──
base_df.unpersist()
rollup_result.unpersist()\
""")

    def explain(self) -> str:
        return (
            "Çok aşamalı aggregation: ham veri cache ile tek kez hesaplanır, "
            "rollup/window/pivot dalları cache'den okur."
        )


class IncrementalETLCase(OptimizationCase):
    """
    Upsert/merge tabanlı incremental pipeline.

    Problem:
      - Her çalıştırmada yeni veri → mevcut tabloyla merge
      - Lookup tablo çok büyük → broadcast olmaz
      - Değişen satırları bul (change detection) → iki kez scan

    Çözüm:
      - Değişen kayıtları filtrele → cache
      - Değişmeyen kısmı DISK_ONLY persist (büyük)
      - Merge sonucu checkpoint (lineage kırma + güvenli yazma)
    """
    name = "incremental_etl"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        lin = profile.lineage
        return lin.estimated_reuse_count >= 2 and not lin.has_iterative_loop

    def apply(self, ctx: "CodegenContext") -> None:
        size_mb = ctx.profile.estimated_mb
        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("incremental_etl", f"""\
# INCREMENTAL ETL — Upsert/Merge Optimizasyonu
# DF boyutu: ~{size_mb:.0f} MB
# ─────────────────────────────────────────────────────────────
# Strateji:
#   existing_df  → MEMORY_AND_DISK (büyük, değişmeyen kısım)
#   changed_df   → MEMORY_ONLY    (küçük, sadece delta)
#   merged_df    → checkpoint         (güvenli yazma, lineage kır)

from pyspark import StorageLevel

# 1. Mevcut tablo
existing_df = spark.read.parquet("s3://bucket/main_table/")
existing_df.persist(StorageLevel.MEMORY_AND_DISK)

# 2. Yeni (incremental) veri
new_df = spark.read.parquet("s3://bucket/incremental/date={{run_date}}/")

# 3. Değişen kayıtları tespit et (change detection)
#    Aynı key, farklı hash → update
#    Sadece new'de var → insert
#    Sadece existing'de var → delete (soft veya hard)
changed_df = (
    new_df.alias("n")
    .join(existing_df.alias("e"), on="primary_key", how="full")
    .filter(
        F.col("n.primary_key").isNull()   # delete
        | F.col("e.primary_key").isNull() # insert
        | (F.hash(*new_df.columns) != F.hash(*existing_df.columns))  # update
    )
)
changed_df.persist(StorageLevel.MEMORY_ONLY)
changed_count = changed_df.count()
print(f"Değişen kayıt: {{changed_count:,}}")

# 4. Merge (upsert mantığı)
merged_df = (
    existing_df.alias("e")
    .join(new_df.alias("n"), on="primary_key", how="full")
    .select(
        F.coalesce(F.col("n.primary_key"), F.col("e.primary_key")).alias("primary_key"),
        F.coalesce(F.col("n.value"), F.col("e.value")).alias("value"),
        F.coalesce(F.col("n.updated_at"), F.col("e.updated_at")).alias("updated_at"),
        # soft delete: n'de yoksa is_deleted=True
        F.when(F.col("n.primary_key").isNull(), F.lit(True))
         .otherwise(F.lit(False)).alias("is_deleted"),
    )
)

# 5. Checkpoint: yazma öncesi DAG'ı kır
#    Partition overwrite ile güvenli atomik yazma
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/etl/")
merged_df = merged_df.checkpoint(eager=True)

# 6. Partition overwrite ile yaz
(
    merged_df
    .repartition(200, "partition_col")
    .write
    .mode("overwrite")
    .partitionBy("partition_col")
    .parquet("s3://bucket/main_table/")
)

# 7. Temizlik
existing_df.unpersist()
changed_df.unpersist()\
""")

    def explain(self) -> str:
        return (
            "Incremental ETL: mevcut tablo MEMORY_AND_DISK, delta MEMORY_ONLY, "
            "merge sonucu checkpoint ile atomik yazma garantisi."
        )


class CachedBroadcastCase(OptimizationCase):
    """
    Sık güncellenen ama join'lerde referans alınan lookup tablosu.

    Senaryo: Exchange rate, product catalog, user segment tablosu
    → Küçük ama her batch'te yenilenmeli
    → Broadcast edilebilir ama stale veri sorun yaratabilir

    Çözüm: broadcast + MEMORY_ONLY + versiyon kontrolü
    """
    name = "cached_broadcast"
    priority = Priority.MEDIUM

    def matches(self, profile: "DataProfile") -> bool:
        return profile.is_small and profile.lineage.estimated_reuse_count >= 3

    def apply(self, ctx: "CodegenContext") -> None:
        size_mb = ctx.profile.estimated_mb
        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_hint("cached_broadcast", f"""\
# CACHED BROADCAST — Sık Referanslanan Küçük Tablo
# Boyut: ~{size_mb:.0f} MB (broadcast uygun ≤ 200 MB)
# ─────────────────────────────────────────────────────────────
# Strateji: lookup_df → cache + broadcast (iki katman)
#   cache    → driver'a re-fetch engeli (Spark serileştirme yükünü azaltır)
#   broadcast → her executor'a gönderilir (shuffle yok)
#
# Yenileme: her N batch'te unpersist + re-persist

from pyspark import StorageLevel

class LookupManager:
    \"\"\"Lookup tablosunu versiyonlu cache ile yönetir.\"\"\"

    def __init__(self, spark, path: str, ttl_batches: int = 10):
        self._spark = spark
        self._path = path
        self._ttl = ttl_batches
        self._df = None
        self._load_count = 0

    def get(self):
        if self._df is None or self._load_count >= self._ttl:
            if self._df is not None:
                self._df.unpersist()
            self._df = (
                self._spark.read.parquet(self._path)
                .persist(StorageLevel.MEMORY_ONLY)
            )
            self._df.count()   # eager
            self._load_count = 0
            print(f"Lookup yenilendi: {{self._path}}")
        self._load_count += 1
        return self._df

lookup_mgr = LookupManager(spark, "s3://bucket/lookup/", ttl_batches=10)

# Her batch'te:
for batch_id, batch_df in enumerate(batch_stream):
    lookup = lookup_mgr.get()   # cache'den veya yeniden yükler
    result = batch_df.join(
        F.broadcast(lookup), on="product_id", how="left"
    )
    result.write.mode("append").parquet(f"s3://bucket/output/batch={{batch_id}}/")\
""")

    def explain(self) -> str:
        return (
            "Küçük, sık referanslanan lookup: broadcast + MEMORY_ONLY ile "
            "sıfır shuffle ve TTL tabanlı otomatik yenileme."
        )


class StagedMaterializationCase(OptimizationCase):
    """
    Çok adımlı pipeline'da hangi noktalara cache/checkpoint konulacağını gösterir.

    Otomatik analiz:
      - Exchange (shuffle) sayısı → aralarına cache koy
      - DAG derinliği > 10 → checkpoint noktası ekle
      - Birden fazla consumer → önce cache
      - Son adım öncesi → checkpoint (güvenli yazma)
    """
    name = "staged_materialization"
    priority = Priority.HIGH

    def matches(self, profile: "DataProfile") -> bool:
        plan = profile.lineage
        return plan.depth > 8 or plan.wide_transform_count >= 3

    def apply(self, ctx: "CodegenContext") -> None:
        depth = ctx.profile.lineage.depth
        exchanges = ctx.profile.lineage.wide_transform_count
        mem = ctx.profile.memory

        ctx.add_import("from pyspark import StorageLevel")
        ctx.add_import("from pyspark.sql import functions as F")
        ctx.add_import("from pyspark.sql.window import Window")
        ctx.add_hint("staged_materialization", f"""\
# STAGED MATERIALİZASYON — Pipeline Kontrol Noktaları
# DAG derinliği: {depth}  |  Shuffle adımı: {exchanges}
# ─────────────────────────────────────────────────────────────
# Kural:
#   Her 2-3 shuffle'dan sonra → cache (recompute maliyeti yüksek)
#   DAG > 20 sonrası         → checkpoint (lineage break)
#   Son yazma öncesi         → checkpoint (atomik güvence)
#   2+ consumer noktası      → persist before fork

from pyspark import StorageLevel

# ── Aşama 1: Kaynak okuma + basit filtreler ──
# (cache değil — ucuz, kaynak doğrudan okunur)
raw = spark.read.parquet("...").filter(F.col("is_valid") == True)

# ── Aşama 2: İlk pahalı join (1. shuffle) ──
stage2 = raw.join(dim_A, on="key_a", how="left")
# → Burada cache: sonraki aşamalarda stage2 tekrar kullanılıyor
stage2.persist(StorageLevel.{mem.recommended_storage_level})

# ── Aşama 3: İkinci join + window (2-3. shuffle) ──
stage3 = (
    stage2
    .join(dim_B, on="key_b", how="inner")
    .withColumn("rank",
        F.rank().over(Window.partitionBy("cat").orderBy(F.desc("val"))))
)
# → Burada checkpoint: DAG derinliği artık ~{min(depth, 15)} adım
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints/pipeline/")
stage3 = stage3.checkpoint(eager=True)
stage2.unpersist()   # stage2 artık gerekmez

# ── Aşama 4: Aggregation + transformasyonlar ──
stage4 = (
    stage3
    .groupBy("category", "region")
    .agg(
        F.sum("revenue").alias("total"),
        F.approx_count_distinct("user_id").alias("uniq_users"),
    )
)

# ── Fan-out: stage4 iki farklı hedefe gidecek ──
stage4.persist(StorageLevel.MEMORY_ONLY)

output_A = stage4.filter(F.col("region") == "EU")
output_B = stage4.filter(F.col("region") != "EU").withColumn("flag", F.lit("non-EU"))

# ── Son yazma öncesi checkpoint (atomik güvence) ──
output_A = output_A.checkpoint(eager=True)
output_B = output_B.checkpoint(eager=True)

output_A.write.mode("overwrite").parquet("s3://bucket/eu_output/")
output_B.write.mode("overwrite").parquet("s3://bucket/non_eu_output/")

stage4.unpersist()\
""")

    def explain(self) -> str:
        return (
            "DAG derinliği > 8 veya 3+ shuffle: her 2-3 shuffle sonrası cache, "
            "derin noktada checkpoint ile pipeline güvenli adımlara bölünür."
        )


# ── Tüm kompleks case'ler ─────────────────────

ALL_COMPLEX_CASES: list[OptimizationCase] = [
    MLPipelineCase(),
    GraphAlgorithmCase(),
    MultiStageAggCase(),
    IncrementalETLCase(),
    CachedBroadcastCase(),
    StagedMaterializationCase(),
]
