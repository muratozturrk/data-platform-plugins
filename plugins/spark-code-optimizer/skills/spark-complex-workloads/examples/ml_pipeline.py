"""ML pipeline with cross-validation: where to persist and why (sparkopt case: ml_pipeline).

Persistence plan
  raw_df / lookup_df  -> no persist      (read once, feeds features_df)
  features_df         -> MEMORY_AND_DISK (randomSplit parent: must be stable; 2 branches)
  train_df            -> MEMORY_AND_DISK (CrossValidator scans it numFolds x grid times)
  test_df             -> no persist      (scored once; persist only if several evaluators)
  predictions         -> no persist      (final, single use)
  RandomForest        -> checkpointInterval needs a *reliable* checkpoint dir

Corrections vs. the sparkopt template: no MEMORY_ONLY_SER (Scala-only), F imported,
lookup_df defined, unpersist only after the last action, the "localCheckpoint every 5
iterations" idea replaced by the estimator's own checkpointInterval.

Run: spark-submit ml_pipeline.py --input s3a://bucket/orders/ --lookup s3a://bucket/customers/ \
         --checkpoint-dir s3a://bucket/_ckpt/ml/
"""
import argparse

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NUMERIC_COLS = ["amount", "items", "days_since_signup"]


def main(input_path: str, lookup_path: str, checkpoint_dir: str, num_folds: int) -> None:
    spark = (
        SparkSession.builder.appName("ml-cv-pipeline")
        # Kryo is a builder/submit setting, never spark.conf.set at runtime.
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    # Reliable dir: RandomForest/GBT/ALS checkpointInterval is silently ignored without it.
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    raw_df = spark.read.parquet(input_path)
    lookup_df = spark.read.parquet(lookup_path).select("customer_id", "segment_score")

    # 1. Feature engineering (join + transforms): expensive, consumed by two branches.
    features_df = (
        raw_df.join(F.broadcast(lookup_df), on="customer_id", how="left")
        .withColumn("log_amount", F.log1p("amount"))
        .na.fill(0, subset=NUMERIC_COLS + ["segment_score"])
        .select("label", "log_amount", "segment_score", *NUMERIC_COLS)
    )
    # Persist BEFORE randomSplit: each split re-evaluates its parent; an unstable parent
    # (re-read source, shuffle order) can make train/test overlap or drop rows.
    features_df.persist(StorageLevel.MEMORY_AND_DISK)
    total = features_df.count()  # eager materialization

    train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
    train_df.persist(StorageLevel.MEMORY_AND_DISK)
    train_df.count()

    assembler = VectorAssembler(
        inputCols=["log_amount", "segment_score", *NUMERIC_COLS], outputCol="raw_features"
    )
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")
    clf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        cacheNodeIds=True,       # node-id cache grows lineage per tree level...
        checkpointInterval=10,   # ...so it is checkpointed to the reliable dir every 10 iters
    )
    pipeline = Pipeline(stages=[assembler, scaler, clf])

    grid = ParamGridBuilder().addGrid(clf.maxDepth, [5, 10]).addGrid(clf.numTrees, [50, 100]).build()
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluator,
        numFolds=num_folds,
        parallelism=2,   # concurrent fits share executor memory with the cached train_df
        seed=42,
    )
    # CrossValidator caches each fold's train/validation subsets itself; train_df is the
    # parent of all of them, so caching it saves numFolds x len(grid) source re-reads.
    model = cv.fit(train_df)

    predictions = model.transform(test_df)  # single use -> no persist
    auc = evaluator.evaluate(predictions)
    print(f"rows={total} folds={num_folds} best AUC={max(model.avgMetrics):.4f} test AUC={auc:.4f}")

    # Unpersist only after the last action that needs the cache.
    train_df.unpersist()
    features_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--lookup", required=True)
    p.add_argument("--checkpoint-dir", required=True, help="HDFS/S3/ABFS path on a cluster")
    p.add_argument("--folds", type=int, default=5)
    a = p.parse_args()
    main(a.input, a.lookup, a.checkpoint_dir, a.folds)
