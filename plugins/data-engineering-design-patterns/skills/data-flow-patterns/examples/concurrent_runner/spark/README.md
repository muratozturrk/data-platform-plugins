# Concurrent Runner -- Spark equivalent

No dedicated code: the run concurrency of a pipeline is owned by the orchestrator/cluster, not
by the job. Spark-side knobs that implement the same idea:

- **AWS EMR**: `StepConcurrencyLevel` (1-256) lets several steps (job runs) execute in
  parallel on one cluster: `aws emr modify-cluster --cluster-id j-XXX --step-concurrency-level 5`.
- **One application, many jobs**: submit independent jobs from threads in the same
  SparkSession and set `spark.scheduler.mode=FAIR` with pools
  (`spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ingestion")`) so one job
  can't monopolise executors -- the in-cluster version of workload management.
- **Kubernetes / YARN**: queues/quotas (YARN capacity scheduler queue, K8s ResourceQuota)
  bound what a team's concurrent runs may consume.

Job requirement for safe concurrency: each run writes only its own partition
(`mode("overwrite")` + `partitionOverwriteMode=dynamic`) and reads no mutable shared state.
