"""OpenLineage events emitted by the ETL job itself.

The Iceberg snapshot id goes into the standard DatasetVersion facet and the run id into the snapshot
summary, so the lineage event and the table commit can be joined exactly instead of by timestamp.
Uses the current client API (openlineage.client.event_v2 / facet_v2); a FileTransport stands in for
the HTTP collector (Marquez or any OpenLineage backend: swap in HttpConfig).
"""
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (data_quality_assertions_dataset as dq, dataset_version_dataset,
                                         source_code_location_job)
from openlineage.client.transport.file import FileConfig, FileTransport

PRODUCER = "https://github.com/my-org/etl-jobs"
NAMESPACE = "datalake-engine"


def client(log_file: str) -> OpenLineageClient:
    return OpenLineageClient(transport=FileTransport(FileConfig(log_file_path=log_file, append=True)))


def run_event(state: RunState, run_id: str, job_name: str, table: str, snapshot_id: int | None = None,
              checks: dict[str, bool] | None = None) -> RunEvent:
    facets = {}
    if snapshot_id is not None:
        facets["version"] = dataset_version_dataset.DatasetVersionDatasetFacet(datasetVersion=str(snapshot_id))
    if checks:
        facets["dataQualityAssertions"] = dq.DataQualityAssertionsDatasetFacet(
            assertions=[dq.Assertion(assertion=name, success=ok) for name, ok in checks.items()])
    return RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=job_name, facets={
            "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
                type="git", url="https://github.com/my-org/etl-jobs", branch="main")}),
        outputs=[OutputDataset(namespace="iceberg://lake", name=table, facets=facets)],
        producer=PRODUCER,
    )
