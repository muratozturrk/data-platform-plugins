"""DAG integrity tests (T-af-001): every generated DAG imports, has no cycles, follows naming and policy."""
import re, pathlib, pytest
from airflow.models import DagBag

DAGS = pathlib.Path(__file__).resolve().parents[2] / "dags"
DAG_ID = re.compile(r"^t-[a-z0-9]{2,16}__[a-z0-9-]+__[a-z0-9-]+$")

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)

def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}

@pytest.mark.parametrize("dag_id", [p.stem for p in DAGS.glob("*.py")])
def test_dag_policy(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"{dag_id} not loaded"
    assert DAG_ID.match(dag.dag_id), f"dag_id {dag.dag_id} violates naming scheme"
    assert dag.default_args.get("owner") and dag.default_args.get("retries", 0) >= 1
    assert {"platform-generated"} <= set(dag.tags)
    dag.test_cycle() if hasattr(dag, "test_cycle") else dag.topological_sort()
    for task in dag.tasks:
        assert re.match(r"^[a-z]+_[a-z0-9_]+$", task.task_id), f"task_id {task.task_id} violates naming scheme"
