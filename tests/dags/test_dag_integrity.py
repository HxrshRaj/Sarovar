"""DAG import / integrity checks. Run inside the airflow container: make test-dags"""
import pytest

pytest.importorskip("airflow.sdk")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

EXPECTED = {"sarovar_ingest": 11, "sarovar_freshness": 4, "sarovar_transform": 3}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", bundle_path="/opt/airflow/dags", bundle_name="dags-folder")


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


@pytest.mark.parametrize("dag_id,n_tasks", EXPECTED.items())
def test_dag_shape(dagbag, dag_id, n_tasks):
    dag = dagbag.dags[dag_id]
    assert len(dag.tasks) == n_tasks
    assert dag.tags and "sarovar" in dag.tags


def test_ingest_is_serialised_and_catches_up(dagbag):
    dag = dagbag.dags["sarovar_ingest"]
    assert dag.catchup is True and dag.max_active_runs == 1  # partition rewrites must not race
    assert {"window_start", "window_end"} <= set(dag.params.keys())


def test_dq_gates_precede_registration(dagbag):
    dag = dagbag.dags["sarovar_ingest"]
    reg = dag.get_task("register_trino")
    upstream = {t.task_id for t in reg.upstream_list}
    assert upstream == {f"dq_{t}" for t in ("users", "merchants", "transactions", "refunds")}


def test_no_cycles(dagbag):
    for dag in dagbag.dags.values():
        dag.validate()
