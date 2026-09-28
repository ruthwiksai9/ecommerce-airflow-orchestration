"""DAG integrity tests — run with pytest before deploying."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_dags_loaded(dagbag):
    assert "ecommerce_etl_pipeline" in dagbag.dags
    assert "ecommerce_etl_backfill" in dagbag.dags


def test_etl_dag_task_count(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_pipeline")
    assert len(dag.tasks) >= 8


def test_etl_dag_schedule(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_pipeline")
    assert dag.schedule_interval == "0 2 * * *"


def test_backfill_dag_no_schedule(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_backfill")
    assert dag.schedule_interval is None


def test_etl_dag_has_required_tasks(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    required = {
        "extract",
        "transform",
        "quality_checks",
        "load_to_warehouse",
        "compute_metrics",
    }
    assert required.issubset(task_ids), f"Missing tasks: {required - task_ids}"


def test_etl_dag_retries(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_pipeline")
    for task in dag.tasks:
        if task.task_id not in ("start", "end"):
            assert task.retries >= 1, f"Task {task.task_id} has no retries configured"


def test_etl_dag_task_dependencies(dagbag):
    dag = dagbag.get_dag("ecommerce_etl_pipeline")
    task_map = {t.task_id: t for t in dag.tasks}

    extract_downstream = {t.task_id for t in task_map["extract"].downstream_list}
    assert "transform" in extract_downstream

    transform_downstream = {t.task_id for t in task_map["transform"].downstream_list}
    assert "quality_checks" in transform_downstream

    load_downstream = {t.task_id for t in task_map["load_to_warehouse"].downstream_list}
    assert "compute_metrics" in load_downstream
