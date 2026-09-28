"""
Ecommerce ETL Pipeline DAG

Orchestrates the full Extract → Transform → Quality → Load → Metrics pipeline.
Runs daily at 02:00 UTC. Retries 3× with exponential backoff on failure.
Sends alerts on SLA miss or task failure.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=2),
}

dag = DAG(
    dag_id="ecommerce_etl_pipeline",
    description="Daily ecommerce ETL: ingest Olist data → PostgreSQL star schema → metrics",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "etl", "production"],
    doc_md=__doc__,
)


# ─── Task functions ──────────────────────────────────────────────────────────

def check_db_connection(**context):
    """Verify warehouse DB is reachable before starting pipeline."""
    hook = PostgresHook(postgres_conn_id="ecommerce_warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    conn.close()
    context["ti"].xcom_push(key="db_check", value="passed")


def extract_data(**context):
    """Download source CSVs with retry logic."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.extract.downloader import extract_all

    logical_date = context["logical_date"].strftime("%Y-%m-%d")
    data_dir = f"/opt/airflow/data/raw/{logical_date}"

    dataframes = extract_all(data_dir)
    row_counts = {name: len(df) for name, df in dataframes.items()}
    context["ti"].xcom_push(key="extract_row_counts", value=row_counts)
    context["ti"].xcom_push(key="data_dir", value=data_dir)

    if not dataframes:
        raise ValueError("Extraction returned no datasets")

    return row_counts


def transform_data(**context):
    """Clean and standardize all datasets."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.extract.downloader import extract_all
    from src.transform.cleaner import transform_all

    data_dir = context["ti"].xcom_pull(key="data_dir", task_ids="extract")
    raw = extract_all(data_dir)
    cleaned, stats = transform_all(raw)

    context["ti"].xcom_push(key="transform_stats", value=stats)
    context["ti"].xcom_push(key="cleaned_count", value=len(cleaned))
    return stats


def run_quality_checks(**context):
    """Execute data quality gates — fail DAG if critical checks fail."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.extract.downloader import extract_all
    from src.transform.cleaner import transform_all
    from src.transform.quality import run_quality_checks as _run_checks

    data_dir = context["ti"].xcom_pull(key="data_dir", task_ids="extract")
    raw = extract_all(data_dir)
    cleaned, _ = transform_all(raw)
    results = _run_checks(cleaned)

    failed = [r for r in results if not r.passed]
    context["ti"].xcom_push(key="quality_failed", value=[r.dataset for r in failed])

    if failed:
        raise ValueError(f"Quality checks failed: {[r.dataset for r in failed]}")

    return {"passed": len(results) - len(failed), "failed": len(failed)}


def branch_on_quality(**context):
    """Route to load if quality passed, to notify_quality_failure otherwise."""
    failed = context["ti"].xcom_pull(key="quality_failed", task_ids="quality_checks")
    if failed:
        return "notify_quality_failure"
    return "load_to_warehouse"


def load_to_warehouse(**context):
    """Batch load cleaned data to raw schema in warehouse."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.extract.downloader import extract_all
    from src.load.loader import load_all
    from src.transform.cleaner import transform_all

    data_dir = context["ti"].xcom_pull(key="data_dir", task_ids="extract")
    raw = extract_all(data_dir)
    cleaned, _ = transform_all(raw)
    stats = load_all(cleaned)

    context["ti"].xcom_push(key="load_stats", value=stats)
    return stats


def run_metrics(**context):
    """Compute aggregated metrics tables."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.metrics.aggregator import run_all_metrics

    results = run_all_metrics()
    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        raise ValueError(f"Metrics failed: {[r['query'] for r in failed]}")
    return results


def pipeline_complete(**context):
    """Log pipeline summary from XComs."""
    extract_counts = context["ti"].xcom_pull(key="extract_row_counts", task_ids="extract")
    load_stats = context["ti"].xcom_pull(key="load_stats", task_ids="load_to_warehouse")
    print(f"Pipeline complete | extracted: {extract_counts} | loaded: {load_stats}")


# ─── Task definitions ─────────────────────────────────────────────────────────

with dag:
    start = EmptyOperator(task_id="start")

    db_check = PythonOperator(
        task_id="check_db_connection",
        python_callable=check_db_connection,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        execution_timeout=timedelta(minutes=30),
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=20),
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
        execution_timeout=timedelta(minutes=10),
    )

    notify_quality_failure = BashOperator(
        task_id="notify_quality_failure",
        bash_command='echo "QUALITY GATE FAILED — pipeline halted. Check Airflow logs."',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
        execution_timeout=timedelta(minutes=30),
    )

    metrics = PythonOperator(
        task_id="compute_metrics",
        python_callable=run_metrics,
        execution_timeout=timedelta(minutes=15),
    )

    complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=pipeline_complete,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # DAG dependency chain
    (
        start
        >> db_check
        >> extract
        >> transform
        >> quality
        >> [load, notify_quality_failure]
    )
    load >> metrics >> complete >> end
    notify_quality_failure >> end
