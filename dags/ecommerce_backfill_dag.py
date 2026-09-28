"""
Ecommerce ETL Backfill DAG

Manual trigger only. Use when pipeline missed runs or data needs reprocessing.
Set start_date and end_date in DAG config:
  {"start_date": "2024-01-01", "end_date": "2024-03-31"}
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="ecommerce_etl_backfill",
    description="Manual backfill for missed ecommerce ETL runs",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "backfill", "manual"],
    doc_md=__doc__,
)


def validate_backfill_config(**context):
    """Validate DAG run config has required date params."""
    conf = context["dag_run"].conf or {}
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    if not start_date or not end_date:
        raise ValueError(
            "Backfill requires config: {\"start_date\": \"YYYY-MM-DD\", \"end_date\": \"YYYY-MM-DD\"}"
        )

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError(f"start_date {start_date} must be before end_date {end_date}")

    delta = (end - start).days
    if delta > 90:
        raise ValueError(f"Backfill window {delta} days exceeds 90-day limit")

    context["ti"].xcom_push(key="backfill_days", value=delta + 1)
    print(f"Backfill validated: {start_date} → {end_date} ({delta + 1} days)")


def run_backfill_pipeline(**context):
    """Re-run ETL for specified date range."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from src.extract.downloader import extract_all
    from src.load.loader import load_all
    from src.metrics.aggregator import run_all_metrics
    from src.transform.cleaner import transform_all
    from src.transform.quality import run_quality_checks

    conf = context["dag_run"].conf
    print(f"Running backfill: {conf['start_date']} → {conf['end_date']}")

    dataframes = extract_all("data/raw")
    cleaned, _ = transform_all(dataframes)
    quality_results = run_quality_checks(cleaned)

    failed_quality = [r for r in quality_results if not r.passed]
    if failed_quality:
        print(f"Warning: quality issues in {[r.dataset for r in failed_quality]}, loading anyway for backfill")

    load_stats = load_all(cleaned)
    metrics_stats = run_all_metrics()

    print(f"Backfill complete | load: {load_stats} | metrics: {len(metrics_stats)} queries")


with dag:
    start = EmptyOperator(task_id="start")

    validate = PythonOperator(
        task_id="validate_config",
        python_callable=validate_backfill_config,
    )

    backfill = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill_pipeline,
        execution_timeout=timedelta(hours=2),
    )

    end = EmptyOperator(task_id="end")

    start >> validate >> backfill >> end
