# Ecommerce Airflow Orchestration

[![CI](https://github.com/ruthwiksai9/ecommerce-airflow-orchestration/actions/workflows/ci.yml/badge.svg)](https://github.com/ruthwiksai9/ecommerce-airflow-orchestration/actions/workflows/ci.yml)

Apache Airflow 2.8 orchestration layer for the [ecommerce-etl-pipeline](https://github.com/ruthwiksai9/ecommerce-etl-pipeline). Runs the full ETL on a daily schedule with retry logic, quality gates, and a custom operator/sensor library.

## DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `ecommerce_etl_pipeline` | `0 2 * * *` (02:00 UTC) | Daily production run |
| `ecommerce_etl_backfill` | Manual trigger only | Reprocess a date range |

## Pipeline Flow

```
start
  └── check_db_connection
        └── extract                          ← download Olist CSVs
              └── transform                  ← Pandas cleaning
                    └── quality_checks       ← null/dupe/FK/range gates
                          ├── load_to_warehouse  ← batch load → PostgreSQL raw schema
                          │     └── compute_metrics ← aggregate reporting tables
                          │           └── pipeline_complete
                          │                 └── end
                          └── notify_quality_failure
                                └── end
```

## Custom Plugins

| Plugin | Type | Description |
|--------|------|-------------|
| `DataQualityGateOperator` | Operator | Runs SQL assertion checks against any table, fails task on violation |
| `DataAvailabilitySensor` | Sensor | Polls table row count until minimum threshold met |

## Quickstart

```bash
git clone https://github.com/ruthwiksai9/ecommerce-airflow-orchestration.git
cd ecommerce-airflow-orchestration

# Start all services
docker-compose up -d

# Initialize connections and unpause DAG
bash scripts/init.sh

# Open Airflow UI
open http://localhost:8080
# Login: admin / admin
```

## Manual Backfill

```bash
# Trigger backfill via Airflow CLI
docker exec airflow_webserver airflow dags trigger ecommerce_etl_backfill \
  --conf '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'

# Or via UI: DAGs → ecommerce_etl_backfill → Trigger DAG w/ config
```

## Testing

```bash
# Install deps
pip install -r requirements.txt

# Run DAG integrity tests (no Airflow server needed)
pytest tests/ -v
```

## Architecture

```
ecommerce-airflow-orchestration/
├── dags/
│   ├── ecommerce_etl_dag.py        # Production daily DAG
│   └── ecommerce_backfill_dag.py   # Manual backfill DAG
├── plugins/
│   ├── operators/
│   │   └── quality_gate_operator.py   # Custom DQ operator
│   └── sensors/
│       └── data_availability_sensor.py # Row-count sensor
├── config/
│   └── airflow.cfg                 # Airflow configuration
├── tests/
│   └── test_dag_integrity.py       # Structural DAG tests
├── scripts/
│   └── init.sh                     # Post-startup bootstrap
└── docker-compose.yml              # Airflow + warehouse stack
```

## Key Design Decisions

**Quality gate branching** — if DQ checks fail, the pipeline halts before load and routes to `notify_quality_failure`. Prevents corrupt data reaching the warehouse.

**Exponential backoff retries** — 3 retries with 5min base delay, capped at 30min. Handles transient network/DB issues without flooding the scheduler.

**XCom for inter-task state** — row counts, data directory path, and quality results passed via XCom rather than shared filesystem.

**Catchup disabled** — `catchup=False` prevents Airflow from triggering all missed runs on first deploy. Use the backfill DAG for intentional historical loads.
