#!/usr/bin/env bash
# Bootstrap Airflow + warehouse — run once after docker-compose up
set -e

echo "Waiting for Airflow to initialize..."
sleep 15

echo "Adding warehouse connection..."
docker exec airflow_webserver airflow connections add 'ecommerce_warehouse' \
  --conn-type 'postgres' \
  --conn-host 'etl-postgres' \
  --conn-login 'etl_user' \
  --conn-password 'etl_password' \
  --conn-schema 'ecommerce_dw' \
  --conn-port '5432'

echo "Unpausing ETL DAG..."
docker exec airflow_webserver airflow dags unpause ecommerce_etl_pipeline

echo "Done. Airflow UI: http://localhost:8080 (admin/admin)"
