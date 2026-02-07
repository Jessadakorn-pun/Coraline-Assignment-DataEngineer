#!/bin/bash
set -e

PROJECT_NAME="coraline-assignment"
DAG_ID="elt_dbt_pipeline"
CSV_PATH="./data/FoodSales.csv"
MAX_RETRY=3
RETRY_COUNT=0

echo "=== Step 0: Check FoodSales.csv (host) ==="
if [ ! -f "$CSV_PATH" ]; then
  echo "ERROR: FoodSales.csv not found at $CSV_PATH"
  exit 1
fi
echo "FoodSales.csv found on host"

echo ""
echo "=== Step 1: Build dbt image ==="
docker build -t custom-model-dbt:latest dbt/
sleep 2

echo ""
echo "=== Step 2: Init Airflow ==="
docker-compose up airflow-init -d
sleep 5

echo ""
echo "=== Step 3: Start services ==="
docker-compose up -d
sleep 5

echo ""
echo "=== Step 4: Verify /data mount BEFORE trigger ==="
docker exec airflow-webserver sh -c "ls /data"
docker exec airflow-scheduler sh -c "ls /data"
sleep 2

echo ""
echo "=== Step 5: Unpause DAG ==="
docker exec airflow-webserver airflow dags unpause $DAG_ID
sleep 5

echo ""
echo "=== Step 6: Manual trigger with retry ==="
docker exec airflow-webserver airflow dags trigger $DAG_ID

echo ""
echo "=== Pipeline completed successfully ==="