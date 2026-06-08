#!/usr/bin/env bash
# Populate the PostgreSQL OLTP database with ~12 months of seed data.
# Runs the generator inside the airflow-scheduler container (it already has
# psycopg2 + Faker installed and shares the docker network with postgres).
#
# Usage:  ./scripts/run_seed.sh
set -euo pipefail

echo ">> Waiting for the stack to be ready (postgres + airflow-scheduler)..."
docker compose up -d postgres airflow-scheduler

# give postgres a moment to finish init
sleep 5

echo ">> Generating & loading seed data into PostgreSQL..."
docker compose exec -T airflow-scheduler python /opt/airflow/pipeline/seed_data.py

echo ">> Done. The OLTP source is now populated."
