#!/usr/bin/env bash
# Tear the stack down and delete every artifact the tutorial produced.
# The metadata DB and MLflow volumes go too - you get a clean Airflow and an
# empty experiment tracker. Your BigQuery tables and Airbyte are left alone.
set -euo pipefail
cd "$(dirname "$0")/.."

read -r -p "Delete containers, the metadata DB and MLflow volumes, and data/? [y/N] " reply
[[ "$reply" == "y" || "$reply" == "Y" ]] || { echo "Aborted."; exit 1; }

docker compose down --volumes --remove-orphans
rm -rf data/* dbt/*/target dbt/*/logs logs/*
touch data/.gitkeep logs/.gitkeep
echo "Clean. Run scripts/up.sh to start over."
