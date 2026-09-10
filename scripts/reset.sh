#!/usr/bin/env bash
# Tear the stack down and delete every artifact the tutorial produced.
# The metadata DB volume goes too - you get a clean Airflow.
set -euo pipefail
cd "$(dirname "$0")/.."

read -r -p "Delete containers, the metadata DB volume, data/ and model_registry/? [y/N] " reply
[[ "$reply" == "y" || "$reply" == "Y" ]] || { echo "Aborted."; exit 1; }

docker compose down --volumes --remove-orphans
rm -rf data/raw data/warehouse model_registry/* logs/*
touch data/.gitkeep model_registry/.gitkeep logs/.gitkeep
echo "Clean. Run scripts/up.sh to start over."
