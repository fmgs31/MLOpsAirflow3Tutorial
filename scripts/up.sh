#!/usr/bin/env bash
# Start the tutorial stack (Linux / macOS / WSL2 / Git Bash).
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { cp env.example .env; echo "Created .env from env.example"; }

# Linux/WSL only: match the container user to yours so bind-mounted files stay writable.
if [ "$(uname -s)" = "Linux" ] && ! grep -q "^AIRFLOW_UID=$(id -u)$" .env 2>/dev/null; then
  sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
  echo "Set AIRFLOW_UID=$(id -u)"
fi

# Build one service, not all six: they share an image tag and parallel builds
# race on it ("image ... already exists").
docker compose build airflow-init
docker compose up -d

echo
echo "Waiting for the API server..."
until curl -sf http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; do sleep 3; done

echo "Airflow 3 UI: http://localhost:8080"
docker compose ps --format "table {{.Service}}\t{{.Status}}"
