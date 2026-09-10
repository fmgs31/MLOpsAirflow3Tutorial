#!/usr/bin/env bash
# Native (no Docker) install: Linux, macOS, or WSL2. Airflow does not run on
# Windows natively - use scripts/up.ps1 or run this inside WSL2.
set -euo pipefail
cd "$(dirname "$0")/.."

AIRFLOW_VERSION=3.3.1
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate

# The constraints file is not optional. Without it pip resolves a dependency set
# Airflow was never tested against.
pip install --upgrade pip
pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME="${PWD}/airflow"
export PYTHONPATH="${PWD}/src"

# `airflow db init` was REMOVED in Airflow 3.
airflow db migrate

# Airflow 3 no longer seeds default connections; FileSensor needs fs_default.
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}' 2>/dev/null || true

cat <<EOF

Done. Every shell that runs Airflow needs:

    export AIRFLOW_HOME=${PWD}/airflow
    export PYTHONPATH=${PWD}/src
    export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/dags
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    source .venv/bin/activate

Then either the all-in-one:

    airflow standalone

or the four processes separately (this is what production looks like):

    airflow api-server --port 8080   # was 'airflow webserver' in Airflow 2
    airflow scheduler
    airflow dag-processor            # its own process in Airflow 3
    airflow triggerer                # needed for deferrable tasks and async deadlines
EOF
