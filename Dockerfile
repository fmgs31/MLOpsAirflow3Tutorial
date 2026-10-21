# Extending the official image is the supported way to add dependencies.
# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.3.1

# Two tools pin versions that clash with Airflow's own dependencies, so each
# gets a virtualenv of its own instead of going into Airflow's environment:
#
#   /opt/venvs/dbt     dbt Core + the BigQuery adapter, called by a BashOperator (§9)
#   /opt/venvs/alibi   alibi-detect, called by @task.external_python (§12)
#
# /opt is root-owned, so the folder is created as root and handed to the
# airflow user; everything else runs as airflow, as the image expects.
USER root
RUN mkdir -p /opt/venvs && chown airflow:0 /opt/venvs
USER airflow

RUN python -m venv /opt/venvs/dbt && \
    /opt/venvs/dbt/bin/pip install --no-cache-dir "dbt-core~=1.12.0" "dbt-bigquery~=1.12.0"

# alibi-detect needs numpy<2, and numpy<2 has no build for the image's Python
# 3.13, so this virtualenv gets a Python 3.12 of its own. `uv` (shipped in the
# image) downloads it; keeping it under /opt/venvs makes it readable whatever
# user the containers run as. pyarrow lets pandas read the Parquet windows the
# monitoring Dag writes (§12); it is the version Airflow's own environment has.
ENV UV_PYTHON_INSTALL_DIR=/opt/venvs/python
RUN uv venv --python 3.12 /opt/venvs/alibi && \
    uv pip install --python /opt/venvs/alibi/bin/python --no-cache \
      "alibi-detect==0.13.0" "pandas<3" "pyarrow==25.0.0"

COPY requirements-image.txt /tmp/requirements-image.txt

# Constraints keep the pre-installed Airflow dependency set intact while the
# extra packages are resolved on top of it. The Python version is read from the
# image so this keeps working if the base tag changes.
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir \
      -r /tmp/requirements-image.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-${PYTHON_VERSION}.txt"
