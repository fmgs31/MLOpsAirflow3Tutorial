# Extending the official image is the supported way to add dependencies.
# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.3.1

COPY requirements-image.txt /tmp/requirements-image.txt

# Constraints keep the pre-installed Airflow dependency set intact while the
# extra packages are resolved on top of it. The Python version is read from the
# image so this keeps working if the base tag changes.
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir \
      -r /tmp/requirements-image.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-${PYTHON_VERSION}.txt"
