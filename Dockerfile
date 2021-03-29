FROM apache/airflow:2.0.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt