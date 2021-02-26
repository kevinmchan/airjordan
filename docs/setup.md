# Setup Airflow project

## Using conda

```bash
# install dependencies
conda create -n airjordan python=3.7 pip=20.2.4
conda activate airjordan beautifulsoup4

# setup airflow
airflow db init
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org

# run airflow
cp example_dag.py ~/airflow/dags/
airflow webserver
airflow scheduler
```

## Using docker
1. [Install docker](https://docs.docker.com/engine/install/ubuntu/)
2. [Install docker-compose*](https://docs.docker.com/compose/install/)
3. [Running airflow with docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

\* If there are issues with permissions, [follow these instructions](https://github.com/circleci/circleci-docs/issues/1323).

## Create PostgresSQL instance

Follow [instructions](https://cloud.google.com/sql/docs/postgres/create-instance) to set up postgres Cloud SQL instance on GCP.

## Inspect postgres instance using pgadmin

```bash
docker pull dpage/pgadmin4
docker run -p 80:80 -e 'PGADMIN_DEFAULT_EMAIL=user@domain.com' -e 'PGADMIN_DEFAULT_PASSWORD=SuperSecret' -d dpage/pgadmin4
```

## Setup access to GCS

Follow [instructions](https://cloud.google.com/storage/docs/reference/libraries?authuser=1#client-libraries-install-python) to set up a service account to access files in GCS bucket.