# Airjordan

An airflow pipeline for building and scoring NBA daily fantasy models, including:
- ETL to load nba stats
- generating model features
- scoring model predictions


## Dependencies

This project requires the following dependencies in order to work:
- A postgres sql database to store nba stats in ETL job: Follow these [instructions](https://cloud.google.com/sql/docs/postgres/create-instance) to set up postgres Cloud SQL instance on GCP.
- Google Cloud Storage bucket to store model objects: Follow these [instructions](https://cloud.google.com/storage/docs/reference/libraries?authuser=1#client-libraries-install-python) to set up a service account to access files in GCS bucket. You'll also need to download private keys required to access your bucket.
- Docker and docker compose: Learn how to [install docker](https://docs.docker.com/engine/install/ubuntu/) and [install docker-compose*](https://docs.docker.com/compose/install/).
- API key for mysportsfeed: Get an API key for nba data feeds at [mysportsfeed.com](https://www.mysportsfeed.com). Low cost options are available for personal use.
- Environment file: Create a .env file in the project root directory (see the [.env.template](./.env.template) file) to store your mysportsfeed api key, postgres credentials and path to GCS json credentials file.


\* If there are issues docker permissions, [follow these instructions](https://github.com/circleci/circleci-docs/issues/1323).


## Run using docker (recommended)

This implementation of the airflow pipeline uses airflow's `CeleryExecutor` which allows for parallel execution and easy scaling up of worker nodes.

```bash
docker-compose up --build
```

The airflow webserver is available on port `8080` on the host machine.

## Run using pipenv

Note by default, this local developer version of the airflow pipeline, using pipenv, uses the `SequentialExecutor` which does not allow for parallel execution.

```bash
# install dependencies
pipenv install --dev 

# activate environment
pipenv shell

# setup airflow
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create --username airflow --password airflow --firstname Anonymous --lastname Admin --role Admin --email admin@example.org

# run airflow
airflow webserver
airflow scheduler
```