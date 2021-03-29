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

```bash
docker-compose up --build
```

## Run using pipenv

```bash
# install dependencies
pipenv install --dev 

# setup airflow
export AIRFLOW_HOME=/path/to/project/directory
airflow db init
airflow users create --username airflow --password airflow --firstname Anonymous --lastname Admin --role Admin --email admin@example.org

# run airflow
airflow webserver
airflow scheduler
```