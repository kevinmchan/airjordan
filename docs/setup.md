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
3. TODO: running airflow with docker-compose

\* If there are issues with permissions, [follow these instructions](https://github.com/circleci/circleci-docs/issues/1323).