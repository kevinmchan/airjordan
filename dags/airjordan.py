import sqlalchemy
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from dependencies.nbadataload import load_games, load_game_logs, load_dfs_stats, load_lineup
from airflow.utils.dates import days_ago
from dotenv import load_dotenv


load_dotenv()


POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PW = Variable.get("POSTGRES_PW")
POSTGRES_HOST = Variable.get("POSTGRES_HOST")
MSF_API_KEY = Variable.get("MSF_API_KEY")
postgres_connection_str = f'postgres+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}/nba'


with DAG(
    'airjordan',
    description='NBA DFS modeling pipeline',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(0),
    tags=['dfs', 'nba'],
) as dag:
    kwargs = {
        'postgres_connection_str': postgres_connection_str,
        'msf_api_key': MSF_API_KEY,
        'season': "{{ dag_run.conf.get('season', '2020-2021-regular') }}",
        'last_n_days': "{{ dag_run.conf.get('last_n_days', 7) }}",
    }

    load_games_op = PythonOperator(
        task_id="load_games",
        python_callable=load_games,
        templates_dict=kwargs,
    )

    load_game_logs_op = PythonOperator(
        task_id="load_game_logs",
        python_callable=load_game_logs,
        templates_dict=kwargs,
    )

    load_lineup_op = PythonOperator(
        task_id="load_lineup",
        python_callable=load_lineup,
        templates_dict=kwargs,
    )

    load_dfs_stats_op = PythonOperator(
        task_id="load_dfs_stats",
        python_callable=load_dfs_stats,
        templates_dict=kwargs,
    )

    load_games_op >> [load_game_logs_op, load_lineup_op, load_dfs_stats_op]