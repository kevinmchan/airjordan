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
    'full_lineup',
    description='NBA DFS lineups data load',
    schedule_interval=None,
    start_date=days_ago(0),
    tags=['dfs', 'nba'],
) as dag:
    for season in (2016, 2017, 2018, 2019, 2020):
        kwargs = {
            'postgres_connection_str': postgres_connection_str,
            'msf_api_key': MSF_API_KEY,
            'season': f"{season}-{season+1}-regular",
            'last_n_days': 1000,
        }

        load_lineup_op = PythonOperator(
            task_id=f"load_lineup_{season}",
            python_callable=load_lineup,
            templates_dict=kwargs,
        )