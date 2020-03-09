import os
from datetime import datetime, timedelta

from airflow import DAG
from operators.odds import OddsApiOperator

DATA_DIR = '/media/montebello/odds_api'

default_args = {
    "owner": "healz",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 7),
    "email": ["healyt22@gmail.com.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id = "Odds",
    default_args = default_args,
    schedule_interval = "0 11 * * *"
)

t1 = OddsApiOperator(
    task_id = 'GetSports',
    endpoint = 'sports',
    out_filepath = os.path.join(DATA_DIR, 'sports', '{{ ds }}.json'),
    dag = dag
)
