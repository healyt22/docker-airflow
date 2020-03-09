from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "healz",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 14),
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
    dag_id = "Tweets",
    default_args = default_args,
    schedule_interval = "0 */1 * * *"
)

t1 = BashOperator(
    task_id = "GetTweetDataFromAPI",
    bash_command = "python ~/git/politopics/politopics/twitter_api.py",
    dag = dag
)
