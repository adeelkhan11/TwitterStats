"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 7, 7, 0, 0),
    'email': ['***@gmail.com'],  # todo: enter email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'ats_get_tweets',
    default_args=default_args,
    schedule_interval='10,30,50 * * * *',
    catchup=False)

python_executable = '~/venv/bin/python3.7'
python_script_path = '~/PycharmProjects/TwitterStats'

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

# t1, t2 and t3 are examples of tasks created by instantiating operators
# get_pending = BashOperator(
#     task_id='get_pending',
#     bash_command='cd {};{} getpending.py'.format(python_script_path, python_executable),
#     dag=dag)

t1 = BashOperator(
    task_id='words_home_timeline',
    bash_command='cd {};{} words.py home_timeline'.format(python_script_path, python_executable),
    dag=dag)

t2 = BashOperator(
    task_id='words_lists',
    bash_command='cd {};{} words.py lists'.format(python_script_path, python_executable),
    dag=dag)

t3 = BashOperator(
    task_id='draft_top_tweets',
    bash_command='cd {};{} drafttoptweets.py'.format(python_script_path, python_executable),
    dag=dag)

t4 = BashOperator(
    task_id='publish',
    bash_command='cd {};{} publish.py'.format(python_script_path, python_executable),
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE)

latest_only >> t1 >> t2 >> t3 >> t4
