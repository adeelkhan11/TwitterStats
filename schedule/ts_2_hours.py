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
    'wait_for_downstream': False,
    'start_date': datetime(2018, 12, 7, 7, 0, 0),
    'email': ['***@gmail.com'],  # todo: enter email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'ats_hourly',
    default_args=default_args,
    schedule_interval='0 3,7,10,13,15,17,19,21-23 * * *',
    catchup=False)

python_executable = '~/venv/bin/python3.7'
python_script_path = '~/PycharmProjects/TwitterStats'

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag,
    trigger_rule=TriggerRule.ALL_DONE)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='words_trends',
    bash_command='cd {};{} words.py trends'.format(python_script_path, python_executable),
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE)

t2 = BashOperator(
    task_id='draft_trends',
    bash_command='cd {};{} drafttrends.py'.format(python_script_path, python_executable),
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE)

t3 = BashOperator(
    task_id='update_lists',
    bash_command='cd {};{} update_lists.py'.format(python_script_path, python_executable),
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE)

latest_only >> t1 >> t2 >> t3
