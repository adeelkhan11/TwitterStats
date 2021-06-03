"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2018, 12, 7, 7, 0, 0),
    'email': ['***@gmail.com'],  # todo: enter email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'ats_daily', default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1)

python_executable = '~/venv/bin/python3.7'
python_script_path = '~/PycharmProjects/TwitterStats'
# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='words_trends',
    bash_command='cd {};{} words.py trends'.format(python_script_path, python_executable),
    dag=dag)

t2 = BashOperator(
    task_id='tweeter_promotion',
    bash_command='cd {};{} tweeter_promotion.py'.format(python_script_path, python_executable),
    dag=dag)

hu = BashOperator(
    task_id='hashtag_update',
    bash_command='cd {};{} hashtag_update.py'.format(python_script_path, python_executable),
    dag=dag)

t4 = BashOperator(
    task_id='findbots',
    bash_command='cd {};{} findbots.py {{{{ ds }}}}'.format(python_script_path, python_executable),
    dag=dag)

t5 = BashOperator(
    task_id='findbots_behaviour',
    bash_command='cd {};{} findbots_behaviour.py'.format(python_script_path, python_executable),
    dag=dag)

dt1 = BashOperator(
    task_id='drafttrends1',
    bash_command='cd {};{} drafttrends.py'.format(python_script_path, python_executable),
    dag=dag)

dt2 = BashOperator(
    task_id='drafttrends2',
    bash_command='cd {};{} drafttrends.py'.format(python_script_path, python_executable),
    dag=dag)

dt3 = BashOperator(
    task_id='drafttrends3',
    bash_command='cd {};{} drafttrends.py'.format(python_script_path, python_executable),
    dag=dag)

t7 = BashOperator(
    task_id='draftstats',
    bash_command='cd {};{} draftstats.py {{{{ ds }}}}'.format(python_script_path, python_executable),
    dag=dag)

t8 = BashOperator(
    task_id='new_db',
    bash_command='cd {};{} new_db.py {{{{ (execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}}}'.format(
        python_script_path,
        python_executable),
    dag=dag)

dim_db = BashOperator(
    task_id='new_dim_db',
    bash_command='cd {};{} new_dim_db.py'.format(
        python_script_path,
        python_executable),
    dag=dag)

archive = BashOperator(
    task_id='archive_data',
    bash_command='rsync --progress --times --files-from=<(find {}/env/dev/data -mtime +185 -type f -exec basename {{}} \;) {}/env/dev/data adeel@192.168.1.4:/volume1/Dump/pakpolstats/data --remove-source-files'.format(
        python_script_path,
        python_script_path),
    dag=dag)

t1 >> dt1 >> dt2 >> dt3 >> hu >> t2 >> t4 >> t5 >> t7 >> t8 >> dim_db >> archive
