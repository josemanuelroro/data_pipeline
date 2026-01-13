#========================================================================
#This DAG executes the scriptcarga_logs.py from spark container
#to generetate a table in silver/gold layer of logs
#------------------------------------------------------------------------
#
#
#
#
#
#
#========================================================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'updater_logs',
    default_args=default_args,
    description='Execute updater_log save every day',
    schedule_interval='0 12 * * *',  
    catchup=False,
    max_active_tasks=3,
    is_paused_upon_creation=False
)



updater_ini = BashOperator(
    task_id='updater_init',
    bash_command='docker exec telegram_monitor python3 /app/general_msj_bot.py "🔼🔼🔼Updating🔼🔼🔼 logs"',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)



updater_process_data = BashOperator(
    task_id='updater_process_data',
    bash_command='docker exec spark python3 /app/scripts/carga_logs.py ',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)


updater_fin = BashOperator(
    task_id='updater_fin',
    bash_command='docker exec telegram_monitor python3 /app/general_msj_bot.py "logs loaded👌"',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)
updater_ini>>updater_process_data>>updater_fin