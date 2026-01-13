#========================================================================
#This DAG executes the script update_ips.py from data_updater container
#to update the IPS table
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
    'updater_dag',
    default_args=default_args,
    description='Execute updater save every sunday',
    schedule_interval='0 20 * * 0',  
    catchup=False,
    max_active_tasks=3,
    is_paused_upon_creation=False
)



updater_ini = BashOperator(
    task_id='updater_init',
    bash_command='docker exec telegram_monitor python3 /app/general_msj_bot.py "🔼🔼🔼Updating🔼🔼🔼 files"',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)


updater_get_data = BashOperator(
    task_id='updater_get_data',
    bash_command='docker exec data_updater python3 /app/sources/update_ips.py ',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)


updater_fin = BashOperator(
    task_id='cold_storage_fin',
    bash_command='docker exec telegram_monitor python3 /app/general_msj_bot.py "data updated & proccessed 👌"',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)
updater_ini>>updater_get_data>>updater_fin