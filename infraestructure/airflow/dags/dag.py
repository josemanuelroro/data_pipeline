
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests


def notificar(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("CHAT_ID")

    mensaje = f"✅ **Task**\n" \
              f"📔 Task: `{task_id}`\n" \
              f"⌛Start Time: {execution_date}"
              

    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                  data={'chat_id': chat_id, 'text': mensaje, 'parse_mode': 'Markdown'})

# Definir argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),  # Ajusta la fecha de inicio
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Crear el DAG
dag = DAG(
    'scrapper_dag',
    default_args=default_args,
    description='Execute scrapper at 5pm',
    schedule_interval='0 19 * * *',  
    catchup=False,
    max_active_tasks=3,
    is_paused_upon_creation=False
)



# Tareas para ejecutar los scrapers dentro del contenedor 'scrapper_py'
dia_scrapper = BashOperator(
    task_id='dia_scrapper',
    bash_command='docker exec scrapper python3 -u /app/dia_scrapper.py &',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    on_success_callback=notificar,
    dag=dag,
)


mercadona_scrapper = BashOperator(
    task_id='mercadona_scrapper',
    bash_command='docker exec scrapper python3 -u /app/mercadona_scrapper.py &',
    execution_timeout=timedelta(minutes=30),
    on_success_callback=notificar,
    dag=dag,
)

bronze_bucket = BashOperator(
    task_id='bronze_bucket',
    bash_command='docker exec spark python3 -u /app/scripts/clean_bronze.py',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)

bronze_monitor_log = BashOperator(
    task_id='bronze_monitor_log',
    bash_command='docker exec telegram_monitor python3 -u /app/bronze_monitor_log.py',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)

[dia_scrapper,mercadona_scrapper]>>bronze_bucket>>bronze_monitor_log
