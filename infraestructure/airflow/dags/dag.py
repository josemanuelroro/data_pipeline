
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definir argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),  # Ajusta la fecha de inicio
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
    dag=dag,
)


mercadona_scrapper = BashOperator(
    task_id='mercadona_scrapper',
    bash_command='docker exec scrapper python3 -u /app/mercadona_scrapper.py &',
    execution_timeout=timedelta(minutes=30),
    #pool='scrapper_pool',
    dag=dag,
)


[dia_scrapper,mercadona_scrapper]
