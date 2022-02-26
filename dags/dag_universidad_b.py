import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',  
    datefmt='%Y-%m-%d'
)
logger = logging.getLogger("univ_b")

#Setting retries at 5
default_args = {
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    start_date=(datetime(2022, 2, 18)) # Fecha de inicio
) as dag:
    # Tareas a ejecutar leyendo sql con pandas
    tarea1 = DummyOperator(task_id='ETL_comahue')
    tarea2 = DummyOperator(task_id='ETL_salvador')

    # Orden de tareas
    tarea1 >> tarea2
