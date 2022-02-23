from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


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