from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'universidades',
    description= 'este es un tutorial de dags',
    schedule_interval= timedelta(hours=1),
    start_date=datetime(2022, 1, 17),
) as dag:
    tarea_1= DummyOperator(task_id='tarea_1')
    tarea_2= DummyOperator(task_id='tarea_2')
    tarea_3= DummyOperator(task_id='tarea_3')

    tarea_1>>[tarea_2,tarea_3]
