from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'universidades',
    description= 'ETL para dos universidades',
    schedule_interval= timedelta(hours=1),
    start_date=datetime(2022, 2, 17),
) as dag:
    tarea_1= DummyOperator(task_id='universidad de ciencias sociales')
    tarea_2= DummyOperator(task_id='universidad J.F. Kenedy')
    [tarea_1,tarea_2]
