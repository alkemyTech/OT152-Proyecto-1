from datetime import datetime
from airflow import DAG, models
from airflow.operators.dummy import DummyOperator




with DAG(
    'universidades',
    description= 'ETL para dos universidades',
    schedule_interval= "@hourly",
    start_date=datetime(2022, 2, 17),
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')
    [tarea_1,tarea_2]
