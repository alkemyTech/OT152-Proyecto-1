from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    'universidades',
    description= 'ETL Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval= "@hourly", #funciona cada hora
    start_date=datetime(2022, 2, 17),
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')
    [tarea_1,tarea_2]
