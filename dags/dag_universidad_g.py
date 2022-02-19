from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import pandas as pd



with DAG(
    'universidades',
    description= 'ETL para dos universidades',
    schedule_interval= "@hourly",
    start_date=datetime(2022, 2, 17),
) as dag:

    """usar PostgresOperator ejecutar query
    universidad_kenedy = PostgresOperator(
    task_id="universidad_kenedy",
    postgres_conn_id="postgres_default",
    sql="sql/query_kenedy.sql",
    )"""   

    """usar PostgresOperator ejecutar query
    universidad_lationamericana = PostgresOperator(
    task_id="universidad_sociales",
    postgres_conn_id="postgres_default",
    sql="sql/query_latinoamericana.sql",
    )"""   


    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_Kenedy')
    [tarea_1,tarea_2]
