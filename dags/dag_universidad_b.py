from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta


#Setting retries at 5
default_args = {
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Run every hour
    start_date=(datetime(2022, 2, 18)) # Starting date
) as dag:
    # SQL queries, using PostgresOperator
    tarea1 = DummyOperator(task_id='query_comahue') # Univ. Nacional Del Comahue. 
    tarea2 = DummyOperator(task_id='query_salvador') # Universidad Del Salvador

    # Processing in pandas, using PythonOperator
    tarea3 = DummyOperator(task_id='process_comahue')
    tarea4 = DummyOperator(task_id='process_salvador')

    # Upload in S3, using future assigned S3 operator
    tarea5 = DummyOperator(task_id='upload_s3')

    # Execution order; queries and processes are particular to each university, 
    # so they are in parallel, then they share the task to upload
    [tarea1 >> tarea2], [tarea3 >> tarea4] >> tarea5
