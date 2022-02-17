from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    'Universities_A_dags',
    description='Perfomrs ELT to two universities',
    # Time between executions (1 hour)
    schedule_interval=timedelta(hours=1),
    # Starting execution date
    start_date=(datetime(2022, 2, 18))
) as dag:

    # Tasks
    task_1 = DummyOperator(task_id='ELT_Universidad_De_Flores')
    task_2 = DummyOperator(task_id='ELT_Universidad_Nacional_De_Villa_Maria')

    # Tasks order
    task_1 >> task_2
