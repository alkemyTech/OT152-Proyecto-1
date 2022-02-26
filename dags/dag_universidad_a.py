from datetime import datetime, timedelta

#from airflow import DAG
#from airflow.operators.dummy import DummyOperator


import logging
from distutils.debug import DEBUG

default_args = {
    'retries': 5, # Quantity of retries before shutdown
    'retry_delay': timedelta(minutes=5) # Wait time before next retry
}
 
# create logger
FORMAT='%(asctime)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,datefmt=("%Y-%m-%d"),format=FORMAT)
logger = logging.getLogger('universidad_a')


with DAG(
    'Universities_A_dags',
    description='Perfomrs ELT to two universities',
    # Time between executions (1 hour)
    schedule_interval=timedelta(hours=1),
    # Starting execution date
    start_date=(datetime(2022, 2, 18))
) as dag:

    # Tasks Univerdisdad las Flores
    sql_universidad_flores = DummyOperator(task_id='sql_universidad_flores')
    processing_universidad_flores = DummyOperator(task_id='processing_universidad_flores')
    load_universidad_flores = DummyOperator(task_id='load_universidad_flores')

    # Tasks Universidad Nacional de Villa Maria
    sql_universidad_villa_maria = DummyOperator(task_id='ELT_Universidad_Nacional_De_Villa_Maria')
    processing_universidad_villa_maria = DummyOperator(task_id='processing_universidad_villa_maria')
    load_universidad_villa_maria = DummyOperator(task_id='load_universidad_villa_maria')

    # Tasks order
    sql_universidad_flores >> processing_universidad_flores >> load_universidad_flores

    sql_universidad_villa_maria >> processing_universidad_villa_maria >> load_universidad_villa_maria
