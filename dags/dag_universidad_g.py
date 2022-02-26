
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from datetime import timedelta

default_args = {
    "retries": 5,  # Try 5 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Basic configuration of the format and instantiation of the logger
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d')
logger = logging.getLogger('univ_g')

with DAG(
    'dag_universidad_g',
    description= 'ETL univ Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval= "@hourly", #funciona cada hora
    start_date=datetime(2022, 2, 17)
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')
    [tarea_1,tarea_2]