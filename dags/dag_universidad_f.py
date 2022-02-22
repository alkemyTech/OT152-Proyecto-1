from datetime import timedelta, datetime
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')

default_args = {
    "retries": 5,  # try 5 times
    "retry_delay": timedelta(minutes=10)  # wait 10 minutes to try again
}
with DAG(
    'Query_Universidad_F',
    description='Realizar consultas sobre Universidad de Moron y Rio Cuarto',
    schedule_interval=timedelta(hours=1), #ejecución cada una hora
    start_date=datetime(2022, 2, 19) #Puse esa fecha porque no fue aclarada en la consigna

) as dag:
    #Queries usando la función read_sql de pandas
    tarea1 = DummyOperator(task_id='Query_F1') #Universidad de Morón
    tarea2 = DummyOperator(task_id='Query_F2') #Universidad de Río Cuarto
    tarea3 = DummyOperator(task_id='Processing_data')#Reading and processing data using read_sql
    tarea4 = DummyOperator(task_id='Upload_S3')#uploading data to s3
    [tarea1, tarea2] >> tarea3 >> tarea4
