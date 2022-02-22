from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging


default_args = {
    "retries": 5, #set retries at 5 according to the task 
    "retry_delay": timedelta(minutes=5) 
}

#config logging
logging.basicConfig(
    filename='test.log', 
    format='%(asctime)s - %(name)s - %(message)s', 
    datefmt='%Y-%m-%d',
    level=logging.INFO)
logger= logging.getLogger('universidad_e')

with DAG(
    'Universidades_E',
    description='OT152-228',
    schedule_interval=timedelta(hours=1),   
    start_date=datetime(2022, 2, 18)
) as dag:
    query_pampa = DummyOperator(task_id='query_Pampa')  #Voy a usar un  PostgresOperator para ejecutar la querie
    query_interamericana = DummyOperator(task_id='query_interamericana') #Voy a usar un  PostgresOperator para ejecutar la querie
    procesamiento_datos = DummyOperator(task_id='procesamiento_datos') #Voy a usar un PythonOperator para procesar los datos
    subir_s3 = DummyOperator(task_id='subir_s3') #Voy a usar un S3 operator para subir los datos a S3
    
    
    
    [query_pampa, query_interamericana] >> procesamiento_datos >> subir_s3