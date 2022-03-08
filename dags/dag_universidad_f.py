from datetime import timedelta, datetime
import logging
import boto3
import os
from os import path

from dotenv import load_dotenv


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')

default_args = {
    "retries": 5,  # try 5 times
    "retry_delay": timedelta(minutes=10)  # wait 10 minutes to try again
}

root_folder = path.abspath(path.join(path.dirname(__file__), ".."))
file = root_folder +'/template.env'

load_dotenv(dotenv_path=file)



    
def _upload_s3():
    bucket_name = os.getenv('BUCKET_NAME')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY'),
    )
    
    with open (f'{root_folder}/txt/pampa.txt','rb') as file:
        s3.upload_fileobj(file,bucket_name,'S3_Pampa.txt')    

with DAG(
    'Query_Universidad_F',
    description='Realizar consultas sobre Universidad de Moron y Rio Cuarto',
    schedule_interval=timedelta(hours=1), #ejecución cada una hora
    start_date=datetime(2022, 2, 19) #Puse esa fecha porque no fue aclarada en la consigna

) as dag:
    #Queries usando la función read_sql de pandas
    tarea1 = DummyOperator(task_id='query_F1') #Universidad de Morón
    tarea2 = DummyOperator(task_id='query_F2') #Universidad de Río Cuarto
    tarea3 = DummyOperator(task_id='processing_data')#Reading and processing data using read_sql
    
    upload_s3 = PythonOperator(task_id='upload_s3',python_callable=_upload_s3,dag=dag)#uploading data to s3
    [tarea1, tarea2] >> tarea3 >> upload_s3
