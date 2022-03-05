
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from datetime import timedelta
import boto3
import os
from os import path
from airflow.operators.python import PythonOperator

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

def load_s3():
    #Configuro la conexion con AWS 
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.environ.get('BUCKET_NAME')

    #Carga del archivo
    root_folder = path.abspath(path.join(path.dirname(__file__), '..'))
    s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    with open(f'{root_folder}/txt/txt_latinoamericana.txt', 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name,'txt_latinoamericana.txt')

with DAG(
    'dag_universidad_g',
    description= 'ETL univ Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval= "@hourly", #funciona cada hora
    start_date=datetime(2022, 2, 17)
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')
    carga_de_s3= PythonOperator(
    task_id='load_s3',
    python_callable=load_s3,
    dag=dag)

    [tarea_1,tarea_2]
    carga_de_s3