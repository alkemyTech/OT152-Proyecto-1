from os import path
from decouple import config
from datetime import timedelta, datetime
from time import strftime
import logging
import boto3
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def upload_s3():
    #declaring credentials
    bucket_name = config('BUCKET_NAME')
    s3 = boto3.client('s3',
                      aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                      ws_secret_access_key=config('AWS_SECRET_ACCESS_KEY')
                      )
    #folder path
    folder = path.abspath(path.join(path.dirname(__file__), '..'))
    with open(f'{folder}/txt/flores.txt', 'rb') as file:
        #uploading file
        s3.upload_fileobj(file, bucket_name, 'S3_flores.txt')
    file.close()

with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,3,4)
    ) as dag:

    universidad_de_flores = DummyOperator(task_id='Universidad_de_flores')
    upload_data = PythonOperator(task_id='uploading_to_s3', python_callable=upload_s3, dag=dag)

    universidad_de_flores >> upload_data
