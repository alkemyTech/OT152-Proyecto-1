import logging
from datetime import timedelta, datetime
from time import strftime
import boto3
from os import path
from decouple import config

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Configuro los loggs acorde a lo que pide la tarea
logging.basicConfig(level=logging.DEBUG,
                    datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

root_folder = path.abspath(path.join(path.dirname(__file__), '..'))


def nacional_upload_s3():
    """
    Upload txt file to S3 server
    Returns:
        None
    """
    # Obtain S3 secrets
    bucket_name = config('BUCKET_NAME')
    aws_access_key_id = config('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY')

    # Create S3 client
    boto_client = boto3.client('s3',
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key
                               )

    # Upload file to S3
    with open(f'{root_folder}/txt/nacional.txt', 'rb') as file:
        boto_client.upload_fileobj(file, bucket_name, 'S3_nacional.txt')


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'universidades_c',
        description='university_processes',
        default_args=default_args,
        # defino ejecucion
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 2, 18)
) as dag:
    # Queries
    # SQL for Universidad Nacional
    # SQL for Universidad de Palermo

    # Data processing
    # pandas as pd

    # Upload data to S3

    universidad_nacional = DummyOperator(task_id='universidad_nacional')
    universidad_de_palermo = DummyOperator(task_id='universidad_de_Palermo')
    task_nacional_upload_s3 = PythonOperator(task_id='nacional_upload_S3', python_callable=nacional_upload_s3)

    [universidad_nacional, universidad_de_palermo] >> task_nacional_upload_s3
