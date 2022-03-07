from os import path

import logging
from datetime import timedelta, datetime
from time import strftime
from decouple import config
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import boto3
from botocore.exceptions import ClientError


# Configuro los loggs acorde a lo que pide la tarea
logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}


def upload_jujuy_s3():
    """
    This functions uploads university txt file to AWS cloud
    """
    # Upload credentials
    bucket_name = config('BUCKET_NAME')
    s3 = boto3.client('s3',
                      aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
                      )
    # Folder path
    folder = path.abspath(path.join(path.dirname(__file__), '..'))

    with open(f'{folder}/txt/jujuy.txt', 'rb') as file:
        try:
            s3.upload_fileobj(file, bucket_name, 'S3_jujuy.txt')
        except ClientError as error:
            print(error)

    # Get elements present in Bucket
    response = s3.list_objects(Bucket=bucket_name)

    # Check if file was succesfully upladed
    file_name = 'S3_jujuy.txt'
    is_file = False
    values = list(response.values())
    for value in values:
        if type(value) == list:
            for v in value:
                if file_name in v.values():
                    is_file = True

    if is_file:
        print('File uploaded to S3')
    else:
        print('File not uploaded to S3')


with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    
    #Queries
    #SQL for Universidad Nacional
    #SQL for Universidad de Palermo
    
    #Data processing
    
    #Upload data to S3    
    
    universidad_nacional= DummyOperator(task_id='universidad_nacional')
    universidad_de_palermo = PythonOperator(task_id='universidad_de_Palermo', python_callable=upload_jujuy_s3)

    universidad_nacional >> universidad_de_palermo
