from datetime import timedelta, datetime
import logging
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')

def upload_moron_s3():
    # S3 credential
    bucket_name = config('BUCKET_NAME')
    # S3 Client
    s3 = boto3.client('s3',
        aws_access_key_id = config('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY'))

    # Folder path
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # Upload Moron file to S3
    
    try:
        with open(f'{root_dir}/txt/moron.txt', 'rb') as file:
            s3.upload_fileobj(file, bucket_name, 'S3_moron.txt')
    except ClientError as e:
        print('Error: ',e)
        return False
    return True


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
    upload_s3_moron = PythonOperator(task_id='upload_s3_moron',python_callable=upload_moron_s3)

    [tarea1, tarea2] >> tarea3 >> [tarea4, upload_s3_moron]
