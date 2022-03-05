import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from os import environ, path
from dotenv import load_dotenv
from airflow.operators.python_operator import PythonOperator
import boto3
from botocore.exceptions import ClientError

# Para moverme por los diferentes directorio
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',  
    datefmt='%Y-%m-%d'
)
logger = logging.getLogger("univ_b")

#Setting retries at 5
default_args = {
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def load_s3():
    """
    sube archivo txt a repositorio s3
        
    Args:
        None
    
    Return:
        FIle load in s3 bucket
    """
    # cargas las credenciales  guardadas en template.env
    file=folder+ '/template.env'
    load_dotenv(dotenv_path=file)
    aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY')
    bucket_name = environ.get('BUCKET_NAME')

    #carga el archivo
    s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    with open(f'{folder}/txt/txt_comahue.txt', 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name,'txt_comahue.txt')

with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    start_date=(datetime(2022, 2, 18)) # Fecha de inicio
) as dag:
    # Tareas a ejecutar leyendo sql con pandas
    tarea1 = DummyOperator(task_id='ETL_comahue')
    tarea2 = DummyOperator(task_id='ETL_salvador')
    cargar_s3= PythonOperator(
    task_id='load_s3',
    python_callable=load_s3,
    dag=dag)

    # Orden de tareas
    tarea1 >> tarea2
    cargar_s3
