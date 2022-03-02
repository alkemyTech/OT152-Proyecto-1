import logging
from datetime import timedelta, datetime
from time import strftime
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from os import environ, path
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as db
from time import strftime
from datetime import timedelta, datetime
from airflow import DAG   
from airflow.operators.dummy import DummyOperator

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

#configura path raiz para mover dentro del proyecto
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))

#configuro los retries acorde a lo que pide la tarea
args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),   
}

#Configuracion de BD
def connection():
    """
    Crea la conexion de la BD postgres segun las credeciales del archivo template.env que debe estar de manera local
    Args:
        None
    Return:
        Engine Instance: retorna la conexion
    """
    #carga las credenciales en template.env
    file=folder+ '/template.env'
    load_dotenv(dotenv_path=file)
    DB_USER = environ.get('DB_USER')
    DB_PASSWORD =environ.get('DB_PASSWORD')
    DB_HOST = environ.get('DB_HOST')
    DB_PORT = environ.get('DB_PORT')
    DB_NAME = environ.get('DB_NAME')

    #Genera la conexion de la base de datos segun las credenciales
    path =f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    con = db.create_engine(path, echo=True)
    return con

def extract(name_query,name_csv):
    """
    Lee query localizada en path /sql y genera archivo .csv 
    
    Args:
        name_query(str): nombre de la query localizada en path /sql
        name_csv(str): nombre que se le va a dar a csv generado
    
    Return:
        file: csv generado en la ruta /csv/name_csv
    """
    with open(f'{folder}/sql/{name_query}.sql') as f:
        query = f.read()
    f.close()
    con=connection()
    df_raw= pd.read_sql_query(query, con)
    df_raw.to_csv(f'{folder}/csv/{name_csv}.csv')

def crear_csv():
    """
    Ejecuta la extraccion de las universidades UTN y Tres de Febrero
    
    Args:
        None
    Return:
        /csv/csv_utn.csv
        /csv/csv_tres_de_febrero.csv
    """
    extract('query_utn','csv_utn')
    extract('query_tres_de_febrero','csv_tres_de_febrero')

with DAG(
    dag_id = "ETL_para_dos_universidades", 
    description = "ETL para dos universidades",
    schedule_interval = "@hourly",
    start_date = datetime(2002,2,18)
    ) as dags:
    """
    Se harán dos consultas SQL para dos universidades: UTN y Tres de Febrero
    usaremos los siguientes operadores:
        airflow.providers.postgres.operators.postgres -> PostgresOperator
    
    Procesamiento de los datos:
        airflow.operators.python_operator -> PythonOperator
    
    Carga de datos a S3:
        airflow.providers.amazon.aws.operators.s3
    """
    sql_query_utn = DummyOperator(task_id = "sql_query_utn")
    sql_query_tres_de_febrero = DummyOperator(task_id = "sql_query_tres_de_febrero")

    procesar_utn = DummyOperator(task_id="procesar_utn")
    procesar_tres_de_febrero = DummyOperator(task_id="procesar_tres_de_febrero")

    uploads3_utn = DummyOperator(task_id="uploads3_utn")
    uploads3_tres_de_febrero = DummyOperator(task_id="uploads3_tres_de_febrero")
    
    generar_csv= PythonOperator(
    task_id='generar_csv',
    python_callable=crear_csv,
    dag=dags
    )

    sql_query_utn >> procesar_utn >> uploads3_utn
    sql_query_tres_de_febrero >> procesar_tres_de_febrero >> uploads3_tres_de_febrero
    generar_csv

