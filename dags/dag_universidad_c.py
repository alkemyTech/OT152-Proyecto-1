import logging 
from datetime import timedelta,datetime
from time import strftime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
import pandas as pd
from os import environ, path
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as db

#Configuro los loggs acorde a lo que pide la tarea
logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}
#configura path raiz para mover dentro del proyecto
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))

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

def extract(name_query,name_txt):
    """
    Lee query localizada en path /sql y genera archivo .txt
    
    Args:
        name_query(str): nombre de la query localizada en path /sql
        name_csv(str): nombre que se le va a dar a csv generado
    
    Return:
        file: txt generado en la ruta /txt/name_txt
    """
    with open(f'{folder}/sql/{name_query}.sql') as f:
        query = f.read()
    f.close()
    con=connection()
    df_raw= pd.read_sql_query(query, con)
    df_raw.to_csv(f'{folder}/txt/{name_txt}.txt')

def crear_txt():
    """
    Ejecuta la extraccion de las universidades UTN y Tres de Febrero
    
    Args:
        None
    Return:
        /csv/csv_utn.csv
        /csv/csv_tres_de_febrero.csv
    """
    extract('query_jujuy','txt_jujuy')
    extract('query_palermo','txt_palermo')
with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    
    #Queries
    #SQL for Universidad Jujuy
    #SQL for Universidad de Palermo
    
    #Data processing 
    #pandas as pd 
    
    #Upload data to S3    
    
    universidad_jujuy= DummyOperator(task_id='universidad_jujuy')
    universidad_de_palermo= DummyOperator(task_id='universidad_de_Palermo')
    generar_txt= PythonOperator(
    task_id='generar_txt',
    python_callable=crear_txt,
    dag=dag)

    universidad_jujuy >> universidad_de_palermo
