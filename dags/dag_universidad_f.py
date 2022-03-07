from datetime import timedelta, datetime
import logging
from decouple import config
from os import path
import psycopg2
from dotenv import load_dotenv

from airflow.operators.python import PythonOperator
import os
from datetime import date
import sqlalchemy
import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from matplotlib.pyplot import connect
from uritemplate import expand

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')


root_folder = path.abspath(path.join(path.dirname(__file__), ".."))
file = root_folder +'/template.env'

load_dotenv(dotenv_path=file)

def db_connection():
    server = os.getenv('SERVER')
    host = os.getenv('HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db = os.getenv('DB')
    
    conexion = psycopg2.connect(host=server, database=db,port=host,user=db_user,password=db_password)
    return conexion    
    

def extract(name_query,):
    df = pd.read_csv(f'{root_folder}/csv/')
    return df

def transform_moron(df_moron):
    df_moron = pd.read_csv(f'{root_folder}/csv/moron.csv')
    df_moron.rename({'universidad':'university','carrerra':'carer',
               'fechaiscripcion':'inscription_date',
               'sexo':'gender','codgoposstal':'postal_code',
               'eemail':'email'},inplace=True)
    
    #Data processing
    df_moron['nombrre'] = df_moron['nombrre'].str.replace(' ','_')
    name = df_moron['nombrre'].str.split('_',expand=True)
    df_moron['first_name'] = name[0]
    df_moron['last_name'] = name[1]
    df_moron['gender'] = df_moron['gender'].replace(['M','F'],['male','female'])

    location = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv')
    df_moron['location'] = location['localidad']
    df_moron.columns = [x.lower().replace(' ','_').strip("_") for x in df_moron.columns]
    
    #Removing extra columns
    df_moron.drop(columns=['nombrre'],inplace=True)

        
    return df_moron

def transform_rio_cuarto(df_rio_cuarto):
    df_rio_cuarto.rename({'univiersities':'university','carrera':'carer',
               'inscriptions_dates':'inscription_date',
               'sexo':'gender','localidad':'location'},inplace=True)
    
    df_rio_cuarto['gender'] = df_rio_cuarto['gender'].replace(['M','F'],['male','female'])
    
    #Clear columns df_rio_cuarto
    df_rio_cuarto['names'] = df_rio_cuarto['names'].str.replace('-','_')
    name = df_rio_cuarto['names'].str.split('_',expand=True)
    df_rio_cuarto['first_name'] = name[0]
    df_rio_cuarto['last_name'] = name[1]
    df_rio_cuarto.columns = [x.lower().replace(' ','_').strip("_") for x in df_rio_cuarto]
    
    #Removin extra columns
    df_rio_cuarto.drop(columns=['names'],inplace=True)
 
    
def load(df,file):
    df.to_csv(f'{root_folder}/txt/{file}.txt',index=None)

def _etl():
    moron_csv = extract('query_moron','%D/%m/%Y')

    df_moron = transform_moron(moron_csv)
    load(df_moron,'txt_moron')
    
    rio_cuarto_csv = extract('query_rio_cuarto','%D/%m/%Y')
    df_rio_cuarto = transform_rio_cuarto(rio_cuarto_csv)
    load(df_rio_cuarto,'txt_rio_cuarto')
    
    
    
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
    [tarea1, tarea2] >> tarea3 >> tarea4
    
    
    create_txt = PythonOperator(
        task_id='create_txt',
        python_callable=_etl,
        dag=dag
    )
    create_txt