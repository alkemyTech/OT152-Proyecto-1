from datetime import timedelta, datetime
import logging
from decouple import config
from os import path
import psycopg2
from dotenv import load_dotenv

from airflow.operators.python import PythonOperator
import os

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
    

def extract(name_query,format_date):
    with open (f'{root_folder}/sql/{name_query}.sql')as f:
        query=f.read()
    f.close()
    conex = db_connection()
    df_raw = pd.read_sql_query(query,conex,parse_dates={'fecha_de_nacicimiento':format_date})
    return df_raw

def clear_str(df):
    df = df.str.lower().replace('_',' ').str.strip()
    return df
    
def transform(df):
    
    df['university'] = clear_str(df['universidad'])
    df['carer'] = clear_str(df['carrerra'])
    df['inscription_date'] = clear_str(df['fechaiscripcion'])                                                                                                                                                                                                                          

    df['postal_code'] = clear_str(df['codgoposstal'])
    df['location'] = clear_str(df['location'])
    df['email'] = clear_str(df['eemail'])
    
    """
    df.rename({'universidad':'university','carrerra':'carer',
               'fechaiscripcion':'inscription_date',
               'sexo':'gender','codgoposstal':'postal_code',
               'eemail':'email'},inplace=True)
    """
    """
    time_now = datetime.now()
    df['age'] = time_now - df.nacimiento
    df['age']= (df['age']/ np.timedelta64(1, 'Y')).astype(int)    
    
    """
    df['first_name'],df['last_name'] = df['nombrre'].str.split(' ',1).str
    df['gender'] = df['sexo'].replace(['M','F'],['male','female'])

    df.drop(columns=['nombrre','sexo'],inplace=True)
    
    df['first_name'] = clear_str(df['first_name'])
    df['last_name'] = clear_str(df['last_name'])
    df['gender'] = clear_str(df['sexo'])
    df['age'] = clear_str(df['age'])

    
    if 'codigo_postal' in df.columns:
        df_postal = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv', 
                                dtype={'codigo_postal': 'str'})
        df = df.merge(df_postal, on = 'codigo_postal')
    return df
    
def load(df,file):
    df.to_csv(f'{root_folder}/txt/{file}.txt')

def _etl():
    df_raw = extract('query_moron','%D/%m/%Y')
    df = transform(df_raw)
    load(df,'txt_moron')
    
    df_raw = extract('query_rio_cuarto','%D/%m/%Y')
    df = transform(df_raw)
    load(df,'txt_rio_cuarto')
    
    
    
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