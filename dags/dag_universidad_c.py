from asyncio import DatagramTransport
import logging
from os import path
from datetime import timedelta, datetime
from time import strftime

from decouple import config
import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

#Configuro los loggs acorde a lo que pide la tarea

logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

#setting up the connection to db:

DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')
DB_TYPE = config('DB_TYPE')

secret = f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sqlalchemy.create_engine(secret)
conn = engine.connect()

#main folder

folder = path.abspath(path.join(path.dirname( __file__ ), '..'))

#uni stands for 'university'

def read_and_save(uni_query, uni_csv):

    file= open(f'{folder}/sql/{uni_query}')

    query = sqlalchemy.text(file.read())
    data = pd.read_sql_query(query, conn)

    print(data)

    data.to_csv(f'{folder}/csv/{uni_csv}')


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
    #pandas as pd 
    
    #Upload data to S3    
    
    universidad_nacional = DummyOperator(task_id='universidad_nacional')
    universidad_de_palermo = DummyOperator(task_id='universidad_de_Palermo')
    read_save = PythonOperator(task_id='Query_and_save_csv', python_callable=read_and_save, dag=dag)


    universidad_nacional >> universidad_de_palermo >> read_save
