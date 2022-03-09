import logging
from os import path, makedirs
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
def connection(): #creating the function as sugested.

    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_TYPE = config('DB_TYPE')

    secret = f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = sqlalchemy.create_engine(secret)
    return engine.connect()


#uni stands for 'university'

def read_and_save(uni_query):
    conn=connection() #calling the function to connect to the db

    #main folder
    folder = path.abspath(path.join(path.dirname( __file__ ), '..'))

    with open(f'{folder}/sql/{uni_query}.sql') as file: #opening the file as sugested

        query = sqlalchemy.text(file.read())
        data = pd.read_sql_query(query, conn)

        csv_folder = path.join(folder, 'csv')
        if not path.exists(csv_folder):
            makedirs(csv_folder)
        print(data)

        data.to_csv(f'{folder}/csv/{uni_query}.csv')

    file.close() #closing file as sugested

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
    read_save_p = PythonOperator(task_id='palermo_read_save', python_callable=read_and_save, op_args=['query_palermo'], dag=dag) #adding the parameters needed to call the function
    read_save_j = PythonOperator(task_id='jujuy_read_save', python_callable=read_and_save, op_args=['query_jujuy'], dag=dag) # same here.

    #running in paralel the dag as sugested
    universidad_nacional >> read_save_j
    universidad_de_palermo >> read_save_p
    