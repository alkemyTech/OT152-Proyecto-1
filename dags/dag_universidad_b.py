import logging
from pickle import TRUE
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from decouple import config
from sqlalchemy import engine
from os import path
import pandas as pd
import sqlalchemy 
import os
from os import remove

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



def save_to_csv(university):
#Configuro la conexion a la base de datos
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_TYPE = config('DB_TYPE')

    conexion = f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = sqlalchemy.create_engine(conexion)
    conn = engine.connect()
#Extraigo las tablas de las consultas y las guardo en un csv
    root_folder = path.abspath(path.join(path.dirname(__file__), '..'))
    os.makedirs(f'{root_folder}/csv', exist_ok = TRUE)
    with open(f'{root_folder}/sql/query_{university}.sql', "r") as f:
        query = f.read()
        data = pd.read_sql_query(query, conn) 
        data.to_csv(f'{root_folder}/csv/universidad_{university}.csv', index=False)


with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    start_date=(datetime(2022, 2, 18)) # Fecha de inicio
) as dag:
    # Tareas a ejecutar leyendo sql con pandas
    tarea1 = PythonOperator(task_id='Query_comahue', python_callable=save_to_csv, op_args=["comahue"], dag=dag)
    tarea2 = PythonOperator(task_id='Query_salvador', python_callable=save_to_csv, op_args=["salvador"], dag=dag)


# Orden de tareas
[tarea1, tarea2]
