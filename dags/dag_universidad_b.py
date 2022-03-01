import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from decouple import config
from sqlalchemy import engine
from os import path
import pandas as pd
import sqlalchemy 


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

def save_to_csv(university):
    file = open(f'{root_folder}/sql/{university}.sql', "r")
    query = sqlalchemy.text(file.read())
    data = pd.read_sql_query(query, conn)

    data.to_csv(f'{root_folder}/csv/universidad_{university}.csv', index=False)


with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    start_date=(datetime(2022, 2, 18)) # Fecha de inicio
) as dag:
    # Tareas a ejecutar leyendo sql con pandas
    tarea1 = DummyOperator(task_id='ETL_comahue')
    tarea2 = DummyOperator(task_id='ETL_salvador')
    save_to_csv = PythonOperator(task_id='Universidades_B_data', python_callable=save_to_csv, dag=dag)


    # Orden de tareas
    tarea1 >> tarea2