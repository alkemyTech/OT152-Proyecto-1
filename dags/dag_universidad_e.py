
from os import path, makedirs

from datetime import timedelta, datetime

import logging

import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from decouple import config


def start_engine():
    """
    This function takes no arguments, returns engine to conect to DataBase

    Returns: sqlarchemy engine
    """
    # Credential loading from .env file
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_TYPE = config('DB_TYPE')

    # Start Engine for DataBase connection
    return create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


def query(*args):
    """
    This function querys the database of a given university, and stores the data in a .csv file in folder csv
    Args: list containg only the name of the university to query
    """

    university = args[0]

    # Path of proyect folder
    basepath = path.abspath(path.join(path.dirname(__file__), '..'))

    # File path of the .sql needed to exectute the query
    slqpath = path.abspath(path.join(basepath, f'sql/query_{university}.sql'))

    # Start engine for Database Conection
    engine = start_engine()
    with engine.begin():
        # open and read sql file for query statement
        with open(slqpath, 'r') as file:
            sql_query = text(file.read())

        # Safe create csv folder for csv files
        csvpath = path.join(basepath, 'csv')
        if not path.exists(csvpath):
            makedirs(csvpath)
        # Query and store data to csv file
        university_df = pd.read_sql_query(sql_query, con=engine)
        university_df.to_csv(f'airflow/dags/OT152-Proyecto-1/csv/universidad_{university}.csv')


default_args = {
    "retries": 5,  # set retries at 5 according to the task
    "retry_delay": timedelta(minutes=5)
}

# config logging
logging.basicConfig(
    filename='test.log',
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.INFO)
logger = logging.getLogger('universidad_e')

with DAG(
        'Universidades_E',
        description='OT152-228',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 2, 23)
) as dag:
    query_pampa = PythonOperator(task_id='query_pampa', python_callable=query, op_args=['pampa'], dag=dag)  # Voy a usar un  PostgresOperator para ejecutar la querie
    query_interamericana = PythonOperator(
        task_id='query_interamericana', python_callable=query, op_args=['interamericana'], dag=dag)  # Voy a usar un  PostgresOperator para ejecutar la querie
    procesamiento_datos = DummyOperator(
        task_id='procesamiento_datos')  # Voy a usar un PythonOperator para procesar los datos
    subir_s3 = DummyOperator(task_id='subir_s3')  # Voy a usar un S3 operator para subir los datos a S3

    [query_pampa, query_interamericana] >> procesamiento_datos >> subir_s3
