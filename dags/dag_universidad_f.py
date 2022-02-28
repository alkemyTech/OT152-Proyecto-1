from datetime import timedelta, datetime
import logging
from decouple import config
from os import path

import sqlalchemy

import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')

# Connection DB credentials
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')

# Initialize connection
db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(db_url)
conn = engine.connect()

# Set root folder path
root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


def extract_data(university):
    """
    Execute SQL query and save response as CSV file
    Args:
        university:

    Returns:
        None
    """
    with open(f"{root_folder}/sql/query_{university}.sql", "r") as f:
        sql = f.read()
        response = pd.read_sql_query(sql, conn)
        response.to_csv(f'{root_folder}/csv/universidad_{university}.csv', index=False)


default_args = {
    "retries": 5,  # try 5 times
    "retry_delay": timedelta(minutes=10)  # wait 10 minutes to try again
}
with DAG(
        'Query_Universidad_F',
        description='Realizar consultas sobre Universidad de Moron y Rio Cuarto',
        schedule_interval=timedelta(hours=1),  # ejecución cada una hora
        start_date=datetime(2022, 2, 19)  # Puse esa fecha porque no fue aclarada en la consigna

) as dag:
    # Queries usando la función read_sql de pandas
    tarea1 = PythonOperator(task_id='Query_F1', python_callable=extract_data, op_args=["moron"],
                            dag=dag)  # Universidad de Morón
    tarea2 = PythonOperator(task_id='Query_F2', python_callable=extract_data, op_args=["rio_cuarto"],
                            dag=dag)  # Universidad de Río Cuarto
    tarea3 = DummyOperator(task_id='Processing_data')  # Reading and processing data using read_sql
    tarea4 = DummyOperator(task_id='Upload_S3')  # uploading data to s3
    [tarea1, tarea2] >> tarea3 >> tarea4
