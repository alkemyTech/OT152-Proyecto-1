from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
from decouple import config
import sqlalchemy
from os import path
from os import remove
from os import mkdir


def execute_queries(university):
    # Configuro la conexion a la base de datos
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_TYPE = config('DB_TYPE')

    conexion = f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = sqlalchemy.create_engine(conexion)
    conn = engine.connect()
    # Configuro un path para la carpeta root
    root_folder = path.abspath(path.join(path.dirname(__file__), '..'))

    with open(f"{root_folder}/sql/query_{university}.sql", "r") as f:
        sql = f.read()
        response = pd.read_sql_query(sql, conn)
        if path.exists(f'{root_folder}/csv'):
            remove(f'{root_folder}/csv')
        mkdir(f'{root_folder}/csv')
        response.to_csv(f'{root_folder}/csv/universidad_{university}.csv',
                        index=False)


default_args = {
    'retries': 5,  # Quantity of retries before shutdown
    'retry_delay': timedelta(minutes=5) # Wait time before next retry
}


# create logger
FORMAT = '%(asctime)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, datefmt=("%Y-%m-%d"),format=FORMAT)
logger = logging.getLogger('universidad_a')

with DAG(
    'Universities_A_dags',
    description='Perfomrs ELT to two universities',
    # Time between executions (1 hour)
    schedule_interval=timedelta(hours=1),
    # Starting execution date
    start_date=(datetime(2022, 2, 18))

) as dag:

    # Tasks Univerdisdad las Flores
    sql_universidad_flores = PythonOperator(task_id='Query_flores',
                                            python_callable=execute_queries,
                                            op_args=["flores"],
                                            dag=dag)

    # Tasks Universidad Nacional de Villa Maria
    sql_universidad_villa_maria = PythonOperator(task_id='Query_villa_maria',
                                                 python_callable=execute_queries,
                                                 op_args=["villa_maria"],
                                                 dag=dag)

    # Tasks order
    [sql_universidad_flores, sql_universidad_villa_maria]
