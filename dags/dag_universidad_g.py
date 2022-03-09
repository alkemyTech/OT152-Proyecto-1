from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging
from decouple import config

import psycopg2
from datetime import datetime,timedelta
import pandas as pd
from os import path
from dotenv import load_dotenv

#main folder location
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))
file = folder +'/template.env'

load_dotenv(dotenv_path=file)

def db_connection():
    server = config('DB_HOST')
    host = config('DB_PORT')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db = config('DB_NAME')
    
    conexion = psycopg2.connect(host=server, database=db,port=host,user=db_user,password=db_password)
    return conexion

def query_db(query,csv):
     with open(f'{folder}/sql/{query}.sql') as f:
        query = f.read()
        f.close()
        con=db_connection()
        df_query= pd.read_sql_query(query, con)
        df_query.to_csv(f'{folder}/csv/{csv}.csv',index=False)



default_args = {
    "retries": 5,  # Try 5 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Basic configuration of the format and instantiation of the logger
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d')
logger = logging.getLogger('univ_g')


with DAG(
    'dag_universidad_g',
    description= 'ETL univ Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval= "@hourly", #funciona cada hora
    start_date=datetime(2022, 2, 17)
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')
    
    csv_latinoamericana = PythonOperator(
        task_id='create_csv_file',
        python_callable=query_db,
        op_args=['query_latinoamericana','latinoamericana'],
        dag=dag
    )
    
    csv_kennedy = PythonOperator(
        task_id='csv_kennedy',
        python_callable=query_db,
        op_args=['query_kenedy','kenedy'],
        dag=dag
    )
    [tarea_1 >> csv_latinoamericana],[tarea_2>>csv_kennedy]
