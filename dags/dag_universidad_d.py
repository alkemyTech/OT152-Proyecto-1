import logging
from datetime import timedelta, datetime
from time import strftime
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
#from airflow.operators import python_operator
import sqlalchemy as db
from os import environ
from dotenv import load_dotenv
import pandas as pd

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

#configuro los retries acorde a lo que pide la tarea
args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    
}
#Configuracion de BD
load_dotenv(dotenv_path='template.env') #set file .env
DB_USER = environ['DB_USER']
DB_PASSWORD =environ['DB_PASSWORD']
DB_HOST = environ['DB_HOST']
PORT = environ['DB_PORT']
DB_NAME = environ['DB_NAME']
path ='postgresql://{}:{}@{}/{}'.format(DB_USER,DB_PASSWORD,DB_HOST, DB_NAME) #Path DB for connection
con = db.create_engine(path, echo=True)

dag = DAG(
    dag_id='dag_universidad_d', 
    default_args=args,
    schedule_interval=timedelta(minutes=2),
    start_date= datetime(2022,2,24)
    )


def etl_extract():
    with open('./sql/query_utn.sql') as f:
        query = f.read()
    f.close()
    con = db.create_engine(path, echo=True)
    df_raw= pd.read_sql_query(query, con)
    df_raw.to_csv('./csv/data_utn.csv')
    logging.info('test')
    return df_raw

df_raw=etl_extract()
task_1= PythonOperator(
    task_id='extract_utn',
    python_callable=etl_extract,
    dag=dag
)