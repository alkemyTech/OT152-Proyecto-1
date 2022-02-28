import logging
from datetime import timedelta, datetime
from time import strftime
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from os import environ, getcwd, path
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as db

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))

#configuro los retries acorde a lo que pide la tarea
args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    
}
#Configuracion de BD
def connection():
    file=folder+ '/template.env'
    load_dotenv(dotenv_path=file) #set file .env
    DB_USER = environ.get('DB_USER')
    DB_PASSWORD =environ.get('DB_PASSWORD')
    DB_HOST = environ.get('DB_HOST')
    PORT = environ.get('DB_PORT')
    DB_NAME = environ.get('DB_NAME')
    path ='postgresql://{}:{}@{}:{}/{}'.format(DB_USER,DB_PASSWORD,DB_HOST, PORT, DB_NAME) #Path DB for connection
    con = db.create_engine(path, echo=True)
    return con

dag = DAG(
    dag_id='dag_universidad_d', 
    default_args=args,
    schedule_interval=timedelta(minutes=5),
    start_date= datetime(2022,2,24)
    )

def etl_extract():
    file=folder+ '/sql/query_utn.sql'
     
    with open(file) as f:
        query = f.read()
    f.close()
    con=connection()
    logging.info(path)
    df_raw= pd.read_sql_query(query, con)
    #df_raw.to_csv('./OT152-Proyecto-1/csv/data_utn.csv')
    logging.info(query)
    
    #return df_raw

#df_raw=etl_extract()
task_1= PythonOperator(
    task_id='extract_utn',
    python_callable=etl_extract,
    dag=dag
)

[task_1]