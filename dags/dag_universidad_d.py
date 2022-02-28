import logging
from datetime import timedelta, datetime
from time import strftime
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from os import environ, path
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

def extract(name_query,name_csv):
    file=folder+ '/sql/'+ name_query+'.sql'
    with open(file) as f:
        query = f.read()
    f.close()
    con=connection()
    df_raw= pd.read_sql_query(query, con)
    df_raw.to_csv(folder+ '/csv/' + name_csv+'.csv')

def crear_csv():
    extract('query_utn','csv_utn')
    extract('query_tres_de_febrero','csv_tres_de_febrero')
    
task_1= PythonOperator(
    task_id='crear_csv',
    python_callable=crear_csv,
    dag=dag
)

[task_1]