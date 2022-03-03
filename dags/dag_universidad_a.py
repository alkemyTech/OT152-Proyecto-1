from datetime import datetime, timedelta
from multiprocessing import connection
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging
from distutils.debug import DEBUG
import os
import pandas as pd
from decouple import config
from pendulum import today
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import numpy as np

## Database information
user = config("DB_USER")
password = config("DB_PASSWORD")
host = config("DB_HOST")
port = config("DB_PORT")
database = config("DB_NAME")

## Database connection
db_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
engine = create_engine(db_url)

## SQL queries file paths (if you are in: "OT152-Proyecto-1" or "dags" directory)
if 'proyecto/dags' in os.getcwd():
    project_folder = os.path.normpath(os.getcwd() + os.sep + os.pardir)
    u_flores = os.path.join(project_folder, 'sql', 'query_flores.sql')
    u_villa_maria = os.path.join(project_folder, 'sql', 'query_villa.sql')
else:
    project_folder = os.getcwd()
    u_flores = os.path.join(project_folder, 'sql', 'query_flores.sql')
    u_villa_maria = os.path.join(project_folder, 'sql', 'query_villa.sql')

## SQL query to csv file
def query_csvfile(**kwargs):
    '''
    Connet to a DB, execute a sql query and save the data in a .csv file
    INPUT: kwargs['file_path'] = path where we can find the .sql file
    OUTPUT: kwargs['file_name'] = name of .csv file with data table.    
    '''
    with engine.connect() as conn:
        path = open(kwargs['file_path'])
        query = text(path.read())
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        csv_path = os.path.join(project_folder, 'csv', kwargs['file_name'])
        df.to_csv(csv_path, sep = ',', index=False)

def extract(name_queary, format_date):
    with open(f'{project_folder}/sql/{name_queary}.sql') as f:
        query = f.read()
    f.close()
    con = connection()
    df_raw = pd.read_sql_query(query, con, 
                                dates = {'fecha_de_nacimiento': format_date})
    return df_raw


def clean_string(df):
    df = df.str.lower().str.replace('_', ' ').str.strip()
    return df

def transform(df):

    df['university'] = clean_string(df['university'])
    df['career'] = clean_string(df['career'])
    df['first_name'] = clean_string(df['first_name'])
    df['last_name'] = clean_string(df['last_name'])
    df['age'] = clean_string(df['age'])
    df['location'] = clean_string(df['location'])
    df['email'] = clean_string(df['email'])
    df['inscription_date'] = clean_string(df['inscription_date'])

    df['gender'] = df['gender'].str.replace('M', 'male')
    df['gender'] = df['gender'].str.replace('F', 'female')

    if 'codigo_postal' in df.columns:
        df_postal = pd.read_csv(f'{project_folder}/csv/codigos_postales.csv', 
                                dtype={'codigo_postal': 'str'})
        df = df.merge(df_postal, on = 'codigo_postal')
        return df

def load (df, file):
    df.to_csv(f'{project_folder}/txt/{file}.txt')

def etl():
    df_raw = extract('query_flores', '%Y/%m/%D')
    df = transform(df_raw)
    load(df, 'txt_flores')


    df_raw = extract('query_villa', '%D/%m/%Y')
    df = transform(df_raw)
    load(df, 'txt_villa')


default_args = {
    'retries': 5, # Quantity of retries before shutdown
    'retry_delay': timedelta(minutes=5) # Wait time before next retry
}

# create logger
FORMAT='%(asctime)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,datefmt=("%Y-%m-%d"),format=FORMAT)
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
    sql_universidad_flores = PythonOperator(task_id='sql_universidad_flores',
                                python_callable = query_csvfile,
                                op_kwargs = {'file_path':'u_flores',
                                'file_name':'flores.csv'})
    processing_universidad_flores = DummyOperator(task_id='processing_universidad_flores')
    load_universidad_flores = DummyOperator(task_id='load_universidad_flores')

    # Tasks Universidad Nacional de Villa Maria
    sql_universidad_villa_maria = PythonOperator(task_id='ELT_Universidad_Nacional_De_Villa_Maria',
                                    python_callable = query_csvfile,
                                    op_kwargs = {'file_path':'u_villa_maria',
                                    'file_name':'villa_maria.csv'})
    processing_universidad_villa_maria = DummyOperator(task_id='processing_universidad_villa_maria')
    load_universidad_villa_maria = DummyOperator(task_id='load_universidad_villa_maria')

    # Tasks order
    sql_universidad_flores >> processing_universidad_flores >> load_universidad_flores

    sql_universidad_villa_maria >> processing_universidad_villa_maria >> load_universidad_villa_maria