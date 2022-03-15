import logging
import os
from os import path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


# Ruta de la carpeta root
root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


# Transformo el csv de flores a txt
def _transform_flores():
    df_flores = pd.read_csv(f'{root_folder}/csv/flores.csv')
    os.makedirs(f'{root_folder}/txt', exist_ok=True)


# Procesamiento de los datos de la universidad de flores

# Convierto todas las columnas en string menos age
    for col in df_flores.columns:
        if col != 'age':
            df_flores[col] = df_flores[col].astype('string')
    columnas = ['university', 'career', 'first_name', 'last_name',
                'age', 'location', 'email', 'inscription_date']
    for col in columnas:
        df_flores[col] = df_flores.apply(lambda x: x[col].replace('_', ' '),
                                         axis=1)
    df_flores['gender'] = ['gender'].str.replace('M', 'male')
    df_flores['gender'] = ['gender'].str.replace('F', 'female')
# Reordeno las columnas
    df_flores = df_flores[['university', 'career', 'inscription_date',
                           'first_name', 'last_name',
                           'gender', 'age', 'postal_code',
                           'location', 'email']]

# Lo guardo en un .txt
    df_flores.to_csv(f'{root_folder}/txt/flores.txt', index=None)


# Transformo el csv de villa maria a txt
def _transform_villa_maria():
    df_villa_maria = pd.read_csv(f'{root_folder}/csv/villa.csv')
    os.makedirs(f'{root_folder}/txt', exist_ok=True)

# Procesamiento de los datos de villa maria

# Convierto todas las columnas en string menos age
    for col in df_villa_maria.columns:
        if col != 'age':
            df_villa_maria[col] = df_villa_maria[col].astype('string')
    cols = ['university', 'career', 'first_name', 'last_name',
            'age', 'location', 'email', 'inscription_date']
    for col in cols:
        df_villa_maria[col] = df_villa_maria.apply(lambda x: x[col].replace('_', ' '),
                                                   axis=1)
    df_villa_maria['gender'] = ['gender'].str.replace('M', 'male')
    df_villa_maria['gender'] = ['gender'].str.replace('F', 'female')
    codigo_postal = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv',
                                dtype={'codigo_postal': 'str'})
    df_villa_maria = df_villa_maria.merge(codigo_postal, on='codigo_postal')
# Reordeno las columnas
    df_villa_maria = df_villa_maria[['university', 'career',
                                     'inscription_date', 'first_name'
                                     'last_name', 'gender',
                                     'age', 'postal_code',
                                     'location', 'email']]
# Lo guardo en un .txt
    df_villa_maria.to_csv(f'{root_folder}/txt/villa.txt', index=None)


default_args = {
    'retries': 5,  # Quantity of retries before shutdown
    'retry_delay': timedelta(minutes=5)  # Wait time before next retry
}


# create logger
FORMAT = '%(asctime)s - %(name)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, datefmt=("%Y-%m-%d"), format=FORMAT)
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

    processing_universidad_flores = PythonOperator(
        task_id='processing_universidad_flores',
        python_callable=_transform_flores,
        dag=dag
    )

# Tasks Universidad Nacional de Villa Maria

processing_universidad_villa_maria = PythonOperator(
    task_id='processing_universidad_villa_maria',
    python_callable=_transform_villa_maria,
    dag=dag
)

# Tasks order
[processing_universidad_flores, processing_universidad_villa_maria]
