from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from os import path, makedirs
import pandas as pd

# Ruta de la carpeta root
root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


# Limpio cualquier espacio innecesario
def clean_string(df):
    df = df.str.lower().str.replace('_', ' ').str.strip()
    return df


# Transformo el csv de flores a txt
def _transform_flores():
    df_flores = pd.read_csv(f'{root_folder}/csv/flores.csv')
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')


# Procesameinto de los datos de la universidad de flores
    df_flores['university'] = clean_string['university']
    df_flores['career'] = clean_string['career']
    df_flores['first_name'] = clean_string['first_name']
    df_flores['last_name'] = clean_string['last_name']
    df_flores['age'] = clean_string['age']
    df_flores['location'] = clean_string['location']
    df_flores['email'] = clean_string['email']
    df_flores['inscription_date'] = clean_string['inscription_date']
    df_flores['gender'] = ['gender'].str.replace('M', 'male')
    df_flores['gender'] = ['gender'].str.replace('F', 'female')


# Lo guardo en un .txt
    df_flores.to_csv(f'{root_folder}/txt/flores.txt', index=None)


# Transformo el csv de villa maria a txt
def _transform_villa_maria():
    df_villa_maria = pd.read_csv(f'{root_folder}/csv/villa.csv')
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')


# Procesamiento de los datos de villa maria
    df_villa_maria['university'] = clean_string['university']
    df_villa_maria['career'] = clean_string['career']
    df_villa_maria['first_name'] = clean_string['first_name']
    df_villa_maria['last_name'] = clean_string['last_name']
    df_villa_maria['age'] = clean_string['age']
    df_villa_maria['location'] = clean_string['location']
    df_villa_maria['email'] = clean_string['email']
    df_villa_maria['inscription_date'] = clean_string['inscription_date']

    df_villa_maria['gender'] = ['gender'].str.replace('M', 'male')
    df_villa_maria['gender'] = ['gender'].str.replace('F', 'female')
    codigo_postal = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv',
                                dtype={'codigo_postal': 'str'})
    df_villa_maria = df_villa_maria.merge(codigo_postal, on='codigo_postal')

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
