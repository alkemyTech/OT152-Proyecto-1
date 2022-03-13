import logging
from time import strftime
from os import path, makedirs
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import pandas as pd

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

# configuro los retries acorde a lo que pide la tarea
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

folder = path.abspath(path.join(path.dirname(__file__), '..'))


def data_process(*university):
    """
    This function formats data read by csv, and stores de processed data into a txt file

    Args: name of the university
    """
    university = university[0]
    # Select column name for universities birthdate column (query names aren't the same)
    birth_date = ''
    if university == 'universidad_utn':
        birth_date = 'birth_date'
    elif university == 'universidad_tres_de_febrero':
        birth_date = 'birth_dates'

    # Get the two dataframes to work with
    df = pd.read_csv(folder + '/csv/' + university + '.csv', index_col=0, parse_dates=['inscription_date', birth_date])
    postal_locations = pd.read_csv(folder + '/csv/codigos_postales.csv')

    postal_locations.columns = ['postal_code', 'location']
    postal_locations['location'] = postal_locations['location'].str.lower()

    # Eliminate suffixes from names
    suffixes = ['mr._', 'mrs._', 'dr._', '_dds', '_dv', '_md']
    for suffix in suffixes:
        df['names'] = df.apply(lambda x: x['names'].strip(suffix), axis=1)

    # Replace all underscores with white spaces
    cols = ['university', 'career', 'names']
    for col in cols:
        df[col] = df.apply(lambda x: x[col].replace('_', ' '), axis=1)

    # Split names to first and last name columns respectively
    df[['first_name', 'last_name']] = df.names.str.split(expand=True, n=1)

    # Gender change m to male, f to female
    df['gender'].mask(df['gender'] == 'm', 'male', inplace=True)
    df['gender'].mask(df['gender'] == 'f', 'female', inplace=True)

    # Merge to get location based on postal code and lower caps
    if university == 'universidad_tres_de_febrero':
        df = df.merge(postal_locations, on='postal_code', how='left')
    elif university == 'universidad_utn':
        df = df.merge(postal_locations, on='location', how='left')
    # Eliminate posbile duplicates from merging and reset index
    df.drop_duplicates(subset=['email'], keep='first', inplace=True)
    df.reset_index(inplace=True)

    # Create age column
    df['age'] = datetime.today().year - df[birth_date].dt.year
    # Correction of bad parsing dates (example: year 41 was parsed as 2041 instead of 1941)
    df['age'].mask(df['age'] < 0, df['age'] + 100, inplace=True)

    # Drop unnecesary columns
    df.drop(['names'], axis=1, inplace=True)
    df.drop([birth_date], axis=1, inplace=True)

    # Reorder columns
    df = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code',
             'location', 'email']]

    # Convert all columns to string but age (int)
    for col in df.columns:
        if col != 'age':
            df[col] = df[col].astype('string')

    # Safe create txt folder
    txtpath = path.join(folder, 'txt')
    if not path.exists(txtpath):
        makedirs(txtpath)
    df.to_csv(folder + f'/txt/{university}.txt')


with DAG(
        dag_id="ETL_para_dos_universidades",
        description="ETL para dos universidades",
        schedule_interval="@hourly",
        start_date=datetime(2002, 2, 18)
) as dags:
    """
    Se harán dos consultas SQL para dos universidades: UTN y Tres de Febrero
    usaremos los siguientes operadores:
        airflow.providers.postgres.operators.postgres -> PostgresOperator
    
    Procesamiento de los datos:
        airflow.operators.python_operator -> PythonOperator
    
    Carga de datos a S3:
        airflow.providers.amazon.aws.operators.s3
    """
    sql_query_utn = DummyOperator(task_id="sql_query_utn")
    sql_query_tres_de_febrero = DummyOperator(task_id="sql_query_tres_de_febrero")

    procesar_utn = PythonOperator(task_id="procesar_utn", python_callable=data_process, op_args=['universidad_utn'])

    procesar_tres_de_febrero = PythonOperator(task_id="procesar_tres_de_febrero",
                                              python_callable=data_process, op_args=['universidad_tres_de_febrero'])

    uploads3_utn = DummyOperator(task_id="uploads3_utn")
    uploads3_tres_de_febrero = DummyOperator(task_id="uploads3_tres_de_febrero")

    sql_query_utn >> procesar_utn >> uploads3_utn
    sql_query_tres_de_febrero >> procesar_tres_de_febrero >> uploads3_tres_de_febrero
