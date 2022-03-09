import logging
from os import path, makedirs
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from decouple import config

# config logging
logging.basicConfig(
    filename='test.log',
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.INFO)
logger = logging.getLogger('universidad_e')

root_folder = path.abspath(path.join(path.dirname(__file__), '..'))


def start_engine():
    """
    This function takes no arguments, returns engine to conect to DataBase

    Returns: sqlarchemy engine
    """
    # Credential loading from .env file
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')
    DB_TYPE = config('DB_TYPE')

    # Start Engine for DataBase connection
    return create_engine(f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


def query(*args):
    """
    This function querys the database of a given university, and stores the data in a .csv file in folder csv
    Args: list containg only the name of the university to query
    """

    university = args[0]

    # Path of proyect folder
    basepath = path.abspath(path.join(path.dirname(__file__), '..'))

    # File path of the .sql needed to exectute the query
    slqpath = path.abspath(path.join(basepath, f'sql/query_{university}.sql'))

    # Start engine for Database Conection
    engine = start_engine()
    with engine.begin():
        # open and read sql file for query statement
        with open(slqpath, 'r') as file:
            sql_query = text(file.read())

        # Safe create csv folder for csv files
        csvpath = path.join(basepath, 'csv')
        if not path.exists(csvpath):
            makedirs(csvpath)
        # Query and store data to csv file
        university_df = pd.read_sql_query(sql_query, con=engine)
        university_df.to_csv(csvpath + f'/universidad_{university}.csv')


def data_processing_interamericana():
    # Read csv from file
    df = pd.read_csv(f"{root_folder}/csv/universidad_interamericana.csv")

    # Rename specific columns
    df.rename(columns={"univiersities": "university", "carrera": "career", "inscription_dates": "inscription_date",
                       "sexo": "gender", "fechas_nacimiento": "age", "localidad": "location"}, inplace=True)
    # Filter dataframe
    filter_interamericana_df(df)

    # Check if txt folder exists and create if not
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')

    # Save to txt
    df.to_csv(f"{root_folder}/txt/interamericana.txt", index=None)


def filter_interamericana_df(df):
    """
    Map functions to columns to filter dataframe
    Args:
        df:
            pandas dataframe
    Returns:
        None
    """

    # Define functions to map
    def filter_string(x):
        return x.lower().replace("-", "_").strip("_")

    def get_date(x):
        # Format the string to use the correct century
        if x[0] != "0":
            x = "19" + x
        else:
            x = "20" + x
        return datetime.strptime(x, "%Y/%b/%d")

    def get_age(x):
        dif = datetime.now() - get_date(x)
        return dif.days // 365

    def get_inscription(x):
        return get_date(x).strftime("%Y-%m-%d")

    def get_first_name(x):
        string = filter_string(x)
        return string[:string.find("_")]

    def get_last_name(x):
        string = filter_string(x)
        return string[string.find("_") + 1:]

    def get_gender(x):
        if x == "M":
            return "male"
        return "female"

    # Load csv codes once
    csv_codes = pd.read_csv(f"{root_folder}/csv/codigos_postales.csv")
    csv_codes["localidad"] = csv_codes["localidad"].apply(lambda x: x.lower().replace(" ", "_").strip("_"))

    def get_postal_code(x):
        """
        Filter postal code from location
        """
        index = csv_codes[csv_codes['localidad'] == x].index[0]
        return csv_codes["codigo_postal"].iloc[index]

    # Apply functions to columns
    df["university"] = "universidad_abierta_interamericana"
    df["career"] = df["career"].apply(filter_string)
    df["inscription_date"] = df["inscription_date"].apply(get_inscription)
    df.insert(3, "first_name", df["names"].apply(get_first_name))
    df.insert(4, "last_name", df["names"].apply(get_last_name))
    df["age"] = df["age"].apply(get_age)
    df["gender"] = df["gender"].apply(get_gender)
    df["location"] = df["location"].apply(filter_string)
    df.insert(8, "postal_code", df["location"].apply(get_postal_code))
    df["email"] = df["email"].apply(filter_string)

    # Remove extra columns
    df.drop(["names", "direcciones"], 1, inplace=True)


def data_processing_pampa():
    # Read csv from file
    df = pd.read_csv(f"{root_folder}/csv/universidad_pampa.csv")
    # Rename specific columns
    df.rename(columns={"universidad": "university", "carrerra": "career", "fechaiscripccion": "inscription_date",
                       "sexo": "gender", "nacimiento": "age", "codgoposstal": "postal_code", "eemail": "email"},
              inplace=True)

    # Filter dataframe
    filter_pampa_df(df)

    # Check if txt folder exists and create if not
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')

    # Save to txt
    df.to_csv(f"{root_folder}/txt/pampa.txt", index=None)


def filter_pampa_df(df):
    """
    Map functions to columns to filter dataframe
    Args:
        df:
            pandas dataframe
    Returns:
        None
    """

    # Define functions to map
    def filter_string(x):
        return x.lower().replace(" ", "_").strip("_")

    def get_date(x):
        return datetime.strptime(x, "%d/%m/%Y")

    def get_age(x):
        dif = datetime.now() - get_date(x)
        return dif.days // 365

    def get_inscription(x):
        return get_date(x).strftime("%Y-%m-%d")

    def get_first_name(x):
        string = filter_string(x)
        return string[:string.find("_")]

    def get_last_name(x):
        string = filter_string(x)
        return string[string.find("_") + 1:]

    def get_gender(x):
        if x == "M":
            return "male"
        return "female"

    # Load csv codes once
    csv_codes = pd.read_csv(f"{root_folder}/csv/codigos_postales.csv")
    csv_codes["localidad"] = csv_codes["localidad"].apply(filter_string)

    def get_location(x):
        """
        Filter location from postal code
        """
        index = csv_codes[csv_codes['codigo_postal'] == x].index[0]
        return csv_codes["localidad"].iloc[index]

    # Apply functions to columns
    df["university"] = "universidad_nacional_de_la_pampa"
    df["career"] = df["career"].apply(filter_string)
    df["inscription_date"] = df["inscription_date"].apply(get_inscription)
    df.insert(3, "first_name", df["nombrre"].apply(get_first_name))
    df.insert(4, "last_name", df["nombrre"].apply(get_last_name))
    df["age"] = df["age"].apply(get_age)
    df["gender"] = df["gender"].apply(get_gender)
    df.insert(9, "location", df["postal_code"].apply(get_location))
    df["email"] = df["email"].apply(filter_string)

    # Remove extra columns
    df.drop(["nombrre", "direccion"], 1, inplace=True)


default_args = {
    "retries": 5,  # set retries at 5 according to the task
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        'Universidades_E',
        description='OT152-228',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 2, 18)
) as dag:
    query_pampa = DummyOperator(task_id='query_pampa')  # Voy a usar un  PostgresOperator para ejecutar la querie
    query_interamericana = DummyOperator(
        task_id='query_interamericana')  # Voy a usar un  PostgresOperator para ejecutar la querie
    data_processing_pampa = PythonOperator(task_id='data_processing_pampa',
                                           python_callable=data_processing_pampa,
                                           dag=dag)  # Voy a usar un PythonOperator para procesar los datos
    data_processing_interamericana = PythonOperator(task_id='data_processing_interamericana',
                                                    python_callable=data_processing_interamericana,
                                                    dag=dag)  # Voy a usar un PythonOperator para procesar los datos
    subir_s3 = DummyOperator(task_id='subir_s3')  # Voy a usar un S3 operator para subir los datos a S3

    [query_pampa >> data_processing_pampa, query_interamericana >> data_processing_interamericana] >> subir_s3
