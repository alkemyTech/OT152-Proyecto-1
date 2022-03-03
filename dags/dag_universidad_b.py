import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from decouple import config
from sqlalchemy import engine
from os import path
import pandas as pd
import sqlalchemy 


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',  
    datefmt='%Y-%m-%d'
)
logger = logging.getLogger("univ_b")

#Setting retries at 5
default_args = {
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

#Configuro la conexion a la base de datos
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')
DB_TYPE = config('DB_TYPE')

conexion = f'{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sqlalchemy.create_engine(conexion)
conn = engine.connect()


#Extraigo las tablas de las consultas y las guardo en un csv
root_folder = path.abspath(path.join(path.dirname(__file__), '..'))

def save_to_csv(university):
    file = open(f'{root_folder}/sql/{university}.sql', "r")
    query = sqlalchemy.text(file.read())
    data = pd.read_sql_query(query, conn)

    data.to_csv(f'{root_folder}/csv/universidad_{university}.csv', index=False)

def clean_and_save(univeristy):
    
    #creating two columns from 'first_name' and 'last_name' from the column 'name'
    data['first_name'], data['last_name'] = data['name'].str.split(' ', 1).str

    #creating an 'age' function to calculate the age by the giving birth date
    def age(born):
        born = datetime.strptime(born, "%Y-%m-%d").date()
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    #applying it
    data['Age'] = data['birth_date'].apply(age)

    #dropping the extra column
    data.drop(columns=['name', 'birth_date'],inplace=True)

    #renaming the columns according to the task
    data.rename(columns={"zip_code" : "postal_code", "sex" : "gender"}, inplace=True)
    data['gender'].replace(['F', 'M'], ['female', 'male'], inplace=True)

    #reading the csv files and creating a dataframe from it to make the merge respectively
    df_postal=pd.read_csv('codigos_postales.csv')
    data['postal_code']=data['postal_code'].astype(int)
    df_postal=pd.read_csv('codigos_postales.csv')

    df_postal['postal_code']=df_postal['codigo_postal']
    df_postal.drop(columns=['codigo_postal'], inplace=True)

    #mergin the dataframe from the csv file
    data=pd.merge(data, df_postal, how='right', on='postal_code')
    data.rename(columns={"localidad" : "location"}, inplace=True)

    #filling Nan on age with '0' because otherwise it doesn't allow me to set the column as int.
    
    data['Age'].fillna(0, inplace=True)
    
    data.astype({'postal_code': str, 'location': str, 'university': str, 'first_name': str, 'last_name': str,
                'gender': str, 'email': str, 'career': str })

    #lambda function to make values in lowercase
    data=data.apply(lambda x: x.astype(str).str.lower())

    data.to_csv(f'{root_folder}/txt/{univeristy}.txt', header=None, index=None, sep='\t', mode='a')

with DAG(
    'Universities_B_dags',
    description='Ejecuta ETL de las universidades B',
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    start_date=(datetime(2022, 2, 18)) # Fecha de inicio
) as dag:
    # Tareas a ejecutar leyendo sql con pandas
    tarea1 = DummyOperator(task_id='ETL_comahue')
    tarea2 = DummyOperator(task_id='ETL_salvador')
    save_to_csv = PythonOperator(task_id='Universidades_B_data', python_callable=save_to_csv, dag=dag)
    save_txt = PythonOperator(task_id='Universidades_B_TXT', python_callable=clean_and_save, dag=dag)

    # Orden de tareas
    tarea1 >> tarea2 >> save_txt