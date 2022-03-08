from ast import Str
from datetime import timedelta, datetime
import logging
from operator import index
from unittest.util import strclass
from decouple import config
from os import makedirs, mkdir, path
import psycopg2
from dotenv import load_dotenv

from airflow.operators.python import PythonOperator
import os
from datetime import date
import sqlalchemy
import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from matplotlib.pyplot import connect
from uritemplate import expand

logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')


root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


def clean_string(data):
    """
    Args:
        data: data obtained from the dataframe
    
    Return:
        Processed data
    """
    return data.lower().replace("-", "").strip()
    
def _transform_moron():
    """
    Args:
        None

    Return:
        file moron_txt 
    """
    df_moron = pd.read_csv(f'{root_folder}/csv/moron.csv')
    
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')
        
    df_moron.rename(columns={'universidad':'university','carrerra':'carer',
               'fechaiscripccion':'inscription_date',
               'sexo':'gender','codgoposstal':'postal_code',
               'eemail':'email'},inplace=True)
    

    
    #Data processing
    df_moron['nombrre'] = df_moron['nombrre'].str.replace(' ','_')
    name = df_moron['nombrre'].str.split('_',expand=True)
    df_moron['first_name'] = name[0]
    df_moron['last_name'] = name[1]
    
    gender_female = df_moron['gender'] =='F'
    gender_male = df_moron['gender'] =='M'
    
    df_moron.loc[gender_female, 'gender'] = 'female'
    df_moron.loc[gender_male, 'gender'] = 'male'
    
    df_moron.university = df_moron['university'].apply(clean_string)
    df_moron.carer = df_moron['carer'].apply(clean_string)
    df_moron.inscription_date = df_moron['inscription_date'].apply(clean_string)
    df_moron.first_name = df_moron['first_name'].apply(clean_string)
    df_moron.last_name = df_moron['last_name'].apply(clean_string)
    df_moron.email = df_moron['email'].apply(clean_string)    
        
    location = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv')
    df_moron['location'] = location['localidad'].apply(lambda x: x.lower().replace(" ",''))

    #Removing extra columns
    df_moron.drop(columns=['nombrre'],inplace=True)
    #Save to txt
    df_moron.to_csv(f'{root_folder}/txt/moron.txt',index=None)        

def _transform_rio_cuarto():
    """
    Args:
        None

    Return:
        file rio_cuarto_txt 
    
    """
    df_rio_cuarto = pd.read_csv(f'{root_folder}/csv/rio_cuarto.csv')
    
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')
        
    df_rio_cuarto.rename(columns={'univiersities':'university','carrera':'carer',
               'inscription_dates':'inscription_date',
               'sexo':'gender','localidad':'location'},inplace=True)
    
    gender_female = df_rio_cuarto['gender'] =='F'
    gender_male = df_rio_cuarto['gender'] =='M'
    
    df_rio_cuarto.loc[gender_female, 'gender'] = 'female'
    df_rio_cuarto.loc[gender_male, 'gender'] = 'male'   
    
    #Clear columns df_rio_cuarto
    df_rio_cuarto['names'] = df_rio_cuarto['names'].str.replace('-','_')
    name = df_rio_cuarto['names'].str.split('_',expand=True)
    df_rio_cuarto['first_name'] = name[0]
    df_rio_cuarto['last_name'] = name[1]
    
    df_rio_cuarto.university = df_rio_cuarto['university'].apply(clean_string)
    df_rio_cuarto.carer = df_rio_cuarto['carer'].apply(clean_string)
    df_rio_cuarto.inscription_date = df_rio_cuarto['inscription_date'].apply(clean_string)
    df_rio_cuarto.first_name = df_rio_cuarto['first_name'].apply(clean_string)
    df_rio_cuarto.last_name = df_rio_cuarto['last_name'].apply(clean_string)
    df_rio_cuarto.location = df_rio_cuarto['location'].apply(clean_string)
    df_rio_cuarto.email = df_rio_cuarto['email'].apply(clean_string)
    
    
    postal_code = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv')
    df_rio_cuarto['postal_code'] = postal_code['codigo_postal']
    

    #Removin extra columns
    df_rio_cuarto.drop(columns=['names'],inplace=True)
    
    #Save to txt
    df_rio_cuarto.to_csv(f'{root_folder}/txt/rio_cuarto.txt',index=None)
 

default_args = {
    "retries": 5,  # try 5 times
    "retry_delay": timedelta(minutes=10)  # wait 10 minutes to try again
}
with DAG(
    'Query_Universidad_F',
    description='Realizar consultas sobre Universidad de Moron y Rio Cuarto',
    schedule_interval=timedelta(hours=1), #ejecución cada una hora
    start_date=datetime(2022, 2, 19) #Puse esa fecha porque no fue aclarada en la consigna

) as dag:
    #Queries usando la función read_sql de pandas
    tarea1 = DummyOperator(task_id='Query_F1') #Universidad de Morón
    tarea2 = DummyOperator(task_id='Query_F2') #Universidad de Río Cuarto
    tarea3 = DummyOperator(task_id='Processing_data')#Reading and processing data using read_sql
    tarea4 = DummyOperator(task_id='Upload_S3')#uploading data to s3
    
    
    moron_txt = PythonOperator(
        task_id='moron_txt',
        python_callable=_transform_moron,
        dag=dag
    )
    rio_cuarto_txt = PythonOperator(
        task_id='rio_cuarto_txt',
        python_callable=_transform_rio_cuarto,
        dag=dag
    )
    [tarea1>>moron_txt, tarea2 >> rio_cuarto_txt] >> tarea4

    