import logging 
from datetime import timedelta,datetime
from time import strftime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
import pandas as pd
from os import environ, path
from dotenv import load_dotenv
import sqlalchemy as db
from airflow.operators.python_operator import PythonOperator
import numpy as np

#Configuro los loggs acorde a lo que pide la tarea
logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}
#configura path raiz para mover dentro del proyecto
folder = path.abspath(path.join(path.dirname( __file__ ), '..'))


def limpiar_string(df):
    """
    Lee la serie y limpia el str minúsculas, sin espacios extras, ni guiones
        
    Args:
        df(serie Pandas): nombre del dataframe y serie ejemplo: DF['campo']
    
    Return:
        df(serie Pandas): Serie Limpia
    """
    df=df.str.lower().str.replace('_',' ').str.strip()
    return df
def transform(df):
    """
    separa el nombre en dos columnas, hace conversion de sexo m=male y f=female, calcula la edad 
    y busca la localidad con la localidad haciendo un merge con el archivo que se encuentra en
    /csv/codigos_postales.csv
        
    Args:
        df(serie Pandas): nombre del dataframe que se quiere transformar
    
    Return:
        df(serie Pandas): Dataframe transformado
    """
    df['universidad']=limpiar_string(df['universidad'])
    df['carrera']= limpiar_string(df['carrera'])
    df['email']= limpiar_string(df['email'])
        
    #separa el nombre en dos columnas
    df['nombre']=df['nombre'].str.replace(' ','_') #normaliza el nombre con "_"
    nombre=df['nombre'].str.split('_', expand=True)
    df['first_name']= nombre[0]
    df['last_name']= nombre[1]
    df.drop(columns=['nombre'],inplace=True)
    df['first_name']= limpiar_string(df['first_name'])
    df['last_name']=limpiar_string(df['last_name'])
    
    #convertir sexo m=male y f=female
    df['sexo']=df['sexo'].str.replace('m', 'male')
    df['sexo']=df['sexo'].str.replace('f', 'female')
    
    #Calculo de la edad
    hoy=datetime.now()  
    df['age']=hoy- df.fecha_de_nacimiento
    df['age']= (df['age']/ np.timedelta64(1, 'Y')).astype(int)
    
    #calculo de localidad
    if 'codigo_postal' in df.columns:
        df_postal=pd.read_csv(f'{folder}/csv/codigos_postales.csv',
                              dtype={'codigo_postal':'str'})
        df=df.merge(df_postal, on='codigo_postal')
    logging.info(df)
    return df
 
def read_csv(name_csv):
    """
    lee el archivo name_csv y retorna el dataframe
        
    Args:
        name_csv(str): nombre del csv a leer
    
    Return:
        df(serie Pandas): Dataframe 
    """
    df=pd.read_csv(f'{folder}/csv/{name_csv}.csv')
    return df
def load_txt():
    
    #ejecuta query de Palermo
    df_raw= read_csv('palermo')
    df = transform(df_raw)
    file='txt_palermo'
    df.to_csv(f'{folder}/txt/{file}.txt')

    #ejecuta query de UTN
    df_raw= read_csv('naciona')
    df = transform(df_raw)
    file='txt_nacional'
    df.to_csv(f'{folder}/txt/{file}.txt')
    
with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    
    #Queries
    #SQL for Universidad UTN
    #SQL for Universidad de Palermo
    
    #Data processing 
    
    
    universidad_nacional = DummyOperator(task_id='universidad_nacional')
    universidad_de_palermo= DummyOperator(task_id='universidad_de_Palermo')
    
    generar_txt= PythonOperator(
    task_id='generar_txt',
    python_callable=load_txt,
    dag=dag)

    universidad_nacional >> universidad_de_palermo
    generar_txt


