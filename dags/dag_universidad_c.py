import logging 
from datetime import timedelta,datetime
from airflow import DAG 
import pandas as pd
from os import path
from airflow.operators.python_operator import PythonOperator
import numpy as np

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
    folder = path.abspath(path.join(path.dirname( __file__ ), '..'))
    columnas= ['universidad','carrera', 'email','first_name', 'last_name']
            
    #separa el nombre en dos columnas
    df['nombre']=df['nombre'].str.replace(' ','_') #normaliza el nombre con "_"
    nombre=df['nombre'].str.split('_', expand=True)
    df['first_name']= nombre[0]
    df['last_name']= nombre[1]
    df.drop(columns=['nombre'],inplace=True)
    
    # actualiza las columnas en minusculas y sin espacios
    for columna in columnas:
        df[columna]=df[columna].str.lower().str.replace('_',' ').str.strip()
    
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
    return df
def load_txt():
    #extrae csv de universidad de Palermo
    folder = path.abspath(path.join(path.dirname( __file__ ), '..'))
    df_raw= pd.read_csv(f'{folder}/csv/query_palermo.csv', infer_datetime_format=True, 
                        parse_dates=['fecha_de_nacimiento'],dtype={'codigo_postal':'str'} )
    df = transform(df_raw)
    df.to_csv(f'{folder}/txt/txt_palermo.txt')

    #extrae csv de universidad de Jujuy
    df_raw= pd.read_csv(f'{folder}/csv/query_jujuy.csv', infer_datetime_format=True,
                        parse_dates=['fecha_de_nacimiento'],dtype={'codigo_postal':'str'})
    df = transform(df_raw)
    df.to_csv(f'{folder}/txt/txt_jujuy.txt')
    
with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:    
    generar_txt= PythonOperator(
    task_id='generar_txt',
    python_callable=load_txt,
    dag=dag)
 
    generar_txt


