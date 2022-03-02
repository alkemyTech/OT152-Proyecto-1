import logging 
from datetime import timedelta,datetime
from time import strftime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
import pandas as pd
from os import environ, path
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as db
from airflow.operators.python_operator import PythonOperator

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

def connection():
    """
    Crea la conexion de la BD postgres segun las credeciales del archivo template.env que debe estar de manera local
    Args:
        None
    Return:
        Engine Instance: retorna la conexion
    """
    #carga las credenciales en template.env
    file=folder+ '/template.env'
    load_dotenv(dotenv_path=file)
    DB_USER = environ.get('DB_USER')
    DB_PASSWORD =environ.get('DB_PASSWORD')
    DB_HOST = environ.get('DB_HOST')
    DB_PORT = environ.get('DB_PORT')
    DB_NAME = environ.get('DB_NAME')

    #Genera la conexion de la base de datos segun las credenciales
    path =f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    con = db.create_engine(path, echo=True)
    return con

def extract(name_query,name_txt):
    """
    Lee query localizada en path /sql y genera archivo .txt
    
    Args:
        name_query(str): nombre de la query localizada en path /sql
        name_csv(str): nombre que se le va a dar a csv generado
    
    Return:
        file: txt generado en la ruta /txt/name_txt
    """
    with open(f'{folder}/sql/{name_query}.sql') as f:
        query = f.read()
    f.close()
    con=connection()
    df_raw= pd.read_sql_query(query, con)
    df_raw.to_csv(f'{folder}/txt/{name_txt}.txt')

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
#def convertir_name(df):

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
    return df

def load():
    """
    Ejecuta la extraccion de las universidades UTN y Tres de Febrero
    
    Args:
        None
    Return:
        /csv/csv_utn.csv
        /csv/csv_tres_de_febrero.csv
    """
    extract('query_jujuy','txt_jujuy')
    extract('query_palermo','txt_palermo')
with DAG(
    'universidades_c',
    description='university_processes',
    default_args= default_args,
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    
    #Queries
    #SQL for Universidad Jujuy
    #SQL for Universidad de Palermo
    
    #Data processing 
    #pandas as pd 
    
    #Upload data to S3    
    
    universidad_jujuy= DummyOperator(task_id='universidad_jujuy')
    universidad_de_palermo= DummyOperator(task_id='universidad_de_Palermo')
    generar_txt= PythonOperator(
    task_id='generar_txt',
    python_callable=crear_txt,
    dag=dag)

    universidad_jujuy >> universidad_de_palermo
