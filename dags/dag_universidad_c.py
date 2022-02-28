import logging 
from datetime import timedelta,datetime
from time import strftime

from airflow import DAG 
from airflow.operators.dummy import DummyOperator


#Configuro los loggs acorde a lo que pide la tarea
logging.basicConfig(level=logging.DEBUG, 
datefmt=strftime("%Y-%m-%d"), 
format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidades C')

default_args = {
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}
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
    
    universidad_jujuy= DummyOperator(task_id='universidad_Jujuy)
    universidad_de_palermo= DummyOperator(task_id='universidad_de_Palermo')


    universidad_jujuy >> universidad_de_palermo
