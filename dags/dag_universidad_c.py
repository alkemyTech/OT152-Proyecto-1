from datetime import timedelta,datetime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
 
with DAG(
    'universidades_c',
    description='procesos_universidades',
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    
    #Consultas
    #SQL para Universidad Nacional
    #SQL para Universidad de Palermo
    
    #Procesamiento de datos
    #pandas as pd 
    
    #Carga de datos en S3
    
    
    universidad_nacional= DummyOperator(task_id='universidad_nacional')
    universidad_de_palermo= DummyOperator(task_id='universidad_de_Palermo')

    universidad_nacional >> universidad_de_palermo