from datetime import timedelta,datetime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
 
with DAG(
    'universidades C',
    description='procesos ETL universidades',
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    ) as dag:
    
    """
    Consultas
    SQL para Universidad Nacional
    SQL para Universidad de Palermo
    
    Procesamiento de datos
    panas as pd 
    
    Carga de datos en S3
    """
    
    tarea_1= DummyOperator(task_id='Universidad Nacional')
    tarea_2= DummyOperator(task_id='Universidad de Palermo')

    tarea_1 >> tarea_2