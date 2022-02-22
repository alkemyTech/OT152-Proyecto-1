from datetime import timedelta,datetime
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
 
with DAG(
    'universidades_c',
    description='university_processes',
    #defino ejecucion
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,2,18)
    )as dag:    
        #Queries
        #SQL for Universidad Nacional
        #SQL for Universidad de Palermo
        
        #Data processing 
        #pandas as pd 
        
        #Upload data to S3    
        
        universidad_nacional = DummyOperator(task_id='universidad_nacional')
        universidad_de_palermo = DummyOperator(task_id='universidad_de_palermo')
        query_sql = DummyOperator(task_id='query_sql')
        data_processing = DummyOperator(task_id='data_processing')
        upload_s3 = DummyOperator(task_id='uploads3')

        [universidad_nacional,universidad_de_palermo] >> query_sql >> data_processing >> upload_s3