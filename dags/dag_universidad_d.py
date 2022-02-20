from datetime import timedelta, datetime
from email import message
from airflow import DAG   
from airflow.operators.dummy import DummyOperator
import logging
from time import strftime

default_args = {

	"Retries" : 5,
    "Retry_delay": timedelta(minutes=5)

}

logging.basicConfig(level=logging.DEBUG, filename='logger.log', datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s:%(levelname)s:%(message)s')
                    
with DAG("universidades_D",
    description='Query Universidades_D',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 2, 20)
    
) as dag:

    """
    Se harán dos consultas SQL para dos universidades: UTN y Tres de Febrero
    Usaremos los siguientes operadores: 
	airflow.providers.postgres.operators.postgres -> PostgresOperator
    Procesamiento de los datos:
    airflow.operators.python_operator -> PythonOperator
    import pandas as pd
    Carga de datos a S3:
    airflow.providers.amazon.aws.operators.s3
	"""
    sql_query_utn = DummyOperator(task_id="Query_UTN")
    sql_query_tres_de_febrero = DummyOperator(task_id="Query_tres_de_febrero")

    proc_utn = DummyOperator(task_id="proc_utn")
    proc_tres_de_febrero = DummyOperator(task_id="proc_3_de_febrero")

    uploadS3_utn = DummyOperator(task_id="Carga_S3_utn")
    uploadS3_tres_de_febrero = DummyOperator(task_id="Carga_S3_tres_de_febrero")


	
    sql_query_utn >> proc_utn >> uploadS3_utn
    sql_query_tres_de_febrero >> proc_tres_de_febrero >> uploadS3_tres_de_febrero

log_critical1 = DummyOperator(task_id='log_query_utn')
log_critical2 = DummyOperator(task_id='log_query_3febrero')
log_critical3 = DummyOperator(task_id='processing_data')
log_critical4 = DummyOperator(task_id='log_load_s3')