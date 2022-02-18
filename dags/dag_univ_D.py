from datetime import timedelta, datetime
from airflow import DAG   
from airflow.operators.dummy import DummyOperator

default_args = {
	description = "ETL para dos universidades",
	schedule_interval = "@hourly"
	start_date = datetime(2022,2,18)
}

with DAG(dag_id="universidades_D", default_args=default_args) as dag:
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
	
    sql_query_utn = DummyOperator(task_id="univ. tecnologica")
	sql_query_tres_de_febrero = DummyOperator(task_id="univ tres de febrero")

    proc_utn = DummyOperator(task_id="proc utn")
    proc_tres_de_febrero = DummyOperator(task_id="proc tres de febrero")

    uploadS3_utn = DummyOperator(task_id="Carga S3 utn")
    uploadS3_tres_de_febrero = DummyOperator(task_id="Carga S3 tres de febrero")


	
sql_query_utn >> proc_utn >> uploadS3_utn
sql_query_tres_de_febrero >> proc_tres_de_febrero >> uploadS3_tres_de_febrero
