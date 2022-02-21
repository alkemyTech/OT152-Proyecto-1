from datetime import timedelta, datetime
from airflow import DAG   
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id = "ETL para dos universidades", 
    description : "ETL para dos universidades",
    scheduyle_interval : "@hourly",
    start_date : datetime(2002,2,18)
    ) as dags:
    """
    Se harán dos consultas SQL para dos universidades: UTN y Tres de Febrero
    usaremos los siguientes operadores:
        airflow.providers.postgres.operators.postgres -> PostgresOperator
    
    Procesamiento de los datos:
        airflow.operators.python_operator -> PythonOperator
    
    Carga de datos a S3:
        airflow.providers.amazon.aws.operators.s3
    """
    sql_query_utn = DummyOperator(task_id = "sql_query_utn")
    sql_query_tres_de_febrero = DummyOperator(task_id = "sql_query_tres_de_febrero")

    procesar_utn = DummyOperator(task_id="procesar_utn")
    procesar_tres_de_febrero = DummyOperator(task_id="procesar_tres_de_febrero")

    uploads3_utn = DummyOperator(task_id="uploads3_utn")
    uploads3_tres_de_febrero = DummyOperator(task_id="uploads3_tres_de_febrero")
    
    sql_query_utn >> procesar_utn >> uploads3_utn
    sql_query_tres_de_febrero >> procesar_tres_de_febrero >> uploads3_tres_de_febrero

