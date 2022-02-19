from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator


default_args = {
    "retries": 5, #set retries at 5 according to the task 
    "retry_delay": timedelta(minutes=5) 
}
with DAG(
    'Universidades_E',
    description='OT152-228',
    schedule_interval=timedelta(hours=1),   
    start_date=datetime(2022, 2, 18)
) as dag:
    query_moron = DummyOperator(task_id='query_moron')  #Voy a usar un  PostgresOperator para ejecutar la querie
    query_riocuarto = DummyOperator(task_id='query_riocuarto') #Voy a usar un  PostgresOperator para ejecutar la querie
    procesamiento_datos = DummyOperator(task_id='procesamiento_datos') #Voy a usar un PythonOperator para procesar los datos
    subir_s3 = DummyOperator(task_id='subir_s3') #Voy a usar un S3 operator para subir los datos a S3

    [query_moron, query_riocuarto] >> procesamiento_datos >> subir_s3