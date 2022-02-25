import logging
from datetime import timedelta, datetime
from time import strftime
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
#from airflow.operators import python_operator

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

#configuro los retries acorde a lo que pide la tarea
args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    
}

dag = DAG(
    dag_id='dag_universidad_d', 
    default_args=args,
    schedule_interval=timedelta(minutes=2),
    start_date= datetime(2022,2,24)
    )

def etl_extract():
    # with open('.\sql\query_utl.sql') as f:
    #     query = f.read()
    # f.close()
    # print(query)
    print('test')
    logging.info('test')

task_1= PythonOperator(
    task_id='extract_utn',
    python_callable=etl_extract,
    dag=dag
)