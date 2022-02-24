import logging
from datetime import timedelta
from time import strftime
from airflow.operators import PythonOperator
from airflow.models import DAG

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger('Universidad_d')

#configuro los retries acorde a lo que pide la tarea
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='dag_universidad_d', default_args=args,
    schedule_interval=None)

run_this