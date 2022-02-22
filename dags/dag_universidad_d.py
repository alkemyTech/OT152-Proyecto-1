from datetime import timedelta, datetime
from airflow import DAG   


#configuro los retries acorde a lo que pide la tarea
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
