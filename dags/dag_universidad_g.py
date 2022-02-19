from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import pandas as pd
from airflow.settings import Session

def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()


create_new_conn(session,
                    {"conn_id": "postgres_1",
                     "conn_type": "postgres",
                     "host": "training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com",
                     "port": 5432,
                     "schema": "public",
                     "login": "alkymer",
                     "password": "alkymer_123"})

with DAG(
    'universidades',
    description= 'ETL para dos universidades',
    schedule_interval= "@hourly",
    start_date=datetime(2022, 2, 17),
) as dag:

    """usar PostgresOperator ejecutar query
    universidad_kenedy = PostgresOperator(
    task_id="universidad_kenedy",
    postgres_conn_id="postgres_default",
    sql="sql/query_kenedy.sql",
    )"""   

    """usar PostgresOperator ejecutar query
    universidad_lationamericana = PostgresOperator(
    task_id="universidad_sociales",
    postgres_conn_id="postgres_default",
    sql="sql/query_latinoamericana.sql",
    )"""   


    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_Kenedy')
    [tarea_1,tarea_2]
