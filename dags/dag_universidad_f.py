from datetime import timedelta, datetime,date
import logging
from os import makedirs, path

from decouple import config
import sqlalchemy
import pandas as pd

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger('univ_f')

root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


def clean_string(data):
    """
    Args:
        data: data obtained from the dataframe
    
    Return:
        Processed data
    """
    return data.lower().replace("-", "").strip()


def age(born):
    """
    Args:
        born: date of birth

    Return:
        age
    """
    born = datetime.strptime(born, "%d/%m/%Y").date()
    today = date.today()
    return today.year - born.year - ((today.month,today.day) < (born.month,born.day))

    
def _transform_moron():
    """
    Args:
        None

    Return:
        file moron_txt 
    """
    df_moron = pd.read_csv(f'{root_folder}/csv/moron.csv')
    
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')
        
    df_moron.rename(columns={'universidad':'university','carrerra':'carer',
               'fechaiscripccion':'inscription_date',
               'sexo':'gender','codgoposstal':'postal_code',
               'eemail':'email'},inplace=True)
    

    
    #Data processing
    df_moron['nombrre'] = df_moron['nombrre'].str.replace(' ','_')
    name = df_moron['nombrre'].str.split('_',expand=True)
    df_moron['first_name'] = name[0]
    df_moron['last_name'] = name[1]
    
    gender_female = df_moron['gender'] =='F'
    gender_male = df_moron['gender'] =='M'
    
    df_moron.loc[gender_female, 'gender'] = 'female'
    df_moron.loc[gender_male, 'gender'] = 'male'
    
    df_moron.university = df_moron['university'].apply(clean_string)
    df_moron.carer = df_moron['carer'].apply(clean_string)
    df_moron.inscription_date = df_moron['inscription_date'].apply(clean_string)
    df_moron.first_name = df_moron['first_name'].apply(clean_string)
    df_moron.last_name = df_moron['last_name'].apply(clean_string)
    df_moron.email = df_moron['email'].apply(clean_string)    
    

    df_moron['age'] = df_moron['nacimiento'].apply(age)
        
    location = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv')
    df_moron['location'] = location['localidad'].apply(lambda x: x.lower().replace(" ",''))

    #Removing extra columns
    df_moron.drop(columns=['nombrre','nacimiento'],inplace=True)
    #Save to txt
    df_moron.to_csv(f'{root_folder}/txt/moron.txt',index=None)        


def _transform_rio_cuarto():
    """
    Args:
        None

    Return:
        file rio_cuarto_txt 
    
    """
    df_rio_cuarto = pd.read_csv(f'{root_folder}/csv/rio_cuarto.csv')
    
    #if the folder path doesn't exist, create it
    if not path.exists(f'{root_folder}/txt'):
        makedirs(f'{root_folder}/txt')
        
    
    df_rio_cuarto.rename(columns={'univiersities':'university','carrera':'carer',
               'inscription_dates':'inscription_date',
               'sexo':'gender','localidad':'location'},inplace=True)
    
    gender_female = df_rio_cuarto['gender'] =='F'
    gender_male = df_rio_cuarto['gender'] =='M'
    
    df_rio_cuarto.loc[gender_female, 'gender'] = 'female'
    df_rio_cuarto.loc[gender_male, 'gender'] = 'male'   
    def age_rio_cuarto(born):
        born = datetime.strptime(born, "%y/%b/%d").date()
        today = date.today()
        return today.year - born.year - ((today.month,today.day) < (born.month,born.day))

    #applying function to calculate age
    df_rio_cuarto['age'] = df_rio_cuarto['fechas_nacimiento'].apply(age_rio_cuarto)
    
    #Clear columns df_rio_cuarto
    df_rio_cuarto['names'] = df_rio_cuarto['names'].str.replace('-','_')
    name = df_rio_cuarto['names'].str.split('_',expand=True)
    df_rio_cuarto['first_name'] = name[0]
    df_rio_cuarto['last_name'] = name[1]
    
    df_rio_cuarto.university = df_rio_cuarto['university'].apply(clean_string)
    df_rio_cuarto.carer = df_rio_cuarto['carer'].apply(clean_string)
    df_rio_cuarto.inscription_date = df_rio_cuarto['inscription_date'].apply(clean_string)
    df_rio_cuarto.first_name = df_rio_cuarto['first_name'].apply(clean_string)
    df_rio_cuarto.last_name = df_rio_cuarto['last_name'].apply(clean_string)
    df_rio_cuarto.location = df_rio_cuarto['location'].apply(clean_string)
    df_rio_cuarto.email = df_rio_cuarto['email'].apply(clean_string)
    
    postal_code = pd.read_csv(f'{root_folder}/csv/codigos_postales.csv')
    df_rio_cuarto['postal_code'] = postal_code['codigo_postal']
    

    #Removin extra columns
    df_rio_cuarto.drop(columns=['names','fechas_nacimiento'],inplace=True)
    
    #Save to txt
    df_rio_cuarto.to_csv(f'{root_folder}/txt/rio_cuarto.txt',index=None)


def init_connection():
    """
    Initializes connection to postgres db
    Returns:
        MockConnection
    """
    # Connection DB credentials
    DB_USER = config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_HOST = config('DB_HOST')
    DB_PORT = config('DB_PORT')
    DB_NAME = config('DB_NAME')

    # Initialize connection
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = sqlalchemy.create_engine(db_url)
    conn = engine.connect()

    return conn


def extract_data(university):
    """
    Execute SQL query and save response as CSV file
    Args:
        university:

    Returns:
        None
    """
    conn = init_connection()

    root_folder = get_root_folder()
    with open(f"{root_folder}/sql/query_{university}.sql", "r") as f:
        sql = f.read()
        response = pd.read_sql_query(sql, conn)
        if not path.exists(f'{root_folder}/csv'):
            makedirs(f'{root_folder}/csv')
        response.to_csv(f'{root_folder}/csv/universidad_{university}.csv', index=False)


def get_root_folder():
    """
    Set root folder path
    Returns:
        String
    """
    root_folder = path.abspath(path.join(path.dirname(__file__), ".."))
    return root_folder


default_args = {
    "retries": 5,  # try 5 times
    "retry_delay": timedelta(minutes=10)  # wait 10 minutes to try again
}
with DAG(
        'Query_Universidad_F',
        description='Realizar consultas sobre Universidad de Moron y Rio Cuarto',
        schedule_interval=timedelta(hours=1),  # ejecución cada una hora
        start_date=datetime(2022, 2, 19)  # Puse esa fecha porque no fue aclarada en la consigna

) as dag:
    tarea1 = PythonOperator(task_id='Query_F1', python_callable=extract_data, op_args=["moron"],
                            dag=dag)  # Universidad de Morón
    tarea2 = PythonOperator(task_id='Query_F2', python_callable=extract_data, op_args=["rio_cuarto"],
                            dag=dag)  # Universidad de Río Cuarto    
    moron_txt = PythonOperator(
        task_id='moron_txt',
        python_callable=_transform_moron,
        dag=dag
    )
    rio_cuarto_txt = PythonOperator(
        task_id='rio_cuarto_txt',
        python_callable=_transform_rio_cuarto,
        dag=dag
    )
    tarea4 = DummyOperator(task_id='Upload_S3')  # uploading data to s3

    [tarea1 >> moron_txt, tarea2 >> rio_cuarto_txt] >> tarea4