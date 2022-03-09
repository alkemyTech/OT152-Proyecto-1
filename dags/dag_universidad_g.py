
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
import os

# Check root_folder
def root_folder(): return(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def open_csv_files(root_directory, folder, file_name):
    '''
    Read csv file with pandas and obtain a Dataframe
    INPUT:
        root_directory: parent directory
        folder: folder where the files are
        file_name: name of the file you want to read
    OUTPUT: dataframe with the file_name data 
    '''
    CP_csv = os.path.join(root_directory, folder, file_name)
    return pd.read_csv(CP_csv, sep=',')
    
### Process Latinoamericana University
def process_latinamericana():
    '''
    Clean 'latinoamericana.csv' data and save it 
    as  'txt_latinoamericana.txt' file 
    '''
    # Get root folder
    root_dir = root_folder()
    
    # Read "codigos_postales.csv" and convert DF to lowercase
    CP_df = open_csv_files(root_dir, 'csv', 'codigos_postales.csv')
    CP_df = CP_df.astype(str).apply(lambda x: x.str.lower())
    
    # Read "latinoamericana.csv" file
    latin_df = open_csv_files(root_dir, 'csv', 'latinoamericana.csv')
    
    data = latin_df.astype(str).apply(lambda x: x.str.lower())    # Convert all dataframe to lowercase
    
    # Get first and last name
    data['first_name'] = data['names'].apply(lambda x: " ".join(x.split('-')[:-1]))   # obtengo el nombre
    data['first_name'] = data['first_name'].apply(lambda x: x.replace('mrs. ','')) # Eliminamos "mrs."
    data['first_name'] = data['first_name'].apply(lambda x: x.replace('mr. ','')) # Eliminamos "mr."
    data['last_name'] = data['names'].apply(lambda x: x.split('-')[-1])   # obtenemos el apellido
    data.drop(columns = 'names', inplace=True)
    
    # Delete extra white space and dash from some columns.
    str_cols = ['universities', 'careers', 'locations']
    data.loc[:, str_cols] = data.loc[:, str_cols].apply(lambda x: x.str.strip()) # delete leading y trailing white space
    data.loc[:, str_cols] = data.loc[:, str_cols].apply(lambda x: x.str.strip('-')) # Eliminamos los guiones leading y trailing
    data.loc[:, str_cols] = data.loc[:, str_cols].apply(lambda x: x.str.replace('-',' '))
    
    # Change 'f' and 'm' to 'female' and 'male'
    data.loc[data['sexo']=='f', 'sexo'] = 'female'
    data.loc[data['sexo']=='m', 'sexo'] = 'male'
    data.rename(columns = {'sexo':'gender'}, inplace=True)
    
    def age(birthdate):
        '''
        Converts given date ('birthdate') to age
        '''
        birth = datetime.strptime(birthdate, "%d-%m-%Y").date()
        today = date.today()
        age = today.year - birth.year - (
            (today.month, today.day) < (birth.month, birth.day))
        if age < 0: age = age+100
        return age

    data['age'] = data['birth_dates'].apply(age)   #  Age (int64)
    data.drop(columns = 'birth_dates', inplace=True)
    
    # Change "inscription dates" format to '%Y-%m-%d'
    data['inscription_date'] = pd.to_datetime(data['inscription_dates'], format='%d-%m-%Y')
    data.drop(columns = 'inscription_dates', inplace=True)
    
    # Add 'postal_code' column      # CP_df.columns: 'codigo_postal' , 'localidad'
    for idx in range(len(CP_df)):
        data.loc[
            (data.locations == CP_df.loc[idx, 'localidad']), 'postal_code'
            ] = CP_df.loc[idx, 'codigo_postal']
            
    # Rename some columns
    data.rename(
        columns = {'universities':'university', 'careers':'career',
            'locations':'location', 'emails':'email'}, inplace=True)
    
    # Change order of columns
    cols = ['university', 'career', 'inscription_date', 'first_name',
        'last_name', 'gender', 'age', 'postal_code', 'location', 'email']
    data = data.reindex(columns = cols)

    # Converts all columns to 'string' except 'age' (int)
    for col in data.columns:
        if col != 'age':
            data[col] = data[col].astype('string')

    # Create 'txt' folder if not exist
    txt_folder = os.path.join(root_dir, 'txt')
    os.makedirs(txt_folder, exist_ok=True)
    
    # Save data to a .txt file
    txt_path = os.path.join(root_dir, 'txt', 'txt_latinoamericana.txt')
    data.to_csv(txt_path, index=False)    # line_terminator ?? 

    return

### Process Kennedy University
def process_kennedy():
    '''
    Clean 'kennedy.csv' data and save it 
    as  'txt_kennedy.txt' file 
    '''
    # Get root folder
    root_dir = root_folder()

    # Read "codigos_postales.csv" and convert DF to lowercase
    CP_df = open_csv_files(root_dir, 'csv', 'codigos_postales.csv')
    CP_df = CP_df.astype(str).apply(lambda x: x.str.lower())
    
    # Read "kennedy.csv" file
    kennedy_df = open_csv_files(root_dir, 'csv', 'kennedy.csv')
    
    # Convert all dataframe to lowercase
    kennedy_df = kennedy_df.astype(str).apply(lambda x: x.str.lower())
    
    # Get first and last name
    kennedy_df['first_name'] = kennedy_df['nombres'].apply(lambda x: " ".join(x.split('-')[:-1]))
    kennedy_df['last_name'] = kennedy_df['nombres'].apply(lambda x: x.split('-')[-1])
    kennedy_df.drop(columns = 'nombres', inplace=True)

    # Delete extra white spaces and dash from some columns
    str_cols = ['universidades', 'carreras']
    kennedy_df.loc[:, str_cols] = kennedy_df.loc[:, str_cols].apply(lambda x: x.str.strip())
    kennedy_df.loc[:, str_cols] = kennedy_df.loc[:, str_cols].apply(lambda x: x.str.strip('-'))
    kennedy_df.loc[:, str_cols] = kennedy_df.loc[:, str_cols].apply(lambda x: x.str.replace('-',' '))
    
    # Change 'f' and 'm' to 'female' and 'male'
    kennedy_df.loc[kennedy_df['sexo']=='f', 'sexo'] = 'female'
    kennedy_df.loc[kennedy_df['sexo']=='m', 'sexo'] = 'male'
    kennedy_df.rename(columns = {'sexo':'gender'}, inplace=True)

    def age(birthdate):
        '''
        Converts given date ('birthdate') to age
        '''
        birth = datetime.strptime(birthdate, "%y-%b-%d").date()
        today = date.today()
        age = today.year - birth.year - (
            (today.month, today.day) < (birth.month, birth.day))
        if age < 0: age = age+100
        return age
    
    # Convert 'birthdate' to age
    kennedy_df['age'] = kennedy_df['fechas_nacimiento'].apply(age)
    kennedy_df.drop(columns = 'fechas_nacimiento', inplace=True)
    
    # Change "inscription dates" format to '%Y-%m-%d'
    kennedy_df['inscription_date'] = pd.to_datetime(
        kennedy_df['fechas_de_inscripcion'], format='%y-%b-%d')
    kennedy_df.drop(columns = 'fechas_de_inscripcion', inplace=True)
    
    # Add 'postal_code' column      # CP_df.columns: 'codigo_postal' , 'localidad'
    for idx in range(len(CP_df)):
        kennedy_df.loc[
            (kennedy_df.codigos_postales == CP_df.loc[idx, 'codigo_postal']), 'location'
            ] = CP_df.loc[idx, 'localidad']
    # Rename some columns
    kennedy_df.rename(
        columns = {'universidades':'university', 'carreras':'career',
            'codigos_postales':'postal_code', 'emails':'email',
            }, inplace=True)
    
    # Change order of columns
    cols = ['university', 'career', 'inscription_date', 'first_name',
        'last_name', 'gender', 'age', 'postal_code', 'location', 'email']
    kennedy_df = kennedy_df.reindex(columns = cols)
    
    # Converts all columns to 'string' except 'age' (int)
    for col in kennedy_df.columns:
        if col != 'age':
            kennedy_df[col] = kennedy_df[col].astype('string')
    
    # Create 'txt' folder if not exist
    txt_folder = os.path.join(root_dir, 'txt')
    os.makedirs(txt_folder, exist_ok=True)
    
    # Save data to a .txt file
    txt_path = os.path.join(root_dir, 'txt', 'txt_kennedy.txt')
    kennedy_df.to_csv(txt_path, index=False)    # line_terminator ?? 
    
    return


default_args = {
    "retries": 5,  # Try 5 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Basic configuration of the format and instantiation of the logger
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d')
logger = logging.getLogger('univ_g')

with DAG(
    'dag_universidad_g',
    description= 'ETL univ Facultad Latinoamericana De Ciencias Sociales y Universidad J. F. Kennedy',
    schedule_interval= "@hourly", #funciona cada hora
    start_date=datetime(2022, 2, 17)
) as dag:

    tarea_1= DummyOperator(task_id='universidad_sociales') 
    tarea_2= DummyOperator(task_id='universidad_kenedy')

    processing_U_latinoamericana = PythonOperator(task_id='processing_U_latinoamericana',
                                    python_callable = process_latinamericana)

    processing_U_kennedy = PythonOperator(task_id='processing_U_kennedy',
                                    python_callable = process_kennedy)
    
    [tarea_1,tarea_2]