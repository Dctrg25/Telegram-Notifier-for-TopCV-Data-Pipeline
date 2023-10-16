import psycopg2
import pandas as pd
import os 
import time
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/Job_pipeline/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')


def Load_table(df, cur):
    #df = df.astype(object).where(pd.notnull(df), None)  # replace pandas.NA with None
    records = df.to_records(index= False)

    column_names = ', '.join(df.columns)

    s_list = ', '.join(['%s'] * len(df.columns))

    query = f"""
        INSERT INTO {'job_description'} ({column_names}) VALUES ({s_list})
    """

    cur.executemany(query, records)

    print(f"Successfully insert data to table {'job_description'}")


def Load_schema():
    connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
    conn = psycopg2.connect(**connect_params)
    cur = conn.cursor()
    conn.set_session(autocommit= True)

    file_name = 'data_' + 'transformed_' + datetime.now().strftime("%d_%m_%Y") + '.csv'
    root_dir = "/home/truong/airflow/dags/Job_pipeline/Transformed_data" #sua path
    filePath = os.path.join(root_dir, file_name)
    while(os.path.isfile(filePath) != True): time.sleep(3)

    df = pd.read_csv(filePath)
    Load_table(df, cur)

    cur.close()
    conn.close()
