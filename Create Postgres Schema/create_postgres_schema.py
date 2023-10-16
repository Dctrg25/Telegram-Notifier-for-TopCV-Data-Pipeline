import psycopg2
import configparser

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/Job_pipeline/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')
# Connect to Postgres
connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
conn = psycopg2.connect(**connect_params)

# Create a cursor object
cur = conn.cursor()
conn.set_session(autocommit= True)
# Create the job_description table with output1 column
cur.execute("""
    DROP TABLE IF EXISTS job_description;        

    CREATE TABLE IF NOT EXISTS job_description (
        job_id SERIAL PRIMARY KEY,
        job_title VARCHAR,
        company_name VARCHAR,
        due_time VARCHAR,
        salary DECIMAl,
        job_description VARCHAR,
        qualification VARCHAR,
        benefit VARCHAR,
        work_location VARCHAR,
        due_date DATE,
        link_jd VARCHAR,
        created_date DATE
    )
""")

# Close the cursor and connection
cur.close()
conn.close()
